import { defineStore } from "pinia";
import {
  consoleApi,
  type Operation,
  type Resource,
  type SubmitOperation,
  type Snapshot
} from "@/api/console";
import { getSession } from "@/utils/auth";
import type { Event as StreamEvent } from "@/generated/management";
import { parseExact } from "@/utils/precision";
import { cacheResources } from "../resource-cache";
let stream: EventSource | null = null;
let poll: ReturnType<typeof setInterval> | null = null;
let reconnect: ReturnType<typeof setTimeout> | null = null;
let watchdog: ReturnType<typeof setInterval> | null = null;
const newer = (next: string, old: string) =>
  BigInt(next || "0") > BigInt(old || "0");

export const useConsoleStore = defineStore("titan-console", {
  state: () => ({
    operations: [] as Operation[],
    resources: [] as Resource[],
    pinnedResourceIds: [] as string[],
    resourcePinOwners: {} as Record<string, string[]>,
    cursor: "",
    connection: "connecting",
    error: "",
    updated: "",
    pending: [] as SubmitOperation[],
    ready: false,
    owner: "",
    role: "",
    generation: 0,
    receivedAt: 0,
    refreshSequence: 0
  }),
  getters: {
    writable: state => state.role === "operator",
    templates: state =>
      state.resources.filter(item => item.kind === "template"),
    versions: state => state.resources.filter(item => item.kind === "version"),
    artifacts: state =>
      state.resources.filter(item => item.kind === "artifact"),
    datasets: state => state.resources.filter(item => item.kind === "dataset"),
    backtests: state =>
      state.resources.filter(item => item.kind === "backtest"),
    runs: state => state.resources.filter(item => item.kind === "run"),
    accounts: state => state.resources.filter(item => item.kind === "account")
  },
  actions: {
    key() {
      return (
        "titan-pending-" + (this.owner || getSession()?.identity.username || "")
      );
    },
    remember() {
      sessionStorage.setItem(
        this.key(),
        JSON.stringify(this.pending.slice(0, 32))
      );
    },
    async refresh() {
      const generation = this.generation;
      const sequence = ++this.refreshSequence;
      const snapshot: Snapshot = await consoleApi.snapshot();
      if (generation !== this.generation || sequence !== this.refreshSequence)
        return;
      const [epoch, revision] = snapshot.cursor.split(":");
      const [oldEpoch, oldRevision] = this.cursor.split(":");
      const merge = <T extends { id: string; revision: string }>(
        incoming: T[],
        existing: T[]
      ) => {
        const known = new Map(existing.map(item => [item.id, item]));
        const rows = incoming.map(item => {
          const old = known.get(item.id);
          return old && newer(old.revision, item.revision) ? old : item;
        });
        const ids = new Set(rows.map(item => item.id));
        return [
          ...existing.filter(
            item => !ids.has(item.id) && newer(item.revision, revision)
          ),
          ...rows
        ].slice(0, 200);
      };
      const changed = !!oldEpoch && oldEpoch !== epoch;
      this.operations = changed
        ? snapshot.operations
        : merge(snapshot.operations, this.operations);
      this.resources = cacheResources(
        snapshot.resources,
        changed ? [] : this.resources,
        this.pinnedResourceIds
      );
      if (changed || !oldEpoch || newer(revision, oldRevision))
        this.cursor = snapshot.cursor;
      this.updated = snapshot.server_time_ns;
      this.ready = true;
      if (changed && stream) {
        stream.close();
        stream = null;
        this.connect();
      }
    },
    async start() {
      const session = getSession();
      if (this.owner && this.owner !== session?.identity.username) this.clear();
      this.owner = session?.identity.username ?? "";
      this.role = session?.identity.role ?? "";
      if (stream || poll) return;
      const generation = this.generation;
      this.pending = parseExact(
        sessionStorage.getItem(this.key()) ?? "[]"
      ).slice(0, 32);
      await this.refresh();
      if (generation !== this.generation) return;
      for (const item of [...this.pending]) {
        const existing = await consoleApi.find(item.idempotency_key);
        if (generation !== this.generation) return;
        if (existing) {
          this.mergeOperation(existing);
          this.pending = this.pending.filter(
            value => value.idempotency_key !== item.idempotency_key
          );
        }
      }
      this.remember();
      this.connect();
      if (!watchdog)
        watchdog = setInterval(() => {
          if (
            this.connection === "connected" &&
            Date.now() - this.receivedAt > 6000
          ) {
            this.connection = "stale";
            this.fallback();
          }
        }, 1000);
    },
    connect() {
      const generation = this.generation;
      if (stream) stream.close();
      stream = new EventSource(
        "/api/v1/events?cursor=" + encodeURIComponent(this.cursor)
      );
      const source = stream;
      const active = () => generation === this.generation && stream === source;
      stream.onopen = () => {
        if (!active()) return;
        this.connection = "connected";
        this.receivedAt = Date.now();
        if (poll) {
          clearInterval(poll);
          poll = null;
        }
      };
      stream.addEventListener("update", event => {
        if (!active()) return;
        const update = parseExact((event as MessageEvent).data) as StreamEvent;
        this.cursor = update.cursor;
        if (update.kind === "operation")
          this.mergeOperation(update.payload as Operation);
        else if (update.kind === "resource")
          this.mergeResource(update.payload as Resource);
      });
      stream.addEventListener("watermark", event => {
        if (!active()) return;
        this.cursor = (event as MessageEvent).lastEventId;
        this.updated = (event as MessageEvent).data;
        this.receivedAt = Date.now();
        this.connection = "connected";
        if (poll) {
          clearInterval(poll);
          poll = null;
        }
      });
      stream.addEventListener("resync_required", () => {
        if (!active()) return;
        this.connection = "resync";
        stream?.close();
        stream = null;
        reconnect = setTimeout(() => {
          if (generation !== this.generation || stream) return;
          this.refresh()
            .then(() => {
              if (generation === this.generation && !stream) this.connect();
            })
            .catch(error => {
              if (generation === this.generation) {
                this.error = error.message;
                this.fallback();
              }
            });
        }, 1000);
      });
      stream.addEventListener("auth_expired", () => {
        if (active()) window.dispatchEvent(new Event("titan:session-expired"));
      });
      stream.onerror = () => {
        if (!active()) return;
        this.connection = "disconnected";
        this.fallback();
      };
    },
    fallback() {
      const generation = this.generation;
      if (!poll)
        poll = setInterval(
          () =>
            this.refresh().catch(error => {
              if (generation === this.generation) this.error = error.message;
            }),
          3000
        );
    },
    offline() {
      if (!this.ready) return;
      this.connection = "disconnected";
      stream?.close();
      stream = null;
      this.fallback();
    },
    online() {
      if (!this.ready) return;
      const generation = this.generation;
      this.refresh()
        .then(() => {
          if (generation === this.generation && !stream) this.connect();
        })
        .catch(error => {
          if (generation === this.generation) {
            this.error = error.message;
            this.fallback();
          }
        });
    },
    mergeOperation(next: Operation) {
      const old = this.operations.find(item => item.id === next.id);
      if (old && !newer(next.revision, old.revision)) return;
      this.operations = [
        next,
        ...this.operations.filter(item => item.id !== next.id)
      ].slice(0, 200);
    },
    mergeResource(next: Resource) {
      this.resources = cacheResources(
        [next],
        this.resources,
        this.pinnedResourceIds
      );
    },
    async pinResources(ids: string[], owner: string) {
      const wanted = [...new Set(ids.filter(Boolean))];
      const owners = { ...this.resourcePinOwners, [owner]: wanted };
      const pinned = [...new Set(Object.values(owners).flat())];
      if (pinned.length > 32 || Object.keys(owners).length > 32)
        throw new Error("固定资源已达上限，请关闭不再使用的详情");
      this.resourcePinOwners = owners;
      this.pinnedResourceIds = pinned;
      const generation = this.generation;
      for (const id of wanted) {
        if (this.resources.some(item => item.id === id)) continue;
        const item = await consoleApi.resource(id);
        if (
          generation !== this.generation ||
          !this.resourcePinOwners[owner]?.includes(id)
        )
          return;
        this.mergeResource(item);
      }
    },
    unpinResources(owner: string) {
      delete this.resourcePinOwners[owner];
      this.pinnedResourceIds = [
        ...new Set(Object.values(this.resourcePinOwners).flat())
      ];
    },
    async submit(action: SubmitOperation["action"], node_id: string) {
      const signature = JSON.stringify({ action, node_id });
      const unresolved = this.pending.find(
        item => item.node_id === node_id && item.action.kind === action.kind
      );
      if (unresolved) {
        if (
          JSON.stringify({
            action: unresolved.action,
            node_id: unresolved.node_id
          }) === signature
        )
          return this.retry(unresolved);
        throw new Error("同类请求受理结果待确认，请先查询原操作");
      }
      const existing = this.operations.find(
        item =>
          ["pending", "running", "unknown"].includes(item.state) &&
          JSON.stringify({ action: item.input, node_id: item.node_id }) ===
            signature
      );
      if (existing) return existing;
      if (this.pending.length >= 32)
        throw new Error("待确认请求已达上限，请先核对原操作");
      const request: SubmitOperation = {
        idempotency_key: crypto.randomUUID(),
        node_id,
        action
      };
      this.pending.push(request);
      this.remember();
      return this.retry(request);
    },
    async retry(request: SubmitOperation) {
      const generation = this.generation;
      try {
        const operation = await consoleApi.submit(request);
        if (generation !== this.generation) return operation;
        this.mergeOperation(operation);
        this.pending = this.pending.filter(
          item => item.idempotency_key !== request.idempotency_key
        );
        this.remember();
        return operation;
      } catch (error) {
        if (generation !== this.generation) throw error;
        const status = (error as any).response?.status;
        if (status >= 400 && status < 500 && status !== 408) {
          this.pending = this.pending.filter(
            item => item.idempotency_key !== request.idempotency_key
          );
          this.remember();
          this.error = (error as Error).message;
        } else this.error = "受理结果待确认，请查询原操作；重试沿用原幂等键。";
        throw error;
      }
    },
    clear() {
      this.generation++;
      stream?.close();
      stream = null;
      if (poll) clearInterval(poll);
      if (reconnect) clearTimeout(reconnect);
      if (watchdog) clearInterval(watchdog);
      watchdog = null;
      poll = null;
      reconnect = null;
      sessionStorage.removeItem(this.key());
      this.pending = [];
      this.operations = [];
      this.resources = [];
      this.pinnedResourceIds = [];
      this.resourcePinOwners = {};
      this.ready = false;
      this.cursor = "";
      this.owner = "";
      this.role = "";
    }
  }
});
window.addEventListener("titan:clear", () => useConsoleStore().clear());
window.addEventListener("offline", () => useConsoleStore().offline());
window.addEventListener("online", () => useConsoleStore().online());
