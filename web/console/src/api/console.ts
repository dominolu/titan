import { http } from "@/utils/http";
import type {
  Action,
  Operation,
  Resource,
  SaveVersion,
  Snapshot,
  SubmitOperation,
  StrategyOperationRequest,
  StrategySummary
} from "@/generated/management";
export const consoleApi = {
  strategies: (offset = 0) =>
    http.get<StrategySummary[]>("/api/v2/strategies", {
      params: { offset, limit: 20 }
    }),
  strategyResources: (
    strategy: string,
    kind: string,
    offset = 0,
    id?: string
  ) =>
    http.get<Resource[]>("/api/v2/resources", {
      params: {
        strategy_id: strategy,
        kind: kind || undefined,
        offset,
        limit: 20,
        resource_id: id
      }
    }),
  resource: (id: string) =>
    http.get<Resource>(`/api/v2/resources/${encodeURIComponent(id)}`),
  log: (item: Resource, offset: number) =>
    http.get<{ text: string; next_offset: string; more: boolean }>(
      `/api/v2/resources/${encodeURIComponent(item.id)}/log`,
      {
        params: {
          ...((item.body as any).mode === "observe"
            ? { worker_session_id: (item.body as any).worker_session_id }
            : { run_generation: (item.body as any).run_generation }),
          offset
        }
      }
    ),
  history: (item: Resource, kind: string, offset: number) =>
    http.get<{ items: unknown[]; source: unknown; recording: unknown }>(
      `/api/v2/resources/${encodeURIComponent(item.id)}/history`,
      {
        params: {
          ...((item.body as any).mode === "observe"
            ? { worker_session_id: (item.body as any).worker_session_id }
            : { run_generation: (item.body as any).run_generation }),
          kind,
          offset,
          limit: 20
        }
      }
    ),
  acknowledge: (item: Resource) =>
    http.post<Resource>(
      `/api/v2/resources/${encodeURIComponent(item.id)}/acknowledge`,
      {
        data: {
          expected_revision: item.revision,
          condition_identity: (item.body as any).condition_identity
        }
      }
    ),
  snapshot: () =>
    http.get<Snapshot>("/api/v2/snapshot", { params: { limit: 200 } }),
  submit: (data: StrategyOperationRequest) =>
    http.post<Operation>("/api/v2/operations", { data }),
  find: (key: string) =>
    http.get<Operation | null>("/api/v2/operations", {
      params: { idempotency_key: key }
    }),
  cancel: (id: string) =>
    http.post<Operation>(`/api/v2/operations/${id}/cancel`),
  save: (data: SaveVersion, id?: string) =>
    http.request<Resource>(
      id ? "patch" : "post",
      id ? `/api/v2/parameter-versions/${id}` : "/api/v2/parameter-versions",
      { data }
    ),
  file: (id: string, path: string) =>
    `/api/v2/files/${encodeURIComponent(id)}/${path.split("/").map(encodeURIComponent).join("/")}`
};
export type {
  Action,
  Operation,
  Resource,
  SaveVersion,
  Snapshot,
  SubmitOperation,
  StrategySummary
};
