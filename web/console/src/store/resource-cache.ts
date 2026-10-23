import type { Resource } from "@/api/console";

const catalogKinds = new Set(["template", "dataset", "binding", "node"]);
const resourceKinds = new Set([
  "version",
  "artifact",
  "deployment",
  "backtest",
  "report",
  "run",
  "account",
  "alert"
]);
const activeStates = new Set([
  "STARTING",
  "LOADING",
  "READY",
  "RUNNING",
  "STOP_REQUESTED",
  "UNKNOWN"
]);
export function cacheResources(
  incoming: Resource[],
  existing: Resource[],
  pinned: string[] = []
): Resource[] {
  const known = new Map(existing.map(item => [item.id, item]));
  for (const item of incoming) {
    const old = known.get(item.id);
    if (!old || BigInt(item.revision || "0") > BigInt(old.revision || "0"))
      known.set(item.id, item);
  }
  const selected = new Set(pinned.slice(0, 32));
  const counts = new Map<string, number>();
  return [...known.values()]
    .sort((a, b) => {
      const av = BigInt(a.revision || "0"),
        bv = BigInt(b.revision || "0");
      return av > bv ? -1 : av < bv ? 1 : 0;
    })
    .filter(item => {
      if (selected.has(item.id)) return true;
      const body = item.body as any;
      const active =
        item.kind === "account" ||
        (item.kind === "alert" && body.active === true) ||
        (item.kind === "run" && activeStates.has(body.process_state));
      const group = catalogKinds.has(item.kind)
        ? "catalog"
        : active
          ? "active"
          : resourceKinds.has(item.kind)
            ? item.kind
            : "other";
      const limit = group === "catalog" ? 1024 : group === "active" ? 512 : 100;
      const count = counts.get(group) ?? 0;
      counts.set(group, count + 1);
      return count < limit;
    });
}
