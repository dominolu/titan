import {
  computed,
  inject,
  onBeforeUnmount,
  reactive,
  ref,
  provide,
  type InjectionKey
} from "vue";
import { consoleApi, type Resource } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
import { body } from "./run-view";
import { useResourcePins } from "./use-resource-pins";

const kinds = [
  "parameter_preset",
  "parameter_version",
  "strategy_version",
  "backtest",
  "report",
  "deployment",
  "run"
];
type Page = {
  items: Resource[];
  offset: number;
  pending: boolean;
  error: string;
  sequence: number;
};
const key: InjectionKey<ReturnType<typeof createStrategy>> =
  Symbol("strategy-workspace");

function createStrategy(id: string) {
  const store = useConsoleStore();
  const pinResources = useResourcePins();
  const identity = store.sessionGeneration;
  const pages = reactive<Record<string, Page>>(
    Object.fromEntries(
      kinds.map(kind => [
        kind,
        { items: [], offset: 0, pending: false, error: "", sequence: 0 }
      ])
    )
  );
  const selected = reactive<Record<string, Resource[]>>({});
  const ready = ref(false);
  let disposed = false;
  const active = () => !disposed && identity === store.sessionGeneration;
  const latest = (item: Resource) => {
    const current =
      store.resources.find(
        row =>
          row.id === item.id && BigInt(row.revision) >= BigInt(item.revision)
      ) ?? item;
    return kinds.includes(current.kind) &&
      typeof body(current).strategy_id === "string" &&
      body(current).strategy_id !== id
      ? undefined
      : current;
  };
  const valid = (item: Resource | undefined): item is Resource => !!item;
  const resources = (kind: string) =>
    pages[kind].items.map(latest).filter(valid);
  const lookup = (kind: string, resourceId: string) =>
    [
      ...resources(kind),
      ...Object.values(selected).flat().map(latest).filter(valid)
    ].find(item => item.kind === kind && item.id === resourceId);
  const options = (kind: string) => [
    ...new Map(
      [
        ...resources(kind),
        ...Object.values(selected)
          .flat()
          .filter(item => item.kind === kind)
          .map(latest)
          .filter(valid)
      ].map(item => [item.id, item])
    ).values()
  ];
  async function load(kind: string, offset = pages[kind].offset) {
    const page = pages[kind],
      sequence = ++page.sequence;
    page.pending = true;
    page.error = "";
    try {
      const items = await consoleApi.strategyResources(id, kind, offset);
      if (!active() || sequence !== page.sequence) return;
      page.items = items;
      page.offset = offset;
      items.forEach(item => store.mergeResource(item));
    } catch (failure) {
      if (active() && sequence === page.sequence)
        page.error = (failure as Error).message;
    } finally {
      if (active() && sequence === page.sequence) page.pending = false;
    }
  }
  const pinSequences = new Map<string, number>();
  async function pin(owner: string, ids: string[]) {
    const sequence = (pinSequences.get(owner) ?? 0) + 1;
    pinSequences.set(owner, sequence);
    const wanted = [...new Set(ids.filter(Boolean))].slice(0, 8);
    const values: Resource[] = [];
    for (const resourceId of wanted) {
      const known =
        Object.values(pages)
          .flatMap(page => page.items)
          .find(item => item.id === resourceId) ??
        Object.values(selected)
          .flat()
          .find(item => item.id === resourceId);
      const item =
        known ?? (await consoleApi.strategyResources(id, "", 0, resourceId))[0];
      if (!active() || pinSequences.get(owner) !== sequence) return;
      if (!item || item.id !== resourceId)
        throw new Error("所选资源不属于当前策略或无权限；不会替换为其他资源");
      values.push(item);
    }
    if (!active() || pinSequences.get(owner) !== sequence) return;
    selected[owner] = values;
    values.forEach(item => store.mergeResource(item));
    await pinResources(
      Object.values(selected)
        .flat()
        .map(item => item.id)
    );
  }
  async function pinCatalog(owner: string, ids: string[]) {
    const sequence = (pinSequences.get(owner) ?? 0) + 1;
    pinSequences.set(owner, sequence);
    const values: Resource[] = [];
    for (const resourceId of [...new Set(ids.filter(Boolean))].slice(0, 8)) {
      const item =
        store.resources.find(row => row.id === resourceId) ??
        (await consoleApi.resource(resourceId));
      if (!active() || pinSequences.get(owner) !== sequence) return;
      if (!["dataset", "binding"].includes(item.kind))
        throw new Error("共享目录资源类型不匹配");
      values.push(item);
    }
    if (!active() || pinSequences.get(owner) !== sequence) return;
    selected[owner] = values;
    values.forEach(item => store.mergeResource(item));
    await pinResources(
      Object.values(selected)
        .flat()
        .map(item => item.id)
    );
  }
  const all = computed(() => [
    ...new Map(
      [
        ...Object.values(pages).flatMap(page => page.items.map(latest)),
        ...Object.values(selected).flat().map(latest)
      ]
        .filter(valid)
        .map(item => [item.id, item])
    ).values()
  ]);
  const name = computed(
    () =>
      all.value.map(item => body(item).strategy_name).find(Boolean) ??
      (id === "unassigned" ? "未归属的历史记录" : "策略工作区")
  );
  const path = (tab: string, selection?: Record<string, string>) => ({
    path: "/strategies/" + encodeURIComponent(id),
    query: { tab, ...selection }
  });
  const operationIds = computed(
    () =>
      new Set(all.value.map(item => body(item).operation_id).filter(Boolean))
  );
  const operations = computed(() =>
    store.operations.filter(op => {
      if (operationIds.value.has(op.id)) return true;
      const input = op.input;
      const references = [
        "parameter_version_id",
        "strategy_version_id",
        "backtest_id",
        "deployment_id",
        "run_id"
      ].flatMap(field => (field in input ? [(input as any)[field]] : []));
      return (
        references.some(resourceId =>
          all.value.some(
            item =>
              item.id === resourceId &&
              (item.node_id === op.node_id || input.kind === "deploy")
          )
        ) ||
        (input.kind === "describe" &&
          options("parameter_preset").some(
            item =>
              item.node_id === op.node_id &&
              body(item).parameter_preset_id === input.parameter_preset_id
          )) ||
        (input.kind === "prepare" &&
          Object.values(selected)
            .flat()
            .some(
              item =>
                item.kind === "dataset" &&
                item.node_id === op.node_id &&
                body(item).dataset_id === input.dataset_id
            ))
      );
    })
  );
  Promise.all(kinds.map(kind => load(kind))).finally(() => {
    if (active()) ready.value = true;
  });
  const timer = setInterval(() => {
    if (active() && document.visibilityState === "visible")
      kinds.forEach(kind => {
        if (!pages[kind].pending) load(kind);
      });
  }, 5000);
  onBeforeUnmount(() => {
    disposed = true;
    clearInterval(timer);
  });
  return {
    id,
    name,
    pages,
    ready,
    resources,
    options,
    lookup,
    load,
    pin,
    pinCatalog,
    path,
    operations,
    all
  };
}
export function provideStrategy(id: string) {
  const value = createStrategy(id);
  provide(key, value);
  return value;
}
export function useStrategy() {
  const value = inject(key);
  if (!value) throw new Error("策略上下文缺失");
  return value;
}
