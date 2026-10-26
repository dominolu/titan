// Run on 192.168.3.88. Exercise real Vue setup, Pinia actions and resource cache.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFile, writeFile } from "node:fs/promises";
const repo = new URL("../", import.meta.url);
const require = createRequire(new URL("web/console/package.json", repo));
const ts = require("typescript"),
  vue = require("vue"),
  pinia = require("pinia");
const { parse, compileScript } = require("@vue/compiler-sfc");
const url = (source) =>
  "data:text/javascript;base64," + Buffer.from(source).toString("base64");
const transpile = (source) =>
  ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ESNext,
    },
  }).outputText;
const read = (path) => readFile(new URL(path, repo), "utf8");
const replacement = (source, name, target) =>
  source
    .replaceAll(JSON.stringify(name), JSON.stringify(target))
    .replaceAll("'" + name + "'", JSON.stringify(target));
globalThis.__researchTestVue = vue;
globalThis.__researchTestPinia = pinia;
globalThis.__researchTestApi = {};
globalThis.document = { visibilityState: "hidden" };
globalThis.window = { addEventListener() {} };
globalThis.sessionStorage = {
  removeItem() {},
  setItem() {},
  getItem() {
    return null;
  },
};
const exportsUrl = (name, value) =>
  url(
    `const v=globalThis.${name};` +
      Object.keys(value)
        .map((key) => `export const ${key}=v[${JSON.stringify(key)}];`)
        .join(""),
  );
const vueUrl = exportsUrl("__researchTestVue", vue),
  piniaUrl = exportsUrl("__researchTestPinia", pinia);
const apiUrl = url("export const consoleApi=globalThis.__researchTestApi;");
const authUrl = url(
  'export const getSession=()=>({identity:{username:"operator"}});',
);
const precisionUrl = url(
  transpile(await read("web/console/src/utils/precision.ts")),
);
const cacheUrl = url(
  transpile(await read("web/console/src/store/resource-cache.ts")),
);
let storeCode = transpile(
  await read("web/console/src/store/modules/console.ts"),
);
for (const [name, target] of [
  ["pinia", piniaUrl],
  ["@/api/console", apiUrl],
  ["@/utils/auth", authUrl],
  ["@/utils/precision", precisionUrl],
  ["../resource-cache", cacheUrl],
])
  storeCode = replacement(storeCode, name, target);
const storeUrl = url(storeCode);
const { useConsoleStore } = await import(storeUrl);
pinia.setActivePinia(pinia.createPinia());
const store = useConsoleStore();
let pinsCode = transpile(
  await read("web/console/src/views/console/use-resource-pins.ts"),
);
pinsCode = replacement(
  replacement(pinsCode, "vue", vueUrl),
  "@/store/modules/console",
  storeUrl,
);
const pinsUrl = url(pinsCode),
  { useResourcePins } = await import(pinsUrl);
const bodyUrl = url(
  'export const body=r=>r?.body??{};export const pretty=v=>JSON.stringify(v??null,null,2);export const raw=v=>String(v??"—");export const operationLabels={};',
);
let scopeCode = transpile(
  await read("web/console/src/views/console/use-research.ts"),
);
for (const [name, target] of [
  ["vue", vueUrl],
  ["@/api/console", apiUrl],
  ["@/store/modules/console", storeUrl],
  ["./run-view", bodyUrl],
  ["./use-resource-pins", pinsUrl],
])
  scopeCode = replacement(scopeCode, name, target);
const scopeUrl = url(scopeCode),
  { provideResearch } = await import(scopeUrl);
const { descriptor } = parse(
  await read("web/console/src/views/console/StrategyParameters.vue"),
);
const messageUrl = url("export const ElMessage={error(){},success(){}};");
let workCode = transpile(
  await read("web/console/src/views/console/use-console-work.ts"),
);
for (const [name, target] of [
  ["vue", vueUrl],
  ["element-plus", messageUrl],
  ["@/store/modules/console", storeUrl],
])
  workCode = replacement(workCode, name, target);
let parameterCode = transpile(
  compileScript(descriptor, { id: "research-regression" }).content,
);
for (const [name, target] of [
  ["vue", vueUrl],
  ["element-plus", messageUrl],
  ["@/api/console", apiUrl],
  ["@/store/modules/console", storeUrl],
  ["@/utils/precision", precisionUrl],
  ["./run-view", bodyUrl],
  ["./use-console-work", url(workCode)],
  ["./use-research", scopeUrl],
])
  parameterCode = replacement(parameterCode, name, target);
parameterCode = parameterCode.replace(
  /"\.\/[^"\n]+\.vue"/g,
  JSON.stringify(url("export default {};")),
);
const Parameters = (await import(url(parameterCode))).default;
const resource = (id, kind, body) => ({
  id,
  kind,
  node_id: "build",
  revision: "1",
  digest: "fixture",
  frozen: false,
  body: { research_id: "research-grid", ...body },
});
const a = resource("build-template-a", "template", {
  template_id: "a",
  parameters: { value: "A" },
});
const b = resource("build-template-b", "template", {
  template_id: "b",
  parameters: { value: "B" },
});
const rowA = resource("version-a", "version", {
  template_id: "a",
  parameters: { value: "A" },
});
const rowB = resource("version-b", "version", {
  template_id: "b",
  parameters: { value: "B" },
});
const deployment = resource("deployment-original", "deployment", {
  max_exposure_quote: "100",
});
const run = resource("run-original", "run", {
  deployment_id: deployment.id,
  process_state: "RUNNING",
});
const host = {
  createElement: () => ({ children: [] }),
  createText: () => ({}),
  createComment: () => ({}),
  setText() {},
  setElementText() {},
  parentNode: () => null,
  nextSibling: () => null,
  insert(el, parent) {
    parent.children.push(el);
  },
  remove() {},
  patchProp() {},
};
const tick = async () => {
  await new Promise((resolve) => setImmediate(resolve));
  await vue.nextTick();
};
const deferred = () => {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
};
const checks = [];
async function mount() {
  store.clear();
  store.role = "operator";
  const delay = deferred();
  globalThis.__researchTestApi.resource = async (id) => {
    const item = [a, b, rowA, rowB, deployment, run].find(
      (row) => row.id === id,
    );
    if (!item) throw new Error("unknown fixture");
    return item;
  };
  globalThis.__researchTestApi.researchResources = async (
    _id,
    kind,
    _offset,
    selectedId,
  ) => {
    if (selectedId === b.id) {
      await delay.promise;
      return [b];
    }
    if (selectedId === a.id) return [a];
    return (
      {
        template: [a],
        version: [rowA, rowB],
        run: [run],
        deployment: [deployment],
      }[kind] ?? []
    );
  };
  let state, scope, pinRun;
  const Editor = {
    setup() {
      state = Parameters.setup({}, { expose() {} });
      return () => vue.h("div");
    },
  };
  const Live = {
    setup() {
      pinRun = useResourcePins();
      return () => vue.h("div");
    },
  };
  const Root = {
    setup() {
      scope = provideResearch("research-grid");
      return () => vue.h("div", [vue.h(Editor), vue.h(Live)]);
    },
  };
  const app = vue.createRenderer(host).createApp(Root);
  app.mount({ children: [] });
  await tick();
  return { state, scope, pinRun, app, delay };
}
{
  const { state, app, delay } = await mount();
  const first = state.editVersion(rowB, false);
  await tick();
  await state.editVersion(rowA, false);
  delay.resolve();
  await first;
  await tick();
  assert.equal(state.selectedTemplate.value, a.id);
  assert.equal(JSON.parse(state.parameters.value).value, "A");
  assert.equal(state.draft.value.id, rowA.id);
  app.unmount();
  checks.push("last-copy-selection-wins");
}
{
  const { state, app, delay } = await mount();
  const pending = state.editVersion(rowB, true);
  await tick();
  state.parameters.value = '{"value":"user-edited"}';
  await tick();
  delay.resolve();
  await pending;
  await tick();
  assert.equal(JSON.parse(state.parameters.value).value, "user-edited");
  app.unmount();
  checks.push("late-copy-preserves-new-user-input");
}
for (const end of ["session", "unmount"]) {
  const { state, app, delay } = await mount();
  const pending = state.editVersion(rowB, false);
  await tick();
  if (end === "session") store.clear();
  else app.unmount();
  delay.resolve();
  await pending;
  await tick();
  assert.equal(state.selectedTemplate.value, a.id);
  assert.equal(JSON.parse(state.parameters.value).value, "A");
  assert.equal(state.draft.value, null);
  if (end === "session") app.unmount();
  checks.push(`late-copy-rejected-after-${end}`);
}
{
  const { scope, pinRun, app } = await mount();
  await scope.pin("live-link", [run.id]);
  await pinRun([run.id, deployment.id]);
  await scope.pin("parameters", [a.id]);
  for (let i = 0; i < 600; i++)
    store.mergeResource({
      ...resource("new-deployment-" + i, "deployment", {}),
      revision: String(i + 10),
    });
  assert(store.resources.some((row) => row.id === deployment.id));
  assert(store.pinnedResourceIds.includes(deployment.id));
  assert.equal(
    store.resources.filter((row) => row.kind === "deployment").length,
    101,
  );
  app.unmount();
  assert.deepEqual(store.pinnedResourceIds, []);
  assert.deepEqual(store.resourcePinOwners, {});
  checks.push(
    "embedded-run-deployment-survives-600-events-and-sibling-edits",
    "unmount-releases-all-pin-owners",
  );
}
{
  store.clear();
  await assert.rejects(
    store.pinResources(
      Array.from({ length: 33 }, (_, i) => String(i)),
      "overflow",
    ),
    /上限/,
  );
  assert.deepEqual(store.resourcePinOwners, {});
  assert.deepEqual(store.pinnedResourceIds, []);
  const delay = deferred();
  globalThis.__researchTestApi.resource = async () => {
    await delay.promise;
    return b;
  };
  const pending = store.pinResources([b.id], "closed-detail");
  store.unpinResources("closed-detail");
  delay.resolve();
  await pending;
  assert(!store.resources.some((row) => row.id === b.id));
  checks.push(
    "pin-capacity-rejection-is-atomic",
    "released-owner-cannot-merge-late-resource",
  );
}
const result = { passed: true, checks };
if (process.argv[2])
  await writeFile(process.argv[2], JSON.stringify(result, null, 2));
console.log(JSON.stringify(result));
