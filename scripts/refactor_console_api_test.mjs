// Run only on the build server: capture calls from the production console API.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";

const require = createRequire(
  new URL("../web/console/package.json", import.meta.url),
);
const ts = require("typescript");
const source = await readFile(
  new URL("../web/console/src/api/console.ts", import.meta.url),
  "utf8",
);
const code = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2022,
  },
}).outputText;
const requests = [];
const response = { captured: true };
const http = {
  get(url, options) {
    requests.push({ url, ...options });
    return Promise.resolve(response);
  },
};
const exports = {};
new Function("require", "exports", code)((name) => {
  assert.equal(name, "@/utils/http", "only the HTTP transport is replaced");
  return { http };
}, exports);
const { consoleApi } = exports;

// Both identities are present to catch wrong-mode selection. Missing selected
// identity must stay undefined even if the other field exists.
const cases = [
  ["run", { mode: "run", run_generation: "9007199254740993", worker_session_id: "ignored" }, { run_generation: "9007199254740993" }],
  ["observe", { mode: "observe", worker_session_id: "worker /?#中文", run_generation: "ignored" }, { worker_session_id: "worker /?#中文" }],
  ["run missing generation", { mode: "run", worker_session_id: "not-a-generation" }, { run_generation: undefined }],
  ["observe missing session", { mode: "observe", run_generation: "not-a-session" }, { worker_session_id: undefined }],
  ["mode absent", { run_generation: "7", worker_session_id: "ignored" }, { run_generation: "7" }],
  ["all fields absent", {}, { run_generation: undefined }],
  ["run null", { mode: "run", run_generation: null }, { run_generation: null }],
  ["observe null", { mode: "observe", worker_session_id: null }, { worker_session_id: null }],
  ["run zero", { mode: "run", run_generation: 0 }, { run_generation: 0 }],
  ["observe empty", { mode: "observe", worker_session_id: "" }, { worker_session_id: "" }],
];
const cursor = { row: "9007199254740997", upper_row: "9007199254741001" };
let count = 0;
for (const [label, body, identity] of cases) {
  const item = { id: "run /?#中文", body: Object.freeze(body) };
  const prefix = "/api/v2/resources/run%20%2F%3F%23%E4%B8%AD%E6%96%87/";
  const calls = [
    ["log", () => consoleApi.log(item, 13), { ...identity, offset: 13 }],
    ["history", () => consoleApi.history(item, "orders", 27), { ...identity, kind: "orders", offset: 27, limit: 20 }],
    ["facts", () => consoleApi.facts(item, "fills", cursor, true), { ...identity, kind: "fills", cursor: JSON.stringify(cursor), newest_first: true, limit: 20 }],
    ["facts", () => consoleApi.facts(item, "equity"), { ...identity, kind: "equity", cursor: undefined, newest_first: false, limit: 20 }],
  ];
  for (const [path, invoke, params] of calls) {
    requests.length = 0;
    assert.equal(await invoke(), response, label + " response");
    assert.deepEqual(requests, [{ url: prefix + path, params }], label + " " + path);
    count++;
  }
}
console.log("console API: " + count + " captured requests passed");
