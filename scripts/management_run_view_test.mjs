// Execute on the build server. Cover precision, missing evidence and source identity.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
const require = createRequire(
  new URL("../web/console/package.json", import.meta.url),
);
const ts = require("typescript");
const compile = (source) =>
  ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ESNext,
    },
  }).outputText;
const moduleUrl = (code) =>
  "data:text/javascript;base64," + Buffer.from(code).toString("base64");
const precision = moduleUrl(
  compile(
    await readFile(
      new URL("../web/console/src/utils/precision.ts", import.meta.url),
      "utf8",
    ),
  ),
);
const source = compile(
  await readFile(
    new URL("../web/console/src/views/console/run-view.ts", import.meta.url),
    "utf8",
  ),
).replace('"@/utils/precision"', JSON.stringify(precision));
const { decimal, scaled, runView, operationsForRun, orderStates, time } =
  await import(moduleUrl(source));
assert.equal(
  decimal("900719925474099312345.6789", 2),
  "900,719,925,474,099,312,345.68",
);
assert.equal(decimal("-0.005", 2), "-0.01");
assert.equal(decimal("9.999", 2), "10.00");
assert.equal(decimal("0"), "0.00");
assert.equal(decimal(null), "—");
assert.equal(decimal("unknown"), "—");
assert.equal(
  scaled("-9007199254740993", { coefficient: "1", scale: "8" }),
  "-90071992.54740993",
);
assert.equal(scaled("3", { coefficient: "25", scale: "2" }), "0.75");
assert.equal(scaled("1", null), null);
assert.equal(scaled("1", { coefficient: "1", scale: "-1" }), null);
assert.equal(scaled("1", { coefficient: "0", scale: "2" }), null);
assert.equal(time(null), "—");
assert.equal(time(undefined), "—");
assert.equal(time("0"), "—");
const nodes = [
  { id: "node-a", node_id: "a", kind: "node", body: { online: true } },
];
const run = {
  id: "one",
  node_id: "a",
  kind: "run",
  body: {
    available: true,
    generation: "1",
    instrument: "BTC_USDT",
    lifecycle: "Running",
    observation: {
      account_snapshot: {
        available: true,
        image: {
          instruments: [{ native_symbol: "BTC_USDT", asset_id: "1" }],
          positions: [{ asset_id: "2", quantity_lots: "9" }],
          risk: {
            assets: [
              {
                asset_id: "1",
                instrument: "BTC_USDT",
                currency: "USDT",
                complete: true,
                actual_notional: { amount: "0" },
                pending_bound_notional: { amount: "0" },
              },
            ],
          },
        },
      },
    },
  },
};
let view = runView(run, "connected", nodes);
assert.equal(view.riskKnown, true);
assert.equal(view.bound, "0");
assert.equal(view.positions.length, 0);
assert.equal(runView(run, "stale", nodes).bound, null);
assert.equal(runView(run, "connected", []).bound, null);
const missing = structuredClone(run);
delete missing.body.observation.account_snapshot.image.risk.assets[0]
  .pending_bound_notional;
assert.equal(runView(missing, "connected", nodes).riskKnown, false);
const wrong = structuredClone(run);
wrong.body.instrument = "OTHER_BTC_USDT";
assert.equal(runView(wrong, "connected", nodes).bound, null);
const conflict = structuredClone(run);
conflict.body.observation.account_snapshot.image.risk.assets[0].instrument =
  "OTHER_BTC_USDT";
assert.equal(runView(conflict, "connected", nodes).bound, null);
const identityMismatch = structuredClone(run);
identityMismatch.body.strategy_id = "77";
identityMismatch.body.observation.strategies = [
  {
    strategy_id: "78",
    business: {
      available: true,
      data: { grid: { grid_profit_tick_lots: "999" } },
    },
  },
];
assert.equal(
  runView(identityMismatch, "connected", nodes).grid.grid_profit_tick_lots,
  undefined,
);
assert.equal(
  operationsForRun(
    [
      { id: "x", node_id: "b", input: { run_id: "one" } },
      { id: "y", node_id: "a", input: { run_id: "one" } },
    ],
    run,
  )[0].id,
  "y",
);
assert.deepEqual(
  [
    orderStates[2],
    orderStates[3],
    orderStates[4],
    orderStates[5],
    orderStates[6],
  ],
  ["已过期", "已成交", "已撤销", "部分成交", "已拒绝"],
);
console.log(
  "run presentation: precision, units, stale facts, instrument/node identity and native statuses passed",
);
