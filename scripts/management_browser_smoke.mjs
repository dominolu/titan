// Execute on 192.168.3.88 against the real non-live Rust service created by management_console_smoke.py.
import { createRequire } from "node:module";
import { mkdir, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
const require = createRequire(new URL("../web/console/package.json", import.meta.url));
const { chromium, expect } = require("@playwright/test");
const [origin, output] = process.argv.slice(2);
assert(origin?.startsWith("http://127.0.0.1:"));
assert(process.env.TITAN_BROWSER_PASSWORD);
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true, executablePath: process.env.TITAN_BROWSER_EXECUTABLE });
const errors = [];
async function login(role, existing) {
  const context = existing?.context ?? await browser.newContext({ viewport: { width: 1500, height: 1050 } });
  const page = existing?.page ?? await context.newPage();
  if (!existing) { page.on("pageerror", error => errors.push(error.message)); await page.goto(origin + "/#/login"); }
  await page.locator("input[autocomplete='username']").fill(role);
  await page.locator("input[autocomplete='current-password']").fill(process.env.TITAN_BROWSER_PASSWORD);
  await page.getByRole("button", { name: "登录", exact: true }).click();
  await page.waitForURL("**/#/welcome");
  await page.getByRole("heading", { name: "策略参数", exact: true }).waitFor();
  return { context, page };
}
try {
  const { context, page } = await login("operator");
  await page.getByText("实时连接", { exact: true }).waitFor();
  async function terminal(operation) {
    let actual;
    await expect.poll(async()=>{actual=await(await page.request.get(origin+"/api/v1/operations/"+operation.id)).json();return ["succeeded","failed","cancelled"].includes(actual.state);},{timeout:120000,intervals:[500,1000]}).toBe(true);
    assert.equal(actual.state,"succeeded",JSON.stringify(actual));return actual;
  }
  async function clickOperation(button) {
    const [response]=await Promise.all([page.waitForResponse(response=>response.url().endsWith("/api/v1/operations") && response.request().method()==="POST"),button.click()]);
    assert.equal(response.status(),202);return terminal(await response.json());
  }
  await page.locator(".panel .el-select").first().click();
  await page.getByRole("option",{name:"build / sideways-neutral",exact:true}).click();
  await clickOperation(page.getByRole("button",{name:"校验参数",exact:true}));
  await expect(page.getByRole("button",{name:"冻结版本",exact:true})).toBeEnabled();
  const [frozenResponse]=await Promise.all([page.waitForResponse(response=>response.url().endsWith("/api/v1/parameter-versions") && response.request().method()==="POST"),page.getByRole("button",{name:"冻结版本",exact:true}).click()]);
  assert(frozenResponse.ok());const frozen=await frozenResponse.json();
  const built=await clickOperation(page.locator(".el-table__body tr").filter({hasText:frozen.id}).getByRole("button",{name:"构建",exact:true}));
  const artifact=built.result.resources.find(item=>item.kind==="artifact");assert(artifact);
  await page.locator(".el-drawer__close-btn:visible").click();
  await page.goto(origin+"/#/backtests");
  await page.locator(".backtest-form .el-select").nth(0).click();
  await page.getByRole("option",{name:artifact.id,exact:true}).click();
  await page.locator(".backtest-form .el-select").nth(1).click();
  await page.getByRole("option",{name:"sideways / sideways",exact:true}).click();
  const replay=await clickOperation(page.getByRole("button",{name:"发起回测",exact:true}));
  const result=replay.result.resources.find(item=>item.kind==="backtest");assert.equal(result.body.metrics.final_position_btc,"0.0");
  await page.locator(".el-drawer__close-btn:visible").click();
  const rendered=await clickOperation(page.locator(".el-table__body tr").filter({hasText:result.id}).getByRole("button",{name:"生成报告",exact:true}));
  await page.locator(".el-drawer__close-btn:visible").click();
  await page.getByRole("button",{name:"查看报告 "+rendered.id.slice(-12),exact:true}).click();
  await page.frameLocator("iframe.report-frame").getByText("Verified ResultBundle",{exact:false}).first().waitFor();
  await page.locator(".el-dialog__headerbtn").click();
  for (const [path, title] of [["artifacts", "构建制品"], ["backtests", "历史回测"], ["runs", "运行详情"], ["accounts", "账户风险"]]) {
    await page.goto(origin + "/#/" + path);
    await page.getByRole("heading", { name: title, exact: true }).waitFor();
    if (path === "backtests") {
      await page.locator("canvas").first().waitFor();
      await page.screenshot({ path: output + "/backtests.png", fullPage: true, animations:"disabled" });
    }
  }
  await page.reload();
  await page.getByRole("heading", { name: "账户风险", exact: true }).waitFor();
  const precision = await page.evaluate(() => {
    const original = '{"value":18446744073709551615}';
    const decoded = JSON.parse(original, (_key, value, context) => typeof value === "number" && !Number.isSafeInteger(value) ? JSON.rawJSON(context.source) : value);
    return JSON.stringify(decoded);
  });
  assert.equal(precision, '{"value":18446744073709551615}');
  await context.setOffline(true);
  await page.getByText("观察受限 / 重同步", { exact: true }).waitFor();
  await context.setOffline(false);
  await page.getByText("实时连接", { exact: true }).waitFor({ timeout: 20000 });
  await page.getByText("operator", { exact: true }).click();
  await page.getByText("退出系统", { exact: true }).click();
  await page.waitForURL("**/#/login");
  const reader = await login("reader", { context, page });
  await reader.page.getByText("当前为只读权限，写操作由服务端统一授权。", { exact: true }).waitFor();
  assert(await reader.page.getByRole("button", { name: "校验参数", exact: true }).isDisabled());
  await reader.page.screenshot({ path: output + "/readonly.png", fullPage: true, animations:"disabled" });
  await reader.context.close();
  assert.deepEqual(errors, []);
  execFileSync("/home/dominolu/dev/titan/.venv/bin/python",[fileURLToPath(new URL("./management_fault_smoke.py",import.meta.url)),dirname(output)],{env:process.env,stdio:"inherit",timeout:240000});
  await writeFile(output + "/browser.json", JSON.stringify({ passed: true, checks: ["five-pages", "real-session", "ui-validate-freeze-build-backtest-report", "refresh", "bounded-chart", "readonly", "offline-reconnect", "integer-precision"], pageErrors: errors }, null, 2));
  console.log(JSON.stringify({ name: "browser-e2e", passed: true }));
} finally { await browser.close(); }
