import { createRequire } from "node:module";
import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
const require = createRequire(
  new URL("../web/console/package.json", import.meta.url),
);
const { chromium, expect } = require("@playwright/test");
const [origin, output, strategyVersion] = process.argv.slice(2);
await mkdir(output, { recursive: true });
const browser = await chromium.launch({
  headless: true,
  executablePath: process.env.TITAN_BROWSER_EXECUTABLE,
});
const context = await browser.newContext({
    viewport: { width: 1600, height: 1100 },
  }),
  page = await context.newPage(),
  errors = [];
page.on("pageerror", (error) => errors.push(error.message));
async function confirm() {
  const responsePromise = page.waitForResponse(
    (response) =>
      response.url().endsWith("/api/v2/operations") &&
      response.request().method() === "POST",
  );
  await page.getByRole("button", { name: "确定", exact: true }).click();
  const response = await responsePromise;
  assert.equal(response.status(), 202, await response.text());
  await page.locator(".el-drawer__close-btn:visible").click();
  await page
    .locator(".el-drawer__close-btn:visible")
    .waitFor({ state: "hidden" });
}
async function login(name) {
  await page.goto(origin + "/#/login");
  await page.locator("input[autocomplete='username']").fill(name);
  await page
    .locator("input[autocomplete='current-password']")
    .fill(process.env.TITAN_BROWSER_PASSWORD);
  await page.getByRole("button", { name: "登录", exact: true }).click();
  await page.waitForURL("**/#/runs");
}
try {
  await login("operator");
  await page.goto(origin + "/#/strategies/grid?tab=live");
  await page.getByRole("heading", { name: "受控部署", exact: true }).waitFor();
  await page.locator(".deployment-form .el-select").nth(0).click();
  await page.getByRole("option", { name: strategyVersion, exact: true }).click();
  await page.locator(".deployment-form .el-select").nth(1).click();
  await page
    .getByRole("option", {
      name: "live / gate / GATE_FUTURE_BTC_USDT",
      exact: true,
    })
    .click();
  await expect(page.locator(".binding-preview")).toContainText("100");
  await page.mouse.move(500, 100);
  await page.screenshot({
    path: output + "/deployment.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.getByRole("button", { name: "部署策略版本", exact: true }).click();
  await page.locator(".el-drawer__close-btn:visible").click();
  await page
    .getByRole("button", { name: "启动", exact: true })
    .waitFor({ timeout: 60000 });
  await page.getByRole("button", { name: "启动", exact: true }).click();
  await page.getByRole("button", { name: "取消", exact: true }).click();
  const cancelled = await (
    await page.request.get(origin + "/api/v2/snapshot")
  ).json();
  assert.equal(
    cancelled.operations.filter((op) => op.input.kind === "start").length,
    0,
  );
  await page.getByRole("button", { name: "启动", exact: true }).click();
  await page.screenshot({
    path: output + "/start-confirmation.png",
    fullPage: true,
    animations: "disabled",
  });
  await confirm();
  await page.goto(origin + "/#/runs");
  await page.getByRole("heading", { name: "运行总览", exact: true }).waitFor();
  await page.locator(".runs-table .text-link").first().waitFor();
  await expect(page.locator(".runs-table .state-tag").first()).toHaveText(
    "运行中",
    { timeout: 60000 },
  );
  await expect(page.locator(".risk-summary .risk-value")).not.toContainText(
    "—",
  );
  await expect(page.locator(".equity-canvas")).toBeVisible({ timeout: 15000 });
  await page.screenshot({
    path: output + "/overview.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.goto(origin + "/#/strategies/grid?tab=live");
  await page.locator(".live-runs-table .text-link").first().click();
  await page
    .getByRole("button", { name: "暂停", exact: true })
    .waitFor({ timeout: 60000 });
  await expect(
    page.getByRole("button", { name: "暂停", exact: true }),
  ).toBeEnabled();
  await page.screenshot({
    path: output + "/running.png",
    fullPage: true,
    animations: "disabled",
  });
  await expect(page.locator(".business-observation")).toContainText("titan.grid", { timeout: 15000 });
  await expect(page.locator(".business-observation")).toContainText("配对毛收益");
  await expect(page.locator(".business-observation")).toContainText("运行内存快照");
  await page.screenshot({path: output + "/business-observation.png",fullPage: true,animations: "disabled"});
  let releasePause, pauseEntered;
  const heldPause = new Promise((resolve) => {
    releasePause = resolve;
  });
  const enteredPause = new Promise((resolve) => {
    pauseEntered = resolve;
  });
  await page.route("**/api/v2/operations", async (route) => {
    if (
      route.request().method() !== "POST" ||
      route.request().postDataJSON()?.action?.kind !== "pause"
    )
      return route.continue();
    const response = await route.fetch();
    pauseEntered();
    await heldPause;
    await route.fulfill({ response });
  });
  await page.getByRole("button", { name: "暂停", exact: true }).click();
  const pauseResponse = page.waitForResponse(
    (response) =>
      response.url().endsWith("/api/v2/operations") &&
      response.request().method() === "POST",
  );
  await page.getByRole("button", { name: "确定", exact: true }).click();
  await enteredPause;
  await expect(
    page.getByRole("button", { name: "恢复运行", exact: true }),
  ).toBeDisabled({ timeout: 15000 });
  await expect(page.locator(".runtime-state")).toContainText("新增开仓已暂停");
  await expect(
    page.getByRole("button", { name: "停止", exact: true }),
  ).toBeEnabled();
  releasePause();
  assert.equal((await pauseResponse).status(), 202);
  await page.locator(".el-drawer__close-btn:visible").click();
  await page
    .locator(".el-drawer__close-btn:visible")
    .waitFor({ state: "hidden" });
  await page.unroute("**/api/v2/operations");
  await page.getByRole("button", { name: "恢复运行", exact: true }).waitFor();
  await page.screenshot({
    path: output + "/paused.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.getByRole("button", { name: "恢复运行", exact: true }).click();
  await confirm();
  await page.getByRole("button", { name: "暂停", exact: true }).waitFor();
  await page.getByRole("button", { name: "停止", exact: true }).click();
  await confirm();
  await page
    .getByRole("button", { name: "从恢复点恢复", exact: true })
    .waitFor({ timeout: 30000 });
  await page.screenshot({
    path: output + "/stopped.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.getByText("operator", { exact: true }).click();
  await page.getByText("退出系统", { exact: true }).click();
  await page.waitForURL("**/#/login");
  await login("reader");
  await page.goto(origin + "/#/runs");
  await page.goto(origin + "/#/strategies/grid?tab=live");
  await page.locator(".live-runs-table .text-link").first().click();
  await expect(
    page.getByRole("button", { name: "从恢复点恢复", exact: true }),
  ).toBeDisabled();
  assert.deepEqual(errors, []);
  await writeFile(
    output + "/browser.json",
    JSON.stringify(
      {
        passed: true,
        pageErrors: errors,
        checks: [
          "deploy",
          "start-cancel-no-operation",
          "start-confirmation",
          "pause",
          "pending-pause-keeps-stop-enabled",
          "resume",
          "stop-confirmation",
          "readonly-controls",
          "grid-public-business-observation-panel",
          "strategy-workspace-live-lifecycle",
        ],
      },
      null,
      2,
    ),
  );
} catch (error) {
  await page
    .screenshot({ path: output + "/failure.png", fullPage: true })
    .catch(() => {});
  throw error;
} finally {
  await browser.close();
}
