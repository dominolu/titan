import { createRequire } from "node:module";
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
const require = createRequire(
  new URL("../web/console/package.json", import.meta.url),
);
const { chromium, expect } = require("@playwright/test");
const [origin, fault, output] = process.argv.slice(2);
const browser = await chromium.launch({
  headless: true,
  executablePath: process.env.TITAN_BROWSER_EXECUTABLE,
});
const context = await browser.newContext({
    viewport: { width: 1500, height: 1050 },
  }),
  page = await context.newPage(),
  errors = [];
page.on("pageerror", (e) => errors.push(e.message));
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
  await page.goto(origin + "/#/accounts");
  await page.getByText("只读账户观测", { exact: true }).waitFor();
  await page.getByText("当前订单", { exact: true }).click();
  await page
    .getByRole("columnheader", { name: "原始精度", exact: true })
    .waitFor();
  await expect(page.locator(".workbench-content")).toContainText("LONG");
  await expect(page.locator(".workbench-content")).toContainText("SHORT");
  await expect(page.locator(".workbench-content")).toContainText("0.0007");
  await page.getByRole("button", { name: "权益与费用", exact: true }).click();
  await expect(page.locator(".workbench-content")).toContainText("123.456772");
  await page.getByRole("button", { name: "平台事实历史", exact: true }).click();
  await page.locator(".fact-history .el-select").click();
  await page.getByRole("option", { name: "订单", exact: true }).click();
  await expect(page.locator(".workbench-content")).toContainText(
    '"scope": "account"',
  );
  await page.getByRole("button", { name: "风险概览", exact: true }).click();
  await page.mouse.move(500, 100);
  await page.screenshot({
    path: output + "/accounts.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.getByRole("button", { name: "宿主日志", exact: true }).click();
  await page.getByText("synthetic worker log", { exact: true }).waitFor();
  await page.locator(".el-dialog__headerbtn").click();
  await page.getByRole("button", { name: "确认已知悉", exact: true }).click();
  await page.getByText("已知悉：operator", { exact: true }).waitFor();
  await page.waitForTimeout(1600);
  await page.reload();
  await page.getByText("已知悉：operator", { exact: true }).waitFor();
  await page.request.get(fault + "/fault/stale");
  await page
    .getByText(
      "观测已过期或账户快照不可用；下方保留最后有效事实，不能据此确认当前仓位。",
      { exact: true },
    )
    .waitFor();
  await expect(
    page.locator(".account-risk-table tbody tr").first(),
  ).toContainText("当前不可确认");
  await expect(page.locator(".risk-value")).toContainText("—");
  await page.screenshot({
    path: output + "/stale-account.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.request.get(fault + "/fault/recover");
  await page.getByRole("button", { name: "确认已知悉", exact: true }).waitFor();
  await page.getByRole("button", { name: "观测与诊断", exact: true }).click();
  await expect(page.locator(".el-drawer:visible")).toContainText(
    '"last_seen_at_ns"',
  );
  await page.locator(".el-drawer__close-btn:visible").click();
  await page
    .locator(".el-drawer__close-btn:visible")
    .waitFor({ state: "hidden" });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(
    await page
      .locator(".console-workspace")
      .evaluate((el) => el.scrollWidth <= el.clientWidth),
    true,
  );
  await page.mouse.move(0, 0);
  await page.screenshot({
    path: output + "/mobile-accounts.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.setViewportSize({ width: 1500, height: 1050 });
  await page.goto(origin + "/#/runs");
  await page.getByRole("heading", { name: "运行总览", exact: true }).waitFor();
  await page
    .getByLabel("搜索运行实例", { exact: true })
    .fill("no-such-instrument");
  await page
    .getByText("没有匹配的实例，请调整搜索或筛选。", { exact: true })
    .waitFor();
  await page.getByLabel("搜索运行实例", { exact: true }).fill("BTC");
  await page.locator(".runs-table .text-link").click();
  await page
    .getByRole("heading", { name: "BTC / USDT", exact: true })
    .waitFor();
  await expect(page.locator(".recording-warning")).toContainText(
    "历史记录范围未证明完整",
  );
  await page.getByRole("button", { name: "订单与持仓", exact: true }).click();
  await page.getByText("LONG", { exact: true }).waitFor();
  await page.getByText("SHORT", { exact: true }).waitFor();
  await expect(page.locator(".workbench-content")).toContainText("0.0007");
  await page.getByRole("button", { name: "成交与历史", exact: true }).click();
  await page.locator(".fact-history .el-select").click();
  await page.getByRole("option", { name: "订单", exact: true }).click();
  await page.locator(".fact-entry").first().waitFor();
  await page.route(
    "**/api/v2/resources/observe-run-fixture/history*",
    async (route) => {
      const offset = Number(
        new URL(route.request().url()).searchParams.get("offset") || "0",
      );
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: Array.from({ length: 20 }, (_, index) => ({
            asset_id: "1",
            record: offset + index + 1,
          })),
          source: { synthetic: true },
          recording: { history_complete: false },
        }),
      });
    },
  );
  await page.getByRole("button", { name: "查询", exact: true }).click();
  await expect(page.locator(".fact-entry")).toHaveCount(20);
  await page.getByRole("button", { name: "下一页", exact: true }).click();
  await expect(page.locator(".fact-history .table-footer")).toContainText(
    "偏移 20",
  );
  await page.request.get(fault + "/fault/refresh");
  await page.getByRole("button", { name: "观测与诊断", exact: true }).click();
  await page.getByText("完整资源原始数据", { exact: true }).click();
  await expect(page.locator(".el-drawer:visible")).toContainText(
    /"view_revision_probe":\s*"?1"?/,
  );
  await page.locator(".el-drawer__close-btn:visible").click();
  await page
    .locator(".el-drawer__close-btn:visible")
    .waitFor({ state: "hidden" });
  await expect(page.locator(".fact-history .table-footer")).toContainText(
    "偏移 20",
  );
  await page.unroute("**/api/v2/resources/observe-run-fixture/history*");
  await page.getByRole("button", { name: "参数与版本", exact: true }).click();
  await page.getByText("参数、策略版本与恢复来源", { exact: true }).waitFor();
  await page.getByRole("button", { name: "运行概览", exact: true }).click();
  await page.request.get(fault + "/fault/stale");
  await expect(page.locator(".runtime-state")).toContainText(
    "当前状态不可确认",
  );
  await expect(page.locator(".risk-value")).toContainText("—");
  await page.screenshot({
    path: output + "/stale-run.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.request.get(fault + "/fault/recover");
  await page.getByRole("button", { name: "观测与诊断", exact: true }).click();
  await page
    .getByRole("heading", { name: "观测与诊断", exact: true })
    .waitFor();
  await page.getByText("授权节点状态与连接原因", { exact: true }).click();
  await expect(
    page
      .locator(".el-drawer:visible details")
      .filter({ hasText: "授权节点状态与连接原因" })
      .locator("pre"),
  ).toContainText('"online"');
  await page.locator(".el-drawer__close-btn:visible").click();
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(
    await page
      .locator(".console-workspace")
      .evaluate((el) => el.scrollWidth <= el.clientWidth),
    true,
  );
  assert.equal(
    await page.evaluate(() => document.documentElement.scrollWidth),
    390,
  );
  await page.screenshot({
    path: output + "/mobile-run.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.setViewportSize({ width: 1500, height: 1050 });
  await page.screenshot({
    path: output + "/observation.png",
    fullPage: true,
    animations: "disabled",
  });
  let releaseHistory, historyEntered;
  const heldHistory = new Promise((resolve) => {
    releaseHistory = resolve;
  });
  const enteredHistory = new Promise((resolve) => {
    historyEntered = resolve;
  });
  await page.route(
    "**/api/v2/resources/observe-run-fixture/history*",
    async (route) => {
      if (new URL(route.request().url()).searchParams.get("kind") !== "stream")
        return route.continue();
      historyEntered();
      await heldHistory;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: [{ late_source_probe: "must-never-leak-after-route-change" }],
          source: { worker_id: "old-fixture" },
          recording: { history_complete: false },
        }),
      });
    },
  );
  await page.getByRole("button", { name: "成交与历史", exact: true }).click();
  await page.locator(".fact-history .el-select").click();
  await page.getByRole("option", { name: "流状态", exact: true }).click();
  await enteredHistory;
  await page.goto(origin + "/#/runs/does-not-exist");
  const released = page.waitForResponse(
    (response) =>
      response.url().includes("observe-run-fixture/history") &&
      response.url().includes("kind=stream"),
  );
  releaseHistory();
  await released;
  await expect(page.locator(".workbench-content")).not.toContainText(
    "must-never-leak-after-route-change",
  );
  await expect(page.locator(".workbench-content")).not.toContainText(
    "账户订单快照",
  );
  await expect(page.locator(".workbench-content")).toContainText(
    /无法加载|实例不存在|404|NOT_FOUND|不存在|无权限/,
  );
  await page.goto(origin + "/#/accounts");
  const oldAlert = await (
    await page.request.get(origin + "/api/v2/resources/observe-alert")
  ).json();
  let releaseAcknowledgement, acknowledgementEntered;
  const heldAcknowledgement = new Promise((resolve) => {
    releaseAcknowledgement = resolve;
  });
  const enteredAcknowledgement = new Promise((resolve) => {
    acknowledgementEntered = resolve;
  });
  await page.route(
    "**/api/v2/resources/observe-alert/acknowledge",
    async (route) => {
      acknowledgementEntered();
      await heldAcknowledgement;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          ...oldAlert,
          revision: "9223372036854775800",
          body: {
            ...oldAlert.body,
            message: "old-session-response-must-not-appear",
            acknowledgement: { actor: "old-operator" },
          },
        }),
      });
    },
  );
  await page.getByRole("button", { name: "确认已知悉", exact: true }).click();
  await enteredAcknowledgement;
  await page.getByText("operator", { exact: true }).click();
  await page.getByText("退出系统", { exact: true }).click();
  await page.waitForURL("**/#/login");
  await login("reader");
  await page.goto(origin + "/#/accounts");
  const acknowledgementReleased = page.waitForResponse((response) =>
    response.url().endsWith("observe-alert/acknowledge"),
  );
  releaseAcknowledgement();
  await acknowledgementReleased;
  await page.unroute("**/api/v2/resources/observe-alert/acknowledge");
  await expect(page.locator(".workbench-content")).not.toContainText(
    "old-session-response-must-not-appear",
  );
  await expect(
    page.getByRole("button", { name: "确认已知悉", exact: true }),
  ).toBeDisabled();
  const session = await (
    await page.request.get(origin + "/api/v2/session")
  ).json();
  const denied = await page.request.post(
    origin + "/api/v2/resources/observe-alert/acknowledge",
    {
      headers: { Origin: origin, "x-csrf-token": session.csrf_token },
      data: { expected_revision: "0", condition_identity: "two" },
    },
  );
  assert.equal(denied.status(), 403);
  assert.deepEqual(errors, []);
  await writeFile(
    output + "/browser.json",
    JSON.stringify(
      {
        passed: true,
        checks: [
          "account-equity-risk-history-diagnostics",
          "separate-hedge-positions-exact-quantity",
          "account-log-and-alert-acknowledgement",
          "stale-risk-does-not-show-current-amounts",
          "source-and-session-late-response-guards",
          "history-pagination-survives-sse",
          "mobile-accounts-and-run",
          "reader-api-and-ui-write-denied",
        ],
        pageErrors: errors,
      },
      null,
      2,
    ),
  );
} catch (error) {
  await page.screenshot({
    path: output + "/failure.png",
    fullPage: true,
    animations: "disabled",
  });
  await writeFile(
    output + "/failure.txt",
    await page.locator("body").innerText(),
  );
  throw error;
} finally {
  await browser.close();
}
