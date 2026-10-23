import { createRequire } from "node:module";
import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
const require = createRequire(
  new URL("../web/console/package.json", import.meta.url),
);
const { chromium, expect } = require("@playwright/test");
const [origin, output, artifact] = process.argv.slice(2);
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
  await page.getByRole("button", { name: "确定", exact: true }).click();
  await page.locator(".el-drawer__close-btn:visible").click();
}
async function login(name) {
  await page.goto(origin + "/#/login");
  await page.locator("input[autocomplete='username']").fill(name);
  await page
    .locator("input[autocomplete='current-password']")
    .fill(process.env.TITAN_BROWSER_PASSWORD);
  await page.getByRole("button", { name: "登录", exact: true }).click();
  await page.waitForURL("**/#/welcome");
}
try {
  await login("operator");
  await page.goto(origin + "/#/artifacts");
  await page.getByRole("heading", { name: "构建制品", exact: true }).waitFor();
  await page.getByText("选择已构建制品", { exact: true }).click();
  await page.getByRole("option", { name: artifact, exact: true }).click();
  await page.getByText("选择已授权实盘绑定", { exact: true }).click();
  await page
    .getByRole("option", {
      name: "live / gate / GATE_FUTURE_BTC_USDT",
      exact: true,
    })
    .click();
  await page.getByRole("button", { name: "部署制品", exact: true }).click();
  await page.locator(".el-drawer__close-btn:visible").click();
  await page
    .getByRole("button", { name: "启动", exact: true })
    .waitFor({ timeout: 60000 });
  await page.getByRole("button", { name: "启动", exact: true }).click();
  await page.getByRole("button", { name: "取消", exact: true }).click();
  const cancelled = await (
    await page.request.get(origin + "/api/v1/snapshot")
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
  await page
    .getByRole("button", { name: "暂停", exact: true })
    .waitFor({ timeout: 60000 });
  await page.getByRole("button", { name: "暂停", exact: true }).click();
  await confirm();
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
    .getByRole("button", { name: "checkpoint 恢复", exact: true })
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
  await expect(
    page.getByRole("button", { name: "checkpoint 恢复", exact: true }),
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
          "resume",
          "stop-confirmation",
          "readonly-controls",
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
