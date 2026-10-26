// Execute on 192.168.3.88 against the real non-live Rust service created by management_console_smoke.py.
import { createRequire } from "node:module";
import { mkdir, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
const require = createRequire(
  new URL("../web/console/package.json", import.meta.url),
);
const { chromium, expect } = require("@playwright/test");
const [origin, output] = process.argv.slice(2);
assert(origin?.startsWith("http://127.0.0.1:"));
assert(process.env.TITAN_BROWSER_PASSWORD);
await mkdir(output, { recursive: true });
const browser = await chromium.launch({
  headless: true,
  executablePath: process.env.TITAN_BROWSER_EXECUTABLE,
});
const errors = [];
const researchPath = "/#/research/research-grid";
async function workspace(page, tab = "parameters", query = {}) {
  await page.goto(
    origin + researchPath + "?" + new URLSearchParams({ tab, ...query }),
  );
  await page
    .getByRole("heading", { name: "网格策略", exact: true, level: 1 })
    .waitFor();
  await expect(page.locator(".research-tabs button.active")).toHaveText(
    {
      parameters: "版本与参数",
      artifacts: "构建记录",
      backtests: "回测与报告",
      live: "策略实盘",
    }[tab],
  );
  await expect(page.locator(".research-workspace")).toBeVisible();
}
async function login(role, existing) {
  const context =
    existing?.context ??
    (await browser.newContext({ viewport: { width: 1500, height: 1050 } }));
  const page = existing?.page ?? (await context.newPage());
  if (!existing) {
    page.on("pageerror", (error) => errors.push(error.message));
    await page.goto(origin + "/#/login");
  }
  await page.locator("input[autocomplete='username']").fill(role);
  await page
    .locator("input[autocomplete='current-password']")
    .fill(process.env.TITAN_BROWSER_PASSWORD);
  await page.getByRole("button", { name: "登录", exact: true }).click();
  await page.waitForURL("**/#/runs");
  await workspace(page);
  return { context, page };
}
try {
  const { context, page } = await login("operator");
  await page.getByText("管理连接正常", { exact: true }).waitFor();
  await expect
    .poll(
      async () =>
        Object.keys(
          JSON.parse(
            await page
              .getByRole("textbox", { name: "参数 JSON", exact: true })
              .inputValue(),
          ),
        ).length,
    )
    .toBeGreaterThan(0);
  async function terminal(operation) {
    let actual;
    await expect
      .poll(
        async () => {
          actual = await (
            await page.request.get(
              origin + "/api/v1/operations/" + operation.id,
            )
          ).json();
          return ["succeeded", "failed", "cancelled"].includes(actual.state);
        },
        { timeout: 120000, intervals: [500, 1000] },
      )
      .toBe(true);
    assert.equal(actual.state, "succeeded", JSON.stringify(actual));
    return actual;
  }
  async function clickOperation(button) {
    const [response] = await Promise.all([
      page.waitForResponse(
        (response) =>
          response.url().endsWith("/api/v1/operations") &&
          response.request().method() === "POST",
      ),
      button.click(),
    ]);
    assert.equal(response.status(), 202);
    return terminal(await response.json());
  }
  await page.locator(".parameter-editor .el-select").first().click();
  await page
    .getByRole("option", { name: "build / sideways-neutral", exact: true })
    .click();
  const editor = page.getByRole("textbox", { name: "参数 JSON", exact: true });
  const initialParameters = await editor.inputValue();
  const [draftResponse] = await Promise.all([
    page.waitForResponse(
      (response) =>
        response.url().endsWith("/api/v1/parameter-versions") &&
        response.request().method() === "POST",
    ),
    page.getByRole("button", { name: "保存草稿", exact: true }).click(),
  ]);
  assert(draftResponse.ok());
  const draft = await draftResponse.json();
  const editedValues = JSON.parse(initialParameters);
  editedValues.max_rejections += 1;
  const editedParameters = JSON.stringify(editedValues, null, 2);
  await editor.fill(editedParameters);
  const [patchResponse] = await Promise.all([
    page.waitForResponse(
      (response) =>
        response.url().endsWith("/api/v1/parameter-versions/" + draft.id) &&
        response.request().method() === "PATCH",
    ),
    page.getByRole("button", { name: "保存草稿", exact: true }).click(),
  ]);
  assert(patchResponse.ok());
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(editor).toHaveValue(editedParameters);
  await clickOperation(
    page.getByRole("button", { name: "校验参数", exact: true }),
  );
  await expect(
    page.getByRole("button", { name: "冻结版本", exact: true }),
  ).toBeEnabled();
  await editor.fill(editedParameters + "\n");
  await expect(
    page.getByRole("button", { name: "冻结版本", exact: true }),
  ).toBeDisabled();
  await clickOperation(
    page.getByRole("button", { name: "校验参数", exact: true }),
  );
  await expect(
    page.getByRole("button", { name: "冻结版本", exact: true }),
  ).toBeEnabled();
  await page.getByRole("button", { name: "表单编辑", exact: true }).click();
  await expect(
    page.locator(".schema-grid .el-form-item").first(),
  ).toBeVisible();
  await page.getByRole("button", { name: "JSON 编辑", exact: true }).click();
  const [frozenResponse] = await Promise.all([
    page.waitForResponse(
      (response) =>
        response.url().endsWith("/api/v1/parameter-versions/" + draft.id) &&
        response.request().method() === "PATCH",
    ),
    page.getByRole("button", { name: "冻结版本", exact: true }).click(),
  ]);
  assert(frozenResponse.ok());
  const frozen = await frozenResponse.json();
  assert(frozen.frozen);
  await page
    .locator(".versions-table tbody tr")
    .filter({ hasText: frozen.id })
    .getByRole("button", { name: "复制参数", exact: true })
    .click();
  const copied = JSON.parse(await editor.inputValue());
  copied.max_rejections += 1;
  await editor.fill(JSON.stringify(copied, null, 2));
  const [copyResponse] = await Promise.all([
    page.waitForResponse(
      (response) =>
        response.url().endsWith("/api/v1/parameter-versions") &&
        response.request().method() === "POST",
    ),
    page.getByRole("button", { name: "保存草稿", exact: true }).click(),
  ]);
  assert(copyResponse.ok());
  const copiedDraft = await copyResponse.json();
  await page
    .locator(".versions-table tbody tr")
    .filter({ hasText: copiedDraft.id })
    .getByRole("button", { name: "编辑草稿", exact: true })
    .click();
  const comparisonSelect = page.locator(
    ".versions-panel .table-toolbar .el-select",
  );
  await comparisonSelect.click();
  await page.getByRole("option", { name: frozen.id, exact: true }).click();
  await page.getByRole("option", { name: copiedDraft.id, exact: true }).click();
  await page.keyboard.press("Escape");
  await expect(page.locator(".version-diff")).toContainText("max_rejections");
  await page.mouse.move(500, 100);
  await page
    .locator(".console-workspace")
    .evaluate((el) => el.scrollTo({ top: 0 }));
  await editor.evaluate((el) => {
    el.scrollTop = 0;
  });
  await expect(page.locator(".el-message:visible")).toHaveCount(0, {
    timeout: 10000,
  });
  await page.screenshot({
    path: output + "/parameters.png",
    fullPage: true,
    animations: "disabled",
  });
  await expect(
    page
      .locator(".versions-table tbody tr")
      .filter({ hasText: frozen.id })
      .getByRole("button", { name: "构建", exact: true }),
  ).toBeEnabled();
  await workspace(page, "artifacts");
  await page.reload();
  await page.locator(".research-workspace .el-select").first().click();
  await page
    .getByRole("option", { name: "build / " + frozen.id, exact: true })
    .click();
  const buildPager = page.locator('.research-pager[data-kind="version"]');
  if (
    await buildPager
      .getByRole("button", { name: "下一页", exact: true })
      .isVisible()
  ) {
    await buildPager
      .getByRole("button", { name: "下一页", exact: true })
      .click();
    await expect(buildPager).toContainText("第 2 页");
    await expect(
      page.getByRole("button", { name: "构建版本", exact: true }),
    ).toBeEnabled();
  }
  const built = await clickOperation(
    page.getByRole("button", { name: "构建版本", exact: true }),
  );
  assert.equal(built.input.version_id, frozen.id);
  const artifact = built.result.resources.find(
    (item) => item.kind === "artifact",
  );
  assert(artifact);
  await page.locator(".el-drawer__close-btn:visible").click();
  await workspace(page, "artifacts");
  await expect(page.locator(".artifacts-table")).toContainText(artifact.id);
  const downloadPath = await page
    .locator(".artifacts-table tbody tr")
    .filter({ hasText: artifact.id })
    .getByRole("link", { name: "下载", exact: true })
    .getAttribute("href");
  const download = await page.request.get(new URL(downloadPath, origin).href);
  assert.equal(download.status(), 200);
  await page
    .getByLabel("搜索构建制品", { exact: true })
    .fill("no-such-artifact");
  await expect(page.locator(".artifacts-table")).toContainText(
    "没有匹配的制品",
  );
  await page.getByLabel("搜索构建制品", { exact: true }).fill(artifact.id);
  await page.mouse.move(500, 100);
  await page
    .locator(".console-workspace")
    .evaluate((el) => el.scrollTo({ top: 0 }));
  await expect(page.locator(".el-message:visible")).toHaveCount(0, {
    timeout: 10000,
  });
  await page.screenshot({
    path: output + "/artifacts.png",
    fullPage: true,
    animations: "disabled",
  });
  await workspace(page, "backtests");
  await page.locator(".backtest-form .el-select").nth(0).click();
  await page.getByRole("option", { name: artifact.id, exact: true }).click();
  await page.locator(".backtest-form .el-select").nth(1).click();
  await page
    .getByRole("option", { name: "sideways / sideways", exact: true })
    .click();
  await clickOperation(
    page.getByRole("button", { name: "准备数据", exact: true }),
  );
  await page.locator(".el-drawer__close-btn:visible").click();
  await page
    .locator(".el-drawer__close-btn:visible")
    .waitFor({ state: "hidden" });
  await page.locator(".backtest-form .el-switch").click();
  await expect(page.getByRole("switch")).toBeChecked();
  await page.locator(".backtest-form .el-switch").click();
  const replay = await clickOperation(
    page.getByRole("button", { name: "发起回测", exact: true }),
  );
  const result = replay.result.resources.find(
    (item) => item.kind === "backtest",
  );
  assert.equal(result.body.metrics.final_position_btc, "0.0");
  await page.locator(".el-drawer__close-btn:visible").click();
  const rendered = await clickOperation(
    page
      .locator(".backtest-results tbody tr")
      .filter({ hasText: result.id })
      .getByRole("button", { name: "生成报告", exact: true }),
  );
  await page.locator(".el-drawer__close-btn:visible").click();
  await page
    .getByRole("button", {
      name: "查看报告 " + rendered.id.slice(-12),
      exact: true,
    })
    .click();
  await page
    .frameLocator("iframe.report-frame")
    .getByText("Verified ResultBundle", { exact: false })
    .first()
    .waitFor();
  await page.locator(".el-dialog__headerbtn").click();
  await expect(page.locator(".workbench-sidebar nav a")).toHaveCount(3);
  await expect(page.locator(".workbench-sidebar nav")).toHaveText(
    /运行总览.*策略研究.*账户风险/s,
  );
  for (const tab of ["parameters", "artifacts", "backtests", "live"]) {
    await workspace(page, tab);
    if (tab === "backtests") {
      await page.locator("canvas").first().waitFor();
      await page.screenshot({
        path: output + "/backtests.png",
        fullPage: true,
        animations: "disabled",
      });
    }
    await page.setViewportSize({ width: 390, height: 844 });
    await page.mouse.move(0, 0);
    await expect
      .poll(() =>
        page
          .locator(".console-workspace")
          .evaluate((el) => el.scrollWidth <= el.clientWidth),
      )
      .toBe(true);
    assert.equal(
      await page.evaluate(() => document.documentElement.scrollWidth),
      390,
      tab,
    );
    await page.screenshot({
      path: output + "/mobile-" + tab + ".png",
      fullPage: true,
      animations: "disabled",
    });
    await page.setViewportSize({ width: 1500, height: 1050 });
  }
  const session = await (
    await page.request.get(origin + "/api/v1/session")
  ).json();
  const headers = { Origin: origin, "x-csrf-token": session.csrf_token };
  const snapshot = await (
    await page.request.get(origin + "/api/v1/snapshot")
  ).json();
  const other = snapshot.resources.find(
    (item) =>
      item.kind === "template" && item.body.template_id === "foreign-grid",
  );
  assert(other);
  const describe = await page.request.post(origin + "/api/v1/operations", {
    headers,
    data: {
      idempotency_key: crypto.randomUUID(),
      node_id: "build",
      action: {
        kind: "describe",
        template_id: "foreign-grid",
        parameters: other.body.parameters,
      },
    },
  });
  assert.equal(describe.status(), 202);
  const valid = await terminal(await describe.json());
  const otherVersion = await page.request.post(
    origin + "/api/v1/parameter-versions",
    {
      headers,
      data: {
        node_id: "build",
        template_id: "foreign-grid",
        parameters: other.body.parameters,
        expected_revision: null,
        freeze_operation_id: valid.id,
      },
    },
  );
  assert(otherVersion.ok());
  const otherFrozen = await otherVersion.json();
  const buildOther = await page.request.post(origin + "/api/v1/operations", {
    headers,
    data: {
      idempotency_key: crypto.randomUUID(),
      node_id: "build",
      action: { kind: "build", version_id: otherFrozen.id },
    },
  });
  assert.equal(buildOther.status(), 202);
  const otherBuilt = await terminal(await buildOther.json());
  const otherArtifact = otherBuilt.result.resources.find(
    (item) => item.kind === "artifact",
  );
  assert.equal(otherArtifact.body.research_id, "research-other-strategy");
  const scoped = await (
    await page.request.get(
      origin +
        "/api/v1/resources?kind=artifact&research_id=research-grid&resource_id=" +
        otherArtifact.id,
    )
  ).json();
  assert.deepEqual(scoped, []);
  await workspace(page, "artifacts");
  await expect(page.locator(".artifacts-table")).not.toContainText(
    otherArtifact.id,
  );
  await workspace(page, "live", { artifact: otherArtifact.id });
  await expect(page.locator(".research-workspace")).toContainText(
    "所选资源不属于当前策略",
  );
  await expect(
    page.getByRole("button", { name: "部署制品", exact: true }),
  ).toBeDisabled();
  for (let index = 0; index < 105; index++) {
    const saved = await page.request.post(
      origin + "/api/v1/parameter-versions",
      {
        headers,
        data: {
          node_id: "build",
          template_id: "foreign-grid",
          parameters: { ...editedValues, max_rejections: 100 + index },
          expected_revision: null,
          freeze_operation_id: null,
        },
      },
    );
    assert(saved.ok());
  }
  for (let index = 0; index < 24; index++) {
    const saved = await page.request.post(
      origin + "/api/v1/parameter-versions",
      {
        headers,
        data: {
          node_id: "build",
          template_id: index % 2 ? "foreign-grid" : "sideways-neutral",
          parameters: { ...editedValues, max_rejections: 10 + index },
          expected_revision: null,
          freeze_operation_id: null,
        },
      },
    );
    assert(saved.ok());
  }
  // Fill the current workspace's first page despite a larger global resource population.
  for (let index = 0; index < 12; index++) {
    const saved = await page.request.post(
      origin + "/api/v1/parameter-versions",
      {
        headers,
        data: {
          node_id: "build",
          template_id: "sideways-neutral",
          parameters: { ...editedValues, max_rejections: 40 + index },
          expected_revision: null,
          freeze_operation_id: null,
        },
      },
    );
    assert(saved.ok());
  }
  await workspace(page, "parameters");
  const versionsPager = page.locator('.research-pager[data-kind="version"]');
  await versionsPager
    .getByRole("button", { name: "刷新记录", exact: true })
    .click();
  await expect(page.locator(".versions-table tbody tr")).toHaveCount(20);
  await expect(page.locator(".versions-table")).not.toContainText(
    "foreign-grid",
  );
  await versionsPager
    .getByRole("button", { name: "下一页", exact: true })
    .click();
  await expect(versionsPager).toContainText("第 2 页");
  await expect(page.locator(".versions-table")).not.toContainText(
    "foreign-grid",
  );
  await versionsPager
    .getByRole("button", { name: "上一页", exact: true })
    .click();
  await page.goto(origin + "/#/research");
  await expect(page.locator(".research-card")).toHaveCount(2);
  await page.mouse.move(500, 100);
  await page.screenshot({
    path: output + "/research-index.png",
    fullPage: true,
    animations: "disabled",
  });
  await page.goto(origin + "/#/artifacts");
  await page.waitForURL("**/#/research?tab=artifacts");
  await page.goto(origin + "/#/accounts");
  await page.reload();
  await page.getByRole("heading", { name: "账户风险", exact: true }).waitFor();
  const precision = await page.evaluate(() => {
    const original = '{"value":18446744073709551615}';
    const decoded = JSON.parse(original, (_key, value, context) =>
      typeof value === "number" && !Number.isSafeInteger(value)
        ? JSON.rawJSON(context.source)
        : value,
    );
    return JSON.stringify(decoded);
  });
  assert.equal(precision, '{"value":18446744073709551615}');
  await context.setOffline(true);
  await page.getByText("连接中断 / 重同步", { exact: true }).waitFor();
  await context.setOffline(false);
  await page
    .getByText("管理连接正常", { exact: true })
    .waitFor({ timeout: 20000 });
  await page.getByText("operator", { exact: true }).click();
  await page.getByText("退出系统", { exact: true }).click();
  await page.waitForURL("**/#/login");
  const reader = await login("reader", { context, page });
  await reader.page
    .getByText("当前为只读权限，写操作由服务端统一授权。", { exact: true })
    .waitFor();
  assert(
    await reader.page
      .getByRole("button", { name: "校验参数", exact: true })
      .isDisabled(),
  );
  await workspace(page, "live");
  await expect(
    page.getByRole("button", { name: "部署制品", exact: true }),
  ).toBeDisabled();
  await workspace(page, "backtests");
  for (const label of ["准备数据", "发起回测"])
    await expect(
      page.getByRole("button", { name: label, exact: true }),
    ).toBeDisabled();
  for (const button of await page
    .getByRole("button", { name: "生成报告", exact: true })
    .all())
    await expect(button).toBeDisabled();
  await workspace(page);
  await reader.page.screenshot({
    path: output + "/readonly.png",
    fullPage: true,
    animations: "disabled",
  });
  await reader.context.close();
  assert.deepEqual(errors, []);
  execFileSync(
    "/home/dominolu/dev/titan/.venv/bin/python",
    [
      fileURLToPath(new URL("./management_fault_smoke.py", import.meta.url)),
      dirname(output),
    ],
    { env: process.env, stdio: "inherit", timeout: 240000 },
  );
  await writeFile(
    output + "/browser.json",
    JSON.stringify(
      {
        passed: true,
        checks: [
          "five-pages",
          "real-session",
          "ui-validate-freeze-build-backtest-report",
          "refresh",
          "bounded-chart",
          "readonly",
          "offline-reconnect",
          "integer-precision",
          "draft-create-edit-copy-diff",
          "catalog-refresh-preserves-editor",
          "changed-parameters-require-validation",
          "schema-form-and-json-editor",
          "initial-template-defaults",
          "artifact-download-search",
          "prepare-and-stress-toggle",
          "four-pages-mobile-width",
          "three-navigation-items",
          "workspace-build-keeps-selected-version-across-pages",
          "per-strategy-workspace-and-alias-grouping",
          "cross-strategy-artifact-selection-rejected",
          "server-filtered-strategy-pagination",
          "legacy-routes-choose-workspace",
        ],
        pageErrors: errors,
      },
      null,
      2,
    ),
  );
  console.log(JSON.stringify({ name: "browser-e2e", passed: true }));
} finally {
  await browser.close();
}
