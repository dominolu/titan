<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from "vue";
import { useRoute } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import { useConsoleStore } from "@/store/modules/console";
import {
  consoleApi,
  type Action,
  type Operation,
  type Resource
} from "@/api/console";
import { parseExact, integerToken, scalarText } from "@/utils/precision";
import ResultChart from "./ResultChart.vue";
import HistoryDrawer from "./HistoryDrawer.vue";
const store = useConsoleStore();
const route = useRoute();
const selectedTemplate = ref("");
const parameters = ref("{}");
const validation = ref("");
const draft = ref<Resource | null>(null);
const comparison = ref<string[]>([]);
const differences = computed(() => {
  const [left, right] = comparison.value.map(id =>
    store.versions.find(item => item.id === id)
  );
  if (!left || !right) return [];
  const a = body(left).parameters,
    b = body(right).parameters;
  return [...new Set([...Object.keys(a), ...Object.keys(b)])]
    .filter(key => pretty(a[key]) !== pretty(b[key]))
    .map(key => ({
      key,
      left: pretty(a[key]) ?? "未设置",
      right: pretty(b[key]) ?? "未设置"
    }));
});
const selectedArtifact = ref("");
const selectedDataset = ref("");
const stress = ref(false);
const busy = ref(false);
const detail = ref<Resource | Operation | null>(null);
const report = ref("");
const selectedResult = ref("");
const activeResult = computed(
  () =>
    store.backtests.find(item => item.id === selectedResult.value) ??
    store.backtests[0]
);
const metricCards = [
  ["净收益 USDT", "net_pnl_usdt"],
  ["收益率 %", "net_return_pct"],
  ["最大回撤 %", "max_drawdown_pct"],
  ["最大实际敞口 USDT", "max_position_notional_usdt"],
  ["含在途敞口上界 USDT", "max_pending_bound_notional_usdt"],
  ["最终持仓 BTC", "final_position_btc"]
];
const section = computed(() =>
  route.path === "/welcome" ? "strategies" : route.path.slice(1)
);
const template = computed(() =>
  store.templates.find(item => item.id === selectedTemplate.value)
);
const labels: Record<string, string> = {
  pending: "待执行",
  running: "执行中",
  succeeded: "成功",
  failed: "失败",
  cancelled: "已取消",
  unknown: "结果未知"
};
const describeResult = computed(() =>
  store.operations.find(item => item.id === validation.value)
);
const schema = computed(() => {
  const last = store.operations.find(
    item =>
      item.state === "succeeded" &&
      item.input.kind === "describe" &&
      template.value &&
      item.input.template_id === body(template.value).template_id &&
      item.node_id === template.value.node_id
  );
  return (last?.result as any)?.metadata?.parameter_schema?.properties ?? {};
});
const templateValues = computed(() => {
  try {
    return parseExact(parameters.value);
  } catch {
    return {};
  }
});
function body(item: Resource): any {
  return item.body;
}
function pretty(value: unknown) {
  return JSON.stringify(value, null, 2);
}
function metricText(value: unknown) {
  const exact = String(value ?? "缺失");
  const match = exact.match(/^(-?\d+)\.(\d{7,})$/);
  return match ? `${match[1]}.${match[2].slice(0, 6)}…` : exact;
}
function setParameter(key: string, value: string, type: string) {
  const values = { ...templateValues.value };
  if (type === "integer") {
    try {
      values[key] = integerToken(value);
    } catch (error) {
      ElMessage.error((error as Error).message);
      return;
    }
  } else if (type === "number") {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      ElMessage.error("请输入有限数值");
      return;
    }
    values[key] = number;
  } else if (type === "boolean") values[key] = value === "true";
  else values[key] = value;
  parameters.value = pretty(values);
  validation.value = "";
}
async function work(run: () => Promise<unknown>) {
  busy.value = true;
  try {
    await run();
  } catch (error) {
    ElMessage.error((error as Error).message);
  } finally {
    busy.value = false;
  }
}
async function action(input: Action, node: string) {
  await work(async () => {
    const op = await store.submit(input, node);
    detail.value = op;
    ElMessage.success("操作已受理，等待执行方确认");
  });
}
async function validate() {
  if (!template.value) return;
  await work(async () => {
    const op = await store.submit(
      {
        kind: "describe",
        template_id: body(template.value).template_id,
        parameters: parseExact(parameters.value)
      },
      template.value.node_id
    );
    validation.value = op.id;
  });
}
async function save(freeze: boolean) {
  if (!template.value) return;
  await work(async () => {
    const item = await consoleApi.save(
      {
        node_id: template.value.node_id,
        template_id: body(template.value).template_id,
        parameters: parseExact(parameters.value),
        expected_revision: draft.value?.revision ?? null,
        freeze_operation_id: freeze ? validation.value : null
      },
      draft.value?.id
    );
    store.mergeResource(item);
    draft.value = item.frozen ? null : item;
    ElMessage.success(freeze ? "参数已冻结" : "草稿已保存");
  });
}
async function editVersion(item: Resource, copy: boolean) {
  selectedTemplate.value = `${item.node_id}-template-${body(item).template_id}`;
  await nextTick();
  parameters.value = pretty(body(item).parameters);
  validation.value = "";
  draft.value = copy ? null : item;
}
async function control(item: Resource, kind: "pause" | "resume" | "stop") {
  const state = body(item);
  await ElMessageBox.confirm(
    `账户 ${state.account}，品种 ${state.instrument}，代次 ${state.generation}，停止方式 ${state.stop_action ?? "由引擎确认"}。操作以实际回执为准。`,
    "确认操作",
    { type: "warning" }
  );
  await action(
    { kind, run_id: item.id, generation: state.generation },
    item.node_id
  );
}
watch(template, item => {
  if (item) {
    parameters.value = pretty(body(item).parameters);
    validation.value = "";
    draft.value = null;
  }
});
watch(
  () => store.templates,
  items => {
    if (!selectedTemplate.value && items.length)
      selectedTemplate.value = items[0].id;
  },
  { immediate: true }
);
onMounted(() => {
  store.start().catch(error => {
    store.error = error.message;
  });
});
</script>

<template>
  <div class="console-page">
    <div style="float: right; margin: 8px 0 8px 16px"><HistoryDrawer /></div>
    <header class="page-heading">
      <div>
        <p class="eyebrow">TITAN / CONTROL</p>
        <h1>{{ route.meta.title }}</h1>
        <p>以执行回执和业务事实确认状态</p>
      </div>
      <div>
        <el-tag
          :type="store.connection === 'connected' ? 'success' : 'warning'"
          >{{
            store.connection === "connected" ? "实时连接" : "观察受限 / 重同步"
          }}</el-tag
        ><el-button class="refresh" @click="work(() => store.refresh())"
          >刷新</el-button
        >
      </div>
    </header>
    <el-alert
      v-if="store.error"
      :title="store.error"
      type="warning"
      show-icon
      @close="store.error = ''"
    />
    <el-alert
      v-if="!store.writable"
      title="当前为只读权限，写操作由服务端统一授权。"
      type="info"
      :closable="false"
    />
    <el-card v-if="store.pending.length" class="panel"
      ><template #header>受理结果待确认</template>
      <div
        v-for="item in store.pending"
        :key="item.idempotency_key"
        class="pending-row"
      >
        <code>{{ item.idempotency_key }}</code
        ><span>{{ item.action.kind }}</span
        ><el-button
          :disabled="!store.writable || busy"
          @click="work(() => store.retry(item))"
          >沿用原键查询 / 重试</el-button
        >
      </div></el-card
    >

    <template v-if="section === 'strategies'">
      <el-card class="panel"
        ><template #header>策略模板与参数</template>
        <el-select
          v-model="selectedTemplate"
          placeholder="选择节点已登记的模板"
          style="width: 100%"
          ><el-option
            v-for="item in store.templates"
            :key="item.id"
            :value="item.id"
            :label="item.node_id + ' / ' + body(item).template_id"
        /></el-select>
        <el-empty
          v-if="!store.templates.length"
          description="尚未收到已授权节点的模板目录"
        />
        <template v-if="template">
          <p class="source">源摘要 {{ body(template).source_digest }}</p>
          <el-form label-position="top" class="parameter-grid"
            ><el-form-item
              v-for="(field, key) in schema"
              :key="key"
              :label="
                String(key) +
                ((field as any).description
                  ? ' · ' + (field as any).description
                  : '')
              "
              ><el-select
                v-if="(field as any).enum"
                :model-value="templateValues[key]"
                @change="
                  value =>
                    setParameter(
                      String(key),
                      String(value),
                      (field as any).type
                    )
                "
                ><el-option
                  v-for="value in (field as any).enum"
                  :key="String(value)"
                  :value="value" /></el-select
              ><el-input
                v-else
                :model-value="scalarText(templateValues[key])"
                @change="
                  value => setParameter(String(key), value, (field as any).type)
                " /></el-form-item
          ></el-form>
          <el-input
            v-model="parameters"
            type="textarea"
            :rows="14"
            aria-label="参数 JSON"
            @input="validation = ''"
          />
          <div class="actions">
            <el-button :disabled="!store.writable || busy" @click="save(false)"
              >保存草稿</el-button
            ><el-button
              type="primary"
              :disabled="!store.writable || busy"
              @click="validate"
              >校验参数</el-button
            ><el-button
              :disabled="
                !store.writable || busy || describeResult?.state !== 'succeeded'
              "
              @click="save(true)"
              >冻结版本</el-button
            ><el-tag v-if="describeResult">{{
              labels[describeResult.state]
            }}</el-tag>
          </div>
        </template>
      </el-card>
      <el-card class="panel"
        ><template #header>参数版本</template>
        <div class="actions">
          <el-select
            v-model="comparison"
            multiple
            :multiple-limit="2"
            placeholder="选择两个版本比较输入"
            style="width: 100%"
            ><el-option
              v-for="item in store.versions"
              :key="item.id"
              :value="item.id"
              :label="item.id"
          /></el-select>
        </div>
        <el-table v-if="comparison.length === 2" :data="differences"
          ><el-table-column prop="key" label="参数" /><el-table-column
            prop="left"
            label="版本一" /><el-table-column
            prop="right"
            label="版本二" /></el-table
        ><el-table :data="store.versions" @row-click="item => (detail = item)"
          ><el-table-column
            prop="id"
            label="版本"
            min-width="260"
            show-overflow-tooltip
          /><el-table-column prop="node_id" label="节点" /><el-table-column
            label="状态"
            ><template #default="{ row }">{{
              row.frozen ? "已冻结" : "草稿"
            }}</template></el-table-column
          ><el-table-column label="操作"
            ><template #default="{ row }"
              ><el-button
                :disabled="!row.frozen || !store.writable || busy"
                @click.stop="
                  action({ kind: 'build', version_id: row.id }, row.node_id)
                "
                >构建</el-button
              ><el-button @click.stop="editVersion(row, true)"
                >复制参数</el-button
              ><el-button
                v-if="!row.frozen"
                :disabled="!store.writable || busy"
                @click.stop="editVersion(row, false)"
                >编辑草稿</el-button
              ></template
            ></el-table-column
          ></el-table
        ></el-card
      >
    </template>

    <template v-if="section === 'artifacts'">
      <el-card class="panel"
        ><template #header>构建制品</template
        ><el-table :data="store.artifacts" @row-click="item => (detail = item)"
          ><el-table-column
            prop="id"
            label="制品"
            min-width="250"
            show-overflow-tooltip
          /><el-table-column prop="node_id" label="节点" /><el-table-column
            label="摘要"
            min-width="250"
            show-overflow-tooltip
            ><template #default="{ row }">{{
              body(row).artifact_digest
            }}</template></el-table-column
          ><el-table-column label="操作"
            ><template #default="{ row }"
              ><el-link
                :href="
                  consoleApi.file(body(row).operation_id, 'strategy.titan')
                "
                target="_blank"
                >下载</el-link
              ></template
            ></el-table-column
          ></el-table
        ><el-empty
          v-if="!store.artifacts.length"
          description="在策略参数页冻结版本后构建"
      /></el-card>
    </template>

    <template v-if="section === 'backtests'">
      <el-card v-if="activeResult" class="panel"
        ><template #header
          ><div class="actions">
            <span>引擎权威结果</span
            ><el-select
              v-model="selectedResult"
              placeholder="选择结果"
              style="width: 320px"
              ><el-option
                v-for="item in store.backtests"
                :key="item.id"
                :value="item.id"
                :label="body(item).dataset_id + ' / ' + item.id.slice(-12)"
            /></el-select></div></template
        ><el-alert
          v-if="body(activeResult).business_success === false"
          title="该回测未成功完成，保留的指标不能表示已清仓。"
          type="error"
          :closable="false" />
        <div class="metric-cards">
          <div v-for="[label, key] in metricCards" :key="key">
            <small>{{ label }}</small
            ><strong
              :title="String(body(activeResult).metrics[key] ?? '缺失')"
              >{{ metricText(body(activeResult).metrics[key]) }}</strong
            >
          </div>
        </div>
        <ResultChart :operation-id="body(activeResult).operation_id"
      /></el-card>
      <el-card class="panel"
        ><template #header>历史回测</template>
        <div class="backtest-form">
          <el-select v-model="selectedArtifact" placeholder="选择制品"
            ><el-option
              v-for="item in store.artifacts"
              :key="item.id"
              :value="item.id"
              :label="item.id" /></el-select
          ><el-select v-model="selectedDataset" placeholder="选择历史场景"
            ><el-option
              v-for="item in store.datasets"
              :key="item.id"
              :value="item.id"
              :label="
                body(item).regime + ' / ' + body(item).dataset_id
              " /></el-select
          ><el-switch v-model="stress" active-text="压力执行模型" /><el-button
            :disabled="!store.writable || busy || !selectedDataset"
            @click="
              action(
                {
                  kind: 'prepare',
                  dataset_id: body(
                    store.datasets.find(i => i.id === selectedDataset)
                  ).dataset_id
                },
                store.datasets.find(i => i.id === selectedDataset).node_id
              )
            "
            >准备数据</el-button
          ><el-button
            type="primary"
            :disabled="
              !store.writable || busy || !selectedArtifact || !selectedDataset
            "
            @click="
              action(
                {
                  kind: 'backtest',
                  artifact_id: selectedArtifact,
                  dataset_id: body(
                    store.datasets.find(i => i.id === selectedDataset)
                  ).dataset_id,
                  stress
                },
                store.artifacts.find(i => i.id === selectedArtifact).node_id
              )
            "
            >发起回测</el-button
          >
        </div></el-card
      >
      <el-card class="panel"
        ><template #header>回测结果</template
        ><el-table :data="store.backtests" @row-click="item => (detail = item)"
          ><el-table-column
            prop="id"
            label="结果"
            min-width="220"
            show-overflow-tooltip
          /><el-table-column label="场景"
            ><template #default="{ row }">{{
              body(row).dataset_id
            }}</template></el-table-column
          ><el-table-column label="完成状态"
            ><template #default="{ row }">{{
              body(row).business_success ? "成功" : "失败，检查残留"
            }}</template></el-table-column
          ><el-table-column label="净收益 USDT"
            ><template #default="{ row }">{{
              body(row).metrics.net_pnl_usdt ?? "缺失"
            }}</template></el-table-column
          ><el-table-column label="操作"
            ><template #default="{ row }"
              ><el-button
                :disabled="!store.writable || busy"
                @click.stop="
                  action({ kind: 'report', backtest_id: row.id }, row.node_id)
                "
                >生成报告</el-button
              ><el-link
                :href="
                  consoleApi.file(body(row).operation_id, 'history/result.json')
                "
                target="_blank"
                >原始结果</el-link
              ></template
            ></el-table-column
          ></el-table
        >
        <div
          v-for="item in store.resources.filter(i => i.kind === 'report')"
          :key="item.id"
          class="actions"
        >
          <el-button
            @click="
              report = consoleApi.file(body(item).operation_id, 'report.html')
            "
            >查看报告 {{ item.id.slice(-12) }}</el-button
          >
        </div></el-card
      >
    </template>

    <template v-if="section === 'runs' || section === 'accounts'">
      <el-card class="panel"
        ><template #header>{{
          section === "runs" ? "运行实例" : "账户风险"
        }}</template
        ><el-table
          :data="section === 'runs' ? store.runs : store.accounts"
          @row-click="item => (detail = item)"
          ><el-table-column
            prop="id"
            label="身份"
            min-width="240"
            show-overflow-tooltip
          /><el-table-column label="账户"
            ><template #default="{ row }">{{
              body(row).account
            }}</template></el-table-column
          ><el-table-column label="实际状态"
            ><template #default="{ row }">{{
              body(row).lifecycle ?? body(row).completeness ?? "查看来源事实"
            }}</template></el-table-column
          ><el-table-column
            v-if="section === 'runs'"
            label="控制"
            min-width="240"
            ><template #default="{ row }"
              ><el-button
                v-for="kind in body(row).allowed_actions ?? []"
                :key="kind"
                :disabled="!store.writable || busy"
                @click.stop="control(row, kind)"
                >{{
                  { pause: "暂停", resume: "恢复运行", stop: "停止" }[kind] ??
                  kind
                }}</el-button
              ></template
            ></el-table-column
          ></el-table
        ><el-empty
          v-if="
            !(section === 'runs' ? store.runs.length : store.accounts.length)
          "
          description="尚无已授权且可确认来源的业务快照；缺失不表示零仓位"
      /></el-card>
    </template>

    <el-card class="panel"
      ><template #header>操作记录</template
      ><el-table
        :data="store.operations"
        max-height="420"
        @row-click="item => (detail = item)"
        ><el-table-column
          prop="id"
          label="操作 ID"
          min-width="250"
          show-overflow-tooltip
        /><el-table-column label="类型"
          ><template #default="{ row }">{{
            row.input.kind
          }}</template></el-table-column
        ><el-table-column prop="node_id" label="节点" /><el-table-column
          label="状态"
          ><template #default="{ row }"
            ><el-tag
              :type="
                row.state === 'succeeded'
                  ? 'success'
                  : row.state === 'failed'
                    ? 'danger'
                    : 'info'
              "
              >{{ labels[row.state] }}</el-tag
            ></template
          ></el-table-column
        ><el-table-column label="操作"
          ><template #default="{ row }"
            ><el-button
              v-if="['pending', 'running', 'unknown'].includes(row.state)"
              :disabled="!store.writable || busy"
              @click.stop="work(() => consoleApi.cancel(row.id))"
              >请求取消</el-button
            ><el-link
              :href="consoleApi.file(row.id, 'worker.log')"
              target="_blank"
              >日志</el-link
            ></template
          ></el-table-column
        ></el-table
      ></el-card
    >
    <el-drawer
      :model-value="!!detail"
      title="来源与实际结果"
      size="min(900px,90vw)"
      @close="detail = null"
    >
      <pre>{{
        pretty(
          detail &&
            ("input" in detail
              ? (store.operations.find(item => item.id === detail.id) ?? detail)
              : (store.resources.find(item => item.id === detail.id) ?? detail))
        )
      }}</pre>
    </el-drawer>
    <el-dialog
      :model-value="!!report"
      title="已校验报告"
      width="90vw"
      @close="report = ''"
      ><iframe :src="report" sandbox="" class="report-frame"
    /></el-dialog>
  </div>
</template>

<style scoped>
.metric-cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
  gap: 20px;
  margin: 12px 0 28px;
}
.metric-cards small {
  display: block;
  color: var(--el-text-color-secondary);
  margin-bottom: 8px;
}
.metric-cards strong {
  font-size: 18px;
  overflow-wrap: anywhere;
  font-variant-numeric: tabular-nums;
}
.console-page {
  padding: 8px 12px 40px;
}
.page-heading {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
}
.page-heading h1 {
  font-size: 28px;
  font-weight: 650;
  margin: 6px 0;
}
.page-heading p {
  color: var(--el-text-color-secondary);
}
.eyebrow {
  font-size: 11px;
  letter-spacing: 0.16em;
}
.panel {
  margin: 18px 0;
}
.refresh {
  margin-left: 12px;
}
.source {
  font: 11px monospace;
  overflow-wrap: anywhere;
  margin: 14px 0;
  color: var(--el-text-color-secondary);
}
.actions,
.pending-row,
.backtest-form {
  display: flex;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
  margin-top: 16px;
}
.backtest-form .el-select {
  width: 260px;
}
.parameter-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 0 20px;
  margin-top: 16px;
}
.report-frame {
  width: 100%;
  height: 75vh;
  border: 0;
}
pre {
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  font: 12px/1.7 monospace;
}
</style>
