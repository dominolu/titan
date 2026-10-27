<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useConsoleStore } from "@/store/modules/console";
import { consoleApi } from "@/api/console";
import { body, raw, decimal } from "./run-view";
import { useConsoleWork } from "./use-console-work";
import ConsoleIcon from "./ConsoleIcon.vue";
import ConsoleDetails from "./ConsoleDetails.vue";
import { useStrategy } from "./use-strategy";
import StrategyHistoryPager from "./StrategyHistoryPager.vue";
import { useRoute } from "vue-router";
import ResultChart from "./ResultChart.vue";
const store = useConsoleStore();
const scope = useStrategy(),
  route = useRoute();
const strategyVersions = computed(() => scope.options("strategy_version"));
const backtests = computed(() => scope.options("backtest"));
const { busy, detail, submit } = useConsoleWork();
const selectedStrategyVersion = ref(""),
  selectedDataset = ref(""),
  selectedResult = ref("");
const linkError = ref("");
const stress = ref(false),
  report = ref(""),
  query = ref(""),
  filter = ref("all");
const strategyVersion = computed(() =>
  scope.lookup("strategy_version", selectedStrategyVersion.value)
);
const dataset = computed(() =>
  store.datasets.find(row => row.id === selectedDataset.value)
);
watch(selectedDataset, id => {
  const generation = store.sessionGeneration;
  scope.pinCatalog("backtest-catalog", [id]).catch(error => {
    if (generation === store.sessionGeneration) store.error = error.message;
  });
});
const activeResult = computed(() =>
  scope.lookup("backtest", selectedResult.value)
);
const reports = computed(() => scope.resources("report"));
const metrics = [
  ["净收益", "net_pnl_usdt", "USDT"],
  ["收益率", "net_return_pct", "%"],
  ["最大回撤", "max_drawdown_pct", "%"],
  ["最大实际敞口", "max_position_notional_usdt", "USDT"],
  ["含在途敞口上界", "max_pending_bound_notional_usdt", "USDT"],
  ["最终持仓", "final_position_btc", "BTC"]
];
const shown = computed(() =>
  scope
    .resources("backtest")
    .filter(
      row =>
        (filter.value === "all" ||
          body(row).business_success === (filter.value === "succeeded")) &&
        `${row.id} ${body(row).dataset_id} ${row.node_id}`
          .toLowerCase()
          .includes(query.value.toLowerCase())
    )
);
watch(
  () => backtests.value,
  items => {
    if (!selectedResult.value && !route.query.result && items.length)
      selectedResult.value = items[0].id;
  },
  { immediate: true }
);
watch(
  () => [
    selectedStrategyVersion.value,
    selectedResult.value,
    detail.value && "kind" in detail.value ? detail.value.id : ""
  ],
  ids => {
    const generation = store.sessionGeneration;
    scope.pin("backtests", ids.filter(Boolean) as string[]).catch(error => {
      if (generation === store.sessionGeneration) store.error = error.message;
    });
  },
  { immediate: true }
);
watch(
  () => [route.query.strategy_version_id, route.query.result],
  async () => {
    const strategyVersionId = String(route.query.strategy_version_id ?? ""),
      resultId = String(route.query.result ?? "");
    const generation = store.sessionGeneration;
    linkError.value = "";
    if (strategyVersionId) selectedStrategyVersion.value = "";
    if (resultId) selectedResult.value = "";
    try {
      await scope.pin(
        "backtest-link",
        [strategyVersionId, resultId].filter(Boolean)
      );
      if (
        generation !== store.sessionGeneration ||
        strategyVersionId !== String(route.query.strategy_version_id ?? "") ||
        resultId !== String(route.query.result ?? "")
      )
        return;
      if (strategyVersionId) selectedStrategyVersion.value = strategyVersionId;
      if (resultId) selectedResult.value = resultId;
    } catch (failure) {
      if (
        generation === store.sessionGeneration &&
        strategyVersionId === String(route.query.strategy_version_id ?? "") &&
        resultId === String(route.query.result ?? "")
      )
        linkError.value = (failure as Error).message;
    }
  },
  { immediate: true }
);
async function prepare() {
  if (dataset.value)
    await submit(
      { kind: "prepare", dataset_id: body(dataset.value).dataset_id },
      dataset.value.node_id
    );
}
async function replay() {
  if (
    !strategyVersion.value ||
    !dataset.value ||
    strategyVersion.value.node_id !== dataset.value.node_id
  )
    return;
  await submit(
    {
      kind: "backtest",
      strategy_version_id: strategyVersion.value.id,
      dataset_id: body(dataset.value).dataset_id,
      stress: stress.value
    },
    strategyVersion.value.node_id
  );
}
</script>
<template>
  <p v-if="linkError" class="notice warning">{{ linkError }}</p>
  <section class="console-card backtest-workspace">
    <div class="card-heading">
      <div>
        <h2>发起历史回测</h2>
        <p>收益与风险使用引擎全量指标，曲线仅用于采样展示</p>
      </div>
      <ConsoleIcon name="chart" />
    </div>
    <div class="card-body backtest-form">
      <div class="form-field">
        <label class="field-label">策略版本</label
        ><el-select
          v-model="selectedStrategyVersion"
          aria-label="回测策略版本"
          placeholder="选择策略版本"
          ><el-option
            v-for="item in strategyVersions"
            :key="item.id"
            :value="item.id"
            :label="item.id"
        /></el-select>
      </div>
      <div class="form-field">
        <label class="field-label">历史场景</label
        ><el-select
          v-model="selectedDataset"
          aria-label="历史场景"
          placeholder="选择历史场景"
          ><el-option
            v-for="item in store.datasets"
            :key="item.id"
            :value="item.id"
            :label="body(item).regime + ' / ' + body(item).dataset_id"
        /></el-select>
      </div>
      <div class="form-field">
        <span class="field-label">执行模型</span
        ><el-switch v-model="stress" active-text="压力执行模型" />
      </div>
      <div class="toolbar">
        <el-button
          :disabled="!store.writable || busy || !dataset"
          @click="prepare"
          >准备数据</el-button
        ><el-button
          type="primary"
          :disabled="
            !store.writable ||
            busy ||
            !strategyVersion ||
            !dataset ||
            strategyVersion.node_id !== dataset.node_id
          "
          @click="replay"
          >发起回测</el-button
        >
      </div>
    </div>
    <div class="table-footer">
      <span>{{
        strategyVersion &&
        dataset &&
        strategyVersion.node_id !== dataset.node_id
          ? "所选策略版本与数据来源节点不同，请选择同一节点的输入。"
          : "固定输入身份与数据准备回执由节点校验；未成功完成的回测仍须检查残留。"
      }}</span
      ><button v-if="dataset" class="text-link" @click="detail = dataset">
        查看数据来源
      </button>
    </div>
  </section>
  <section class="section-gap result-workspace">
    <div class="result-heading">
      <div>
        <p class="eyebrow">ENGINE RESULTS</p>
        <h2>引擎权威结果</h2>
      </div>
      <el-select
        v-model="selectedResult"
        aria-label="回测结果"
        placeholder="选择结果"
        style="width: 320px"
        ><el-option
          v-for="item in backtests"
          :key="item.id"
          :value="item.id"
          :label="body(item).dataset_id + ' / ' + item.id.slice(-12)"
      /></el-select>
    </div>
    <template v-if="activeResult"
      ><div
        v-if="body(activeResult).business_success !== true"
        class="notice warning"
      >
        该回测未成功完成，保留的指标不能表示已清仓。
      </div>
      <div class="metric-grid backtest-metrics">
        <article
          v-for="[label, key, unit] in metrics"
          :key="key"
          class="metric-card"
        >
          <p>{{ label }}</p>
          <strong class="number" :title="raw(body(activeResult).metrics?.[key])"
            >{{
              decimal(
                body(activeResult).metrics?.[key],
                key === "final_position_btc" ? 6 : 2
              )
            }}<small>{{ unit }}</small></strong
          ><span :title="raw(body(activeResult).metrics?.[key])"
            >原始值 {{ raw(body(activeResult).metrics?.[key]) }}</span
          >
        </article>
      </div>
      <section class="console-card">
        <div class="card-heading">
          <div>
            <h2>权益与风险轨迹</h2>
            <p>
              {{ body(activeResult).dataset_id }} · {{ activeResult.node_id }}
            </p>
          </div>
          <button class="text-link" @click="detail = activeResult">
            查看完整结果 →
          </button>
        </div>
        <ResultChart :operation-id="body(activeResult).operation_id" /></section
    ></template>
    <div v-else class="console-card compact-empty">
      {{
        selectedResult
          ? "所选结果当前不可用，请重新选择。"
          : "尚无回测结果；准备数据后发起回测。"
      }}
    </div>
  </section>
  <section class="console-card section-gap">
    <div class="card-heading">
      <h2>
        回测结果 <span class="subtle">{{ backtests.length }}</span>
      </h2>
      <label class="search-box"
        ><ConsoleIcon name="search" /><input
          v-model="query"
          aria-label="搜索回测结果"
          placeholder="搜索结果、场景或节点"
      /></label>
    </div>
    <div class="table-toolbar">
      <div class="view-filters">
        <button
          v-for="[value, label] in [
            ['all', '全部'],
            ['succeeded', '成功完成'],
            ['failed', '未成功完成']
          ]"
          :key="value"
          :class="{ active: filter === value }"
          @click="filter = value"
        >
          {{ label }}
        </button>
      </div>
    </div>
    <div class="table-scroll">
      <table class="data-table backtest-results">
        <thead>
          <tr>
            <th>结果 / 场景</th>
            <th>完成状态</th>
            <th>净收益 USDT</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in shown" :key="row.id">
            <td>
              <button class="text-link" @click="selectedResult = row.id">
                {{ row.id }}</button
              ><span class="cell-note"
                >{{ body(row).dataset_id }} · {{ row.node_id }}</span
              >
            </td>
            <td>
              <span
                class="state-tag"
                :class="{ warning: body(row).business_success !== true }"
                >{{
                  body(row).business_success === true
                    ? "成功"
                    : "失败，检查残留"
                }}</span
              >
            </td>
            <td class="number">{{ raw(body(row).metrics?.net_pnl_usdt) }}</td>
            <td>
              <el-button
                size="small"
                :disabled="!store.writable || busy"
                @click="
                  submit({ kind: 'report', backtest_id: row.id }, row.node_id)
                "
                >生成报告</el-button
              ><a
                class="text-link"
                :href="
                  consoleApi.file(body(row).operation_id, 'history/result.json')
                "
                target="_blank"
                rel="noopener"
                >原始结果</a
              ><button class="text-link" @click="detail = row">查看来源</button
              ><router-link
                class="text-link"
                :to="
                  scope.path('live', {
                    strategy_version_id: body(row).strategy_version_id
                  })
                "
                >用于实盘</router-link
              >
            </td>
          </tr>
          <tr v-if="!shown.length">
            <td colspan="4" class="compact-empty">暂无匹配的回测结果</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
  <StrategyHistoryPager kind="backtest" />
  <section class="console-card section-gap">
    <div class="card-heading">
      <h2>
        已生成报告 <span class="subtle">{{ reports.length }}</span>
      </h2>
      <span class="subtle">保留现有报告生成与校验链路</span>
    </div>
    <div class="report-list">
      <div v-for="item in reports" :key="item.id">
        <div>
          <strong>{{ item.id }}</strong
          ><span class="cell-note">{{ item.node_id }}</span>
        </div>
        <el-button
          @click="
            report = consoleApi.file(body(item).operation_id, 'report.html')
          "
          >查看报告 {{ item.id.slice(-12) }}</el-button
        >
      </div>
      <p v-if="!reports.length" class="compact-empty">
        选择回测结果生成报告后，在此查看。
      </p>
    </div>
  </section>
  <StrategyHistoryPager kind="report" />
  <ConsoleDetails :item="detail" @close="detail = null" /><el-dialog
    :model-value="!!report"
    title="已校验报告"
    width="94vw"
    class="console-drawer"
    destroy-on-close
    @close="report = ''"
    ><iframe
      :key="report"
      :src="report"
      title="回测报告"
      sandbox=""
      class="report-frame"
  /></el-dialog>
</template>
