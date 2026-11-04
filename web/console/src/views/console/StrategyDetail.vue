<script setup lang="ts">
import { computed, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { provideStrategy } from "./use-strategy";
import StrategyParameters from "./StrategyParameters.vue";
import StrategyVersions from "./StrategyVersions.vue";
import HistoricalBacktests from "./HistoricalBacktests.vue";
import StrategyLive from "./StrategyLive.vue";
import ConsoleIcon from "./ConsoleIcon.vue";
import OperationsPanel from "./OperationsPanel.vue";
import SimulationExamples from "./SimulationExamples.vue";
import { simulationForStrategy } from "./simulation-examples";
const route = useRoute(),
  router = useRouter();
const scope = provideStrategy(String(route.params.strategyId));
const project = computed(() =>
  route.query.project === "live" ||
  (!route.query.project && route.query.tab === "live")
    ? "live"
    : "backtest"
);
const tabs = computed(() =>
  project.value === "live"
    ? [
        ["parameters", "版本与参数"],
        ["versions", "策略版本"],
        ["live", "实盘部署与运行"]
      ]
    : [
        ["parameters", "版本与参数"],
        ["versions", "策略版本"],
        ["backtests", "回测与报告"]
      ]
);
const active = computed(() =>
  tabs.value.some(t => t[0] === route.query.tab)
    ? String(route.query.tab)
    : "parameters"
);
const example = computed(() =>
  simulationForStrategy(scope.id, String(scope.name.value))
);
const exampleTab = computed(
  () => active.value === "live" || active.value === "backtests"
);
const showingExample = computed(
  () => !!example.value && exampleTab.value && route.query.demo !== "0"
);
function setExample(show: boolean) {
  router.replace({
    path: route.path,
    query: { ...route.query, demo: show ? "1" : "0" }
  });
}
const component = computed(
  () =>
    ({
      parameters: StrategyParameters,
      versions: StrategyVersions,
      backtests: HistoricalBacktests,
      live: StrategyLive
    })[active.value]
);
watch(active, () => {
  document.querySelector(".console-workspace")?.scrollTo({ top: 0 });
});
</script>
<template>
  <router-link :to="'/strategies/' + project" class="back-link"
    ><ConsoleIcon name="back" />{{
      project === "live" ? "实盘策略" : "策略回测"
    }}</router-link
  >
  <section class="page-title">
    <div>
      <p class="eyebrow">STRATEGY / WORKSPACE</p>
      <h1>{{ scope.name.value }}</h1>
      <p>固定版本与实际运行分别管理，参数变更不影响已运行实例</p>
    </div>
    <div class="toolbar">
      <span class="unit-tag">{{
        project === "live" ? "实盘策略" : "策略回测"
      }}</span
      ><router-link
        class="text-link"
        :to="{
          path: route.path,
          query: {
            project: project === 'live' ? 'backtest' : 'live',
            tab: project === 'live' ? 'backtests' : 'live'
          }
        }"
        >{{
          project === "live" ? "前往策略回测 →" : "前往实盘策略 →"
        }}</router-link
      >
    </div>
  </section>
  <div v-if="scope.id === 'unassigned'" class="notice warning">
    这些历史记录缺少可证明的策略归属，保留原始关联与操作入口，不会自动归入其他策略。
  </div>
  <nav class="detail-tabs strategy-tabs" aria-label="策略管理环节">
    <button
      v-for="[value, label] in tabs"
      :key="value"
      :class="{ active: active === value }"
      :aria-pressed="active === value"
      @click="router.replace(scope.path(value))"
    >
      {{ label }}
    </button>
  </nav>
  <p v-if="!scope.ready.value" class="notice">
    正在按策略身份加载参数、版本与运行记录…
  </p>
  <div v-else class="strategy-workspace">
    <nav v-if="example && exampleTab" class="detail-tabs" aria-label="数据来源">
      <button
        :class="{ active: showingExample }"
        :aria-pressed="showingExample"
        @click="setExample(true)"
      >
        模拟示例
      </button>
      <button
        :class="{ active: !showingExample }"
        :aria-pressed="!showingExample"
        @click="setExample(false)"
      >
        真实记录
      </button>
    </nav>
    <SimulationExamples
      v-if="showingExample && example"
      :example="example"
      :mode="active === 'live' ? 'live' : 'backtest'"
    />
    <KeepAlive v-else><component :is="component" /></KeepAlive>
  </div>
  <div v-if="!showingExample" class="section-gap">
    <OperationsPanel :items="scope.operations.value" />
  </div>
</template>
