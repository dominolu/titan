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
const route = useRoute(),
  router = useRouter();
const scope = provideStrategy(String(route.params.strategyId));
const tabs = [
  ["parameters", "版本与参数"],
  ["versions", "策略版本"],
  ["backtests", "回测与报告"],
  ["live", "策略实盘"]
];
const active = computed(() =>
  tabs.some(t => t[0] === route.query.tab)
    ? String(route.query.tab)
    : "parameters"
);
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
  <router-link to="/strategies" class="back-link"
    ><ConsoleIcon name="back" />全部策略</router-link
  >
  <section class="page-title">
    <div>
      <p class="eyebrow">STRATEGY / WORKSPACE</p>
      <h1>{{ scope.name.value }}</h1>
      <p>固定版本与实际运行分别管理，参数变更不影响已运行实例</p>
    </div>
    <span class="unit-tag">策略详情</span>
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
    <KeepAlive><component :is="component" /></KeepAlive>
  </div>
  <div class="section-gap">
    <OperationsPanel :items="scope.operations.value" />
  </div>
</template>
