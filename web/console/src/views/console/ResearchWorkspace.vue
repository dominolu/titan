<script setup lang="ts">
import { computed, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { provideResearch } from "./use-research";
import StrategyParameters from "./StrategyParameters.vue";
import ResearchArtifacts from "./ResearchArtifacts.vue";
import HistoricalBacktests from "./HistoricalBacktests.vue";
import StrategyLive from "./StrategyLive.vue";
import ConsoleIcon from "./ConsoleIcon.vue";
import OperationsPanel from "./OperationsPanel.vue";
const route = useRoute(),
  router = useRouter();
const scope = provideResearch(String(route.params.researchId));
const tabs = [
  ["parameters", "版本与参数"],
  ["artifacts", "构建记录"],
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
      artifacts: ResearchArtifacts,
      backtests: HistoricalBacktests,
      live: StrategyLive
    })[active.value]
);
watch(active, () => {
  document.querySelector(".console-workspace")?.scrollTo({ top: 0 });
});
</script>
<template>
  <router-link to="/research" class="back-link"
    ><ConsoleIcon name="back" />全部策略</router-link
  >
  <section class="page-title">
    <div>
      <p class="eyebrow">STRATEGY / RESEARCH WORKSPACE</p>
      <h1>{{ scope.name.value }}</h1>
      <p>固定版本与实际运行分别管理，研究参数变更不影响已运行实例</p>
    </div>
    <span class="unit-tag">策略研究工作区</span>
  </section>
  <div v-if="scope.id === 'unassigned'" class="notice warning">
    这些历史记录缺少可证明的策略归属，保留原始关联与操作入口，不会自动归入其他策略。
  </div>
  <nav class="detail-tabs research-tabs" aria-label="策略研究环节">
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
    正在按策略身份加载研究与实盘记录…
  </p>
  <div v-else class="research-workspace">
    <KeepAlive><component :is="component" /></KeepAlive>
  </div>
  <div class="section-gap">
    <OperationsPanel :items="scope.operations.value" />
  </div>
</template>
