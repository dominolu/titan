<script setup lang="ts">
import { computed } from "vue";
import type { Resource } from "@/api/console";
import { decimal, raw, body, rows, runView, pretty } from "./run-view";
import { useConsoleStore } from "@/store/modules/console";
import ConsoleIcon from "./ConsoleIcon.vue";
const props = defineProps<{ item?: Resource }>();
const store = useConsoleStore();
const view = computed(() =>
  runView(
    props.item,
    store.connection,
    store.resources.filter(r => r.kind === "node")
  )
);
const deployment = computed(() =>
  store.resources.find(r => r.id === view.value.state.deployment_id)
);
const limit = computed(() => body(deployment.value).max_exposure_quote);
const maximum = computed(() =>
  rows(view.value.financial.max_exposure).find(
    r =>
      r.instrument === view.value.state.instrument &&
      (!view.value.currency || r.currency === view.value.currency)
  )
);
const ratio = computed(() => {
  const amount = Number(view.value.bound),
    upper = Number(limit.value);
  return view.value.riskKnown &&
    Number.isFinite(amount) &&
    Number.isFinite(upper) &&
    upper > 0
    ? Math.min(100, Math.max(0, (amount / upper) * 100))
    : 0;
});
</script>
<template>
  <article class="console-card risk-summary">
    <div class="card-heading">
      <h2>风险与完整性</h2>
      <ConsoleIcon name="shield" />
    </div>
    <div class="risk-value number" :title="raw(view.bound)">
      {{ decimal(view.bound)
      }}<small>{{ view.currency || "计价币缺失" }}</small>
    </div>
    <p class="subtle">
      含在途敞口上界 · {{ view.riskKnown ? "引擎确认值" : "当前不可确认" }}
    </p>
    <div
      v-if="limit != null"
      class="risk-meter"
      role="img"
      :aria-label="`风险上限 ${raw(limit)}；当前上界 ${raw(view.bound)}`"
    >
      <i :style="{ width: ratio + '%' }" />
    </div>
    <p v-if="limit != null" class="risk-limit">
      配置上限 {{ raw(limit) }} {{ view.currency }} · 单实例
    </p>
    <dl class="risk-lines">
      <div>
        <dt>实际名义敞口</dt>
        <dd class="number" :title="raw(view.actual)">
          {{ decimal(view.actual) }} {{ view.currency }}
        </dd>
      </div>
      <div>
        <dt>采样最大含在途上界</dt>
        <dd class="number" :title="raw(maximum?.max_pending_bound_notional)">
          {{ decimal(maximum?.max_pending_bound_notional) }}
          {{ maximum?.currency }}
        </dd>
      </div>
    </dl>
    <p class="completeness-note">
      <ConsoleIcon name="info" />{{
        view.riskKnown
          ? "最大值来自持久化采样，不代表连续行情最大值。"
          : "观测、估值或意图证据不完整，缺失值不能按零处理。"
      }}
    </p>
    <details class="secondary-details">
      <summary>完整性原因</summary>
      <pre>{{ pretty(view.risk?.missing ?? view.image.risk?.missing) }}</pre>
    </details>
  </article>
</template>
