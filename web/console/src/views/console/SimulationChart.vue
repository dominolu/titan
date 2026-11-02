<script setup lang="ts">
import { computed } from "vue";
import { decimal } from "./run-view";
const props = defineProps<{ values: number[]; mode: "backtest" | "live" }>();
const bounds = computed(() => {
  const low = Math.min(...props.values),
    high = Math.max(...props.values);
  const padding = Math.max((high - low) * 0.15, 10);
  return { low: low - padding, high: high + padding };
});
const points = computed(() =>
  props.values.map((value, index) => ({
    x: 82 + (index * 770) / Math.max(props.values.length - 1, 1),
    y:
      25 +
      ((bounds.value.high - value) / (bounds.value.high - bounds.value.low)) *
        190,
    value
  }))
);
const line = computed(() => points.value.map(p => `${p.x},${p.y}`).join(" "));
const area = computed(() => `82,215 ${line.value} 852,215`);
</script>
<template>
  <div class="simulation-chart">
    <svg viewBox="0 0 900 255" role="img" aria-label="模拟权益曲线，单位 USDT">
      <title>模拟权益曲线，仅用于演示</title>
      <g v-for="index in 5" :key="index">
        <line
          x1="82"
          x2="852"
          :y1="25 + (index - 1) * 47.5"
          :y2="25 + (index - 1) * 47.5"
          stroke="#e8edef"
        />
        <text
          x="72"
          :y="29 + (index - 1) * 47.5"
          text-anchor="end"
          fill="#718092"
          font-size="11"
        >
          {{
            decimal(
              bounds.high - ((index - 1) * (bounds.high - bounds.low)) / 4,
              0
            )
          }}
        </text>
      </g>
      <polygon :points="area" fill="#e7f3ef" />
      <polyline
        :points="line"
        fill="none"
        stroke="#0a8375"
        stroke-width="3"
        stroke-linejoin="round"
      />
      <circle
        v-for="(point, index) in points"
        :key="index"
        :cx="point.x"
        :cy="point.y"
        r="4"
        fill="#0a8375"
        stroke="white"
        stroke-width="2"
      >
        <title>模拟采样 {{ index + 1 }}：{{ decimal(point.value) }} USDT</title>
      </circle>
      <text x="82" y="242" fill="#718092" font-size="11">
        {{ mode === "backtest" ? "2026-10-25" : "模拟运行开始" }}
      </text>
      <text x="852" y="242" text-anchor="end" fill="#718092" font-size="11">
        {{ mode === "backtest" ? "2026-11-01" : "静态示例快照" }}
      </text>
    </svg>
    <p class="subtle">手工构造的模拟采样点；收益与回撤仅对本示例曲线计算。</p>
  </div>
</template>
<style scoped>
.simulation-chart {
  padding: 16px 20px;
}
svg {
  display: block;
  width: 100%;
  min-height: 150px;
}
</style>
