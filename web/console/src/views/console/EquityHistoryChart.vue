<script setup lang="ts">
import { computed, ref, watch, onMounted, onBeforeUnmount, toRef } from "vue";
import { init, use, type ECharts } from "echarts/core";
import { LineChart } from "echarts/charts";
import { GridComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import type { Resource } from "@/api/console";
import { useFactHistory } from "./use-fact-history";
import { object, rows, raw, time, pretty } from "./run-view";
use([LineChart, GridComponent, TooltipComponent, CanvasRenderer]);
const props = defineProps<{ item?: Resource; stale?: boolean }>();
const kind = ref("equity_sample"),
  currency = ref("");
const { items, provenance, pending, error, load } = useFactHistory(
  toRef(props, "item"),
  kind
);
const samples = computed(() => items.value.map(row => object(row.body ?? row)));
const currencies = computed(() => [
  ...new Set(
    samples.value.flatMap(s => rows(s.equity).map(e => String(e.currency)))
  )
]);
watch(currencies, values => {
  if (!values.includes(currency.value)) currency.value = values[0] ?? "";
});
const points = computed(() =>
  samples.value
    .flatMap(sample => {
      const equity = rows(sample.equity).find(
        e => e.currency === currency.value && e.valuation_complete === true
      );
      const ns = raw(sample.captured_at_ns),
        amount = raw(equity?.amount);
      if (!/^\d+$/.test(ns) || !/^-?\d+(\.\d+)?$/.test(amount)) return [];
      const timestamp = Number(BigInt(ns) / 1000000n),
        value = Number(amount);
      return Number.isFinite(timestamp) && Number.isFinite(value)
        ? [{ value: [timestamp, value], exact: amount, ns }]
        : [];
    })
    .sort((a, b) => a.value[0] - b.value[0])
);
const element = ref<HTMLDivElement>();
let chart: ECharts | null = null,
  resize: ResizeObserver | null = null;
let refreshTimer: ReturnType<typeof setInterval> | null = null;
function draw() {
  if (!chart) return;
  chart.setOption(
    {
      animation: false,
      grid: { left: 22, right: 80, top: 22, bottom: 30 },
      tooltip: {
        trigger: "axis",
        renderMode: "richText",
        formatter: (params: any) => {
          const point = params[0]?.data;
          return point
            ? `${time(point.ns, true)}\n${point.exact} ${currency.value}`
            : "";
        }
      },
      xAxis: {
        type: "time",
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: "#718092", hideOverlap: true }
      },
      yAxis: {
        type: "value",
        scale: true,
        position: "right",
        splitLine: { lineStyle: { color: "#edf0f3", type: "dashed" } },
        axisLabel: { color: "#718092" }
      },
      series: [
        {
          type: "line",
          name: currency.value,
          data: points.value,
          showSymbol: points.value.length === 1,
          symbolSize: 6,
          lineStyle: { width: 2, color: "#248d74" },
          itemStyle: { color: "#248d74" },
          areaStyle: { color: "#e4f2eb", opacity: 0.45 }
        }
      ]
    },
    true
  );
  chart.resize();
}
onMounted(() => {
  chart = init(element.value);
  resize = new ResizeObserver(() => chart?.resize());
  resize.observe(element.value);
  draw();
  refreshTimer = setInterval(() => {
    if (
      !pending.value &&
      (props.item?.body as any)?.available === true &&
      document.visibilityState === "visible"
    )
      load(0);
  }, 5000);
});
watch(points, draw, { flush: "post" });
onBeforeUnmount(() => {
  if (refreshTimer) clearInterval(refreshTimer);
  resize?.disconnect();
  chart?.dispose();
  chart = null;
});
</script>
<template>
  <article class="console-card equity-history">
    <div class="card-heading">
      <div>
        <h2>账户净权益 <span class="unit-tag">账户级</span></h2>
        <p class="subtle">
          {{ stale ? "最后有效记录" : "已记录采样" }} · 不归因为策略净收益
        </p>
      </div>
      <div class="toolbar">
        <el-select
          v-if="currencies.length"
          v-model="currency"
          aria-label="权益图币种"
          style="width: 95px"
          ><el-option
            v-for="entry in currencies"
            :key="entry"
            :value="entry"
            :label="entry" /></el-select
        ><el-button :loading="pending" @click="load(0)">刷新采样</el-button>
      </div>
    </div>
    <p v-if="error" class="notice warning">{{ error }}</p>
    <div
      v-show="points.length"
      ref="element"
      class="equity-canvas"
      role="img"
      :aria-label="`${currency} 账户权益采样曲线；精确值见采样明细`"
    />
    <p v-if="!points.length" class="chart-empty">
      {{
        pending
          ? "正在读取权益采样…"
          : "暂无可绘制的已记录权益采样；缺失不表示权益为零。"
      }}
    </p>
    <div class="chart-caption">
      <span><i /> 净权益 / {{ currency || "原币种" }}</span
      ><span>最近一页最多 20 条 · 图表近似，明细保留精度</span>
    </div>
    <details class="secondary-details">
      <summary>采样明细与来源</summary>
      <div class="table-scroll">
        <table class="data-table">
          <thead>
            <tr>
              <th>采样时间（本地）</th>
              <th>净权益 / {{ currency }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(point, index) in points" :key="index">
              <td>{{ time(point.ns, true) }}</td>
              <td class="number">{{ point.exact }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <pre>{{ pretty(provenance) }}</pre>
    </details>
  </article>
</template>
