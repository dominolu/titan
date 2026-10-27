<script setup lang="ts">
import { ref, watch, onMounted, onBeforeUnmount } from "vue";
import { init, use, type ECharts } from "echarts/core";
import { LineChart } from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  LegendComponent
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { http } from "@/utils/http";
import { consoleApi } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
use([
  LineChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  CanvasRenderer
]);
const props = defineProps<{ operationId: string }>();
const store = useConsoleStore();
const element = ref<HTMLDivElement>();
const error = ref("");
let chart: ECharts | null = null;
let observer: ResizeObserver | null = null;
let generation = 0;
async function load() {
  const current = ++generation;
  const identity = store.sessionGeneration;
  chart?.clear();
  error.value = "";
  if (!props.operationId || !chart) return;
  try {
    const table = await http.get<{ columns: string[]; points: string[][] }>(
      consoleApi.file(props.operationId, "history/chart.json")
    );
    if (
      current !== generation ||
      identity !== store.sessionGeneration ||
      !chart
    )
      return;
    const index = (name: string) => table.columns.indexOf(name);
    chart.setOption(
      {
        animation: false,
        color: ["#0a8375", "#7a90aa", "#c9a569"],
        tooltip: { trigger: "axis" },
        legend: {
          data: ["净权益", "实际名义持仓", "含在途名义上界"],
          textStyle: { color: "#718092", fontSize: 10 }
        },
        grid: { left: 65, right: 65, top: 50, bottom: 35 },
        xAxis: { type: "time" },
        yAxis: [
          { type: "value", scale: true },
          { type: "value", scale: true }
        ],
        series: [
          ["净权益", "equity_usdt", 0],
          ["实际名义持仓", "position_notional_usdt", 1],
          ["含在途名义上界", "reserved_notional_usdt", 1]
        ].map(([name, field, axis]) => ({
          name,
          type: "line",
          yAxisIndex: axis,
          showSymbol: false,
          data: table.points.map(row => [
            Number(row[index("time_ns")]) / 1e6,
            Number(row[index(String(field))])
          ])
        }))
      },
      true
    );
    error.value = "";
  } catch (failure) {
    if (current === generation && identity === store.sessionGeneration)
      error.value = (failure as Error).message;
  }
}
onMounted(() => {
  chart = init(element.value);
  observer = new ResizeObserver(() => chart?.resize());
  observer.observe(element.value);
  load();
});
watch(() => props.operationId, load);
onBeforeUnmount(() => {
  generation++;
  observer?.disconnect();
  chart?.dispose();
  chart = null;
});
</script>
<template>
  <el-alert v-if="error" :title="error" type="warning" :closable="false" />
  <div ref="element" class="result-chart" />
  <p class="caption">
    曲线为有界采样展示，收益、最大回撤与最大敞口使用引擎全量指标。
  </p>
</template>
<style scoped>
.caption {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin: 10px 20px 18px;
}
</style>
