<script setup lang="ts">
import { computed } from "vue";
import { object, raw, decimal, time } from "./run-view";
const props = defineProps<{ observation: Record<string, any> }>();
const data = computed(() => object(props.observation.data));
const phases: Record<string, string> = {
  created: "已创建",
  warming: "准备中",
  initializing: "初始化中",
  running: "运行中",
  resetting: "重置中",
  paused: "业务暂停",
  stopping: "业务停止中",
  stopped: "业务已停止",
  error: "业务错误",
  reconciling: "对账中"
};
</script>
<template>
  <dl class="risk-lines">
    <div>
      <dt>网格业务阶段</dt>
      <dd>{{ phases[data.phase] ?? raw(data.phase) }}</dd>
    </div>
    <div>
      <dt>网格锚点 / ticks</dt>
      <dd>{{ raw(data.anchor_ticks) }}</dd>
    </div>
    <div>
      <dt>完成配对</dt>
      <dd>{{ raw(data.completed_pairs) }}</dd>
    </div>
    <div>
      <dt>配对数量 / lots</dt>
      <dd>{{ raw(data.paired_lots) }}</dd>
    </div>
    <div>
      <dt>配对毛收益 / tick·lot</dt>
      <dd>{{ decimal(data.paired_gross_tick_lots, 4) }}</dd>
    </div>
    <div>
      <dt>策略期望库存 / lots</dt>
      <dd>{{ raw(data.expected_inventory_lots) }}</dd>
    </div>
    <div>
      <dt>最后处理行情</dt>
      <dd>{{ time(data.last_market_at_ns, true) }}</dd>
    </div>
  </dl>
  <p class="subtle grid-note">
    配对毛收益未计手续费与资金费。期望库存来自策略，实际持仓由账户服务确认；ticks/lots
    使用本次运行绑定的品种步长。
  </p>
</template>
