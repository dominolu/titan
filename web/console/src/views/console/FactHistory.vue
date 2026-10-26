<script setup lang="ts">
import { ref, toRef } from "vue";
import type { Resource } from "@/api/console";
import { useFactHistory } from "./use-fact-history";
import { object, raw, pretty, time } from "./run-view";
const props = defineProps<{ item?: Resource }>();
const kind = ref("fill");
const types = [
  ["fill", "独立成交"],
  ["ledger", "交易所账单"],
  ["order", "订单"],
  ["position", "持仓"],
  ["balance", "余额"],
  ["equity_sample", "权益采样"],
  ["funding", "资金费"],
  ["cashflow", "资金流"],
  ["stream", "流状态"]
];
const { items, provenance, offset, pending, error, load } = useFactHistory(
  toRef(props, "item"),
  kind
);
const fact = (entry: any) => object(entry.body ?? entry);
</script>
<template>
  <section class="console-card fact-history">
    <div class="card-heading">
      <div>
        <h2>平台事实历史</h2>
        <p class="subtle">按原来源、单位与记录范围展示</p>
      </div>
      <div class="toolbar">
        <el-select v-model="kind" aria-label="事实类型" style="width: 150px"
          ><el-option
            v-for="[value, label] in types"
            :key="value"
            :value="value"
            :label="label" /></el-select
        ><el-button :loading="pending" @click="load(0)">查询</el-button>
      </div>
    </div>
    <p v-if="error" class="notice warning">{{ error }}</p>
    <div v-if="!pending && !items.length" class="compact-empty">
      该来源未记录此类事实；不代表交易所完整历史为空。
    </div>
    <details
      v-for="(entry, index) in items"
      :key="offset + index"
      class="fact-entry"
    >
      <summary>
        <span class="unit-tag">{{ types.find(t => t[0] === kind)?.[1] }}</span
        ><span>{{
          time(
            fact(entry).captured_at_ns ??
              fact(entry).exchange_ts_ns ??
              fact(entry).received_at_ns
          )
        }}</span
        ><span>{{
          raw(
            fact(entry).instrument ??
              fact(entry).asset_id ??
              fact(entry).currency
          )
        }}</span
        ><span>记录 {{ offset + index + 1 }}</span>
      </summary>
      <pre>{{ pretty(entry) }}</pre>
    </details>
    <div class="table-footer">
      <div class="toolbar">
        <el-button
          :disabled="pending || offset === 0"
          @click="load(Math.max(0, offset - 20))"
          >上一页</el-button
        ><span>偏移 {{ offset }}</span
        ><el-button
          :disabled="pending || items.length < 20"
          @click="load(offset + 20)"
          >下一页</el-button
        >
      </div>
      <span>每页最多 20 条</span>
    </div>
    <details class="secondary-details">
      <summary>查看记录范围与来源</summary>
      <pre>{{ pretty(provenance) }}</pre>
    </details>
  </section>
</template>
