<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useConsoleStore } from "@/store/modules/console";
import {
  body,
  decimal,
  raw,
  runView,
  scaled,
  symbolLabel,
  time
} from "./run-view";
import ConsoleIcon from "./ConsoleIcon.vue";
import EquityHistoryChart from "./EquityHistoryChart.vue";
import RiskCard from "./RiskCard.vue";
import RunAlerts from "./RunAlerts.vue";
import { useResourcePins } from "./use-resource-pins";
const store = useConsoleStore();
const pinResources = useResourcePins();
const selectedId = ref(""),
  query = ref(""),
  filter = ref("all");
const nodes = computed(() => store.resources.filter(r => r.kind === "node"));
const entries = computed(() =>
  store.runs.map(item => ({
    item,
    view: runView(item, store.connection, nodes.value)
  }))
);
watch(
  () => store.runs,
  runs => {
    if (!selectedId.value && runs.length) selectedId.value = runs[0].id;
  },
  { immediate: true }
);
const selected = computed(() =>
  store.runs.find(r => r.id === selectedId.value)
);
const view = computed(() =>
  runView(selected.value, store.connection, nodes.value)
);
const selectedEquity = computed(() =>
  view.value.equity.length === 1 &&
  view.value.equity[0].valuation_complete === true
    ? view.value.equity[0]
    : null
);
const confirmed = computed(
  () =>
    entries.value.filter(r => !r.view.stale && r.view.lifecycle === "Running")
      .length
);
const uncertain = computed(
  () => entries.value.filter(r => r.view.stale).length
);
const pending = computed(
  () =>
    store.operations.filter(op =>
      ["pending", "running", "unknown"].includes(op.state)
    ).length
);
const shown = computed(() =>
  entries.value.filter(({ item, view: v }) => {
    const matches =
      filter.value === "all" ||
      (filter.value === "unknown"
        ? v.stale
        : !v.stale && v.lifecycle === filter.value);
    return (
      matches &&
      `${item.id} ${v.state.account} ${v.state.instrument} ${v.strategy.strategy_key ?? ""}`
        .toLowerCase()
        .includes(query.value.toLowerCase())
    );
  })
);
watch(
  () => [selected.value?.id, view.value.state.deployment_id],
  async () => {
    const identity = store.sessionGeneration;
    try {
      await pinResources(
        [selected.value?.id, view.value.state.deployment_id].filter(Boolean)
      );
    } catch (failure) {
      if (identity === store.sessionGeneration)
        store.error = (failure as Error).message;
    }
  },
  { immediate: true }
);
</script>
<template>
  <section class="page-title">
    <div>
      <p class="eyebrow">WORKSPACE / OPERATIONS</p>
      <h1>运行总览</h1>
      <p>运行事实、账户风险与操作进度</p>
    </div>
    <div class="toolbar">
      <el-select
        v-if="store.runs.length"
        v-model="selectedId"
        placeholder="选择观测实例"
        aria-label="总览观测实例"
        style="width: 280px"
        ><el-option
          v-for="run in store.runs"
          :key="run.id"
          :value="run.id"
          :label="`${body(run).account} / ${body(run).instrument} / ${run.id}`" /></el-select
      ><router-link
        v-if="selected"
        :to="'/runs/' + encodeURIComponent(selected.id)"
        class="primary-link"
        >查看策略 <ConsoleIcon name="arrow"
      /></router-link>
    </div>
  </section>
  <div v-if="!store.ready" class="notice">正在同步管理快照…</div>
  <div v-if="selected && view.stale" class="notice warning">
    观测已过期或账户快照不可用；保留最后有效事实，不能据此确认当前仓位和敞口。
  </div>
  <section class="metric-grid">
    <article class="metric-card">
      <p>账户净权益 <ConsoleIcon name="monitor" /></p>
      <strong class="number" :title="raw(selectedEquity?.amount)"
        >{{ view.equity.length > 1 ? "多币种" : decimal(selectedEquity?.amount)
        }}<small>{{ selectedEquity?.currency }}</small></strong
      ><span
        >{{ selected ? raw(view.state.account) : "尚未选择来源" }} ·
        {{ view.stale ? "最后有效快照" : "账户级，不代表策略收益" }}</span
      >
    </article>
    <article class="metric-card">
      <p>已确认运行实例 <ConsoleIcon name="settings" /></p>
      <strong class="number"
        >{{ confirmed }}<small>/ {{ store.runs.length }} 个</small></strong
      ><span>{{ uncertain }} 个观测受限 · 当前已加载范围</span>
    </article>
    <article class="metric-card">
      <p>所选来源订单记录 <ConsoleIcon name="box" /></p>
      <strong class="number"
        >{{
          selected && view.snapshot.available === true
            ? view.orders.length
            : "—"
        }}<small>条</small></strong
      ><span>账户订单快照 · 包含已知终态</span>
    </article>
    <article class="metric-card amber">
      <p>未完成操作 <ConsoleIcon name="clock" /></p>
      <strong class="number">{{ pending }}<small>项</small></strong
      ><span>待执行、执行中及结果未知</span>
    </article>
  </section>
  <div v-if="selected" class="overview-split">
    <EquityHistoryChart
      :key="selected.id"
      :item="selected"
      :stale="view.stale"
    /><RiskCard :item="selected" />
  </div>
  <section class="console-card">
    <div class="card-heading">
      <h2>
        策略实例 <span class="subtle">{{ store.runs.length }}</span>
      </h2>
      <label class="search-box"
        ><ConsoleIcon name="search" /><input
          v-model="query"
          type="search"
          placeholder="搜索名称、账户或品种"
          aria-label="搜索运行实例"
      /></label>
    </div>
    <div class="table-toolbar">
      <div class="view-filters">
        <button
          v-for="[value, label] in [
            ['all', '全部'],
            ['Running', '运行中'],
            ['Paused', '已暂停'],
            ['unknown', '观测受限']
          ]"
          :key="value"
          :class="{ active: filter === value }"
          :aria-pressed="filter === value"
          @click="filter = value"
        >
          {{ label }}
        </button>
      </div>
      <span class="subtle">当前缓存范围 · 更早实例见历史查询</span>
    </div>
    <div class="table-scroll">
      <table class="data-table runs-table">
        <thead>
          <tr>
            <th>策略 / 实例</th>
            <th>运行状态</th>
            <th>持仓快照</th>
            <th>含在途风险上界</th>
            <th>采样时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="entry in shown" :key="entry.item.id">
            <td>
              <div class="instance-name">
                <span class="asset-icon">{{
                  symbolLabel(entry.view.state.instrument).slice(0, 1)
                }}</span>
                <div>
                  <router-link
                    :to="'/runs/' + encodeURIComponent(entry.item.id)"
                    >{{
                      entry.view.strategy.strategy_key ||
                      symbolLabel(entry.view.state.instrument)
                    }}</router-link
                  ><span class="cell-note" :title="entry.item.id"
                    >{{ entry.view.state.account }} · {{ entry.item.id }}</span
                  >
                </div>
              </div>
            </td>
            <td>
              <span
                class="state-tag"
                :class="{
                  warning: entry.view.stale,
                  neutral: entry.view.lifecycle !== 'Running'
                }"
                >{{ entry.view.stale ? "最后确认 · " : ""
                }}{{ entry.view.status }}</span
              ><span class="cell-note">{{
                raw(entry.view.state.process_state)
              }}</span>
            </td>
            <td class="number">
              <template v-if="entry.view.positions.length === 1"
                >{{
                  raw(
                    scaled(
                      entry.view.positions[0].quantity_lots,
                      entry.view.instrument?.quantity_lot
                    )
                  )
                }}<span class="cell-note"
                  >{{ symbolLabel(entry.view.state.instrument) }} ·
                  {{ entry.view.stale ? "最后确认" : "账户事实" }}</span
                ></template
              ><template v-else
                >{{ entry.view.positions.length || "—"
                }}<span class="cell-note"
                  >持仓按品种与方向分别查看</span
                ></template
              >
            </td>
            <td class="number" :title="raw(entry.view.bound)">
              {{ decimal(entry.view.bound) }}
              <span class="subtle">{{ entry.view.currency }}</span
              ><span class="cell-note">{{
                entry.view.riskKnown ? "引擎确认值" : "缺失 / 不完整"
              }}</span>
            </td>
            <td>{{ time(entry.view.observation.captured_at_ns) }}</td>
            <td>
              <router-link
                class="text-link"
                :to="'/runs/' + encodeURIComponent(entry.item.id)"
                >查看详情 →</router-link
              >
            </td>
          </tr>
          <tr v-if="!shown.length">
            <td colspan="6" class="compact-empty">
              {{
                store.runs.length
                  ? "没有匹配的实例，请调整搜索或筛选。"
                  : "尚无已授权的运行快照；缺失不表示零仓位。"
              }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div class="table-footer">
      <span>当前显示 {{ shown.length }} 个实例</span
      ><span>账户记录与策略收益分别展示</span>
    </div>
  </section>
  <RunAlerts />
</template>
