<script setup lang="ts">
import { computed, ref } from "vue";
import type { SimulationStrategy } from "./simulation-examples";
import { simulationMetrics } from "./simulation-examples";
import { decimal } from "./run-view";
import SimulationChart from "./SimulationChart.vue";
const props = defineProps<{
  example: SimulationStrategy;
  mode: "backtest" | "live";
}>();
const selected = ref("");
const result = computed(
  () =>
    props.example.backtests.find(item => item.id === selected.value) ??
    props.example.backtests[0]
);
const backtestMetrics = computed(() => simulationMetrics(result.value.equity));
const runMetrics = computed(() => simulationMetrics(props.example.run.equity));
</script>
<template>
  <section class="simulation-examples" aria-label="模拟数据示例">
    <div class="notice simulation-notice">
      <strong>模拟示例</strong
      ><span
        >收益、持仓、订单和状态均为虚构数据，仅用于界面预览。此视图只读。</span
      >
    </div>
    <template v-if="mode === 'backtest'">
      <section class="console-card">
        <div class="card-heading">
          <div>
            <h2>
              模拟回测记录
              <span class="subtle">{{ example.backtests.length }} 组</span>
            </h2>
            <p>模拟期间：2026-10-25 至 2026-11-01 · 初始资金 10,000 USDT</p>
          </div>
          <span class="state-tag">模拟数据</span>
        </div>
        <div class="table-scroll">
          <table class="data-table">
            <thead>
              <tr>
                <th>场景</th>
                <th>状态</th>
                <th>模拟净收益 / USDT</th>
                <th>模拟最大回撤</th>
                <th>成交笔数</th>
                <th>详情</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="item in example.backtests"
                :key="item.id"
                :class="{ 'simulation-selected': item.id === result.id }"
              >
                <td>
                  {{ item.name }}<span class="cell-note">{{ item.id }}</span>
                </td>
                <td>
                  <span
                    class="state-tag"
                    :class="{ warning: item.status.includes('终止') }"
                    >{{ item.status }}</span
                  >
                </td>
                <td
                  class="number"
                  :class="{
                    'warning-text': simulationMetrics(item.equity).pnl < 0
                  }"
                >
                  {{ decimal(simulationMetrics(item.equity).pnl) }}
                </td>
                <td>{{ decimal(simulationMetrics(item.equity).drawdown) }}%</td>
                <td>{{ item.trades }}</td>
                <td>
                  <button
                    class="text-link"
                    :aria-label="'查看模拟回测 ' + item.name"
                    :aria-pressed="result.id === item.id"
                    @click="selected = item.id"
                  >
                    查看详情 →
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
      <div class="result-heading section-gap">
        <div>
          <p class="eyebrow">SIMULATED RESULTS</p>
          <h2>{{ result.name }}</h2>
          <p class="subtle">{{ result.description }}</p>
        </div>
      </div>
      <section class="metric-grid backtest-metrics">
        <article
          v-for="metric in [
            ['模拟净收益', decimal(backtestMetrics.pnl), 'USDT'],
            ['模拟收益率', decimal(backtestMetrics.returnPct), '%'],
            ['模拟最大回撤', decimal(backtestMetrics.drawdown), '%'],
            ['模拟最大敞口', decimal(result.maxExposure), 'USDT'],
            ['模拟胜率', decimal(result.winRate), '%'],
            ['模拟手续费', decimal(result.fees), 'USDT']
          ]"
          :key="metric[0]"
          class="metric-card"
        >
          <p>{{ metric[0] }}</p>
          <strong class="number"
            >{{ metric[1] }}<small>{{ metric[2] }}</small></strong
          ><span>仅供预览 · {{ result.id }}</span>
        </article>
      </section>
      <section class="console-card section-gap">
        <div class="card-heading">
          <h2>模拟权益曲线</h2>
          <span class="subtle"
            >期末权益 {{ decimal(backtestMetrics.final) }} USDT</span
          >
        </div>
        <SimulationChart :values="result.equity" mode="backtest" />
      </section>
    </template>
    <template v-else>
      <section class="metric-grid simulation-run-metrics">
        <article class="metric-card">
          <p>模拟运行状态</p>
          <strong>{{ example.run.status }}</strong
          ><span>示例时长 {{ example.run.uptime }}</span>
        </article>
        <article class="metric-card">
          <p>模拟累计收益</p>
          <strong class="number"
            >{{ decimal(runMetrics.pnl) }}<small>USDT</small></strong
          ><span>模拟净权益 {{ decimal(runMetrics.final) }} USDT</span>
        </article>
        <article class="metric-card">
          <p>模拟含在途敞口上界</p>
          <strong class="number"
            >{{ decimal(example.run.exposure) }}<small>USDT</small></strong
          ><span>模拟预算 {{ decimal(example.run.exposureLimit) }} USDT</span>
        </article>
        <article class="metric-card">
          <p>模拟成交 / 延迟</p>
          <strong class="number"
            >{{ example.run.fills }}<small>笔</small></strong
          ><span>模拟回报延迟 {{ example.run.latency }} ms</span>
        </article>
      </section>
      <section class="console-card section-gap">
        <div class="card-heading">
          <div>
            <h2>模拟策略实例</h2>
            <p>{{ example.run.account }} · {{ example.run.instruments }}</p>
          </div>
          <span class="state-tag">{{ example.run.status }}</span>
        </div>
        <div class="card-body">
          <dl class="parameter-grid">
            <div>
              <dt>模拟实例编号</dt>
              <dd>{{ example.run.id }}</dd>
            </div>
            <div>
              <dt>快照来源</dt>
              <dd>静态模拟数据 · 2026-11-02</dd>
            </div>
          </dl>
        </div>
        <SimulationChart :values="example.run.equity" mode="live" />
      </section>
      <section class="console-card section-gap">
        <div class="card-heading">
          <h2>
            模拟持仓
            <span class="subtle">{{ example.run.positions.length }}</span>
          </h2>
          <span class="subtle">价格单位 USDT</span>
        </div>
        <div class="table-scroll">
          <table class="data-table">
            <thead>
              <tr>
                <th>品种</th>
                <th>方向</th>
                <th>数量</th>
                <th>开仓均价</th>
                <th>模拟标记价</th>
                <th>浮动盈亏 / USDT</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="position in example.run.positions"
                :key="position.symbol"
              >
                <td>{{ position.symbol }}</td>
                <td>{{ position.side }}</td>
                <td>{{ position.quantity }}</td>
                <td>{{ position.entry }}</td>
                <td>{{ position.mark }}</td>
                <td>{{ position.pnl }}</td>
              </tr>
              <tr v-if="!example.run.positions.length">
                <td colspan="6" class="compact-empty">该模拟快照无持仓</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
      <section class="console-card section-gap">
        <div class="card-heading">
          <h2>
            模拟挂单 <span class="subtle">{{ example.run.orders.length }}</span>
          </h2>
        </div>
        <div class="table-scroll">
          <table class="data-table">
            <thead>
              <tr>
                <th>订单编号</th>
                <th>品种</th>
                <th>方向</th>
                <th>价格 / USDT</th>
                <th>数量</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="order in example.run.orders" :key="order.id">
                <td>{{ order.id }}</td>
                <td>{{ order.symbol }}</td>
                <td>{{ order.side }}</td>
                <td>{{ order.price }}</td>
                <td>{{ order.quantity }}</td>
                <td>
                  <span class="state-tag">{{ order.status }}</span>
                </td>
              </tr>
              <tr v-if="!example.run.orders.length">
                <td colspan="6" class="compact-empty">该模拟快照无挂单</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
      <section class="console-card section-gap">
        <div class="card-heading"><h2>模拟事件与告警</h2></div>
        <div class="card-body">
          <div
            v-for="alert in example.run.alerts"
            :key="alert.time"
            class="notice"
            :class="{ warning: alert.level === '预警' }"
          >
            <strong>{{ alert.level }} · {{ alert.time }}</strong
            ><span>{{ alert.text }}</span>
          </div>
        </div>
      </section>
    </template>
    <section class="console-card section-gap">
      <div class="card-heading">
        <h2>模拟参数</h2>
        <span class="subtle">示例展示值</span>
      </div>
      <div class="card-body">
        <dl class="parameter-grid">
          <div v-for="(value, label) in example.parameters" :key="label">
            <dt>{{ label }}</dt>
            <dd>{{ value }}</dd>
          </div>
        </dl>
      </div>
    </section>
  </section>
</template>
<style scoped>
.simulation-notice {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 16px;
  border: 1px solid #d4e6df;
  background: #eef7f3;
}
.simulation-selected {
  background: #f4faf7;
}
.simulation-run-metrics .metric-card strong {
  font-size: 24px;
}
</style>
