<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useConsoleStore } from "@/store/modules/console";
import { body, raw, rows, pretty, decimal, runView, time } from "./run-view";
import ConsoleIcon from "./ConsoleIcon.vue";
import EquityHistoryChart from "./EquityHistoryChart.vue";
import RiskCard from "./RiskCard.vue";
import RunAlerts from "./RunAlerts.vue";
import FactHistory from "./FactHistory.vue";
import AccountSnapshot from "./AccountSnapshot.vue";
import HostLog from "./HostLog.vue";
import BusinessObservationPanel from "./BusinessObservationPanel.vue";
import OperationsPanel from "./OperationsPanel.vue";
import { useResourcePins } from "./use-resource-pins";
const store = useConsoleStore();
const pinResources = useResourcePins();
const selected = ref(""),
  tab = ref("summary"),
  diagnostics = ref(false);
const item = computed(() =>
  store.accounts.find(row => row.id === selected.value)
);
const nodes = computed(() =>
  store.resources.filter(row => row.kind === "node")
);
const view = computed(() => runView(item.value, store.connection, nodes.value));
const equity = computed(() =>
  view.value.equity.length === 1 &&
  view.value.equity[0].valuation_complete === true
    ? view.value.equity[0]
    : null
);
const net = computed(() =>
  view.value.net.length === 1 ? view.value.net[0] : null
);
const drawdown = computed(() =>
  view.value.drawdown.length === 1 ? view.value.drawdown[0] : null
);
watch(
  () => store.accounts,
  items => {
    if (!selected.value && items.length) selected.value = items[0].id;
  },
  { immediate: true }
);
watch([selected, () => body(item.value).worker_session_id], () => {
  tab.value = "summary";
  diagnostics.value = false;
});
watch(
  selected,
  id => {
    const generation = store.sessionGeneration;
    pinResources([id]).catch(error => {
      if (generation === store.sessionGeneration) store.error = error.message;
    });
  },
  { immediate: true }
);
const knownRisk = (risk: any) => !view.value.stale && risk.complete === true;
</script>
<template>
  <section class="page-title">
    <div>
      <p class="eyebrow">WORKSPACE / ACCOUNT RISK</p>
      <h1>账户风险</h1>
      <p>账户事实与原币种指标，保留来源、时间和完整性</p>
    </div>
    <div class="toolbar">
      <el-select
        v-model="selected"
        aria-label="账户观测来源"
        placeholder="选择观测来源"
        style="width: 320px"
        ><el-option
          v-for="row in store.accounts"
          :key="row.id"
          :value="row.id"
          :label="`${body(row).account} / ${row.id}`" /></el-select
      ><HostLog :item="item" /><el-button
        :disabled="!item"
        @click="diagnostics = true"
        >观测与诊断</el-button
      >
    </div>
  </section>
  <div v-if="!item" class="console-card compact-empty">
    {{
      selected
        ? "所选账户来源当前不可用，请重新选择。"
        : "尚无已授权且可确认来源的业务快照；缺失不表示零仓位"
    }}
  </div>
  <template v-else>
    <div class="runtime-state" :class="{ warning: view.stale }">
      <div>
        <strong>{{ view.status }}</strong
        >{{ raw(view.state.account) }} / {{ raw(view.state.instrument) }} ·
        {{ item.node_id }} · 代次 {{ raw(view.state.run_generation) }}
      </div>
      <span>最后采样 {{ time(view.observation.captured_at_ns, true) }}</span>
    </div>
    <div v-if="view.stale" class="notice warning">
      观测已过期或账户快照不可用；下方保留最后有效事实，不能据此确认当前仓位。
    </div>
    <div
      v-if="view.financial.recording?.history_complete !== true"
      class="completeness-note recording-warning"
    >
      <ConsoleIcon name="info" /><span
        >历史记录不完整，净收益及风险指标以平台完整性说明为准。</span
      ><button class="text-link" @click="diagnostics = true">查看完整性</button>
    </div>
    <section class="metric-grid">
      <article class="metric-card">
        <p>账户净权益 <ConsoleIcon name="monitor" /></p>
        <strong class="number" :title="raw(equity?.amount)"
          >{{ view.equity.length > 1 ? "多币种" : decimal(equity?.amount)
          }}<small>{{ equity?.currency }}</small></strong
        ><span
          >{{ view.stale ? "最后有效快照" : "账户级事实" }} ·
          不代表策略收益</span
        >
      </article>
      <article class="metric-card">
        <p>账户采样净收益</p>
        <strong class="number" :title="raw(net?.amount)"
          >{{ view.net.length > 1 ? "多币种" : decimal(net?.amount)
          }}<small>{{ net?.currency }}</small></strong
        ><span>原币种 · 按已记录范围展示</span>
      </article>
      <article class="metric-card">
        <p>账户采样最大回撤</p>
        <strong class="number" :title="raw(drawdown?.amount)"
          >{{ view.drawdown.length > 1 ? "多币种" : decimal(drawdown?.amount)
          }}<small>{{ drawdown?.currency }}</small></strong
        ><span>原币金额 · 非策略回撤百分比</span>
      </article>
      <article class="metric-card">
        <p>独立成交记录 <ConsoleIcon name="chart" /></p>
        <strong class="number"
          >{{ raw(view.financial.independent_fills_observed)
          }}<small>条</small></strong
        ><span>持久化观测范围 · 不冒充完整账户历史</span>
      </article>
    </section>
    <nav class="detail-tabs" aria-label="账户详情">
      <button
        v-for="[value, label] in [
          ['summary', '风险概览'],
          ['orders', '当前订单'],
          ['history', '平台事实历史'],
          ['financial', '权益与费用'],
          ['strategies', '策略与恢复点']
        ]"
        :key="value"
        :class="{ active: tab === value }"
        :aria-pressed="tab === value"
        @click="tab = value"
      >
        {{ label }}
      </button>
    </nav>
    <template v-if="tab === 'summary'"
      ><div class="overview-split">
        <EquityHistoryChart
          :key="item.id"
          :item="item"
          :stale="view.stale"
        /><RiskCard :item="item" />
      </div>
      <section class="console-card">
        <div class="card-heading">
          <div>
            <h2>账户逐品种敞口</h2>
            <p>新鲜卖一价保守估值 · 不跨品种或币种求和</p>
          </div>
          <span class="unit-tag">引擎确认值</span>
        </div>
        <div class="table-scroll">
          <table class="data-table account-risk-table">
            <thead>
              <tr>
                <th>品种 / 计价币</th>
                <th>实际名义敞口</th>
                <th>含在途敞口上界</th>
                <th>完整性</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="(risk, index) in rows(view.image.risk?.assets)"
                :key="index"
              >
                <td>
                  {{ raw(risk.instrument)
                  }}<span class="cell-note">{{ raw(risk.currency) }}</span>
                </td>
                <td class="number">
                  {{
                    knownRisk(risk) ? raw(risk.actual_notional?.amount) : "—"
                  }}
                </td>
                <td class="number">
                  {{
                    knownRisk(risk)
                      ? raw(risk.pending_bound_notional?.amount)
                      : "—"
                  }}
                </td>
                <td>
                  <span
                    class="state-tag"
                    :class="{ warning: !knownRisk(risk) }"
                    >{{ knownRisk(risk) ? "已确认" : "当前不可确认" }}</span
                  ><span class="cell-note" :title="raw(risk.missing)">{{
                    raw(risk.missing)
                  }}</span>
                </td>
              </tr>
              <tr v-if="!rows(view.image.risk?.assets).length">
                <td colspan="4" class="compact-empty">
                  缺少风险估值信息；不作为零敞口。
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <p class="table-note">
          未确认成交、未知委托或缺少估值信息时保留不完整标记。历史值见采样最大风险与原始诊断。
        </p>
      </section>
      <section class="console-card section-gap">
        <div class="card-heading">
          <h2>采样最大风险</h2>
          <span class="subtle">来自持久化记录，不代表连续行情最大值</span>
        </div>
        <div class="table-scroll">
          <table class="data-table">
            <thead>
              <tr>
                <th>历史观测品种</th>
                <th>计价币</th>
                <th>最大实际敞口（采样）</th>
                <th>最大含在途上界（采样）</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="(row, index) in rows(view.financial.max_exposure)"
                :key="index"
              >
                <td>{{ raw(row.instrument) }}</td>
                <td>{{ raw(row.currency) }}</td>
                <td>{{ raw(row.max_actual_notional) }}</td>
                <td>{{ raw(row.max_pending_bound_notional) }}</td>
              </tr>
              <tr v-if="!rows(view.financial.max_exposure).length">
                <td colspan="4" class="compact-empty">
                  尚无可确认的风险采样最大值
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </section></template
    >
    <AccountSnapshot
      v-if="tab === 'orders'"
      :image="view.image"
      :stale="view.stale"
    />
    <FactHistory
      v-if="tab === 'history'"
      :key="item.id + ':' + view.state.run_generation"
      :item="item"
    />
    <section v-if="tab === 'financial'" class="console-card">
      <div class="card-heading">
        <h2>权益与费用（保留原币种，不合并估值）</h2>
        <span class="subtle">账户记录不能冒充策略收益</span>
      </div>
      <div class="table-scroll">
        <table class="data-table">
          <thead>
            <tr>
              <th>币种</th>
              <th>权益</th>
              <th>估值完整</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, index) in view.equity" :key="index">
              <td>{{ row.currency }}</td>
              <td class="number">{{ raw(row.amount) }}</td>
              <td>{{ raw(row.valuation_complete) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <dl class="parameter-grid">
        <div>
          <dt>账户采样净收益（原币）</dt>
          <dd>{{ raw(view.financial.net_pnl) }}</dd>
        </div>
        <div>
          <dt>采样最大回撤（原币）</dt>
          <dd>{{ raw(view.financial.max_drawdown) }}</dd>
        </div>
        <div>
          <dt>归属</dt>
          <dd>{{ raw(view.financial.attribution) }}</dd>
        </div>
        <div>
          <dt>账户状态</dt>
          <dd>{{ raw(view.observation.account_health) }}</dd>
        </div>
      </dl>
      <details open class="secondary-details">
        <summary>已记录费用与账户表现</summary>
        <pre>{{
          pretty({
            observed_fees: view.financial.observed_fees,
            native_performance: view.financial.native_performance,
            performance: view.financial.performance
          })
        }}</pre>
      </details>
    </section>
    <section v-if="tab === 'strategies'" class="console-card">
      <div class="card-heading">
        <h2>策略业务观测与恢复信息</h2>
        <span class="subtle">业务快照与账户事实分别展示</span>
      </div>
      <div
        v-if="!rows(view.observation.strategies).length"
        class="compact-empty"
      >
        当前来源未运行策略
      </div>
      <details
        v-for="(strategy, index) in rows(view.observation.strategies)"
        :key="index"
        class="secondary-details"
        open
      >
        <summary>
          {{ raw(strategy.strategy_key) }} / {{ raw(strategy.lifecycle) }}
        </summary>
        <p class="subtle">
          命令通道 {{ raw(strategy.command_gate_open) }} / 激活门
          {{ raw(strategy.activation_gate_open) }}
        </p>
        <BusinessObservationPanel
          :observation="strategy.business_observation ?? {}"
          :stale="view.stale"
        />
        <details class="secondary-details">
          <summary>恢复信息</summary>
          <pre>{{ pretty(strategy.recovery) }}</pre>
        </details>
      </details>
    </section>
    <RunAlerts :item="item" />
    <el-drawer
      v-model="diagnostics"
      title="观测与诊断"
      size="min(900px,95vw)"
      class="console-drawer"
      ><dl class="parameter-grid">
        <div>
          <dt>进程</dt>
          <dd>{{ raw(view.state.process_state) }}</dd>
        </div>
        <div>
          <dt>模式</dt>
          <dd>
            {{
              view.state.mode === "observe"
                ? "只读账户观测"
                : raw(view.state.mode)
            }}
          </dd>
        </div>
        <div>
          <dt>捕获时间 ns</dt>
          <dd>{{ raw(view.observation.captured_at_ns) }}</dd>
        </div>
        <div>
          <dt>账户健康</dt>
          <dd>{{ raw(view.observation.account_health) }}</dd>
        </div>
      </dl>
      <details class="secondary-details" open>
        <summary>节点连接与观测来源</summary>
        <pre>{{ pretty(nodes) }}</pre>
      </details>
      <details class="secondary-details" open>
        <summary>完整性与原始诊断</summary>
        <pre>{{
          pretty({
            reason: view.state.reason,
            account: view.observation.account_diagnostics,
            recording: view.financial.recording,
            ledger_coverage: view.financial.ledger_coverage,
            performance: view.financial.performance,
            issues: view.financial.diagnostics,
            completeness: view.image.completeness,
            risk: view.image.risk
          })
        }}</pre>
      </details>
      <details class="secondary-details">
        <summary>完整资源原始数据</summary>
        <pre>{{ pretty(item) }}</pre>
      </details></el-drawer
    >
  </template>
  <div class="section-gap"><OperationsPanel /></div>
</template>
