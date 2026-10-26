<script setup lang="ts">
import { computed, ref, watch, onBeforeUnmount } from "vue";
import { useRoute } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import type { Action } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
import {
  body,
  rows,
  raw,
  pretty,
  decimal,
  scaled,
  time,
  symbolLabel,
  runView,
  operationsForRun,
  actionLabels,
  operationLabels,
  orderStates
} from "./run-view";
import ConsoleIcon from "./ConsoleIcon.vue";
import EquityHistoryChart from "./EquityHistoryChart.vue";
import RiskCard from "./RiskCard.vue";
import RunAlerts from "./RunAlerts.vue";
import FactHistory from "./FactHistory.vue";
import OperationsPanel from "./OperationsPanel.vue";
import AccountSnapshot from "./AccountSnapshot.vue";
import HostLog from "./HostLog.vue";
import { useResourcePins } from "./use-resource-pins";
const route = useRoute(),
  store = useConsoleStore();
const props = defineProps<{ runId?: string; embedded?: boolean }>();
const pinResources = useResourcePins();
defineEmits<{ close: [] }>();
const id = computed(() => props.runId ?? String(route.params.id ?? ""));
const item = computed(() => store.runs.find(r => r.id === id.value));
const nodes = computed(() => store.resources.filter(r => r.kind === "node"));
const view = computed(() => runView(item.value, store.connection, nodes.value));
const deployment = computed(() =>
  store.resources.find(r => r.id === view.value.state.deployment_id)
);
const loadError = ref(""),
  loading = ref(false),
  tab = ref("summary"),
  diagnostics = ref(false),
  receipts = ref(false);
const busy = ref<string[]>([]);
const operations = computed(() =>
  operationsForRun(store.operations, item.value)
);
const inProgress = computed(() =>
  operations.value.filter(op =>
    ["pending", "running", "unknown"].includes(op.state)
  )
);
const lastReceipt = computed(() => operations.value[0]);
const priceOrders = computed(() =>
  view.value.orders.filter(
    order =>
      view.value.instrument &&
      raw(order.asset_id) === raw(view.value.instrument.asset_id)
  )
);
const drawdown = computed(() =>
  view.value.drawdown.length === 1 ? view.value.drawdown[0] : null
);
const stopLabel = computed(
  () =>
    ({ flatten: "撤单并平仓", cancel: "撤单后留仓" })[
      view.value.state.stop_action
    ] ?? raw(view.value.state.stop_action)
);
const controlDisabled = (kind: string) =>
  !store.writable ||
  busy.value.includes(kind) ||
  (kind !== "stop" &&
    (busy.value.length > 0 ||
      inProgress.value.length > 0 ||
      store.pending.some(
        request =>
          request.node_id === item.value?.node_id &&
          "run_id" in request.action &&
          request.action.run_id === item.value?.id
      ))) ||
  (view.value.stale && ["pause", "resume"].includes(kind));
const parameterFields = [
  ["order_qty_lots", "单笔数量", "lots"],
  ["lower_price_ticks", "下边界", "ticks"],
  ["upper_price_ticks", "上边界", "ticks"],
  ["buy_levels", "买侧层数", "层"],
  ["sell_levels", "卖侧层数", "层"],
  ["max_notional_tick_lots", "策略名义上限", "tick·lot"],
  ["market_stale_after_ns", "行情过期时间", "ns"],
  ["max_loss_tick_lots", "亏损保护", "tick·lot"]
];
let sourceSequence = 0,
  disposed = false;
watch(
  () => [id.value, store.generation],
  async () => {
    const sequence = ++sourceSequence;
    tab.value = "summary";
    diagnostics.value = false;
    receipts.value = false;
    loadError.value = "";
    loading.value = true;
    try {
      await pinResources([id.value]);
    } catch (failure) {
      if (sequence === sourceSequence && !disposed)
        loadError.value = (failure as Error).message;
    } finally {
      if (sequence === sourceSequence && !disposed) loading.value = false;
    }
  },
  { immediate: true }
);
watch(
  () => [item.value?.id, view.value.state.deployment_id],
  async () => {
    if (!item.value) return;
    const identity = store.generation;
    try {
      await pinResources(
        [item.value.id, view.value.state.deployment_id].filter(Boolean)
      );
    } catch (failure) {
      if (!disposed && identity === store.generation)
        store.error = (failure as Error).message;
    }
  },
  { immediate: true }
);
async function control(kind: string) {
  if (
    !item.value ||
    controlDisabled(kind) ||
    !view.value.actions.includes(kind)
  )
    return;
  const source = item.value,
    generation = view.value.state.generation,
    checkpoint = view.value.state.checkpoint_id;
  const identity = store.generation;
  if (kind === "recover" && !checkpoint) {
    ElMessage.error("缺少可确认的 checkpoint 身份");
    return;
  }
  const meaning =
    kind === "pause"
      ? "暂停禁止新增开仓，继续接收事实并处理撤单与保护性减仓。"
      : kind === "stop"
        ? "停止完成需等待撤单、成交与持仓确认，不因请求受理而清零。"
        : kind === "recover"
          ? "按原制品、绑定与 checkpoint 进行恢复核验，不补充原运行期限。"
          : "恢复运行仍由引擎校验账户和交易入口。";
  try {
    await ElMessageBox.confirm(
      `${meaning}\n账户 ${raw(view.value.state.account)}，品种 ${raw(view.value.state.instrument)}，实例 ${source.id}，代次 ${raw(generation)}，停止方式 ${stopLabel.value}。操作以实际回执为准。`,
      "确认操作",
      {
        type: "warning",
        confirmButtonText: "确定",
        cancelButtonText: "取消",
        customClass: "console-confirmation"
      }
    );
  } catch {
    return;
  }
  // Confirmation must not authorize a different resource, generation or refreshed capability.
  if (
    disposed ||
    identity !== store.generation ||
    item.value?.id !== source.id ||
    view.value.state.generation !== generation ||
    controlDisabled(kind) ||
    !view.value.actions.includes(kind) ||
    (kind === "recover" && view.value.state.checkpoint_id !== checkpoint)
  ) {
    ElMessage.warning("目标状态已变更，请重新确认");
    return;
  }
  busy.value = [...busy.value, kind];
  try {
    const action: Action =
      kind === "recover"
        ? { kind, run_id: source.id, generation, checkpoint_id: checkpoint }
        : {
            kind: kind as "pause" | "resume" | "stop",
            run_id: source.id,
            generation
          };
    await store.submit(action, source.node_id);
    if (
      !disposed &&
      identity === store.generation &&
      item.value?.id === source.id
    ) {
      receipts.value = true;
      ElMessage.success("操作已受理，等待执行方确认");
    }
  } catch (failure) {
    if (!disposed && identity === store.generation)
      ElMessage.error((failure as Error).message);
  } finally {
    busy.value = busy.value.filter(entry => entry !== kind);
  }
}
const sides: Record<string, string> = {
  "0": "未指定",
  "1": "买入",
  "2": "卖出"
};
onBeforeUnmount(() => {
  disposed = true;
  sourceSequence++;
});
</script>
<template>
  <button v-if="embedded" class="back-link text-link" @click="$emit('close')">
    <ConsoleIcon name="back" />返回策略实盘
  </button>
  <router-link v-else to="/runs" class="back-link"
    ><ConsoleIcon name="back" />返回运行总览</router-link
  >
  <div v-if="loading && !item" class="notice">正在按实例身份加载…</div>
  <div v-else-if="!item" class="notice warning">
    {{
      loadError
        ? `无法加载此实例：${loadError}。不会替换为其他实例。`
        : "实例不存在、已超出可见范围或无权限；不会替换为其他实例。"
    }}
  </div>
  <template v-else>
    <section class="page-title detail-title">
      <div>
        <div class="title-with-status">
          <span class="asset-icon">{{
            symbolLabel(view.state.instrument).slice(0, 1)
          }}</span>
          <h1>
            {{
              view.strategy.strategy_key || symbolLabel(view.state.instrument)
            }}
          </h1>
          <span
            class="state-tag"
            :class="{
              warning: view.stale,
              neutral: view.lifecycle !== 'Running'
            }"
            >{{ view.stale ? "最后确认 · " : "" }}{{ view.status }}</span
          >
        </div>
        <p class="detail-meta">
          <span>{{ item.id }}</span
          ><span
            >{{ raw(view.state.account) }} /
            {{ raw(view.state.instrument) }}</span
          ><span>运行代次 {{ raw(view.state.generation) }}</span
          ><span>{{ item.node_id }}</span>
        </p>
      </div>
      <div class="toolbar">
        <el-button @click="diagnostics = true">观测与诊断</el-button
        ><HostLog :item="item" /><el-button
          v-for="action in view.actions"
          :key="action"
          :type="action === 'stop' ? 'danger' : 'default'"
          :plain="action === 'stop'"
          :disabled="controlDisabled(action)"
          @click="control(action)"
          >{{ actionLabels[action] }}</el-button
        >
      </div>
    </section>
    <div
      class="runtime-state"
      :class="{
        warning: view.stale || view.strategy.command_gate_open === false
      }"
    >
      <div>
        <strong>{{
          view.stale ? "观测受限 · 当前状态不可确认" : view.status
        }}</strong
        ><span v-if="!view.stale && view.lifecycle === 'Paused'"
          >新增开仓已暂停 · 成交与保护性处理继续</span
        ><span v-else
          >命令通道
          {{
            view.stale
              ? "当前未知"
              : view.strategy.command_gate_open === true
                ? "已开放"
                : view.strategy.command_gate_open === false
                  ? "关闭"
                  : "未提供"
          }}</span
        >
      </div>
      <span>最后采样 {{ time(view.observation.captured_at_ns, true) }}</span>
    </div>
    <p
      v-if="
        view.financial.recording &&
        view.financial.recording.history_complete !== true
      "
      class="completeness-note recording-warning"
    >
      <ConsoleIcon
        name="info"
      />历史记录范围未证明完整；回撤与最大敞口仅表示已记录采样。<button
        class="text-link"
        @click="diagnostics = true"
      >
        查看完整性
      </button>
    </p>
    <div v-if="inProgress.length" class="notice warning">
      <strong>管理操作尚未完成</strong> ·
      {{
        inProgress
          .map(op => `${op.input.kind}：${operationLabels[op.state]}`)
          .join(" / ")
      }}<button class="text-link" @click="receipts = true">
        查看同一操作回执
      </button>
    </div>
    <nav class="detail-tabs" aria-label="策略详情内容">
      <button
        v-for="[value, label] in [
          ['summary', '运行概览'],
          ['orders', '订单与持仓'],
          ['history', '成交与历史'],
          ['parameters', '参数与版本'],
          ['events', '事件与操作']
        ]"
        :key="value"
        :class="{ active: tab === value }"
        :aria-pressed="tab === value"
        @click="tab = value"
      >
        {{ label }}
      </button>
    </nav>
    <template v-if="tab === 'summary'">
      <section class="metric-grid">
        <article class="metric-card">
          <p>网格配对毛收益</p>
          <strong class="number" :title="raw(view.grid.grid_profit_tick_lots)"
            >{{ decimal(view.grid.grid_profit_tick_lots, 4)
            }}<small>tick·lot</small></strong
          ><span
            >已配对 {{ raw(view.grid.completed_pairs) }} ·
            未计费用与资金费</span
          >
        </article>
        <article class="metric-card">
          <p>策略净收益</p>
          <strong>—</strong
          ><span class="warning-text">当前接口未提供可确认的策略收益归属</span>
        </article>
        <article class="metric-card">
          <p>账户采样最大回撤</p>
          <strong class="number" :title="raw(drawdown?.amount)"
            >{{ view.drawdown.length > 1 ? "多币种" : decimal(drawdown?.amount)
            }}<small>{{ drawdown?.currency }}</small></strong
          ><span>账户级原币金额 · 非策略回撤百分比</span>
        </article>
        <article class="metric-card">
          <p>品种持仓快照</p>
          <strong class="number"
            >{{
              view.positions.length === 1
                ? decimal(
                    scaled(
                      view.positions[0].quantity_lots,
                      view.instrument?.quantity_lot
                    ),
                    6
                  )
                : view.positions.length
                  ? view.positions.length
                  : "—"
            }}<small>{{
              view.positions.length === 1 ? "基础币" : "项"
            }}</small></strong
          ><span
            >{{ view.stale ? "最后确认值" : "账户事实" }} ·
            按持仓方向分别展示</span
          >
        </article>
      </section>
      <div class="detail-split">
        <EquityHistoryChart :item="item" :stale="view.stale" />
        <article class="console-card grid-state">
          <div class="card-heading">
            <h2>网格状态</h2>
            <span class="unit-tag">已提交 checkpoint</span>
          </div>
          <div v-if="!view.business.available" class="compact-empty">
            {{ view.business.reason || "尚无已提交的业务快照" }}
          </div>
          <template v-else
            ><dl class="risk-lines">
              <div>
                <dt>锚点 / ticks</dt>
                <dd :title="raw(view.grid.anchor_ticks)">
                  {{ raw(view.grid.anchor_ticks) }}
                </dd>
              </div>
              <div>
                <dt>业务状态码</dt>
                <dd>{{ raw(view.grid.status) }}</dd>
              </div>
              <div>
                <dt>完成配对</dt>
                <dd>{{ raw(view.grid.completed_pairs) }}</dd>
              </div>
            </dl>
            <div class="price-ladder">
              <div
                v-for="(order, index) in priceOrders.slice(0, 6)"
                :key="index"
                class="price-level"
              >
                <span :class="{ positive: raw(order.side) === '1' }">{{
                  sides[raw(order.side)] || raw(order.side)
                }}</span
                ><i /><strong
                  class="number"
                  :title="raw(order.price_ticks) + ' ticks'"
                  >{{
                    raw(scaled(order.price_ticks, view.instrument?.price_tick))
                  }}</strong
                >
              </div>
            </div>
            <p class="subtle grid-note">
              账户品种委托 · 不从意图推造交易所事实
            </p></template
          >
        </article>
      </div>
      <div class="detail-split">
        <section class="console-card">
          <div class="card-heading">
            <h2>
              {{ view.stale ? "最后确认订单" : "品种订单快照" }}
              <span class="subtle">{{ priceOrders.length }}</span>
            </h2>
            <button class="text-link" @click="tab = 'orders'">
              全部订单 →
            </button>
          </div>
          <div class="table-scroll">
            <table class="data-table">
              <thead>
                <tr>
                  <th>方向</th>
                  <th>价格</th>
                  <th>基础币数量</th>
                  <th>状态</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="(order, index) in priceOrders.slice(0, 6)"
                  :key="index"
                >
                  <td>
                    <span
                      class="state-tag"
                      :class="{ neutral: raw(order.side) !== '1' }"
                      >{{ sides[raw(order.side)] || raw(order.side) }}</span
                    >
                  </td>
                  <td class="number">
                    {{
                      raw(
                        scaled(order.price_ticks, view.instrument?.price_tick)
                      )
                    }}
                  </td>
                  <td class="number">
                    {{
                      raw(
                        scaled(
                          order.quantity_lots,
                          view.instrument?.quantity_lot
                        )
                      )
                    }}
                  </td>
                  <td>
                    {{ orderStates[raw(order.status)] || raw(order.status) }}
                  </td>
                </tr>
                <tr v-if="!priceOrders.length">
                  <td colspan="4" class="compact-empty">
                    当前快照没有该品种的订单记录；不代表交易所完整历史为空。
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <p class="table-note">
            {{
              view.stale
                ? "观测受限，不能确认当前委托。"
                : "订单、独立成交与持仓分别确认。"
            }}数值由账户元数据精确换算展示。
          </p>
        </section>
        <RiskCard :item="item" />
      </div>
    </template>
    <AccountSnapshot
      v-if="tab === 'orders'"
      :image="view.image"
      :stale="view.stale"
    />
    <FactHistory
      v-if="tab === 'history'"
      :key="item.id + ':' + view.state.generation"
      :item="item"
    />
    <template v-if="tab === 'parameters'"
      ><div class="notice">
        运行实例引用固定制品和参数，修改参数须建立新版本。恢复继续沿用原期限与身份校验。
      </div>
      <section class="console-card">
        <div class="card-heading"><h2>参数、制品与恢复来源</h2></div>
        <dl class="parameter-grid">
          <div>
            <dt>账户 / 原生品种</dt>
            <dd>
              {{ raw(view.state.account) }} / {{ raw(view.state.instrument) }}
            </dd>
          </div>
          <div>
            <dt>制品摘要</dt>
            <dd class="mono">
              {{ raw(view.strategy.artifact_digest) }}
            </dd>
          </div>
          <div>
            <dt>运行代次</dt>
            <dd>{{ raw(view.state.generation) }}</dd>
          </div>
          <div>
            <dt>停止方式</dt>
            <dd>{{ stopLabel }}</dd>
          </div>
          <div>
            <dt>Checkpoint 身份</dt>
            <dd class="mono">
              {{ raw(view.state.checkpoint_id ?? view.business.checkpoint_id) }}
            </dd>
          </div>
          <div>
            <dt>状态结构版本</dt>
            <dd>{{ raw(view.business.state_schema_version) }}</dd>
          </div>
          <div>
            <dt>最大风险敞口</dt>
            <dd>
              {{ raw(body(deployment).max_exposure_quote) }} · 以绑定计价币为准
            </dd>
          </div>
          <div>
            <dt>原运行期限</dt>
            <dd>{{ time(view.state.expires_at_ns, true) }}</dd>
          </div>
        </dl>
        <dl v-if="view.business.available" class="parameter-grid">
          <div v-for="[field, label, unit] in parameterFields" :key="field">
            <dt>{{ label }}</dt>
            <dd>{{ raw(view.config[field]) }} {{ unit }}</dd>
          </div>
        </dl>
        <details class="secondary-details">
          <summary>已提交的策略配置</summary>
          <pre>{{ pretty(view.config) }}</pre>
        </details>
        <details class="secondary-details">
          <summary>完整业务状态与 checkpoint 元数据</summary>
          <pre>{{ pretty(view.observation.strategies) }}</pre>
        </details>
      </section></template
    >
    <OperationsPanel v-if="tab === 'events'" :run="item" />
    <RunAlerts :item="item" />
    <el-drawer
      v-model="diagnostics"
      title="观测与诊断"
      size="min(900px,95vw)"
      class="console-drawer"
      ><p class="notice" :class="{ warning: view.stale }">
        {{
          view.stale
            ? "当前状态不可完整确认，下方为最后有效事实。"
            : "账户记录与策略收益分别展示。"
        }}
      </p>
      <dl class="parameter-grid">
        <div>
          <dt>来源 / 节点</dt>
          <dd>{{ item.node_id }} / {{ raw(view.observation.source) }}</dd>
        </div>
        <div>
          <dt>采样时间（本地）</dt>
          <dd>{{ time(view.observation.captured_at_ns, true) }}</dd>
        </div>
        <div>
          <dt>账户健康</dt>
          <dd>{{ raw(view.observation.account_health) }}</dd>
        </div>
        <div>
          <dt>历史记录完整</dt>
          <dd>{{ raw(view.financial.recording?.history_complete) }}</dd>
        </div>
        <div>
          <dt>账户采样净收益</dt>
          <dd>{{ raw(view.financial.net_pnl) }}</dd>
        </div>
        <div>
          <dt>费用 / 资金费归属</dt>
          <dd>{{ raw(view.financial.attribution) }} · 不视为策略收益</dd>
        </div>
      </dl>
      <details class="secondary-details">
        <summary>授权节点状态与连接原因</summary>
        <pre>{{ pretty(nodes) }}</pre>
      </details>
      <details class="secondary-details" open>
        <summary>完整性、权益、费用与采样最大风险</summary>
        <pre>{{
          pretty({
            financial: view.financial,
            equity: view.equity,
            completeness: view.image.completeness,
            risk: view.image.risk,
            account_diagnostics: view.observation.account_diagnostics
          })
        }}</pre>
      </details>
      <details class="secondary-details">
        <summary>完整资源原始数据</summary>
        <pre>{{ pretty(item) }}</pre>
      </details></el-drawer
    >
    <el-drawer
      v-model="receipts"
      title="来源与实际结果"
      size="min(1000px,95vw)"
      class="console-drawer"
      ><p class="notice">
        {{
          lastReceipt
            ? `${lastReceipt.input.kind} · ${operationLabels[lastReceipt.state]}`
            : "没有相关操作"
        }}。受理不代表业务完成；页面继续观察实际运行状态。
      </p>
      <OperationsPanel :run="item"
    /></el-drawer>
  </template>
</template>
