<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { consoleApi, type Resource } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
const props = defineProps<{ items: Resource[] }>();
const store = useConsoleStore();
const selected = ref("");
const item = computed(
  () => props.items.find(row => row.id === selected.value) ?? props.items[0]
);
const body = computed<any>(() => item.value?.body ?? {});
const observation = computed<any>(() => body.value.observation ?? {});
const account = computed<any>(() => observation.value.account_snapshot ?? {});
const image = computed<any>(
  () => account.value.image ?? account.value.last_valid_image ?? {}
);
const financial = computed<any>(() => observation.value.financial ?? {});
const alerts = computed(() =>
  store.resources.filter(
    row =>
      row.kind === "alert" &&
      (row.body as any).account === body.value.account &&
      ((row.body as any).active === true || !!(row.body as any).acknowledgement)
  )
);
const nodes = computed(() =>
  store.resources.filter(row => row.kind === "node")
);
const sourceStale = computed(
  () =>
    body.value.observation_stale ||
    store.connection !== "connected" ||
    !nodes.value.some(
      row =>
        row.node_id === item.value?.node_id &&
        (row.body as any).environment === "live" &&
        (row.body as any).online === true
    )
);
const kind = ref("fill"),
  offset = ref(0),
  pending = ref(false),
  error = ref("");
const history = ref<unknown[]>([]),
  provenance = ref<unknown>(null);
const logVisible = ref(false),
  logText = ref(""),
  logOffset = ref(0),
  logMore = ref(false),
  logBusy = ref(false);
async function viewLog(next = false) {
  if (!item.value || logBusy.value) return;
  const source = item.value,
    generation = store.generation;
  logBusy.value = true;
  try {
    const result = await consoleApi.log(source, next ? logOffset.value : 0);
    if (item.value?.id !== source.id || store.generation !== generation) return;
    const offset = Number(result.next_offset);
    if (!Number.isSafeInteger(offset) || offset < 0 || offset > 8_388_608)
      throw new Error("日志偏移超出协议范围");
    logText.value = result.text;
    logOffset.value = offset;
    logMore.value = result.more;
    logVisible.value = true;
  } catch (e) {
    ElMessage.error((e as Error).message);
  } finally {
    logBusy.value = false;
  }
}
let sequence = 0;
const text = (value: unknown) =>
  value === null || value === undefined
    ? "缺失"
    : typeof value === "string"
      ? value
      : JSON.stringify(value);
const pretty = (value: unknown) => JSON.stringify(value, null, 2);
const rows = (value: unknown) => (Array.isArray(value) ? value : []);
async function load() {
  if (!item.value) return;
  const current = ++sequence;
  pending.value = true;
  error.value = "";
  try {
    const result = await consoleApi.history(
      item.value,
      kind.value,
      offset.value
    );
    if (current !== sequence) return;
    history.value = result.items;
    provenance.value = { source: result.source, recording: result.recording };
  } catch (e) {
    if (current === sequence) {
      error.value = (e as Error).message;
      history.value = [];
    }
  } finally {
    if (current === sequence) pending.value = false;
  }
}
watch(
  () => [item.value?.id, body.value.generation, kind.value],
  () => {
    sequence++;
    logVisible.value = false;
    offset.value = 0;
    history.value = [];
    provenance.value = null;
    load();
  },
  { immediate: true }
);
async function acknowledge(row: Resource) {
  try {
    store.mergeResource(await consoleApi.acknowledge(row));
  } catch (e) {
    ElMessage.error((e as Error).message);
  }
}
</script>
<template>
  <el-card class="observation">
    <template #header>节点连接与观测来源</template>
    <div v-for="node in nodes" :key="node.id" class="source-line">
      <el-tag :type="(node.body as any).online ? 'success' : 'danger'"
        >{{ node.node_id }} /
        {{ (node.body as any).online ? "节点可达" : "节点失联" }}</el-tag
      >
      <span
        >最后成功时间 ns {{ text((node.body as any).last_seen_at_ns) }}</span
      >
      <span>{{ (node.body as any).reason }}</span>
    </div>
    <el-empty
      v-if="!items.length"
      description="尚无已授权且可确认来源的业务快照；缺失不表示零仓位"
    />
    <template v-else>
      <el-select v-model="selected" placeholder="选择观测来源"
        ><el-option
          v-for="row in items"
          :key="row.id"
          :value="row.id"
          :label="`${(row.body as any).account} / ${row.id}`"
      /></el-select>
      <el-alert
        v-if="sourceStale || account.available !== true"
        title="观测已过期或账户快照不可用；下方保留最后有效事实，不能据此确认当前仓位。"
        type="error"
        :closable="false"
      />
      <el-alert
        v-if="financial.recording?.history_complete !== true"
        title="历史记录不完整，净收益及风险指标以平台完整性说明为准。"
        type="warning"
        :closable="false"
      />
      <el-descriptions :column="2" border>
        <el-descriptions-item label="进程">{{
          text(body.process_state)
        }}</el-descriptions-item>
        <el-descriptions-item label="模式">{{
          body.mode === "observe" ? "只读账户观测" : text(body.mode)
        }}</el-descriptions-item>
        <el-descriptions-item label="账户 / 品种"
          >{{ body.account }} / {{ body.instrument }}</el-descriptions-item
        >
        <el-descriptions-item label="运行代次">{{
          text(body.generation)
        }}</el-descriptions-item>
        <el-descriptions-item label="账户状态">{{
          text(observation.account_health)
        }}</el-descriptions-item>
        <el-descriptions-item label="捕获时间 ns">{{
          text(observation.captured_at_ns)
        }}</el-descriptions-item>
        <el-descriptions-item label="独立成交记录数">{{
          text(financial.independent_fills_observed)
        }}</el-descriptions-item>
        <el-descriptions-item label="归属"
          >{{
            text(financial.attribution)
          }}；账户记录不能冒充策略收益</el-descriptions-item
        >
        <el-descriptions-item label="账户采样净收益（原币）">{{
          text(financial.net_pnl)
        }}</el-descriptions-item>
        <el-descriptions-item label="采样最大回撤（原币）">{{
          text(financial.max_drawdown)
        }}</el-descriptions-item>
      </el-descriptions>
      <el-alert
        title="敞口按新鲜卖一价保守估值；最大值来自已记录采样。未确认成交、未知委托或缺少估值信息时保留不完整标记。"
        type="info"
        :closable="false"
      />
      <el-table :data="rows(image.risk?.assets)">
        <el-table-column prop="instrument" label="品种" />
        <el-table-column prop="currency" label="计价币" />
        <el-table-column label="实际名义敞口"
          ><template #default="{ row }">{{
            text(row.actual_notional?.amount)
          }}</template></el-table-column
        >
        <el-table-column label="含在途敞口上界"
          ><template #default="{ row }">{{
            text(row.pending_bound_notional?.amount)
          }}</template></el-table-column
        >
        <el-table-column label="完整性"
          ><template #default="{ row }">{{
            row.complete ? "已确认" : text(row.missing)
          }}</template></el-table-column
        >
      </el-table>
      <el-table :data="rows(financial.max_exposure)">
        <el-table-column prop="instrument" label="历史观测品种" />
        <el-table-column prop="currency" label="计价币" />
        <el-table-column
          prop="max_actual_notional"
          label="最大实际敞口（采样）"
        />
        <el-table-column
          prop="max_pending_bound_notional"
          label="最大含在途上界（采样）"
        />
      </el-table>
      <el-collapse>
        <el-collapse-item
          title="权益与费用（保留原币种，不合并估值）"
          name="equity"
        >
          <el-table :data="rows(image.equity)"
            ><el-table-column prop="currency" label="币种" /><el-table-column
              prop="amount"
              label="权益"
            /><el-table-column label="估值完整"
              ><template #default="{ row }">{{
                text(row.valuation_complete)
              }}</template></el-table-column
            ></el-table
          >
          <pre>{{ pretty(financial.observed_fees ?? null) }}</pre>
          <pre>{{ pretty(financial.native_performance ?? null) }}</pre>
          <pre>{{ pretty(financial.performance ?? null) }}</pre>
        </el-collapse-item>
        <el-collapse-item title="当前订单" name="orders"
          ><el-table :data="rows(image.orders)"
            ><el-table-column prop="asset_id" label="资产" /><el-table-column
              prop="side"
              label="方向" /><el-table-column
              prop="price_ticks"
              label="价格 ticks" /><el-table-column
              prop="quantity_lots"
              label="数量 lots" /><el-table-column
              prop="filled_quantity_lots"
              label="累计成交 lots" /><el-table-column
              prop="status"
              label="状态"
          /></el-table>
          <pre>{{ pretty(image.orders ?? null) }}</pre>
        </el-collapse-item>
        <el-collapse-item title="持仓与余额" name="positions">
          <pre>{{
            pretty({
              positions: image.positions,
              balances: image.balances,
              instruments: image.instruments,
              currencies: image.currencies
            })
          }}</pre>
        </el-collapse-item>
        <el-collapse-item title="网格业务状态 / checkpoint" name="strategies"
          ><el-empty
            v-if="!rows(observation.strategies).length"
            description="当前来源未运行策略"
          />
          <div
            v-for="strategy in rows(observation.strategies)"
            :key="strategy.strategy_key"
          >
            <el-tag
              >{{ strategy.strategy_key }} / {{ strategy.lifecycle }}</el-tag
            ><span>
              下单门 {{ text(strategy.command_gate_open) }} / 激活门
              {{ text(strategy.activation_gate_open) }}</span
            >
            <pre>{{ pretty(strategy.business) }}</pre>
          </div></el-collapse-item
        >
        <el-collapse-item title="完整性与原始诊断" name="diagnostics">
          <pre>{{
            pretty({
              reason: body.reason,
              account: observation.account_diagnostics,
              recording: financial.recording,
              ledger_coverage: financial.ledger_coverage,
              performance: financial.performance,
              issues: financial.diagnostics,
              completeness: image.completeness,
              risk: image.risk
            })
          }}</pre>
        </el-collapse-item>
      </el-collapse>
      <div v-for="row in alerts" :key="row.id" class="source-line">
        <el-tag :type="(row.body as any).active ? 'danger' : 'info'"
          >{{ text((row.body as any).code) }} /
          {{ (row.body as any).active ? "生效" : "已恢复" }}</el-tag
        >
        <span
          >{{ text((row.body as any).source) }} ·
          {{ text((row.body as any).message) }}</span
        >
        <el-button
          v-if="!(row.body as any).acknowledgement"
          :disabled="!store.writable"
          @click="acknowledge(row)"
          >确认已知悉</el-button
        >
        <span v-else
          >已知悉：{{ text((row.body as any).acknowledgement.actor) }}</span
        >
      </div>
      <div class="source-line">
        <h3>平台事实历史</h3>
        <el-button :disabled="logBusy" @click="viewLog()">宿主日志</el-button>
      </div>
      <div class="source-line">
        <el-select v-model="kind"
          ><el-option
            v-for="[key, label] in [
              ['fill', '独立成交'],
              ['ledger', '交易所账单'],
              ['order', '订单'],
              ['position', '持仓'],
              ['balance', '余额'],
              ['equity_sample', '权益采样'],
              ['funding', '资金费'],
              ['cashflow', '资金流'],
              ['stream', '流状态']
            ]"
            :key="key"
            :value="key"
            :label="label" /></el-select
        ><el-button :disabled="pending" @click="load">查询</el-button
        ><el-button
          :disabled="pending || offset === 0"
          @click="
            offset = Math.max(0, offset - 20);
            load();
          "
          >上一页</el-button
        ><el-button
          :disabled="pending || history.length < 20"
          @click="
            offset += 20;
            load();
          "
          >下一页</el-button
        ><span>偏移 {{ offset }}</span>
      </div>
      <el-alert v-if="error" :title="error" type="error" :closable="false" />
      <el-empty
        v-else-if="!pending && !history.length"
        description="该来源未记录此类事实；不代表交易所完整历史为空"
      />
      <pre v-for="(entry, index) in history" :key="index">{{
        pretty(entry)
      }}</pre>
      <pre>{{ pretty(provenance) }}</pre>
    </template>
    <el-dialog
      v-model="logVisible"
      title="宿主日志（每页最多 64 KiB）"
      width="80%"
    >
      <pre>{{ logText || "暂无日志" }}</pre>
      <el-button :disabled="logBusy || !logMore" @click="viewLog(true)"
        >日志下一页</el-button
      ></el-dialog
    >
  </el-card>
</template>
<style scoped>
.observation {
  margin-bottom: 18px;
}
.observation :deep(.el-alert),
.observation :deep(.el-descriptions) {
  margin-top: 14px;
}
.source-line {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px;
  margin: 12px 0;
}
.source-line .el-select {
  width: 180px;
}
pre {
  max-height: 460px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-all;
  font-size: 12px;
  background: var(--el-fill-color-light);
  padding: 12px;
  border-radius: 4px;
}
h3 {
  margin-top: 18px;
}
</style>
