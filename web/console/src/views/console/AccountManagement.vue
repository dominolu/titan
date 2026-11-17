<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { ElMessage } from "element-plus";
import {
  consoleApi,
  type AccountProfileInput,
  type Resource
} from "@/api/console";
import { getSession } from "@/utils/auth";
import { useConsoleStore } from "@/store/modules/console";
import { useConsoleWork } from "./use-console-work";
import { body, time } from "./run-view";
import AccountRisk from "./AccountRisk.vue";
const store = useConsoleStore();
const work = useConsoleWork();
const accounts = ref<Resource[]>([]),
  pending = ref(false),
  error = ref("");
const tab = ref("directory"),
  search = ref(""),
  dialog = ref(false),
  editing = ref<Resource>();
const identity = computed(() => getSession()?.identity);
const nodes = computed(() => identity.value?.nodes ?? []);
const environments = computed(() =>
  (identity.value?.environments ?? []).filter(e =>
    ["backtest", "live"].includes(e)
  )
);
const exchanges = {
  binance: "Binance",
  okx: "OKX",
  hyperliquid: "Hyperliquid",
  gate: "Gate",
  backpack: "Backpack",
  lighter: "Lighter"
};
const form = reactive<AccountProfileInput>({
  node_id: "",
  name: "",
  exchange: "binance",
  environment: "backtest",
  credential_ref: "",
  notes: "",
  enabled: true,
  expected_revision: null
});
const filtered = computed(() =>
  accounts.value.filter(item =>
    `${body(item).name} ${body(item).exchange} ${item.node_id}`
      .toLowerCase()
      .includes(search.value.toLowerCase())
  )
);
let disposed = false,
  sequence = 0;
onBeforeUnmount(() => {
  disposed = true;
  sequence++;
});
async function load() {
  const request = ++sequence,
    generation = store.sessionGeneration;
  pending.value = true;
  error.value = "";
  try {
    const values = await consoleApi.accounts();
    if (
      !disposed &&
      request === sequence &&
      generation === store.sessionGeneration
    )
      accounts.value = values;
  } catch (failure) {
    if (
      !disposed &&
      request === sequence &&
      generation === store.sessionGeneration
    )
      error.value = (failure as Error).message;
  } finally {
    if (!disposed && request === sequence) pending.value = false;
  }
}
onMounted(load);
function open(item?: Resource) {
  editing.value = item;
  Object.assign(
    form,
    {
      node_id: nodes.value[0] ?? "",
      name: "",
      exchange: "binance",
      environment: environments.value[0] ?? "backtest",
      credential_ref: "",
      notes: "",
      enabled: true,
      expected_revision: null
    },
    item ? body(item) : {},
    { expected_revision: item?.revision ?? null }
  );
  if (item) form.node_id = item.node_id;
  dialog.value = true;
}
async function save() {
  if (!form.name.trim() || !form.node_id) {
    ElMessage.warning("请填写账户名称并选择所属节点");
    return;
  }
  // Submit only explicit public directory fields, never observation bodies or credentials.
  const input: AccountProfileInput = {
    node_id: form.node_id,
    name: form.name,
    exchange: form.exchange,
    environment: form.environment,
    credential_ref: form.credential_ref,
    notes: form.notes,
    enabled: form.enabled,
    expected_revision: form.expected_revision
  };
  await work.perform(
    () => consoleApi.saveAccount(input, editing.value?.id),
    () => {
      dialog.value = false;
      ElMessage.success("账户资料已保存");
      void load();
    }
  );
}
</script>
<template>
  <nav class="detail-tabs" aria-label="账户管理视图">
    <button :class="{ active: tab === 'directory' }" @click="tab = 'directory'">
      账户列表
    </button>
    <button :class="{ active: tab === 'risk' }" @click="tab = 'risk'">
      风险与账户事实
    </button>
  </nav>
  <AccountRisk v-if="tab === 'risk'" />
  <template v-else>
    <section class="page-title">
      <div>
        <p class="eyebrow">WORKSPACE / ACCOUNTS</p>
        <h1>账户管理</h1>
        <p>登记交易账户、管理资料与凭据引用，查看账户风险和运行事实</p>
      </div>
      <div class="toolbar">
        <el-button :loading="pending" @click="load">刷新账户</el-button
        ><el-button
          type="primary"
          :disabled="!store.writable || !nodes.length"
          @click="open()"
          >添加账户</el-button
        >
      </div>
    </section>
    <section class="metric-grid account-metrics">
      <article class="metric-card">
        <p>已登记账户</p>
        <strong class="number">{{ accounts.length }}<small>个</small></strong
        ><span>账户资料目录</span>
      </article>
      <article class="metric-card">
        <p>模拟 / 回测账户</p>
        <strong class="number"
          >{{ accounts.filter(a => body(a).environment === "backtest").length
          }}<small>个</small></strong
        ><span>用于离线研究与验证</span>
      </article>
      <article class="metric-card">
        <p>实盘账户资料</p>
        <strong class="number"
          >{{ accounts.filter(a => body(a).environment === "live").length
          }}<small>个</small></strong
        ><span>连接与交易授权由所属节点管理</span>
      </article>
    </section>
    <p v-if="error" class="notice warning">{{ error }}</p>
    <section class="console-card">
      <div class="card-heading">
        <h2>
          账户列表 <span class="subtle">{{ filtered.length }}</span>
        </h2>
        <el-input
          v-model="search"
          aria-label="搜索账户"
          placeholder="搜索账户名称、交易所或节点"
          clearable
          style="max-width: 300px"
        />
      </div>
      <el-table
        :data="filtered"
        row-key="id"
        empty-text="尚无账户资料，点击“添加账户”开始登记"
        style="width: 100%"
      >
        <el-table-column label="账户名称" min-width="190"
          ><template #default="{ row }"
            ><strong>{{ body(row).name }}</strong>
            <p class="subtle">{{ body(row).notes || "—" }}</p></template
          ></el-table-column
        >
        <el-table-column label="交易所" min-width="120"
          ><template #default="{ row }">{{
            exchanges[body(row).exchange] ?? body(row).exchange
          }}</template></el-table-column
        >
        <el-table-column label="用途" min-width="120"
          ><template #default="{ row }"
            ><span class="state-tag">{{
              body(row).environment === "live" ? "实盘" : "模拟 / 回测"
            }}</span></template
          ></el-table-column
        >
        <el-table-column prop="node_id" label="所属节点" width="110" />
        <el-table-column label="登记状态" min-width="120"
          ><template #default="{ row }">{{
            body(row).enabled ? "已登记 · 未连接" : "已停用资料"
          }}</template></el-table-column
        >
        <el-table-column label="更新时间" min-width="150"
          ><template #default="{ row }">{{
            time(body(row).updated_at_ns)
          }}</template></el-table-column
        >
        <el-table-column label="操作" width="90"
          ><template #default="{ row }"
            ><el-button
              link
              type="primary"
              :disabled="!store.writable"
              @click="open(row)"
              >编辑</el-button
            ></template
          ></el-table-column
        >
      </el-table>
      <div class="table-footer">
        <span class="subtle"
          >登记资料后，连接状态、余额与持仓以账户事实为准。</span
        ><button class="text-link" @click="tab = 'risk'">
          查看风险与事实 →
        </button>
      </div>
    </section>
  </template>
  <el-dialog
    v-model="dialog"
    :title="editing ? '编辑账户' : '添加账户'"
    width="600px"
    :close-on-click-modal="false"
    :close-on-press-escape="!work.busy.value"
    :show-close="!work.busy.value"
  >
    <el-form
      label-position="top"
      :disabled="work.busy.value"
      @submit.prevent="save"
    >
      <el-form-item label="账户名称" required
        ><el-input
          v-model="form.name"
          aria-label="账户名称"
          maxlength="64"
          placeholder="例如：做市研究模拟账户"
      /></el-form-item>
      <div class="catalog-form-grid">
        <el-form-item label="交易所" required
          ><el-select v-model="form.exchange" aria-label="交易所"
            ><el-option
              v-for="(label, value) in exchanges"
              :key="value"
              :label="label"
              :value="value" /></el-select></el-form-item
        ><el-form-item label="用途" required
          ><el-select v-model="form.environment" aria-label="账户用途"
            ><el-option
              v-for="environment in environments"
              :key="environment"
              :value="environment"
              :label="
                environment === 'live' ? '实盘' : '模拟 / 回测'
              " /></el-select
        ></el-form-item>
      </div>
      <el-form-item label="所属节点" required
        ><el-select v-model="form.node_id" aria-label="所属节点"
          ><el-option
            v-for="node in nodes"
            :key="node"
            :label="node"
            :value="node" /></el-select
      ></el-form-item>
      <el-form-item label="凭据引用（可稍后配置）"
        ><el-input
          v-model="form.credential_ref"
          aria-label="凭据引用"
          placeholder="secret://env/ACCOUNT_CREDENTIALS"
        />
        <p class="subtle">
          填写节点已有的凭据引用；密钥由所属节点保管。
        </p></el-form-item
      >
      <el-form-item label="备注"
        ><el-input
          v-model="form.notes"
          aria-label="账户备注"
          maxlength="128"
          placeholder="账户用途或团队说明"
      /></el-form-item>
      <el-form-item label="资料状态"
        ><el-switch
          v-model="form.enabled"
          active-text="启用资料"
          inactive-text="停用资料"
      /></el-form-item>
    </el-form>
    <template #footer
      ><el-button :disabled="work.busy.value" @click="dialog = false"
        >取消</el-button
      ><el-button type="primary" :loading="work.busy.value" @click="save"
        >保存账户</el-button
      ></template
    >
  </el-dialog>
</template>
