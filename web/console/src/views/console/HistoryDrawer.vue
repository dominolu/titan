<script setup lang="ts">
import { ref } from "vue";
import { http } from "@/utils/http";
import type { Operation, Resource } from "@/api/console";
const opened = ref(false);
const busy = ref(false);
const error = ref("");
const kind = ref("operations");
const page = ref(0);
const items = ref<Array<Operation | Resource>>([]);
const selected = ref<unknown>(null);
const from = ref<Date | null>(null);
const to = ref<Date | null>(null);
const run = ref("");
const types = [
  ["operations", "管理操作"],
  ["parameter_version", "参数版本"],
  ["strategy_version", "策略版本"],
  ["backtest", "回测结果"],
  ["run", "运行实例"],
  ["account", "账户快照"],
  ["alert", "告警"]
];
async function load(next = 0) {
  busy.value = true;
  error.value = "";
  try {
    items.value = await http.get<Array<Operation | Resource>>(
      kind.value === "operations" ? "/api/v2/operations" : "/api/v2/resources",
      {
        params: {
          kind: kind.value,
          offset: next * 20,
          limit: 20,
          from_ns: from.value
            ? (BigInt(from.value.getTime()) * 1000000n).toString()
            : undefined,
          to_ns: to.value
            ? (BigInt(to.value.getTime()) * 1000000n).toString()
            : undefined,
          run_id: run.value || undefined
        }
      }
    );
    page.value = next;
  } catch (failure) {
    error.value = (failure as Error).message;
  } finally {
    busy.value = false;
  }
}
function show() {
  opened.value = true;
  load();
}
</script>
<template>
  <el-button @click="show">历史查询</el-button>
  <el-drawer
    v-model="opened"
    title="按服务端权限分页查询"
    size="min(1000px,95vw)"
  >
    <el-alert v-if="error" :title="error" type="error" />
    <el-form inline
      ><el-form-item label="类型"
        ><el-select v-model="kind" style="width: 150px"
          ><el-option
            v-for="[value, label] in types"
            :key="value"
            :value="value"
            :label="label" /></el-select></el-form-item
      ><template v-if="kind === 'operations'"
        ><el-form-item label="开始"
          ><el-date-picker v-model="from" type="datetime" /></el-form-item
        ><el-form-item label="结束"
          ><el-date-picker v-model="to" type="datetime" /></el-form-item
        ><el-form-item label="运行 ID"
          ><el-input v-model="run" /></el-form-item></template
      ><el-button :loading="busy" @click="load()">查询</el-button></el-form
    >
    <el-table :data="items" @row-click="item => (selected = item)"
      ><el-table-column
        prop="id"
        label="身份"
        min-width="260"
        show-overflow-tooltip
      /><el-table-column prop="node_id" label="节点" /><el-table-column
        label="状态"
        ><template #default="{ row }">{{
          row.state ?? (row.frozen ? "已冻结" : row.kind)
        }}</template></el-table-column
      ></el-table
    >
    <div class="pager">
      <el-button :disabled="page === 0 || busy" @click="load(page - 1)"
        >上一页</el-button
      ><span>第 {{ page + 1 }} 页 · 每页 20 条</span
      ><el-button :disabled="items.length < 20 || busy" @click="load(page + 1)"
        >下一页</el-button
      >
    </div>
    <pre v-if="selected">{{ JSON.stringify(selected, null, 2) }}</pre>
  </el-drawer>
</template>
<style scoped>
.pager {
  display: flex;
  gap: 15px;
  align-items: center;
  margin: 20px 0;
}
pre {
  font: 12px/1.7 monospace;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}
</style>
