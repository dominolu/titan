<script setup lang="ts">
import { computed, ref, onBeforeUnmount } from "vue";
import { ElMessage } from "element-plus";
import { consoleApi, type Resource, type Operation } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
import {
  operationsForRun,
  operationLabels,
  raw,
  pretty,
  time
} from "./run-view";
const props = defineProps<{ run?: Resource; items?: Operation[] }>();
const store = useConsoleStore();
const operations = computed(
  () => props.items ?? operationsForRun(store.operations, props.run)
);
const selectedId = ref("");
const selected = computed(() =>
  store.operations.find(op => op.id === selectedId.value)
);
const busy = ref("");
let disposed = false;
onBeforeUnmount(() => {
  disposed = true;
});
async function cancel(id: string) {
  const identity = store.generation;
  busy.value = id;
  try {
    const result = await consoleApi.cancel(id);
    if (disposed || identity !== store.generation) return;
    store.mergeOperation(result);
    ElMessage.success("取消请求已受理，等待执行确认");
  } catch (failure) {
    if (!disposed && identity === store.generation)
      ElMessage.error((failure as Error).message);
  } finally {
    busy.value = "";
  }
}
</script>
<template>
  <section class="console-card operations-panel">
    <div class="card-heading">
      <h2>
        操作记录 <span class="subtle">{{ operations.length }}</span>
      </h2>
      <span class="subtle">受理与业务完成分别确认</span>
    </div>
    <div class="table-scroll">
      <table class="data-table">
        <thead>
          <tr>
            <th>操作 / 身份</th>
            <th>状态</th>
            <th>更新时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="op in operations" :key="op.id">
            <td>
              <button class="text-link" @click="selectedId = op.id">
                {{ op.input.kind }}</button
              ><span class="cell-note" :title="op.id">{{ op.id }}</span>
            </td>
            <td>
              <span
                class="state-tag"
                :class="{ warning: ['unknown', 'failed'].includes(op.state) }"
                >{{ operationLabels[op.state] }}</span
              ><span v-if="op.cancel_requested" class="cell-note"
                >取消已申请，等待确认</span
              >
            </td>
            <td>{{ time(op.updated_at_ns) }}</td>
            <td>
              <el-button
                v-if="['pending', 'running', 'unknown'].includes(op.state)"
                size="small"
                :disabled="!store.writable || busy === op.id"
                @click="cancel(op.id)"
                >请求取消</el-button
              ><a
                class="text-link"
                :href="consoleApi.file(op.id, 'worker.log')"
                target="_blank"
                rel="noopener"
                >日志</a
              ><button class="text-link" @click="selectedId = op.id">
                查看回执
              </button>
            </td>
          </tr>
          <tr v-if="!operations.length">
            <td colspan="4" class="compact-empty">
              当前缓存中没有相关操作；更早记录可在历史查询中查看。
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
  <el-drawer
    :model-value="!!selected"
    title="来源与实际结果"
    size="min(820px,95vw)"
    class="console-drawer"
    @close="selectedId = ''"
    ><p class="notice">
      操作状态：{{ raw(selected?.state) }}。执行结果以节点及引擎回执为准。
    </p>
    <pre>{{ pretty(selected) }}</pre>
  </el-drawer>
</template>
