<script setup lang="ts">
import { computed, ref, onBeforeUnmount } from "vue";
import { ElMessage } from "element-plus";
import { consoleApi, type Resource } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
import { body, raw } from "./run-view";
import ConsoleIcon from "./ConsoleIcon.vue";
const props = defineProps<{ item?: Resource }>();
const store = useConsoleStore(),
  busy = ref("");
let disposed = false;
onBeforeUnmount(() => {
  disposed = true;
});
const alerts = computed(() =>
  store.resources.filter(
    r =>
      r.kind === "alert" &&
      (body(r).active || body(r).acknowledgement) &&
      (!props.item ||
        (r.node_id === props.item.node_id &&
          body(r).account === body(props.item).account))
  )
);
async function acknowledge(row: Resource) {
  const identity = store.sessionGeneration;
  busy.value = row.id;
  try {
    const result = await consoleApi.acknowledge(row);
    if (disposed || identity !== store.sessionGeneration) return;
    store.mergeResource(result);
  } catch (failure) {
    if (!disposed && identity === store.sessionGeneration)
      ElMessage.error((failure as Error).message);
  } finally {
    busy.value = "";
  }
}
</script>
<template>
  <section v-for="alert in alerts" :key="alert.id" class="attention-line">
    <ConsoleIcon name="bell" />
    <div>
      <strong
        >{{ raw(body(alert).code) }}
        <span class="subtle">{{
          body(alert).active ? "生效" : "已恢复"
        }}</span></strong
      >
      <p>{{ raw(body(alert).message) }}</p>
      <small
        >{{ alert.node_id }} · {{ raw(body(alert).source) }} ·
        {{ raw(body(alert).account) }}</small
      >
    </div>
    <el-button
      v-if="!body(alert).acknowledgement"
      :disabled="!store.writable || busy === alert.id"
      size="small"
      @click="acknowledge(alert)"
      >确认已知悉</el-button
    ><span v-else class="subtle"
      >已知悉：{{ body(alert).acknowledgement.actor }}</span
    >
  </section>
</template>
