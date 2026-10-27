<script setup lang="ts">
import { ref, watch, onBeforeUnmount } from "vue";
import { ElMessage } from "element-plus";
import { consoleApi, type Resource } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
import { body } from "./run-view";
const props = defineProps<{ item?: Resource }>();
const store = useConsoleStore();
const visible = ref(false),
  text = ref(""),
  offset = ref(0),
  more = ref(false),
  busy = ref(false);
let sequence = 0,
  disposed = false;
watch(
  [
    () => props.item?.id,
    () => body(props.item).run_generation ?? body(props.item).worker_session_id,
    () => store.sessionGeneration
  ],
  () => {
    sequence++;
    visible.value = false;
    text.value = "";
    offset.value = 0;
    more.value = false;
    busy.value = false;
  }
);
onBeforeUnmount(() => {
  disposed = true;
  sequence++;
});
async function load(next = false) {
  if (!props.item || busy.value) return;
  const item = props.item,
    generation = store.sessionGeneration,
    runGeneration = body(item).run_generation ?? body(item).worker_session_id,
    request = ++sequence;
  const active = () =>
    !disposed &&
    request === sequence &&
    generation === store.sessionGeneration &&
    item.id === props.item?.id &&
    runGeneration ===
      (body(props.item).run_generation ?? body(props.item).worker_session_id);
  busy.value = true;
  try {
    const result = await consoleApi.log(item, next ? offset.value : 0);
    if (!active()) return;
    const position = Number(result.next_offset);
    if (!Number.isSafeInteger(position) || position < 0 || position > 8_388_608)
      throw new Error("日志偏移超出协议范围");
    text.value = result.text;
    offset.value = position;
    more.value = result.more;
    visible.value = true;
  } catch (failure) {
    if (active()) ElMessage.error((failure as Error).message);
  } finally {
    if (active()) busy.value = false;
  }
}
</script>
<template>
  <el-button :loading="busy" :disabled="!item" @click="load()"
    >宿主日志</el-button
  >
  <el-dialog
    v-model="visible"
    title="宿主日志（每页最多 64 KiB）"
    width="min(1000px,95vw)"
    class="console-drawer"
  >
    <pre>{{ text || "暂无日志" }}</pre>
    <el-button :disabled="busy || !more" @click="load(true)"
      >日志下一页</el-button
    ></el-dialog
  >
</template>
