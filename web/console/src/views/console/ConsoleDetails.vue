<script setup lang="ts">
import { computed } from "vue";
import { useConsoleStore } from "@/store/modules/console";
import type { Resource, Operation } from "@/api/console";
import { pretty } from "./run-view";
const props = defineProps<{ item: Resource | Operation | null }>();
defineEmits<{ close: [] }>();
const store = useConsoleStore();
const current = computed(() =>
  !props.item
    ? null
    : "input" in props.item
      ? store.operations.find(row => row.id === props.item?.id)
      : store.resources.find(row => row.id === props.item?.id)
);
</script>
<template>
  <el-drawer
    :model-value="!!item"
    title="来源与实际结果"
    size="min(900px,95vw)"
    class="console-drawer"
    @close="$emit('close')"
  >
    <p class="notice">
      管理受理与业务完成分别确认；原始字段、精度和来源完整保留。
    </p>
    <pre>{{ pretty(current) }}</pre>
  </el-drawer>
</template>
