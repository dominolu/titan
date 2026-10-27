<script setup lang="ts">
import { computed } from "vue";
import { useStrategy } from "./use-strategy";
const props = defineProps<{ kind: string }>();
const scope = useStrategy();
const page = computed(() => scope.pages[props.kind]);
</script>
<template>
  <div class="strategy-pager" :data-kind="kind">
    <span v-if="page.error" class="warning-text">{{ page.error }}</span
    ><span v-else class="subtle"
      >策略内查询 · 第 {{ Math.floor(page.offset / 20) + 1 }} 页</span
    >
    <div class="toolbar">
      <el-button
        v-if="page.offset > 0"
        size="small"
        :disabled="page.pending"
        @click="scope.load(kind, Math.max(0, page.offset - 20))"
        >上一页</el-button
      ><el-button size="small" :loading="page.pending" @click="scope.load(kind)"
        >刷新记录</el-button
      ><el-button
        v-if="page.items.length === 20"
        size="small"
        :disabled="page.pending"
        @click="scope.load(kind, page.offset + 20)"
        >下一页</el-button
      >
    </div>
  </div>
</template>
