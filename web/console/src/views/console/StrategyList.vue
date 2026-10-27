<script setup lang="ts">
import { ref, onBeforeUnmount, onMounted, watch } from "vue";
import { consoleApi, type StrategySummary } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
import ConsoleIcon from "./ConsoleIcon.vue";
import { useRoute } from "vue-router";
const route = useRoute();
const store = useConsoleStore();
const items = ref<StrategySummary[]>([]),
  offset = ref(0),
  pending = ref(false),
  error = ref("");
let sequence = 0,
  disposed = false;
async function load(next = offset.value) {
  const request = ++sequence,
    generation = store.sessionGeneration;
  pending.value = true;
  error.value = "";
  try {
    const result = await consoleApi.strategies(next);
    if (
      disposed ||
      request !== sequence ||
      generation !== store.sessionGeneration
    )
      return;
    items.value = result;
    offset.value = next;
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
onMounted(() => load());
watch(
  () => store.ready,
  ready => {
    if (ready && !pending.value) load();
  }
);
const timer = setInterval(() => {
  if (!pending.value && document.visibilityState === "visible") load();
}, 5000);
onBeforeUnmount(() => {
  disposed = true;
  sequence++;
  clearInterval(timer);
});
</script>
<template>
  <section class="page-title">
    <div>
      <p class="eyebrow">WORKSPACE / STRATEGY RESEARCH</p>
      <h1>策略</h1>
      <p>围绕每个策略管理参数版本、策略版本、回测报告和策略实盘</p>
    </div>
    <el-button :loading="pending" @click="load()">刷新策略</el-button>
  </section>
  <p v-if="error" class="notice warning">{{ error }}</p>
  <section class="strategy-grid">
    <article
      v-for="item in items"
      :key="item.id"
      class="console-card strategy-card"
    >
      <div class="card-heading">
        <span class="strategy-symbol"><ConsoleIcon name="settings" /></span
        ><span
          class="state-tag"
          :class="{ warning: item.id === 'unassigned' }"
          >{{ item.parameter_preset_count ? "已登记策略" : "历史记录" }}</span
        >
      </div>
      <div class="card-body">
        <h2>{{ item.name }}</h2>
        <p class="subtle digest-text">{{ item.id }}</p>
        <div class="strategy-counts">
          <div>
            <strong>{{ item.parameter_preset_count }}</strong
            ><span>参数预设</span>
          </div>
          <div>
            <strong>{{ item.resource_count }}</strong
            ><span>参数、版本与运行记录</span>
          </div>
        </div>
        <p class="subtle">版本与参数 · 策略版本 · 回测与报告 · 策略实盘</p>
      </div>
      <div class="table-footer">
        <router-link
          class="text-link"
          :to="{
            path: '/strategies/' + encodeURIComponent(item.id),
            query: { tab: String(route.query.tab ?? 'parameters') }
          }"
          >进入策略详情 →</router-link
        ><router-link
          class="text-link"
          :to="{
            path: '/strategies/' + encodeURIComponent(item.id),
            query: { tab: 'live' }
          }"
          >策略实盘</router-link
        >
      </div>
    </article>
  </section>
  <div v-if="!pending && !items.length" class="console-card compact-empty">
    尚无已授权的策略记录；策略由节点登记。
  </div>
  <div class="strategy-pager">
    <span class="subtle"
      >第 {{ Math.floor(offset / 20) + 1 }} 页 · 由服务端按权限分页</span
    >
    <div class="toolbar">
      <el-button
        :disabled="pending || !offset"
        @click="load(Math.max(0, offset - 20))"
        >上一页</el-button
      ><el-button
        :disabled="pending || items.length < 20"
        @click="load(offset + 20)"
        >下一页</el-button
      >
    </div>
  </div>
</template>
