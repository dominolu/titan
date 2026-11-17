<script setup lang="ts">
import { computed, ref, onBeforeUnmount, onMounted, watch } from "vue";
import { consoleApi, type StrategySummary } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
import ConsoleIcon from "./ConsoleIcon.vue";
import { useRoute } from "vue-router";
import StrategyUpload from "./StrategyUpload.vue";
import {
  simulationForStrategy,
  simulationMetrics
} from "./simulation-examples";
const route = useRoute();
const store = useConsoleStore();
const project = computed(() =>
  route.meta.project === "live" ? "live" : "backtest"
);
const search = ref("");
const visibleItems = computed(() =>
  items.value.filter(item =>
    `${item.name} ${item.id}`.toLowerCase().includes(search.value.toLowerCase())
  )
);
function path(id: string, tab: string) {
  return {
    path: "/strategies/" + encodeURIComponent(id),
    query: { project: project.value, tab }
  };
}
const exampleFor = (item: StrategySummary) =>
  simulationForStrategy(item.id, item.name);
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
      <p class="eyebrow">
        WORKSPACE /
        {{ project === "live" ? "LIVE STRATEGIES" : "STRATEGY BACKTESTS" }}
      </p>
      <h1>{{ project === "live" ? "实盘策略" : "策略回测" }}</h1>
      <p>
        {{
          project === "live"
            ? "选择策略版本、绑定账户并管理实盘部署与运行"
            : "管理策略源码与参数，编译版本、准备数据并比较回测结果"
        }}
      </p>
    </div>
    <div class="toolbar">
      <el-button :loading="pending" @click="load()">刷新策略</el-button
      ><StrategyUpload @saved="load(0)" />
    </div>
  </section>
  <section class="catalog-context">
    <div>
      <strong>{{
        project === "live" ? "从验证到部署" : "从研究到验证"
      }}</strong>
      <p>
        {{
          project === "live"
            ? "源码登记 → 参数校验 → 编译版本 → 账户授权 → 部署与运行"
            : "源码登记 → 参数校验 → 编译版本 → 历史数据 → 回测与报告"
        }}
      </p>
    </div>
    <el-input
      v-model="search"
      clearable
      aria-label="搜索策略"
      placeholder="搜索当前页的名称或标识"
      style="max-width: 270px"
    />
  </section>
  <p v-if="error" class="notice warning">{{ error }}</p>
  <section class="strategy-grid">
    <article
      v-for="item in visibleItems"
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
        <p class="subtle">
          {{
            project === "live"
              ? "已登记源码 · 进入详情选择版本与部署"
              : "已登记源码 · 进入详情校验与回测"
          }}
        </p>
        <div v-if="exampleFor(item)" class="simulation-card-summary">
          <span class="state-tag">模拟数据</span>
          <template v-if="project === 'backtest'"
            ><strong>{{ exampleFor(item).backtests.length }} 组模拟回测</strong
            ><span
              >基准场景收益
              {{
                simulationMetrics(
                  exampleFor(item).backtests[0].equity
                ).returnPct.toFixed(2)
              }}%</span
            ></template
          >
          <template v-else
            ><strong>{{ exampleFor(item).run.status }}</strong
            ><span
              >模拟收益
              {{
                simulationMetrics(exampleFor(item).run.equity).pnl.toFixed(2)
              }}
              USDT · 持仓 {{ exampleFor(item).run.positions.length }} 笔</span
            ></template
          >
        </div>
      </div>
      <div class="table-footer">
        <router-link class="text-link" :to="path(item.id, 'parameters')"
          >参数与版本 →</router-link
        ><router-link
          class="text-link"
          :to="path(item.id, project === 'live' ? 'live' : 'backtests')"
          >{{
            exampleFor(item)
              ? project === "live"
                ? "查看模拟运行"
                : "查看模拟回测"
              : project === "live"
                ? "部署与运行"
                : "回测与报告"
          }}</router-link
        >
      </div>
    </article>
  </section>
  <div
    v-if="!pending && !visibleItems.length"
    class="console-card compact-empty"
  >
    {{
      search
        ? "当前页没有匹配的策略"
        : "尚无策略，添加或上传单文件 Python 策略开始使用。"
    }}
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
<style scoped>
.simulation-card-summary {
  display: grid;
  justify-items: start;
  gap: 8px;
  margin-top: 18px;
  padding-top: 16px;
  border-top: 1px solid #e8eeeb;
  font-size: 12px;
  color: #536476;
}
</style>
