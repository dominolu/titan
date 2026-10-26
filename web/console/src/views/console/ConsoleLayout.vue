<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRoute } from "vue-router";
import { ElMessage } from "element-plus";
import { useConsoleStore } from "@/store/modules/console";
import { useUserStoreHook } from "@/store/modules/user";
import ConsoleIcon from "./ConsoleIcon.vue";
import HistoryDrawer from "./HistoryDrawer.vue";
import OperationsPanel from "./OperationsPanel.vue";
import { body } from "./run-view";
import "./workbench.css";
const route = useRoute(),
  store = useConsoleStore(),
  user = useUserStoreHook();
const operationsOpen = ref(false),
  mobileMenu = ref(false),
  refreshing = ref(false);
const content = ref<HTMLElement>();
const workspace = ref<HTMLElement>();
watch(
  () => route.fullPath,
  () => {
    mobileMenu.value = false;
    workspace.value?.scrollTo({ top: 0 });
  },
  { flush: "post" }
);
const navigation = [
  { path: "/runs", label: "运行总览", icon: "monitor" },
  { path: "/research", label: "策略研究", icon: "settings" },
  { path: "/accounts", label: "账户风险", icon: "shield" }
];
const nodes = computed(() => store.resources.filter(r => r.kind === "node"));
const connected = computed(
  () => nodes.value.filter(n => body(n).online === true).length
);
async function refresh() {
  refreshing.value = true;
  try {
    await store.start();
    await store.refresh();
  } catch (failure) {
    ElMessage.error((failure as Error).message);
  } finally {
    refreshing.value = false;
  }
}
async function retry(request: (typeof store.pending)[number]) {
  try {
    await store.retry(request);
  } catch (failure) {
    ElMessage.error((failure as Error).message);
  }
}
onMounted(() =>
  store.start().catch(failure => {
    store.error = failure.message;
  })
);
</script>
<template>
  <div ref="workspace" class="console-workspace">
    <a class="skip-content" href="#" @click.prevent="content?.focus()"
      >跳到主要内容</a
    >
    <button
      v-if="mobileMenu"
      class="nav-mask"
      aria-label="关闭导航"
      @click="mobileMenu = false"
    />
    <aside
      class="workbench-sidebar"
      :class="{ 'mobile-open': mobileMenu }"
      aria-label="管理导航"
    >
      <router-link to="/runs" class="titan-brand" aria-label="Titan 运行总览"
        ><span class="titan-mark">T</span
        ><span>TITAN<small>EXECUTION CONSOLE</small></span></router-link
      >
      <div class="workspace-label">
        <ConsoleIcon name="settings" /><span
          >Titan 工作空间<small>交易管理工作台</small></span
        >
      </div>
      <p class="nav-caption">工作台</p>
      <nav>
        <router-link
          v-for="entry in navigation"
          :key="entry.path"
          :to="entry.path"
          :aria-label="entry.label"
          :class="{
            selected:
              route.path === entry.path ||
              route.path.startsWith(entry.path + '/')
          }"
          @click="mobileMenu = false"
          ><ConsoleIcon :name="entry.icon" /><span>{{
            entry.label
          }}</span></router-link
        >
      </nav>
      <div v-if="store.runs.length" class="recent-runs">
        <p class="nav-caption">运行实例</p>
        <router-link
          v-for="run in store.runs.slice(0, 3)"
          :key="run.id"
          :to="'/runs/' + encodeURIComponent(run.id)"
          @click="mobileMenu = false"
          ><span class="nav-run-dot" /><span :title="run.id">{{
            body(run).instrument || run.id
          }}</span></router-link
        >
      </div>
      <div class="sidebar-health">
        <span>节点可达 {{ connected }} / {{ nodes.length }}</span
        ><small>状态以节点与引擎回执为准</small>
        <p>TITAN ENGINE <span>CONTROL</span></p>
      </div>
    </aside>
    <div class="workbench-shell">
      <header class="workbench-topbar">
        <div class="workbench-crumb">
          <button
            class="mobile-toggle"
            aria-label="展开导航"
            @click="mobileMenu = !mobileMenu"
          >
            ☰</button
          ><router-link to="/runs">工作空间</router-link><span>/</span
          ><span>{{ route.meta.title }}</span>
        </div>
        <div class="topbar-actions">
          <span
            class="connection-pill"
            :class="{ warning: store.connection !== 'connected' }"
            >{{
              store.connection === "connected"
                ? "管理连接正常"
                : "连接中断 / 重同步"
            }}</span
          ><HistoryDrawer /><el-button :loading="refreshing" @click="refresh"
            >刷新</el-button
          ><button
            class="icon-action"
            aria-label="查看操作记录"
            @click="operationsOpen = true"
          >
            <ConsoleIcon name="clock" /></button
          ><el-dropdown @command="user.logOut()"
            ><button class="user-menu">
              <span class="avatar">{{
                user.username.slice(0, 2).toUpperCase()
              }}</span
              ><span>{{ user.username }}</span></button
            ><template #dropdown
              ><el-dropdown-menu
                ><el-dropdown-item command="logout"
                  >退出系统</el-dropdown-item
                ></el-dropdown-menu
              ></template
            ></el-dropdown
          >
        </div>
      </header>
      <main ref="content" class="workbench-content" tabindex="-1">
        <el-alert
          v-if="store.error"
          :title="store.error"
          type="warning"
          :closable="false"
          class="workspace-alert"
        />
        <div v-if="!store.writable" class="notice">
          当前为只读权限，写操作由服务端统一授权。
        </div>
        <section v-if="store.pending.length" class="notice warning">
          <strong>受理结果待确认</strong>
          <div
            v-for="request in store.pending"
            :key="request.idempotency_key"
            class="pending-action"
          >
            <span>{{ request.action.kind }} · {{ request.node_id }}</span
            ><code>{{ request.idempotency_key }}</code
            ><el-button
              :disabled="!store.writable"
              size="small"
              @click="retry(request)"
              >沿用原键查询 / 重试</el-button
            >
          </div>
        </section>
        <router-view :key="route.path" />
        <footer class="workbench-footer">
          <span>Titan 管理工作台</span
          ><span>金额、风控与恢复结论来自业务引擎</span>
        </footer>
      </main>
    </div>
    <el-drawer
      v-model="operationsOpen"
      title="操作记录"
      size="min(1000px,95vw)"
      class="console-drawer"
      ><OperationsPanel
    /></el-drawer>
  </div>
</template>
