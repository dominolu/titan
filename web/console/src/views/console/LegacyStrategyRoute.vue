<script setup lang="ts">
import { onMounted, onBeforeUnmount, ref } from "vue";
import { useRoute, useRouter } from "vue-router";
import { consoleApi } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
const route = useRoute(),
  router = useRouter(),
  store = useConsoleStore();
const error = ref("");
let disposed = false;
onBeforeUnmount(() => {
  disposed = true;
});
onMounted(async () => {
  const generation = store.sessionGeneration;
  try {
    const items = await consoleApi.strategies();
    if (disposed || generation !== store.sessionGeneration) return;
    const tab =
      route.path === "/welcome"
        ? "parameters"
        : ["/versions", "/artifacts"].includes(route.path)
          ? "versions"
          : "backtests";
    await router.replace({
      path:
        items.length === 1
          ? "/strategies/" + encodeURIComponent(items[0].id)
          : "/strategies",
      query: { tab }
    });
  } catch (failure) {
    if (!disposed && generation === store.sessionGeneration)
      error.value = (failure as Error).message;
  }
});
</script>
<template>
  <p class="notice" :class="{ warning: error }">
    {{ error || "正在打开策略详情…" }}
  </p>
</template>
