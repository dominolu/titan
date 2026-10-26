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
  const generation = store.generation;
  try {
    const items = await consoleApi.research();
    if (disposed || generation !== store.generation) return;
    const tab =
      route.path === "/welcome"
        ? "parameters"
        : route.path === "/artifacts"
          ? "artifacts"
          : "backtests";
    await router.replace({
      path:
        items.length === 1
          ? "/research/" + encodeURIComponent(items[0].id)
          : "/research",
      query: { tab }
    });
  } catch (failure) {
    if (!disposed && generation === store.generation)
      error.value = (failure as Error).message;
  }
});
</script>
<template>
  <p class="notice" :class="{ warning: error }">
    {{ error || "正在打开策略研究工作区…" }}
  </p>
</template>
