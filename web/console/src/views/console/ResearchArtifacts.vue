<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useConsoleStore } from "@/store/modules/console";
import { consoleApi } from "@/api/console";
import { body, raw } from "./run-view";
import { useResearch } from "./use-research";
import { useConsoleWork } from "./use-console-work";
import ConsoleIcon from "./ConsoleIcon.vue";
import ConsoleDetails from "./ConsoleDetails.vue";
import ResearchPager from "./ResearchPager.vue";
const store = useConsoleStore(),
  scope = useResearch();
const { busy, detail, submit } = useConsoleWork();
const query = ref(""),
  selectedVersion = ref("");
const frozen = computed(() =>
  scope.options("version").filter(item => item.frozen)
);
const version = computed(() => scope.lookup("version", selectedVersion.value));
watch(
  () => [
    selectedVersion.value,
    detail.value && "kind" in detail.value ? detail.value.id : ""
  ],
  ids => {
    const generation = store.generation;
    scope.pin("builds", ids.filter(Boolean)).catch(error => {
      if (generation === store.generation) store.error = error.message;
    });
  },
  { immediate: true }
);
const shown = computed(() =>
  scope
    .resources("artifact")
    .filter(row =>
      `${row.id} ${row.node_id} ${body(row).artifact_digest}`
        .toLowerCase()
        .includes(query.value.toLowerCase())
    )
);
</script>
<template>
  <section class="console-card">
    <div class="card-heading">
      <div>
        <h2>构建策略版本</h2>
        <p>编译产物归属于固定源码与参数版本，可供回测及实盘复用</p>
      </div>
      <span class="unit-tag">编译节点执行</span>
    </div>
    <div class="card-body toolbar">
      <el-select
        v-model="selectedVersion"
        aria-label="待构建版本"
        placeholder="选择已冻结的参数版本"
        style="width: min(600px, 100%)"
        ><el-option
          v-for="item in frozen"
          :key="item.id"
          :value="item.id"
          :label="item.node_id + ' / ' + item.id" /></el-select
      ><el-button
        type="primary"
        :disabled="!version?.frozen || !store.writable || busy"
        @click="
          submit({ kind: 'build', version_id: version.id }, version.node_id)
        "
        >构建版本</el-button
      >
    </div>
    <ResearchPager kind="version" />
  </section>
  <section class="console-card section-gap">
    <div class="card-heading">
      <h2>
        编译产物
        <span class="subtle">{{ scope.resources("artifact").length }}</span>
      </h2>
      <label class="search-box"
        ><ConsoleIcon name="search" /><input
          v-model="query"
          aria-label="搜索构建制品"
          placeholder="搜索制品、摘要或节点"
      /></label>
    </div>
    <div class="table-scroll">
      <table class="data-table artifacts-table">
        <thead>
          <tr>
            <th>制品</th>
            <th>节点 / 输入版本</th>
            <th>摘要</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in shown" :key="row.id">
            <td>
              <button class="text-link" @click="detail = row">
                {{ row.id }}
              </button>
            </td>
            <td>
              {{ row.node_id
              }}<span class="cell-note">{{ raw(body(row).version_id) }}</span>
            </td>
            <td>
              <span
                class="digest-cell"
                :title="raw(body(row).artifact_digest)"
                >{{ raw(body(row).artifact_digest) }}</span
              >
            </td>
            <td>
              <a
                class="text-link"
                :href="
                  consoleApi.file(body(row).operation_id, 'strategy.titan')
                "
                target="_blank"
                rel="noopener"
                >下载</a
              ><router-link
                class="text-link"
                :to="scope.path('backtests', { artifact: row.id })"
                >用于回测</router-link
              ><router-link
                class="text-link"
                :to="scope.path('live', { artifact: row.id })"
                >用于实盘</router-link
              >
            </td>
          </tr>
          <tr v-if="!shown.length">
            <td colspan="4" class="compact-empty">
              {{
                scope.resources("artifact").length
                  ? "没有匹配的制品"
                  : "在策略参数页冻结版本后构建"
              }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div class="table-footer">
      <span>仅展示后端登记的制品 · 完整身份见来源详情</span>
    </div>
  </section>

  <ResearchPager kind="artifact" /><ConsoleDetails
    :item="detail"
    @close="detail = null"
  />
</template>
