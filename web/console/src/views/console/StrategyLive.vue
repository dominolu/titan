<script setup lang="ts">
import { computed, ref, watch, onBeforeUnmount } from "vue";
import { ElMessageBox } from "element-plus";
import type { Resource } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
import { body, raw, pretty, runView, decimal } from "./run-view";
import { useConsoleWork } from "./use-console-work";
import ConsoleIcon from "./ConsoleIcon.vue";
import ConsoleDetails from "./ConsoleDetails.vue";
import { useResearch } from "./use-research";
import ResearchPager from "./ResearchPager.vue";
import RunDetail from "./RunDetail.vue";
import { useRoute, useRouter } from "vue-router";
const store = useConsoleStore();
const scope = useResearch(),
  route = useRoute(),
  router = useRouter();
const artifacts = computed(() => scope.options("artifact"));
const nodes = computed(() =>
  store.resources.filter(row => row.kind === "node")
);
const entries = computed(() =>
  scope
    .resources("run")
    .map(item => ({ item, view: runView(item, store.connection, nodes.value) }))
);
const selectedRun = computed(() =>
  scope.lookup("run", String(route.query.run ?? ""))
);
const selectionError = ref("");
const { busy, detail, submit } = useConsoleWork();
const selectedArtifact = ref(""),
  selectedBinding = ref("");
const confirming = ref(false);
let disposed = false;
onBeforeUnmount(() => {
  disposed = true;
});
watch(
  () => [route.query.run, route.query.artifact],
  async () => {
    selectionError.value = "";
    const runId = String(route.query.run ?? ""),
      artifactId = String(route.query.artifact ?? "");
    const generation = store.generation;
    if (artifactId) selectedArtifact.value = "";
    try {
      await scope.pin("live-link", [runId, artifactId].filter(Boolean));
      if (
        generation !== store.generation ||
        runId !== String(route.query.run ?? "") ||
        artifactId !== String(route.query.artifact ?? "")
      )
        return;
      if (artifactId) selectedArtifact.value = artifactId;
    } catch (failure) {
      if (
        generation === store.generation &&
        runId === String(route.query.run ?? "") &&
        artifactId === String(route.query.artifact ?? "")
      )
        selectionError.value = (failure as Error).message;
    }
  },
  { immediate: true, flush: "post" }
);
const bindings = computed(() =>
  store.resources.filter(
    row => row.kind === "binding" && body(row).control_enabled === true
  )
);
watch(selectedBinding, id => {
  scope.pinCatalog("live-catalog", [id]).catch(error => {
    if (!disposed) store.error = error.message;
  });
});
const binding = computed(() =>
  bindings.value.find(row => row.id === selectedBinding.value)
);
const artifact = computed(() =>
  scope.lookup("artifact", selectedArtifact.value)
);
const deployments = computed(() => scope.resources("deployment"));
watch(
  () => [
    selectedArtifact.value,
    detail.value && "kind" in detail.value ? detail.value.id : ""
  ],
  ids => {
    const generation = store.generation;
    scope
      .pin(
        "live-form",
        ids
          .filter(Boolean)
          .filter(id => id !== selectedBinding.value) as string[]
      )
      .catch(error => {
        if (!disposed && generation === store.generation)
          store.error = error.message;
      });
  },
  { immediate: true }
);
const bindingFor = (item: Resource) =>
  bindings.value.find(
    row =>
      row.node_id === item.node_id &&
      body(row).binding_id === body(item).binding_id
  );
async function deploy() {
  if (!artifact.value || !binding.value) return;
  await submit(
    {
      kind: "deploy",
      artifact_id: artifact.value.id,
      binding_id: body(binding.value).binding_id
    },
    binding.value.node_id
  );
}
async function startDeployment(item: Resource) {
  if (!store.writable || busy.value || confirming.value || !bindingFor(item))
    return;
  const generation = store.generation,
    identity = JSON.stringify(item),
    bindingIdentity = JSON.stringify(bindingFor(item));
  const state = body(item);
  confirming.value = true;
  let confirmed = false;
  try {
    await ElMessageBox.confirm(
      `账户 ${state.account}，品种 ${state.instrument}，最大敞口 ${state.max_exposure_quote}，价格保护 ${state.max_price_deviation_bps} bps，期限 ${state.max_duration_seconds} 秒，停止方式 ${state.stop_action ?? "由引擎确认"}。`,
      "确认启动",
      { type: "warning", customClass: "console-confirmation" }
    );
    confirmed = true;
  } catch {
    /* Dismissal does not submit an operation. */
  } finally {
    confirming.value = false;
  }
  const current = deployments.value.find(row => row.id === item.id);
  if (
    !confirmed ||
    disposed ||
    generation !== store.generation ||
    !store.writable ||
    !current ||
    JSON.stringify(current) !== identity ||
    JSON.stringify(bindingFor(current)) !== bindingIdentity
  )
    return;
  await submit({ kind: "start", deployment_id: current.id }, current.node_id);
}
</script>
<template>
  <p v-if="selectionError" class="notice warning">{{ selectionError }}</p>
  <RunDetail
    v-if="route.query.run && selectedRun"
    :run-id="selectedRun.id"
    embedded
    @close="router.replace(scope.path('live'))"
  />
  <div v-else-if="route.query.run" class="notice warning">
    正在核对实盘实例归属；无法核对时不会显示其他实例。
  </div>
  <template v-else>
    <section class="metric-grid">
      <article class="metric-card">
        <p>可部署编译产物</p>
        <strong>{{ artifacts.length }}<small>当前页</small></strong
        ><span>固定策略版本与摘要</span>
      </article>
      <article class="metric-card">
        <p>授权实盘绑定</p>
        <strong>{{ bindings.length }}<small>个</small></strong
        ><span>最终适配由节点与引擎校验</span>
      </article>
      <article class="metric-card">
        <p>部署记录</p>
        <strong>{{ deployments.length }}<small>当前页</small></strong
        ><span>部署与启动分别确认</span>
      </article>
      <article class="metric-card">
        <p>实盘实例</p>
        <strong>{{ entries.length }}<small>当前页</small></strong
        ><span>按账户、品种与代次分别展示</span>
      </article>
    </section>
    <div class="editor-split">
      <section class="console-card deployment-form">
        <div class="card-heading">
          <div>
            <h2>受控部署</h2>
            <p>选择构建制品与已授权的目标绑定</p>
          </div>
          <span class="unit-tag">固定制品身份</span>
        </div>
        <div class="card-body">
          <div class="form-stack">
            <label class="field-label">构建制品</label
            ><el-select
              v-model="selectedArtifact"
              aria-label="部署制品"
              placeholder="选择已构建制品"
              ><el-option
                v-for="item in artifacts"
                :key="item.id"
                :value="item.id"
                :label="item.id" /></el-select
            ><label class="field-label">目标绑定</label
            ><el-select
              v-model="selectedBinding"
              aria-label="部署绑定"
              placeholder="选择已授权实盘绑定"
              ><el-option
                v-for="item in bindings"
                :key="item.id"
                :value="item.id"
                :label="`${item.node_id} / ${body(item).account} / ${body(item).instrument}`"
            /></el-select>
          </div>
          <div v-if="binding" class="binding-preview">
            <span class="state-tag">授权边界</span>
            <dl class="parameter-grid">
              <div
                v-for="[label, value] in [
                  ['账户', body(binding).account],
                  ['交易品种', body(binding).instrument],
                  ['最大敞口', body(binding).max_exposure_quote],
                  ['价格保护 / bps', body(binding).max_price_deviation_bps],
                  ['运行期限 / 秒', body(binding).max_duration_seconds],
                  ['停止方式', body(binding).stop_action]
                ]"
                :key="String(label)"
              >
                <dt>{{ label }}</dt>
                <dd>{{ raw(value) }}</dd>
              </div>
            </dl>
            <details class="secondary-details">
              <summary>完整绑定定义</summary>
              <pre>{{ pretty(binding) }}</pre>
            </details>
          </div>
        </div>
        <div class="table-footer">
          <span>部署不会自动启动策略</span
          ><el-button
            type="primary"
            :disabled="!store.writable || busy || !artifact || !binding"
            @click="deploy"
            >部署制品</el-button
          >
        </div>
      </section>
      <aside class="console-card context-card">
        <div class="card-heading">
          <h2>制品来源</h2>
          <ConsoleIcon name="box" />
        </div>
        <dl class="risk-lines">
          <div>
            <dt>构建节点</dt>
            <dd>{{ raw(artifact?.node_id) }}</dd>
          </div>
          <div>
            <dt>输入版本</dt>
            <dd>{{ raw(body(artifact).version_id) }}</dd>
          </div>
        </dl>
        <div class="context-note">
          <strong>部署前核对身份</strong>
          <p>
            节点校验签名、ABI
            与制品摘要，启动前确认账户风险边界。执行结果见同一管理操作回执。
          </p>
        </div>
        <details open class="secondary-details">
          <summary>制品 SHA256</summary>
          <p class="digest-text">{{ raw(body(artifact).artifact_digest) }}</p>
        </details>
        <button
          v-if="artifact"
          class="text-link card-link"
          @click="detail = artifact"
        >
          查看完整制品来源 →
        </button>
      </aside>
    </div>
    <section class="console-card section-gap">
      <div class="card-heading">
        <h2>
          部署版本 <span class="subtle">{{ deployments.length }}</span>
        </h2>
        <span class="subtle">启动前再次确认授权边界</span>
      </div>
      <div class="table-scroll">
        <table class="data-table deployments-table">
          <thead>
            <tr>
              <th>部署 / 节点</th>
              <th>账户 / 品种</th>
              <th>风险边界</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in deployments" :key="row.id">
              <td>
                <button class="text-link" @click="detail = row">
                  {{ row.id }}</button
                ><span class="cell-note">{{ row.node_id }}</span>
              </td>
              <td>
                {{ body(row).account
                }}<span class="cell-note">{{ body(row).instrument }}</span>
              </td>
              <td>
                最大敞口 {{ raw(body(row).max_exposure_quote)
                }}<span class="cell-note"
                  >{{ raw(body(row).max_price_deviation_bps) }} bps ·
                  {{ raw(body(row).max_duration_seconds) }} 秒</span
                >
              </td>
              <td>
                <el-button
                  size="small"
                  :disabled="
                    !store.writable || busy || confirming || !bindingFor(row)
                  "
                  @click="startDeployment(row)"
                  >启动</el-button
                ><button class="text-link" @click="detail = row">
                  查看来源
                </button>
              </td>
            </tr>
            <tr v-if="!deployments.length">
              <td colspan="4" class="compact-empty">
                尚无部署版本；选择制品与绑定后提交部署。
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
    <ResearchPager kind="deployment" />
    <section class="console-card section-gap">
      <div class="card-heading">
        <h2>策略实盘实例</h2>
        <span class="subtle">当前策略 · 运行状态以引擎回执为准</span>
      </div>
      <div class="table-scroll">
        <table class="data-table live-runs-table">
          <thead>
            <tr>
              <th>账户 / 品种</th>
              <th>状态 / 代次</th>
              <th>含在途敞口上界</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="entry in entries" :key="entry.item.id">
              <td>
                {{ raw(entry.view.state.account)
                }}<span class="cell-note">{{
                  raw(entry.view.state.instrument)
                }}</span>
              </td>
              <td>
                <span class="state-tag" :class="{ warning: entry.view.stale }"
                  >{{ entry.view.stale ? "最后确认 · " : ""
                  }}{{ entry.view.status }}</span
                ><span class="cell-note"
                  >代次 {{ raw(entry.view.state.generation) }}</span
                >
              </td>
              <td>{{ decimal(entry.view.bound) }} {{ entry.view.currency }}</td>
              <td>
                <router-link
                  class="text-link"
                  :to="scope.path('live', { run: entry.item.id })"
                  >查看实盘 / 生命周期 →</router-link
                ><span class="cell-note" :title="entry.item.id">{{
                  entry.item.id
                }}</span>
              </td>
            </tr>
            <tr v-if="!entries.length">
              <td colspan="4" class="compact-empty">
                该策略尚无当前页可见的实盘实例；部署后确认启动。
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
    <ResearchPager kind="run" />
  </template>
  <ConsoleDetails :item="detail" @close="detail = null" />
</template>
