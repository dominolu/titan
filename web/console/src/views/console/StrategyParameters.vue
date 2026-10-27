<script setup lang="ts">
import { computed, nextTick, ref, watch, onBeforeUnmount } from "vue";
import { ElMessage } from "element-plus";
import { consoleApi, type Resource } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";
import { parseExact, integerToken, scalarText } from "@/utils/precision";
import { body, pretty, raw, operationLabels } from "./run-view";
import { useConsoleWork } from "./use-console-work";
import ConsoleDetails from "./ConsoleDetails.vue";
import { useStrategy } from "./use-strategy";
import StrategyHistoryPager from "./StrategyHistoryPager.vue";
import ConsoleIcon from "./ConsoleIcon.vue";
const store = useConsoleStore();
const scope = useStrategy();
const presets = computed(() => scope.options("parameter_preset"));
const versions = computed(() => scope.resources("parameter_version"));
const { busy, detail, perform, submit } = useConsoleWork();
const selectedPreset = ref("");
let editSequence = 0,
  disposed = false;
onBeforeUnmount(() => {
  disposed = true;
  editSequence++;
});
const parameters = ref("{}");
const draft = ref<Resource | null>(null);
const validation = ref("");
const comparison = ref<string[]>([]);
const editor = ref("json");
const query = ref("");
const filter = ref("all");
const preset = computed(() =>
  scope.lookup("parameter_preset", selectedPreset.value)
);
const formIdentity = computed(() =>
  JSON.stringify([
    preset.value?.id,
    preset.value?.revision,
    parameters.value,
    draft.value?.id,
    draft.value?.revision
  ])
);
const describeResult = computed(() =>
  store.operations.find(row => row.id === validation.value)
);
const schema = computed(() => {
  const last = store.operations.find(
    row =>
      row.state === "succeeded" &&
      row.input.kind === "describe" &&
      row.node_id === preset.value?.node_id &&
      row.input.parameter_preset_id === body(preset.value).parameter_preset_id
  );
  return (last?.result as any)?.metadata?.parameter_schema?.properties ?? {};
});
const parameterValues = computed(() => {
  try {
    return parseExact(parameters.value);
  } catch {
    return {};
  }
});
const shown = computed(() =>
  versions.value.filter(
    row =>
      (filter.value === "all" || row.frozen === (filter.value === "frozen")) &&
      `${row.id} ${row.node_id} ${body(row).parameter_preset_id}`
        .toLowerCase()
        .includes(query.value.toLowerCase())
  )
);
const differences = computed(() => {
  const [left, right] = comparison.value.map(id =>
    scope.lookup("parameter_version", id)
  );
  if (!left || !right) return [];
  const a = body(left).parameters ?? {},
    b = body(right).parameters ?? {};
  return [...new Set([...Object.keys(a), ...Object.keys(b)])]
    .filter(
      key => !(key in a) || !(key in b) || pretty(a[key]) !== pretty(b[key])
    )
    .map(key => ({
      key,
      left: a[key] === undefined ? "未设置" : pretty(a[key]),
      right: b[key] === undefined ? "未设置" : pretty(b[key])
    }));
});
watch(
  () => presets.value,
  items => {
    if (!selectedPreset.value && items.length)
      selectedPreset.value = items[0].id;
  },
  { immediate: true }
);
watch(
  () => preset.value?.id,
  () => {
    if (preset.value) {
      parameters.value = pretty(body(preset.value).parameters);
      draft.value = null;
    }
  },
  { immediate: true }
);
watch(formIdentity, () => {
  validation.value = "";
});
watch(
  () => [
    selectedPreset.value,
    draft.value?.id,
    ...comparison.value,
    detail.value && "kind" in detail.value ? detail.value.id : ""
  ],
  ids => {
    const generation = store.sessionGeneration;
    scope.pin("parameters", ids.filter(Boolean) as string[]).catch(error => {
      if (generation === store.sessionGeneration) store.error = error.message;
    });
  },
  { immediate: true }
);
function setParameter(key: string, value: string, type: string) {
  try {
    const values = { ...parameterValues.value };
    if (type === "integer") values[key] = integerToken(value);
    else if (type === "number") {
      const number = Number(value);
      if (!value.trim() || !Number.isFinite(number))
        throw new Error("请输入有限数值");
      values[key] = number;
    } else if (type === "boolean") values[key] = value === "true";
    else values[key] = value;
    parameters.value = pretty(values);
  } catch (failure) {
    ElMessage.error((failure as Error).message);
  }
}
async function validate() {
  if (!preset.value) return;
  const source = preset.value,
    identity = formIdentity.value;
  await perform(
    () =>
      store.submit(
        {
          kind: "describe",
          parameter_preset_id: body(source).parameter_preset_id,
          parameters: parseExact(parameters.value)
        },
        source.node_id
      ),
    op => {
      if (identity === formIdentity.value) validation.value = op.id;
    }
  );
}
async function save(freeze: boolean) {
  if (!preset.value || (freeze && describeResult.value?.state !== "succeeded"))
    return;
  const source = preset.value,
    currentDraft = draft.value,
    identity = formIdentity.value;
  await perform(
    () =>
      consoleApi.save(
        {
          node_id: source.node_id,
          parameter_preset_id: body(source).parameter_preset_id,
          parameters: parseExact(parameters.value),
          expected_revision: currentDraft?.revision ?? null,
          freeze_operation_id: freeze ? validation.value : null
        },
        currentDraft?.id
      ),
    item => {
      store.mergeResource(item);
      if (identity === formIdentity.value)
        draft.value = item.frozen ? null : item;
      ElMessage.success(freeze ? "参数已冻结" : "草稿已保存");
    }
  );
}
async function editVersion(item: Resource, copy: boolean) {
  const sequence = ++editSequence,
    generation = store.sessionGeneration,
    previousPreset = selectedPreset.value,
    previousForm = formIdentity.value;
  const active = () =>
    !disposed &&
    sequence === editSequence &&
    generation === store.sessionGeneration;
  const presetId = `${item.node_id}-template-${body(item).parameter_preset_id}`;
  try {
    await scope.pin("edit-source", [presetId]);
  } catch (failure) {
    if (active()) ElMessage.error((failure as Error).message);
    return;
  }
  if (
    !active() ||
    selectedPreset.value !== previousPreset ||
    formIdentity.value !== previousForm
  )
    return;
  selectedPreset.value = presetId;
  await nextTick();
  if (!active() || selectedPreset.value !== presetId) return;
  parameters.value = pretty(body(item).parameters);
  draft.value = copy ? null : item;
  validation.value = "";
}
</script>
<template>
  <div class="editor-split">
    <section class="console-card parameter-editor">
      <div class="card-heading">
        <div>
          <h2>
            参数工作区 <span v-if="draft" class="unit-tag">编辑草稿</span>
          </h2>
          <p>参数由策略描述接口校验，冻结后不可修改</p>
        </div>
        <span class="state-tag" :class="{ neutral: !describeResult }">{{
          describeResult ? operationLabels[describeResult.state] : "待校验"
        }}</span>
      </div>
      <div class="card-body">
        <label class="field-label">参数预设</label
        ><el-select
          v-model="selectedPreset"
          aria-label="参数预设"
          placeholder="选择节点已登记的参数预设"
          style="width: 100%"
          ><el-option
            v-for="item in presets"
            :key="item.id"
            :value="item.id"
            :label="item.node_id + ' / ' + body(item).parameter_preset_id"
        /></el-select>
        <StrategyHistoryPager kind="parameter_preset" />
      </div>
      <div v-if="!preset" class="compact-empty">
        {{
          selectedPreset
            ? "所选参数预设当前不可用，请重新选择；已编辑参数仍保留。"
            : "尚未收到已授权节点的参数预设目录"
        }}
      </div>
      <template v-else>
        <div class="table-toolbar">
          <div class="view-filters">
            <button
              v-for="[value, label] in [
                ['json', 'JSON 编辑'],
                ['form', '表单编辑']
              ]"
              :key="value"
              :class="{ active: editor === value }"
              @click="editor = value"
            >
              {{ label }}
            </button>
          </div>
          <span class="subtle">{{ draft ? draft.id : "新版本输入" }}</span>
        </div>
        <div class="card-body editor-body">
          <el-input
            v-show="editor === 'json'"
            v-model="parameters"
            type="textarea"
            :rows="16"
            aria-label="参数 JSON"
            class="json-editor"
            :readonly="!store.writable"
          />
          <template v-if="editor === 'form'"
            ><p v-if="!Object.keys(schema).length" class="notice">
              先校验参数获取策略声明的字段；也可使用 JSON 编辑完整输入。
            </p>
            <el-form label-position="top" class="schema-grid"
              ><el-form-item
                v-for="(field, key) in schema"
                :key="key"
                :label="
                  String(key) +
                  ((field as any).description
                    ? ' · ' + (field as any).description
                    : '')
                "
                ><el-select
                  v-if="
                    (field as any).enum || (field as any).type === 'boolean'
                  "
                  :model-value="parameterValues[key]"
                  :disabled="!store.writable"
                  @change="
                    value =>
                      setParameter(
                        String(key),
                        String(value),
                        (field as any).type
                      )
                  "
                  ><el-option
                    v-for="value in (field as any).enum ?? [true, false]"
                    :key="String(value)"
                    :value="value"
                    :label="String(value)" /></el-select
                ><el-input
                  v-else
                  :model-value="scalarText(parameterValues[key])"
                  :readonly="!store.writable"
                  @change="
                    value =>
                      setParameter(String(key), value, (field as any).type)
                  " /></el-form-item></el-form
          ></template>
        </div>
        <div class="table-footer">
          <span>整数输入保留原始精度 · 冻结绑定成功校验回执</span>
          <div class="toolbar">
            <el-button :disabled="!store.writable || busy" @click="save(false)"
              >保存草稿</el-button
            ><el-button :disabled="!store.writable || busy" @click="validate"
              >校验参数</el-button
            ><el-button
              type="primary"
              :disabled="
                !store.writable || busy || describeResult?.state !== 'succeeded'
              "
              @click="save(true)"
              >冻结版本</el-button
            >
          </div>
        </div>
      </template>
    </section>
    <aside class="console-card context-card">
      <div class="card-heading">
        <h2>参数预设与版本来源</h2>
        <ConsoleIcon name="shield" />
      </div>
      <dl class="risk-lines">
        <div>
          <dt>执行节点</dt>
          <dd>{{ raw(preset?.node_id) }}</dd>
        </div>
        <div>
          <dt>参数预设</dt>
          <dd>{{ raw(body(preset).parameter_preset_id) }}</dd>
        </div>
        <div>
          <dt>编辑状态</dt>
          <dd>{{ draft ? "已有草稿" : "新版本" }}</dd>
        </div>
        <div>
          <dt>校验回执</dt>
          <dd>
            {{
              describeResult
                ? operationLabels[describeResult.state]
                : "尚未校验"
            }}
          </dd>
        </div>
      </dl>
      <div class="context-note">
        <strong>冻结输入，再进入构建</strong>
        <p>
          修改输入后需要重新校验。草稿可继续编辑；冻结版本可复制参数生成新版本。
        </p>
      </div>
      <details class="secondary-details" open>
        <summary>源文件摘要</summary>
        <p class="digest-text">{{ raw(body(preset).source_digest) }}</p>
      </details>
      <details v-if="describeResult" class="secondary-details">
        <summary>校验结果与字段定义</summary>
        <pre>{{ pretty(describeResult) }}</pre>
      </details>
    </aside>
  </div>
  <section class="console-card section-gap versions-panel">
    <div class="card-heading">
      <h2>
        参数版本 <span class="subtle">{{ versions.length }}</span>
      </h2>
      <label class="search-box"
        ><ConsoleIcon name="search" /><input
          v-model="query"
          aria-label="搜索参数版本"
          placeholder="搜索版本、节点或参数预设"
      /></label>
    </div>
    <div class="table-toolbar">
      <div class="view-filters">
        <button
          v-for="[value, label] in [
            ['all', '全部'],
            ['frozen', '已冻结'],
            ['draft', '草稿']
          ]"
          :key="value"
          :class="{ active: filter === value }"
          @click="filter = value"
        >
          {{ label }}
        </button>
      </div>
      <el-select
        v-model="comparison"
        multiple
        :multiple-limit="2"
        aria-label="比较参数版本"
        placeholder="选择两个版本比较输入"
        style="width: min(450px, 100%)"
        ><el-option
          v-for="item in versions"
          :key="item.id"
          :value="item.id"
          :label="item.id"
      /></el-select>
    </div>
    <div v-if="comparison.length === 2" class="version-diff">
      <p class="subtle">仅比较参数输入 · {{ comparison.join(" / ") }}</p>
      <div class="table-scroll">
        <table class="data-table">
          <thead>
            <tr>
              <th>参数</th>
              <th>版本一</th>
              <th>版本二</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in differences" :key="row.key">
              <td>{{ row.key }}</td>
              <td>{{ row.left }}</td>
              <td>{{ row.right }}</td>
            </tr>
            <tr v-if="!differences.length">
              <td colspan="3" class="compact-empty">两个版本的参数输入一致</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
    <div class="table-scroll">
      <table class="data-table versions-table">
        <thead>
          <tr>
            <th>版本 / 参数预设</th>
            <th>节点</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in shown" :key="row.id">
            <td>
              <button class="text-link" @click="detail = row">
                {{ row.id }}</button
              ><span class="cell-note">{{
                body(row).parameter_preset_id
              }}</span>
            </td>
            <td>{{ row.node_id }}</td>
            <td>
              <span class="state-tag" :class="{ neutral: !row.frozen }">{{
                row.frozen ? "已冻结" : "草稿"
              }}</span>
            </td>
            <td>
              <el-button
                size="small"
                :disabled="!row.frozen || !store.writable || busy"
                @click="
                  submit(
                    { kind: 'compile', parameter_version_id: row.id },
                    row.node_id
                  )
                "
                >构建</el-button
              ><button class="text-link" @click="editVersion(row, true)">
                复制参数</button
              ><button
                v-if="!row.frozen"
                class="text-link"
                :disabled="!store.writable || busy"
                @click="editVersion(row, false)"
              >
                编辑草稿
              </button>
            </td>
          </tr>
          <tr v-if="!shown.length">
            <td colspan="4" class="compact-empty">
              {{
                versions.length
                  ? "没有匹配的参数版本"
                  : "保存草稿或冻结参数后，版本将在此显示。"
              }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div class="table-footer">
      <span>当前缓存 {{ shown.length }} 个版本 · 更早记录见历史查询</span
      ><router-link class="text-link" :to="scope.path('versions')"
        >查看策略版本 →</router-link
      >
    </div>
  </section>
  <StrategyHistoryPager kind="parameter_version" />
  <ConsoleDetails :item="detail" @close="detail = null" />
</template>
