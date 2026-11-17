<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref } from "vue";
import { ElMessage } from "element-plus";
import { consoleApi, type StrategySourceInput } from "@/api/console";
import { getSession } from "@/utils/auth";
import { useConsoleStore } from "@/store/modules/console";
import { useConsoleWork } from "./use-console-work";
const emit = defineEmits<{ saved: [id: string] }>();
const store = useConsoleStore(),
  work = useConsoleWork();
const open = ref(false),
  pasted = ref(false),
  fileError = ref("");
const nodes = computed(() => [
  ...new Set(
    store.resources
      .filter(
        item =>
          item.kind === "node" && (item.body as any).environment === "backtest"
      )
      .map(item => item.node_id)
  )
]);
const allowed = computed(
  () =>
    store.writable &&
    getSession()?.identity.environments.includes("backtest") &&
    nodes.value.length > 0
);
const form = reactive<StrategySourceInput>({
  node_id: "",
  strategy_id: "",
  name: "",
  filename: "",
  source: ""
});
const sourceBytes = computed(
  () => new TextEncoder().encode(form.source).length
);
let fileSequence = 0;
onBeforeUnmount(() => {
  fileSequence++;
});
function show(create: boolean) {
  fileSequence++;
  Object.assign(form, {
    node_id: nodes.value[0] ?? "",
    strategy_id: "",
    name: "",
    filename: "",
    source: ""
  });
  pasted.value = create;
  fileError.value = "";
  open.value = true;
}
async function selectFile(event: Event) {
  const sequence = ++fileSequence;
  fileError.value = "";
  const input = event.target as HTMLInputElement,
    file = input.files?.[0];
  form.source = "";
  form.filename = "";
  if (!file) return;
  if (
    !/^[a-zA-Z0-9][a-zA-Z0-9_.-]*\.py$/.test(file.name) ||
    file.size > 262144
  ) {
    fileError.value =
      "请选择 256 KiB 以内的单个 .py 文件，文件名使用英文、数字、下划线或短横线";
    input.value = "";
    return;
  }
  try {
    const source = new TextDecoder("utf-8", { fatal: true }).decode(
      await file.arrayBuffer()
    );
    if (!open.value || sequence !== fileSequence) return;
    form.source = source;
    form.filename = file.name;
    if (!form.strategy_id)
      form.strategy_id = file.name
        .replace(/\.py$/, "")
        .replace(/[^a-zA-Z0-9_-]/g, "-");
    if (!form.name) form.name = file.name.replace(/\.py$/, "");
  } catch {
    if (open.value && sequence === fileSequence)
      fileError.value = "源码必须使用 UTF-8 编码";
  }
}
async function save() {
  if (
    !/^[a-zA-Z0-9_-]{1,128}$/.test(form.strategy_id) ||
    !form.name.trim() ||
    !form.source.trim() ||
    !form.filename.endsWith(".py") ||
    sourceBytes.value > 262144
  ) {
    fileError.value = "请填写策略名称、英文标识和 .py 文件名，并提供源码";
    return;
  }
  fileError.value = "";
  await work.perform(
    () => consoleApi.uploadStrategy({ ...form }),
    result => {
      store.mergeResource(result);
      open.value = false;
      ElMessage.success("策略源码已登记，可进入详情校验参数与编译");
      emit("saved", String((result.body as any).strategy_id));
    }
  );
}
</script>
<template>
  <el-button :disabled="!allowed" @click="show(true)">添加策略</el-button
  ><el-button type="primary" :disabled="!allowed" @click="show(false)"
    >上传策略</el-button
  >
  <el-dialog
    v-model="open"
    :title="pasted ? '添加策略' : '上传策略'"
    width="680px"
    :close-on-click-modal="false"
    :close-on-press-escape="!work.busy.value"
    :show-close="!work.busy.value"
    @closed="form.source = ''"
  >
    <p class="subtle">
      登记单文件 Python
      策略，共用于实盘与回测。登记后可校验参数、编译版本，再选择回测或实盘部署。
    </p>
    <el-form
      label-position="top"
      :disabled="work.busy.value"
      @submit.prevent="save"
    >
      <el-form-item label="构建节点" required
        ><el-select v-model="form.node_id" aria-label="策略构建节点"
          ><el-option
            v-for="node in nodes"
            :key="node"
            :value="node"
            :label="node" /></el-select
      ></el-form-item>
      <div class="catalog-form-grid">
        <el-form-item label="策略名称" required
          ><el-input
            v-model="form.name"
            aria-label="策略名称"
            maxlength="64"
            placeholder="例如：跨所做市策略" /></el-form-item
        ><el-form-item label="策略标识" required
          ><el-input
            v-model="form.strategy_id"
            aria-label="策略标识"
            maxlength="128"
            placeholder="例如：cross_exchange_mm"
        /></el-form-item>
      </div>
      <template v-if="pasted"
        ><el-form-item label="文件名" required
          ><el-input
            v-model="form.filename"
            aria-label="策略文件名"
            placeholder="strategy.py" /></el-form-item
        ><el-form-item label="策略源码" required
          ><el-input
            v-model="form.source"
            aria-label="策略源码"
            type="textarea"
            :rows="10"
            placeholder="粘贴使用 Titan SDK 的单文件 Python 策略" /></el-form-item
      ></template>
      <el-form-item v-else label="策略文件" required
        ><input
          class="source-file-input"
          type="file"
          accept=".py"
          aria-label="策略源码文件"
          :disabled="work.busy.value"
          @change="selectFile"
        />
        <p class="subtle">单个 .py 文件，UTF-8 编码，最大 256 KiB</p>
        <span v-if="form.filename" class="state-tag"
          >{{ form.filename }} · {{ sourceBytes }} bytes</span
        ></el-form-item
      >
    </el-form>
    <p v-if="fileError" role="alert" class="notice warning">{{ fileError }}</p>
    <template #footer
      ><el-button :disabled="work.busy.value" @click="open = false"
        >取消</el-button
      ><el-button type="primary" :loading="work.busy.value" @click="save">{{
        pasted ? "保存策略" : "上传并登记"
      }}</el-button></template
    >
  </el-dialog>
</template>
