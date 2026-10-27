<script setup lang="ts">
import { computed } from "vue";
import { object, rows, raw, time } from "./run-view";
import { gridObservationCompatible } from "./observation-view";
import GridObservationPanel from "./GridObservationPanel.vue";
const props = defineProps<{
  observation: Record<string, any>;
  stale: boolean;
}>();
const grid = computed(() => gridObservationCompatible(props.observation));
const fields = computed(() =>
  rows(object(props.observation.schema).fields).slice(0, 64)
);
const data = computed(() => object(props.observation.data));
</script>
<template>
  <article class="console-card business-observation">
    <div class="card-heading">
      <h2>业务观测</h2>
      <span class="unit-tag"
        >{{ raw(observation.schema_id) }} / v{{
          raw(observation.schema_version)
        }}</span
      >
    </div>
    <div v-if="observation.available !== true" class="compact-empty">
      {{ observation.reason || "业务观测尚不可用" }}
    </div>
    <template v-else>
      <p v-if="stale || observation.complete !== true" class="notice warning">
        {{
          stale
            ? "当前显示最后确认的业务观测。"
            : "部分字段不可确认，请查看字段诊断。"
        }}
      </p>
      <GridObservationPanel v-if="grid" :observation="observation" />
      <template v-else>
        <p class="subtle">当前协议没有兼容的专用面板，按公开字段定义展示。</p>
        <dl class="risk-lines">
          <div v-for="field in fields" :key="field.name">
            <dt>{{ field.label || field.name }} / {{ field.unit }}</dt>
            <dd>{{ raw(data[field.name]) }}</dd>
          </div>
        </dl>
      </template>
      <p
        v-if="observation.source?.kind === 'runtime_publication'"
        class="subtle"
      >
        来源：运行内存快照 {{ raw(observation.source?.publication_sequence) }} ·
        成功回调水位 {{ raw(observation.source?.callback_sequence) }} · 采集时间
        {{ time(observation.source?.captured_at_ns, true) }}
      </p>
      <p v-else class="subtle">
        历史来源：恢复点 {{ raw(observation.source?.recovery_sequence) }} ·
        事件水位 {{ raw(observation.source?.event_committed_sequence) }}
      </p>
      <details
        v-if="Object.keys(object(observation.field_reasons)).length"
        class="secondary-details"
      >
        <summary>字段诊断</summary>
        <pre>{{ observation.field_reasons }}</pre>
      </details>
    </template>
  </article>
</template>
