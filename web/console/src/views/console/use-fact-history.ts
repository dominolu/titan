import { ref, watch, onBeforeUnmount, type Ref } from "vue";
import { consoleApi, type Resource } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";

export function useFactHistory(
  item: Ref<Resource | undefined>,
  kind: Ref<string>
) {
  const store = useConsoleStore();
  const items = ref<any[]>([]),
    provenance = ref<unknown>(null);
  const offset = ref(0),
    pending = ref(false),
    error = ref("");
  let sequence = 0,
    disposed = false;
  const available = () => (item.value?.body as any)?.available === true;
  async function load(next = offset.value) {
    const source = item.value;
    const current = ++sequence,
      generation = store.sessionGeneration,
      wasAvailable = available();
    if (!source) {
      items.value = [];
      pending.value = false;
      return;
    }
    pending.value = true;
    error.value = "";
    const active = () =>
      !disposed &&
      current === sequence &&
      generation === store.sessionGeneration &&
      source.id === item.value?.id &&
      ((source.body as any)?.run_generation ??
        (source.body as any)?.worker_session_id) ===
        ((item.value?.body as any)?.run_generation ??
          (item.value?.body as any)?.worker_session_id);
    try {
      const result = await consoleApi.history(source, kind.value, next);
      if (!active()) return;
      items.value = result.items;
      provenance.value = { source: result.source, recording: result.recording };
      offset.value = next;
    } catch (failure) {
      if (active()) {
        error.value = (failure as Error).message;
        items.value = [];
        provenance.value = null;
      }
    } finally {
      if (active()) {
        pending.value = false;
        if (
          !wasAvailable &&
          available() &&
          (error.value || !items.value.length)
        )
          load(next);
      }
    }
  }
  watch(
    [
      () => item.value?.id,
      () =>
        (item.value?.body as any)?.run_generation ??
        (item.value?.body as any)?.worker_session_id,
      kind,
      () => store.sessionGeneration
    ],
    () => {
      sequence++;
      items.value = [];
      provenance.value = null;
      offset.value = 0;
      error.value = "";
      load(0);
    },
    { immediate: true }
  );
  watch(available, ready => {
    if (ready && !pending.value && (error.value || !items.value.length)) load();
  });
  onBeforeUnmount(() => {
    disposed = true;
    sequence++;
  });
  return { items, provenance, offset, pending, error, load };
}
