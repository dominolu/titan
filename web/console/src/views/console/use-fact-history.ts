import { ref, watch, onBeforeUnmount, type Ref } from "vue";
import { consoleApi, type Resource } from "@/api/console";
import { useConsoleStore } from "@/store/modules/console";

export function useFactHistory(
  item: Ref<Resource | undefined>,
  kind: Ref<string>,
  legacy?: Ref<boolean>,
  newestFirst = false
) {
  const store = useConsoleStore();
  const items = ref<any[]>([]),
    provenance = ref<unknown>(null);
  const offset = ref(0),
    pending = ref(false),
    error = ref("");
  const hasNext = ref(false);
  const cursors = new Map<number, unknown>();
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
      const result = legacy?.value
        ? await consoleApi.history(source, kind.value, next)
        : await consoleApi.facts(
            source,
            kind.value,
            next === 0 ? undefined : cursors.get(next),
            newestFirst
          );
      if (!active()) return;
      items.value = result.items;
      const page = result as typeof result & {
        next_cursor?: unknown;
        upper_row?: string;
      };
      hasNext.value = legacy?.value
        ? result.items.length === 20
        : Boolean(page.next_cursor);
      if (page.next_cursor) cursors.set(next + 20, page.next_cursor);
      provenance.value = {
        source: result.source,
        recording: result.recording,
        upper_row: page.upper_row
      };
      offset.value = next;
    } catch (failure) {
      if (active()) {
        error.value = (failure as Error).message;
        hasNext.value = false;
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
      () => legacy?.value,
      () => store.sessionGeneration
    ],
    () => {
      sequence++;
      items.value = [];
      provenance.value = null;
      offset.value = 0;
      cursors.clear();
      hasNext.value = false;
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
  return { items, provenance, offset, pending, error, hasNext, load };
}
