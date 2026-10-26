import { ref, onBeforeUnmount } from "vue";
import { ElMessage } from "element-plus";
import { useConsoleStore } from "@/store/modules/console";
import type { Action, Operation, Resource } from "@/api/console";

// Request UI only; operation state and idempotency remain in the console store.
export function useConsoleWork() {
  const store = useConsoleStore();
  const busy = ref(false);
  const detail = ref<Resource | Operation | null>(null);
  let disposed = false;
  onBeforeUnmount(() => {
    disposed = true;
  });
  async function perform<T>(
    request: () => Promise<T>,
    accepted?: (value: T) => void
  ) {
    if (busy.value || !store.writable) return;
    const generation = store.generation;
    busy.value = true;
    try {
      const result = await request();
      if (!disposed && generation === store.generation) accepted?.(result);
    } catch (failure) {
      if (!disposed && generation === store.generation)
        ElMessage.error((failure as Error).message);
    } finally {
      if (!disposed) busy.value = false;
    }
  }
  async function submit(input: Action, node: string) {
    await perform(
      () => store.submit(input, node),
      result => {
        detail.value = result;
        ElMessage.success("操作已受理，等待执行方确认");
      }
    );
  }
  return { busy, detail, perform, submit };
}
