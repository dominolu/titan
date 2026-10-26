import { onBeforeUnmount } from "vue";
import { useConsoleStore } from "@/store/modules/console";

let nextOwner = 0;

// Each mounted view owns only its selections; kept-alive siblings retain theirs.
export function useResourcePins() {
  const store = useConsoleStore(),
    owner = `view-${++nextOwner}`;
  let disposed = false;
  onBeforeUnmount(() => {
    disposed = true;
    store.unpinResources(owner);
  });
  return (ids: string[]) =>
    disposed ? Promise.resolve() : store.pinResources(ids, owner);
}
