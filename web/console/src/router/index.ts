import { createRouter, type RouteRecordRaw } from "vue-router";
import home from "./modules/home";
import remaining from "./modules/remaining";
import { getHistoryMode } from "./utils";
import { ensureSession } from "@/utils/auth";
import { usePermissionStoreHook } from "@/store/modules/permission";
import NProgress from "@/utils/progress";
export const constantRoutes = [home] as RouteRecordRaw[];
export const constantMenus = [home, ...remaining];
export const remainingPaths = remaining.map(route => route.path);
export const router = createRouter({
  history: getHistoryMode(import.meta.env.VITE_ROUTER_HISTORY),
  routes: [...constantRoutes, ...remaining] as RouteRecordRaw[],
  scrollBehavior: () => ({ top: 0 })
});
export function resetLoadedPaths() {}
export function resetRouter() {
  usePermissionStoreHook().clearAllCachePage();
}
router.beforeEach(async to => {
  NProgress.start();
  if (to.path === "/login") return true;
  try {
    await ensureSession();
    usePermissionStoreHook().handleWholeMenus([]);
    document.title = String(to.meta.title ?? "管理平台") + " | Titan";
    return true;
  } catch {
    return "/login";
  }
});
router.afterEach(() => NProgress.done());
export default router;
