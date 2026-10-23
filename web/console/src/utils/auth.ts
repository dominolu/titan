import { useUserStoreHook } from "@/store/modules/user";
import { storageLocal } from "@pureadmin/utils";
import { http, setCsrfToken } from "@/utils/http";
import type { Session } from "@/generated/management";
import { API_VERSION, EXPECTED_BUILD } from "@/generated/management";
export interface DataInfo<T> {
  expires?: T;
  avatar?: string;
  username?: string;
  nickname?: string;
  roles?: string[];
  permissions?: string[];
}
export const userKey = "user-info";
let current: Session | null = null;
let pending: Promise<Session> | null = null;
let generation = 0;
export function setSession(session: Session) {
  if (session.api_version !== API_VERSION || session.build !== EXPECTED_BUILD)
    throw new Error("页面与管理程序版本不匹配，请刷新或回滚配套发布物");
  current = session;
  setCsrfToken(session.csrf_token);
  const info = {
    username: session.identity.username,
    nickname: session.identity.username,
    avatar: "",
    roles: [session.identity.role],
    permissions:
      session.identity.role === "operator"
        ? ["console:read", "console:write"]
        : ["console:read"]
  };
  storageLocal().setItem(userKey, info);
  const store = useUserStoreHook();
  store.SET_USERNAME(info.username);
  store.SET_NICKNAME(info.nickname);
  store.SET_ROLES(info.roles);
  store.SET_PERMS(info.permissions);
}
export function getSession() {
  return current;
}
export async function ensureSession() {
  if (current) return current;
  const attempt = generation;
  if (!pending)
    pending = http
      .get<Session>("/api/v1/session")
      .then(value => {
        if (attempt !== generation) throw new Error("会话已变更");
        setSession(value);
        return value;
      })
      .finally(() => {
        pending = null;
      });
  return pending;
}
export function removeToken() {
  generation++;
  pending = null;
  current = null;
  setCsrfToken("");
  storageLocal().removeItem(userKey);
  localStorage.removeItem("authorized-token");
  document.cookie = "authorized-token=; Max-Age=0; Path=/";
  window.dispatchEvent(new Event("titan:clear"));
}
export const hasPerms = (value: string | string[]) =>
  (Array.isArray(value) ? value : [value]).every(item =>
    useUserStoreHook().permissions.includes(item)
  );
window.addEventListener("titan:session-expired", () => {
  removeToken();
  if (!location.hash.startsWith("#/login")) location.hash = "#/login";
});
