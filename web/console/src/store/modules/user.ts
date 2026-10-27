import { defineStore } from "pinia";
import { store, router, resetRouter, routerArrays } from "../utils";
import { useMultiTagsStoreHook } from "./multiTags";
import { setSession, removeToken } from "@/utils/auth";
import { http } from "@/utils/http";
import type { Session } from "@/generated/management";
export const useUserStore = defineStore("pure-user", {
  state: () => ({
    avatar: "",
    username: "",
    nickname: "",
    roles: [] as string[],
    permissions: [] as string[],
    isRemembered: false,
    loginDay: 1
  }),
  actions: {
    SET_AVATAR(value: string) {
      this.avatar = value;
    },
    SET_USERNAME(value: string) {
      this.username = value;
    },
    SET_NICKNAME(value: string) {
      this.nickname = value;
    },
    SET_ROLES(value: string[]) {
      this.roles = value;
    },
    SET_PERMS(value: string[]) {
      this.permissions = value;
    },
    SET_ISREMEMBERED(value: boolean) {
      this.isRemembered = value;
    },
    SET_LOGINDAY(value: number) {
      this.loginDay = value;
    },
    async loginByUsername(data: { username: string; password: string }) {
      const session = await http.post<Session>("/api/v2/session", { data });
      setSession(session);
      return session;
    },
    async logOut() {
      try {
        await http.request("delete", "/api/v2/session");
      } finally {
        this.username = "";
        this.roles = [];
        this.permissions = [];
        removeToken();
        useMultiTagsStoreHook().handleTags("equal", [...routerArrays]);
        resetRouter();
        router.push("/login");
      }
    }
  }
});
export function useUserStoreHook() {
  return useUserStore(store);
}
