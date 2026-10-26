<script setup lang="ts">
import { reactive, ref } from "vue";
import { useRouter } from "vue-router";
import { useUserStoreHook } from "@/store/modules/user";
const form = reactive({ username: "", password: "" });
const busy = ref(false);
const error = ref("");
const router = useRouter();
async function login() {
  busy.value = true;
  error.value = "";
  try {
    await useUserStoreHook().loginByUsername(form);
    form.password = "";
    await router.push("/runs");
  } catch (failure) {
    error.value = String((failure as Error).message);
  } finally {
    busy.value = false;
  }
}
</script>
<template>
  <main class="titan-login">
    <el-card class="login-card">
      <p class="eyebrow">TITAN CONTROL</p>
      <h1>交易管理平台</h1>
      <p class="description">配置、编译、回测与运行观察</p>
      <el-alert v-if="error" :title="error" type="error" :closable="false" />
      <el-form label-position="top" @submit.prevent="login">
        <el-form-item label="用户名"
          ><el-input v-model="form.username" autocomplete="username" required
        /></el-form-item>
        <el-form-item label="密码"
          ><el-input
            v-model="form.password"
            type="password"
            autocomplete="current-password"
            show-password
            required
        /></el-form-item>
        <el-button
          type="primary"
          native-type="submit"
          :loading="busy"
          style="width: 100%"
          >登录</el-button
        >
      </el-form>
    </el-card>
  </main>
</template>
<style scoped>
.titan-login {
  min-height: 100vh;
  display: grid;
  place-items: center;
  background: #f2f5fa;
}
.login-card {
  width: min(420px, 90vw);
  padding: 20px;
}
.eyebrow {
  font-size: 12px;
  letter-spacing: 0.16em;
  color: #3658a4;
}
h1 {
  font-size: 28px;
  font-weight: 650;
  margin: 12px 0;
}
.description {
  color: #65728a;
  margin-bottom: 28px;
}
</style>
