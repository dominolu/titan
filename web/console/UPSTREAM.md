# PureAdmin 上游记录

- 仓库：https://github.com/pure-admin/pure-admin-thin
- 提交：`f0ff132561ab684bb78379239adf29f9a38ac7f1`
- 引入日期：2026-10-23。
- 许可证：MIT，原许可证保留在 `LICENSE`。
- 保留布局、主题、组件、Axios、Pinia 与构建基础；替换认证、路由及业务页面，移除 Mock 插件和演示入口。
- 会话使用 Rust HttpOnly Cookie；浏览器不保存访问或刷新 token。构建与验证仅在 `192.168.3.88` 执行。
