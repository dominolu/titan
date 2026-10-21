# Gate GitHub 交付快照验证

2026-10-21。本次发布包含 Gate CrossEx broker、账户/行情工厂、账户事件必要修复、L2 借用解码、公共精确数量换算、实际 V13 回调测量工具及脱敏验证证据。

本地同时存在此前的 CLI/Python 编译链迁移。本次仅纳入测量工具实际依赖的 `titan-strategy-runtime` V13 制品构建接口及其依赖、测试；CLI/Python 前端迁移、项目环境约定及其他迁移文档保持本地未提交状态。README 与 Cargo.lock 按此范围分别暂存，没有整体提交工作区。

## 精确快照

- 基线：`39f1267b6412165fc7adc005719bc9e09dcef9a8`。
- 验证输入来自 Git 暂存树 `f4ba03a69f1d00a0ffbfe7b8ca3e93ae0ee014e1` 的 archive，没有从混合工作区复制待验证代码。
- 编译服务器独立目录：`/home/dominolu/dev/titan-gatecrossex-publish-20261021-f4ba03a6`，Rust/Cargo 1.94.1。复用原编译 target 和 Python 虚拟环境；SDK 路径显式指向该交付快照。
- [发布源码清单](published-source-files.sha256)覆盖 370 个文件，包含暂存版本的 README、Cargo.lock 和已有 fixture，排除验证文档；[逐文件核对](logs/source-check.log)全部通过。
- 下列验证完成后只增加本交付记录及日志，并在本地优化报告增加交付范围链接；产品代码和依赖未再修改。

## 结果

| 命令 | 结果 | 日志 |
| --- | --- | --- |
| `cargo fmt --all -- --check` | 退出 0 | [fmt](logs/fmt.log) |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 退出 0 | [Clippy](logs/clippy.log) |
| `cargo test --locked --workspace --all-targets --all-features` | **508 passed / 0 failed / 12 ignored**，退出 0 | [workspace](logs/workspace.log) |
| `cargo test --locked --workspace --all-features --doc` | **2 passed / 0 failed / 1 ignored**，退出 0 | [doc](logs/doc.log) |

机器汇总见 [results.json](logs/results.json)，原始退出码见 [exit-codes.tsv](logs/exit-codes.tsv)。辅助 `git diff --no-index --check /dev/null 新文件` 返回 1 表示文件存在差异，输出为空，没有空白错误诊断；该项单独保留，不计为构建或测试失败。

[此前本地优化记录](../gate-crossex-local-optimization/README.md)的 512 项来自包含 CLI/Python 迁移的完整工作区；本交付快照没有那 4 项 CLI 迁移测试，因此独立重跑结果为 508。此前实盘和延迟记录继续保留当时的完整源文件摘要、制品摘要和实际覆盖范围，不用当前快照身份替换历史证据。本次发布验证没有重新访问实盘账户或下单。

提交内容不含密钥文件、账户数据库、编译制品或运行缓存。实盘证据沿用实盘服务器脱敏后取回的版本；公开行情原始 CSV 与日志显式纳入版本控制，使报告中的分析可复核。
