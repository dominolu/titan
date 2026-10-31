# GitHub 提交前的跨所做市兼容验证

日期：2026-10-31。主干基线：`732c4e6322e1ccafcc9437d4c9e5acf4b7f678ef`。

此次将先前保留在工作区的跨所做市策略、独立测试、研究设计、历史验收记录及问题/重构笔记提交 GitHub，不修改策略业务实现。两份旧会话笔记补充历史范围说明，避免将其权限、待办和问题状态当成当前主干状态。

策略和专项测试的 SHA256 与既有跨所做市验收记录一致。本轮另外在编译服务器 `192.168.3.88`，使用刚完成分支合并验证的 Rust CLI 与主干 SDK，运行专项回归：

```bash
TITAN_STRATEGY_PYTHON=/home/dominolu/dev/titan/.venv/bin/python \
TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk" \
PYTHONPATH="$PWD:$PWD/python/titan-strategy-sdk" \
TITAN_TEST_BINARY=/tmp/titan-single-file-target/debug/titan \
/home/dominolu/dev/titan/.venv/bin/python -m unittest discover \
  -s python/titan-strategy-sdk/tests -p test_cross_exchange_mm.py -v
```

结果：**35 项全部通过，9.503 秒，退出码 0**。包含 30 项业务测试和 5 项真实原生编译/回调及 CLI 正负回放测试。见[测试日志](cross-exchange-mm-tests.txt)及[环境与摘要](environment.txt)。本轮不将先前固定旧基线的 150 项 SDK 验证当成本轮全量 SDK 结果。

当前 Rust 平台的全工作区检查、Clippy 及 788 项 Rust/2 项 doctest 结果见[前一轮分支合并验证](../merge-all-branches/README.md)；本轮只新增策略/文档，没有再次修改或重跑 Rust 平台。

策略仍保留既定能力边界：双市场受控执行门禁尚未扩大，Stop 可以保留匹配的跨所仓位，未访问真实账户、未实盘下单、未部署。研究设计及实施计划保留其原始基线与待实施状态，不按“本次已提交”宣称其中能力已经完成。
