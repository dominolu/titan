# Review R1–R7 修复与非实盘验证

本轮依据 `docs/system_refactor_checklist.md` 的 7 项 review 发现修复通用账户、执行及策略宿主逻辑。未把恢复或账户模式兼容逻辑移入 pair_arb。原始 review 及负向探针保留在相邻的 `review/` 目录。

| 编号 | 修复 | 回归覆盖 |
| --- | --- | --- |
| R1 | AccountConnector 提供复合恢复快照；AccountServiceCore 委托连接器捕获；真实 AccountRuntime 在同一投影锁下读取订单、持仓、余额并使用共同提交版本。分类最后更新版本继续保留，一致性条件仍检查账户、Ready、非零 epoch 和共同版本。 | 使用真实 AccountProjection 的 bootstrap 数据，经实际 AccountRuntime 接口返回复合快照；订单/持仓分类版本不同仍可恢复。 |
| R2 | 所有 invoke 命令提交失败统一 fail_runtime；timer 的回调、checkpoint、排空和入队错误关闭 activation 并保存错误。意图 flush 成功前不得调用 broker。 | 原生 AOT timer 下单，注入意图 flush 失败，断言 Failed/closed，mock broker 被调用即失败；纳入标准验证脚本。 |
| R3 | 拒绝同步回调中的嵌套 sequenced 发布，涵盖同来源、跨来源和跨引擎；保留来源顺序锁和无来源序号的发布。 | 两个来源并发互相发布，嵌套调用明确返回 ReentrantSource，两条外层发布在有界等待内完成；保留原有同来源保护测试。 |
| R4 | 每个 submit future 捕获 revision/epoch 及共享准入标志；首次 poll 复查，三家原生 BrokerApi 在请求准备后、网络传输准入前再次检查。明确未发送的拒绝清理 pending 映射，已准入或结果未知的请求继续对账。 | 实际 direct_submit future 延迟 poll 后停止或完整重连必须拒绝；旧 epoch、ready 失效和异步准备后的 guard 复查。 |
| R5 | 有账户绑定时始终添加宿主所需六类账户事实，独立于策略回调声明；合并路由并避免重复。 | BBO-only manifest 的六类订阅及 schema/routing 回归；ServiceCore 实际创建并恢复带账户的 BBO-only AOT 策略，推送账户失效后暂停并关闭 activation。 |
| R6 | 从交易所读取持仓模式，不修改账户模式；Binance、OKX 接受已支持的双向模式；空 Net 建一条零仓，空 Hedge 建 LONG/SHORT 两条零仓，未知模式且无事实则拒绝。合成记录 exchange_ts=0，不使用本地时钟伪造交易所覆盖边界。 | Net/Hedge REST 空列表、零仓和单边仓；真实 bootstrap 后接收较早时间的 LONG/SHORT 缓冲事实；重复或矛盾方向仍拒绝；账户模式校验测试。 |
| R7 | Hyperliquid 必须订阅并确认 clearinghouseState 完整账户流；完整图片提供数量、均价、未实现盈亏、清算价；缺失已绑定币种明确归零。删除仅推数量的 fill 推算和异步初始局部 REST 持仓发布，保留 OrderUpdates 成交处理。 | 完整开仓/增仓字段更新、quantity-only fill 不覆盖全量事实、清算价清空和空图片平仓；缺字段或错误 user 拒绝；新增私有订阅确认条件。 |

R4 的传输准入点位于原生 broker 完成异步准备之后、进入网络请求之前；准入后并发发生失效的请求属于在途请求，不能伪称未发送。外部实现若在默认 submit_order_fenced 内仍有异步准备，需像三家原生实现一样覆盖此方法并在准备后调用 guard。

R7 的完整流格式依据 [Hyperliquid 官方 WebSocket subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions) 中的 clearinghouseState subscription 及 ClearinghouseState envelope。该链路尚未在真实账户上验收。

所有格式化、Clippy、构建、测试和 AOT 编译均在 `192.168.3.88` 的 `/home/dominolu/dev/titan-refactor-20261012-741d2de5` 执行。本地只编辑、审查和记录源码。该隔离目录没有 Git 元数据，同步前以此前 280 个源码的 SHA256 清单确认未被外部修改；本轮仅按 282 个源码路径白名单同步，没有复制账户凭据或本地构建产物。

最终验证：`bash scripts/verify_system_refactor.sh` 返回 0。

- `cargo fmt --all -- --check`、workspace/all-targets/all-features 的 `check` 和 `clippy -- -D warnings` 全部通过。
- Rust 446 passed，0 failed，12 ignored；doctest 2 passed，0 failed，1 ignored。
- Python SDK 31 passed，reporting 4 passed，共 35 passed。
- v13_smoke、pair_arb、timer-submit 三个 AOT 制品构建通过；artifact、pair、ServiceCore 恢复/账户失效、通用恢复、订单排空、定时意图 flush 失败、CLI bundle/report/SIGTERM 集成均通过。
- 定时失败日志：`lifecycle=Failed, gate_open=false`。
- 12 项真实交易所测试保持 ignored，没有启用实盘账户、部署或下单/撤单。

完整原始结果在 [logs/](logs/)，关键日志：[Rust](logs/rust-test.log)、[Clippy](logs/clippy.log)、[ServiceCore](logs/recovery-smoke.log)、[定时失败](logs/timer-failure-smoke.log)、[CLI](logs/cli-integration.log)。本轮结果代表非实盘回归通过，不替代真实交易所链路验收。

最终 [源码 SHA256 清单](source-files.sha256) 覆盖 282 个同步路径，清单自身 SHA256 为 `f0fda12bb8de6b7523a4ca3022380b7d79a9863e3c3e52cbb3b5a958fb81a976`。本地取回的源码逐项匹配编译服务器清单。新增 timer-submit AOT bundle SHA256 为 `0849272a4a9487ef1ad3c618106658d1f875cf7b3731dd84e7a6f9aa8caf1162`；另外两个制品见 [artifact-checksums.txt](logs/artifact-checksums.txt)。

Git 提交基准为 `586f0f47`。本次提交同时包含此前的 Clippy、通用恢复、双向持仓修复，以及 R1–R7 修复和对应验证记录。
