# Gate CrossEx 实盘验收

2026-10-21。**Gate CrossEx broker 的受控实盘下单、改单、撤单及私有推送验收通过。** 本次使用用户指定的 `43.130.234.54`，凭据来源为账户系统 `http://10.126.126.1/arb/accounts` 中唯一启用的 CrossEx 账户：ID `1`、名称 `gate`、底层所 `GATE`。实现和能力边界见 [broker 说明](../../../gate_crossex_broker.md)。

## 环境与制品

- 本地只修改、审查、同步和计算文件校验和。格式、Clippy、构建和非实盘测试全部在 `192.168.3.88` 执行；真实账户访问及所有交易请求全部在实盘服务器执行。
- Titan 基线 `39f1267b6412165fc7adc005719bc9e09dcef9a8` 加本任务未提交源码；保留原有编译链等未提交工作。本次没有创建提交。gate_arb 输入身份沿用[迁移记录](../gate-crossex-complete/gate-arb-inputs.json)。
- 编译源码隔离目录：`/home/dominolu/dev/titan-gatecrossex-live-20261021-39f1267b`，从此前验证目录复制，未覆盖服务器已有工作区。Rust/Cargo 1.94.1，x86_64 Linux；复用先前服务器生成的测试 fixture，不上传本地构建产物。
- 实盘目录：`/home/ubuntu/titan-crossex-live-20261021`；只部署探针，未替换现有应用或启动长期策略。实盘主机 x86_64 / glibc 2.39，NTP 同步。[主机和最终部署身份](evidence/host-identity-final.json)。
- 最终探针 `probe.final` 来自编译服务器，经移除调试信息后 SHA256 为 `833eaf293c3dadb11398bc8130f8fc08a3dafb8361393c82a652ccfe79f8021f`，实盘服务器逐字节摘要一致。
- 346 项[源码清单](source-files.sha256) 与 [源码核对日志](logs/source-verification.log) 标识最终代码和配置，清单 SHA256 `7a4912bef19fe1c11b43811695d496ee0444ae60e13d92bee368b18219fb759a`；清单不含验证证据、构建输出和二进制 fixture。fixture 身份与[上一阶段](../gate-crossex-complete/README.md)一致。

凭据仅在实盘主机内通过现有账户服务解密，原数据库使用只读连接，临时文件权限 `0600`、目录 `0700`。密钥没有输出到终端、同步到本地或编译服务器；日志先在实盘主机通过[脱敏收集脚本](collect_live_evidence.py)删除密钥、用户 ID、账户金额和其他品种仓位后取回。测试结束删除临时凭据，原账户配置和密钥不变，见 [清理记录](evidence/credential-cleanup.json)。

## 实盘发现与修复

| 问题 | 最终实现及回归 |
| --- | --- |
| `risk_limits` 缺少必填 `symbols`，真实请求返回 400 | 原生方法要求非空品种列表，并验证 symbol；HTTP 测试核对编码后的 query 和公开请求不带认证。[首次失败证据](evidence/readonly-v1.jsonl)。 |
| Gate/OKX FUTURE 公共数量是底层合约张数 | 行情单独读取官方线性合约元数据：Gate `quanto_multiplier`、OKX `ctVal × ctMult`，换算为基础币；REST 订单继续使用基础币、乘数 1。未知单位或元数据失败时拒绝输出；允许经核对的显式 override。 |
| 浮点换算产生尾数，精确 lot 解析可能拒绝盘口 | 盘口用检查溢出的十进制乘法；回归包含 `33560 × 0.0001 = 3.356`、Depth/BBO/Trade 单位和错误元数据。 |
| Gate OI WS 不一定提供查询所需初始快照 | Gate 查询使用官方公开 futures ticker 的 `total_size` 并按合约面值换算；观测时间为查询接收时间，不把超时当成零。[第二次失败证据](evidence/readonly-v2.jsonl)。 |
| tick/lot 浮点计算的请求字符串可能带尾数；极小步长精度被识别为 0 | 对齐校验后按规则精度输出规范十进制；保留 1e-9 精度。未对齐请求仍拒绝，不静默取整。 |
| 改单已受理，但首次 REST 查询仍返回旧价格 | 最多 3 秒仅轮询事实，确认所请求价格与数量均生效；超时或冲突终态返回结果未知，交平台对账，不重发 PUT。新增旧值、分步更新和冲突终态回归。[首轮实盘失败与安全撤单](evidence/passive-attempt-v1.jsonl)。 |

Gate BTC 公共合约元数据与原始 CrossEx 行情在实盘主机核对了数量单位。OKX CrossEx 原始行情同样为合约数量，但该主机访问 OKX 原生公共元数据时收到 HTTP 错误，**没有将 OKX 路径标记为实盘验收通过**，也未用默认乘数 1 绕过。

## 最终非实盘验证

所有命令在编译服务器、上述隔离源码目录执行，依赖解析使用 `--locked`；失败即停止后续构建和部署。

| 命令 | 结果 | 日志 |
| --- | --- | --- |
| `cargo fmt --all -- --check` | 通过 | [fmt](logs/titan-crossex-live-fmt-final.log) |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 通过 | [Clippy](logs/titan-crossex-live-clippy-final.log) |
| `cargo test --locked --workspace --all-targets --all-features` | **507 passed / 0 failed / 12 ignored**；connector 282 passed；含探针 2 项风险保护测试 | [workspace](logs/titan-crossex-live-workspace-final.log) |
| `cargo test --locked --workspace --all-features --doc` | 2 passed / 0 failed / 1 ignored | [doctest](logs/titan-crossex-live-doc-final.log) |
| `cargo check --locked -p connector --no-default-features --features gatecrossex --lib` | 编译通过；有 21 条关闭其他 feature 后的 unused/dead-code 警告，不作为无警告结果 | [独立 feature](logs/titan-crossex-live-feature-final.log) |
| `cargo build --locked -p connector --example gate_crossex_live_probe` | 通过；只部署此服务器产物 | [build](logs/titan-crossex-live-binary-final.log) |

相对上一阶段 496 项，新增 9 项 connector 回归和 2 项探针风险测试。ignored 维持原状，没有统一启用其他 broker 的真实账户测试。仅文档和证据整理发生在最终编译之后，最终生产代码及探针没有再修改。

## 实盘覆盖与交易结果

每轮下单前均执行完整只读检查，任一检查失败禁止进入交易。最终[交易轮次日志](evidence/passive-final.jsonl)与[独立收尾复核](evidence/postflight-final.jsonl)均退出 `0`，stderr 为空；同名 `.exit` / `.stderr` 文件保留在 evidence 目录。

通过项目包括：账户原始与统一快照、合约/杠杆持仓、挂单、杠杆查询、费率、可划转币种、风险限额、历史划转/账本/订单/成交/收益、交易规则、ticker/五档盘口/资金费/OI。真实 AccountRuntime 工厂使用作用域凭据完成 bootstrap 并进入 `Ready`；账户工厂主动触发重连后重新 ready。order、position、asset、margin_position、margin_interest 五个原生私有频道全部收到确认码 `100000`。Titan 探针内部 AccountId `900001` 仅为临时运行时绑定，不是账户系统 ID。

测试使用主网 `GATE_FUTURE_BTC_USDT`，账户原有 `CROSS_EXCHANGE / SINGLE` 模式。目标品种初始无仓位、无挂单；其他品种已有运行中的网格和挂单，未对其发送交易或取消请求。

| 项目 | 首轮（修复前） | 最终轮（修复后） |
| --- | --- | --- |
| client tag | `titanlive-86229355f87dcdb7` | `titanlive-33a0fa2bb4e893e1` |
| order ID | `2224073367101952` | `2224074180801280` |
| 单笔数量 | 0.0001 BTC | 0.0001 BTC |
| 类型 | BUY / LIMIT / post-only（GTX→POC） | BUY / LIMIT / post-only（GTX→POC） |
| 限价 → 改价 | 81159.9 → 81077.1 | 81157.0 → 81074.2 |
| 名义金额上限 | 10 USDT，按新鲜卖一及数量检查；实际限价金额约 8.12 USDT | 10 USDT，按新鲜卖一及数量检查；实际限价金额约 8.12 USDT |
| 价格保护 | 约低于买一 2%，改价进一步远离市场；每次写入前复核至少低于新鲜买一 1% | 同左 |
| 验收 | 下单 REST/WS 通过；旧价格被探针拒绝，进入精确撤单；最终查询显示改价已生效 | 下单、改单 REST/WS 通过；撤单后 REST 与 WS 均确认 `Canceled` |
| 清理 | 自己的订单已撤、0 成交、0 BTC 持仓、无本次残留挂单 | 自己的订单已撤、0 成交、0 BTC 持仓、无本次残留挂单 |

两个测试订单先后执行，前单确认清理完成后才开始后单。探针在写入前持久化唯一 client tag；中断、超时或验证失败均进入按该订单身份的清理。没有使用 cancel-all、市场平仓、转账、修改杠杆/账户模式或启动长期策略。最终又独立重读目标品种，确认 BTC 持仓、挂单均为零。首轮失败日志没有覆盖或伪装为通过。

## 验收边界

本次完成迁移代码的最终回归及 **Gate 该账户、BTC USDT 合约的被动订单生命周期**。没有为验证而制造真实成交，因此真实部分成交、成交费用及成交后仓位变化仍只具有模拟回归证据；不应从“0 成交”推断这部分已作实盘验证。杠杆持仓为空，仅完成读取和频道订阅，未进行杠杆交易。其他底层所、其他业务/品种、资金划转/账户模式写操作、非受控网络故障及长期稳定性未作为本轮实盘验收范围。
