# 永续 DEX 与跨所交易实施及验收计划

日期：2026-10-27  
状态：规划任务全部待实施；本次仅完成调研和设计文档。  
设计：[完整技术方案](cross_venue_trading_design.md)；证据：[固定源码调研](perp_dex_research.md)。

## 1. 交付边界与依赖

首个可交易闭环为 Lighter Core–EdgeX 的同经济资产线性永续，包含已验证原生下撤单、独立成交、成本模型、多腿风控、增量 hedge、未知订单恢复和停止屏障。准入以 provider/部署/账户模式/市场/订单类型为粒度，不以代码文件存在为粒度。

不将同时接入全部 15 个 provider 作为首个发布条件；所有 provider 已在技术设计给出实施路径，按下表逐批推进。任何批次缺少官方规范、签名/结果查证或对应风险证据时保持未准入，不以默认值或禁用风控推进。

本计划依赖正在演进的[数据持久化计划](data_persistence_implementation_plan.md)，特别是指定提交确认、原始恢复载荷/hash、代次/owner、固定引用和未知提交关闭语义。开工时重新核对实际代码及验收记录；不把其设计状态或本次工作区改动当作已完成依赖。

## 2. 工作拆分

所有任务初始状态为 **待实施**。每项完成必须链接新的证据目录；旧系统测试数量不作为完成证明。

| 编号 | 工作 | 主要位置 | 前置 | 完成条件 |
| --- | --- | --- | --- | --- |
| X01 | 冻结基线、协议来源、依赖许可证与能力 schema | docs、connector 配置 | 无 | 源码 SHA+工作区清单；官方 spec/签名库身份及缺项表 |
| X02 | 部署/native account/signer/collateral/经济资产身份 | domain types、注册与绑定 | X01 | Core/RH、key 轮换、共享 agent/抵押域冲突用例通过 |
| X03 | 精确元数据与 equivalence binding | connector、market/account bindings | X02 | tick/lot/乘数/FX/交易时段校验；非法合约拒绝 |
| X04 | 能力声明与运行准入 | factories、CLI/runtime 组装 | X02 | Supported/Unsupported/Unverified 和 evidence scope；不允许运行时猜能力 |
| X05 | 多绑定预交易风险与预占 | account service、runtime_v13 | X03–X04 | 解除单行情限制需新门禁通过；所有可能单腿成交分支可被覆盖 |
| X06 | Lighter Core 目录及行情 | connector/lighter、market factory | X03–X04 | snapshot/delta、缺口、source 重连、按量报价与坏簿拒绝 |
| X07 | Lighter signer/账户/执行 | connector/lighter、account factory | X01–X06 | 固定 signer 向量；nonce 域/submit/cancel/original lookup/独立 fill/bootstrap |
| X08 | EdgeX 目录及行情 | connector/edgex、market factory | X03–X04 | 最新官方 schema、元数据/深度与序列恢复 |
| X09 | EdgeX signer/账户/执行 | connector/edgex、account factory | X01、X05、X08 | 官方向量、post-only/IOC、原单查询、账户一致映像及私有成交 |
| X10 | Runtime execution plan、原生准备提交屏障与多腿恢复 | strategy runtime、connector context | X05、X07、X09、持久化依赖 | 语义意图及 native prepare 均先提交再发送；任何 crash 点不重复写、不释放未知风险 |
| X11 | pair_arb 新版增量对冲及成本模型 | strategies/pair_arb、SDK 所需适配 | X10 | source/schema 显式新版本；partial、dust、在途区间、补偿预算、Stop 通过 |
| X12 | 回放/回测与故障仿真 | hftbacktest、fixtures | X06–X11 | 无未来数据、独立多所时延/成交/费用/funding、结果可重复 |
| X13 | 多所保证金/财务与风险观测 | account service、查询/管理 | X05–X12 | gross/net、原生 collateral、费用/funding unknown、损失锁存与压力结果 |
| X14 | 全链路资源与延迟验收 | EventEngine/runtime、远端探针 | X10–X13 | 保留停止容量；磁盘/队列故障关闭；输出测量范围和尾延迟 |
| X15 | 编译发布与受控双腿验收 | 编译服务器→实盘服务器 | X01–X14 | 冻结产物、最小风险参数、真实残留/账本核对；另需明确实盘授权 |
| X16 | Extended、GRVT、Backpack 扩展 | connector/factories/config | X15 或等价模拟平台基线 | 每所独立复用相同准入契约并重做协议/账户/交易矩阵 |
| X17 | 其他 venue/部署扩展 | 对应 connector | X16 基础能力 | 按需求逐所关闭技术设计 §18 的前置项 |
| X18 | 滚动价差、funding carry、跨所网格 | 策略/回测/观测 | X11–X14、所需费用/资金流水 | 独立策略版本、模型与故障验收，不复用交易闭环证据代替策略证据 |

规模与排期在 X01 冻结官方协议、signer 和现有工作区后估算；不以“一个 HTTP 接口需要一天”推断完整交易接入周期。

## 3. 逐所准入顺序与专项门禁

| 批次 | Provider | 专项门禁 |
| --- | --- | --- |
| A | Lighter Core、EdgeX | 完整首个双腿执行、持久化恢复、费用/保证金与停止闭环 |
| B | Extended、GRVT、Backpack | Starknet 异步/external ID；GRVT 会话/签名；Backpack Ed25519 时间窗各自验收 |
| C | Aster、Paradex、ApeX Omni | 不同 HMAC/API、Stark session、zk/seed signer 不能借用相似产品通过 |
| D | Nado、Ethereal、StandX | x18/appendix/轮询新鲜度；linked signer/subaccount；JWT+临时 body signer |
| E | Arcus、Lighter RH、Entropy、Ondo | 官方协议补证；RH 独立 chain；HIP-3/agent/共享抵押；HMAC/accountID/cross 抵押 |

批次代表工程依赖和建议顺序，业务有明确需求时可调整 C–E 优先级；不能跳过 A 的公共不变量。某所只完成公开行情时在管理目录显示 MarketOnly，不能供交易账户表单选择。

每个 provider 维护准入表：deployment/network、native account mode、collateral、instrument kind、market 范围、order/TIF、证据等级、协议/库 SHA、最近复核时间、测试/部署证据链接和排除项。新市场不能自动继承其他合约的 tick/lot/流动性结论。

## 4. 协议与身份验收矩阵

| 类别 | 必测用例 | 成功标准 |
| --- | --- | --- |
| 官方签名 | 正向向量、字段顺序、大小写、空值、expiry、chain、body/query 字节 | 与固定官方实现逐字节/可恢复身份对照；错误域拒绝 |
| 数值 | 非十进制/NaN、负值、极小 tick、非 10 次幂 step、乘数、i128 溢出 | 不静默舍入、错单位或溢出；保护价方向正确 |
| 部署 | Core/RH 相同 account/key index；mainnet/testnet；HTTP redirect | 不串 host/domain/key；无自动跨部署回退 |
| 账户 | key 轮换、共享 signer、子账户同名、agent 不属于 owner/已过期 | 原生身份稳定；重复占用/nonce 冲突拒绝 |
| 合约 | 同 ticker 不同资产、HIP-3、合约/基础币数量、元数据更新、停牌 | 无等价证据不交易；已冻结 binding 不静默改路由 |
| 盘口 | delta 前 snapshot、重复/乱序、合法非连续序号、缺口、checksum、过量深度 | 按原生规则建簿；缺口后先关门再补齐 |
| 账户流 | 先 fill 后 ACK、先 position 后 fill、bootstrap 中断、旧会话迟到 | 单调投影、不伪造零仓/终态，Ready 有共同提交边界 |
| 历史 | 分页截断、重复 trade、同 ID 不同内容、查单短暂 404、窗口过期 | 缺失为 unknown/冲突；不能补零或仅凭不在活动列表放行 |
| 权限 | 无交易权限、只读查询、有转账权限 key、builder 默认值 | 策略仅可调用已允许交易面；不发 transfer/approval/referral |

## 5. 执行与恢复故障矩阵

对以下事件顺序进行确定性回放，并对独立事件做受约束排列。生成/运行矩阵只在编译服务器进行；无需为每一代码分支写镜像测试，以风险不变量和跨层结果为验收对象。

| 故障/竞态 | 必须观测的结果 |
| --- | --- |
| maker 分三笔成交，第二次 hedge 尚在途 | 增量只消费一次，在途区间计入，不重复 hedge |
| maker 部分成交后撤单，terminal 早于最后 fill | 无提前 replacement/完成；补齐独立 fill 后收敛 |
| 双腿 IOC：0/0、满/满、满/0、0/满、部分/部分 | 每种结果正确计算库存、资金和补偿；无双 HTTP 成功即完成 |
| 原单发送后 timeout，REST 暂无记录，稍后 fill | UNKNOWN 预占保留，查同一 client ID，无新身份重发 |
| 一腿已发送，另一腿派单本地拒绝 | 第一腿风险仍有效，策略进入预授权补偿或人工处理 |
| 反向 hedge 结果未知后又到旧 fill | 不重复开反向仓，按全部可能成交区间更新 |
| dust 小于对手 minimum，但单腿期限到 | 撤剩余主腿、限制新增，暴露不可交易尾仓，不凑大单 |
| quote 过期但订单确认/stop deadline 未到 | 行情门关闭，事实与定时器继续，停止不冻结 |
| 主腿取消失败+跨所对手失联 | 不凭取消请求释放风险；触发规定动作/占用保留 |
| 重复 Stop/迟到 Resume，HALT 后旧 Start 完成 | 原 stop deadline 不延长，HALT 不重开 |
| 意图提交前/后、native prepare 屏障前/后、每腿传输前/后、fill 应用前/后崩溃 | 恢复不重放未知写、不漏掉单腿敞口、不倒退代次；未发送 nonce 空洞不猜测修复 |
| runtime.db 超时/磁盘满/提交结果未知/旧备份 | 故障关闭；不回退文件或新 namespace 绕开 |
| 序号缺口、writer 崩溃、缺历史成交窗口 | 证据覆盖明确，不声称恢复成功 |
| 队列/执行任务/事实池饱和 | 有界拒绝与关门，Stop/query 资源不被开仓耗尽 |
| 有仓位 Stop：CancelOnly/DrainHedge/FlattenOwned | 分别证明目标；留仓不标已平，未决不释放占用 |

关键不变量：每个经济 intent 只有一个稳定原始 client 身份；每个独立 fill 最多一次财务应用；真实/可能成交始终落在风险包络内；未证明 terminal 与 fill/position 一致不释放风险；所有外部写都有合法意图和准入证据；迟到事件不能重开失效运行。

## 6. 风控数值与财务验证

1. 用仿真固定价格、fees、FX、IM/MM 档位及独立延迟，核对线性 PnL/资金费符号、gross/net/delta、深度 VWAP、价格保护与 rounding。
2. 同时挂买卖订单时，验证单边全部成交分支；把双向仓位净成 0 也必须保留 gross/保证金风险。
3. 模拟权威持仓滞后与 fill 投影领先，核对去重包络；不能三处重复计入，也不能少计未知量。
4. 逐所验证 available 已扣挂单保证金时的预占去重；USDT/USDC 换汇与脱锚不使用固定 1。
5. 第一腿损失所和第二腿盈利所分开计算压力后权益，禁止跨所浮盈抵消清算缓冲。
6. 日损失与最大裸露时长在重启/时钟跳变后不重置；外部充值不伪造策略盈利。
7. 缺少真实 fee/rebate/funding 证据时净收益为 unknown，仍可展示已证明毛收益及账户权益。
8. maker 成交后紧急 hedge 允许负价差但受独立救援预算限制；超过预算必须明确结束自动动作。

示例 fixture 的任何金额、价格和波动均属合成测试，不能作为生产账户默认风险配置。验收报告必须携带实际风险 profile revision。

## 7. 回测与研究验收

使用与实盘相同的量价、身份、cost/hedge policy 与风险定义。策略只能读本地已交付行情/账户，不能从 exchange-authoritative 状态预知未到达成交。两所独立模拟 order arrival、ACK、trade/position delivery、断线与部分成交；maker 撮合使用可解释队列/延迟假设，不以触价必全成作为唯一模型。

数据集冻结两端市场、元数据/乘数、时间范围、缺口、资金费结算点、手续费假设与 FX；按时间进行训练/验证/样本外，不以全部样本选参数后声称样本外。报告 turnover、净/毛 PnL、费用磨损、最大回撤、最大裸露/时长、补偿次数/成本、库存峰值、margin buffer、UNKNOWN/数据缺口和敏感性分析。

滚动中心用决策前历史，资金费预测保留原发布时间，不能用事后实际资金费当事前预测。新运行可以重放固定 trace，但本计划不承诺任意中断的完整回测撮合器续跑。

## 8. 环境、发布与受控实盘步骤

本地仅编辑/审查。所有构建、代码生成、格式、Clippy、单元/集成、原生策略编译、回放与负载验证在 `192.168.3.88` 执行。同步前记录远端仓库路径、HEAD、dirty 文件与当前运行；使用隔离目录及明确同步清单，不覆盖已有改动、不同步密钥或本地构建产物。

受控实盘必须另获相应授权并在远程实盘服务器进行，不在编译机或本地读取真实账户。进入该阶段前固定：目标账户与部署、市场/position side、原生 minimum、每腿数量、maker/IOC 价格界限、最大含在途敞口、最坏补偿/手续费、持仓与订单起始状态、期限及 Stop 目标。

建议顺序：只读身份/模式/元数据 → 单所最小 post-only 下撤与私有确认 → 原生 IOC 零/少量成交及独立 fill/费用验证 → 双腿有限 slot → 部分成交和停止/恢复。真实市场无法稳定制造的故障分支采用编译服务器模拟，不宣称实盘覆盖。每一步核对实际挂单/持仓/未知订单与最终残留；失败不得进入下一阶段。

不从本次文档请求推导实盘发单、长期运行、自动加仓/转账/改模式的授权。上线后的长期观察也需明确范围、期限、风险配置及停止条件。

## 9. 证据包与发布门禁

拟议新验收目录：`docs/verification/<实际日期>/cross-venue/<阶段>/`。当前不创建伪造日志或通过报告。

每阶段包含：基线 commit+源文件 hash 清单、dirty patch 身份、官方 spec/SDK/native 库版本与许可证、命令和退出码、工具链/主机/CPU/时间、冻结参数与模拟假设、测试日志、故障 trace、产物 SHA256、部署摘要、验证范围/未验证项。凭据、signed payload、私有 key 不进入公开证据包；私有原生身份可在受限证据中关联，公开材料脱敏。

| 发布门 | 放行标准 |
| --- | --- |
| G0 文档/协议 | 本设计不变量明确；最新官方协议与许可证缺口有明确阻塞 |
| G1 接入 | 协议/单位/身份/行情/账户模拟和公网只读均通过 |
| G2 执行 | 提交、未知结果、独立成交、两腿全部失败分支与恢复通过 |
| G3 风险 | 多腿预占、各所保证金、压力、损失、停止及资源上限通过 |
| G4 产物 | 指定编译服务器构建且源码、依赖、策略签名/摘要一致 |
| G5 受控实盘 | 已授权范围内账户/订单/成交/残留验证；排除项明确 |
| G6 持续运行 | 另行授权及规定观察窗口的稳定性证据；不得从 G5 推定 |

文档交付完成不等于 G1–G6 完成。首期没有长期实盘稳定性证据时，只能发布对应受控能力范围，不对所有账户/市场作普遍可用性承诺。
