# 市场载荷承载重构验收

2026-10-28，基线 `4f94a4b1`。本地仅编辑/审查；全部构建、格式化、静态检查、测试及基准在 `192.168.3.88` 的独立目录 `/home/dominolu/dev/titan-payload-20261028-4f94a4b1` 完成，没有真实账户访问或部署。

## 变化

- 常规市场消息继续使用预分配池；超过块大小时在同一槽内按实际字节分配。handle、generation、队列、引用计数与零复制扇出保持原路径。取消预留/最后释放均归还内存和预算，失败预留不消耗槽。
- 超块载荷仅共享一个在途字节预算，默认 128 MiB；正常路径不访问该原子计数。其他事件池保持原限制。
- 删除 2730 条解码限制、16384 价位 book 限制、固定块与 REST limit 的重复启动校验。Bootstrap 的 4096 批/32768 条组合上限改为每品种 32 MiB 估算字节预算，接收时只记新消息，不扫描历史缓存。
- 原始深度与投影 Vec 复用；深度不再构建 Tick，Top100 不再 collect 两个临时 Vec。保留全部收到的远档价位，同时通过快照边界验证 Top100 覆盖。
- 不分片、不截断；固定块不再触发流恢复。真实字节预算/队列耗尽、分配失败、协议/序号错误仍显式失效。

## 验证

- [Rust 全工作区](rust-test.log)：`cargo test --workspace --all-targets --all-features`，**669 passed、0 failed、24 ignored**。
- [Clippy](clippy.log)：全工作区/all-targets/all-features/`-D warnings` 通过；最后补充的精确边界探针另经 [Clippy](probe-clippy.log) 与 [构建](probe-build.log) 通过；[最终格式检查](fmt-check.log)通过。
- [doctest](doctest.log)：**2 passed、0 failed、1 ignored**；[SDK](sdk-test.log)：**123 passed**。没有执行 ignored 网络/实盘测试。
- [原生完整链路](depth-pipeline.log)：用 16 KiB 固定块处理原 48052 B 快照，并通过 2728/2729/2730/4000/40000/65535 条完整批次；最大 **1572892 B**。40000 条 diff 不重置、不重同步；原删除补位、失效、新代次快照和离线重建回归保留。
- [执行准入](depth-execution.log)：在线/离线六场景一致，失效时撤单与已证明减仓继续可用，新增/超量风险拒绝。
- 新增单测覆盖共享载荷地址、取消预留、最后引用归还预算、预算耗尽后正常池路径可用、失败预留归还槽、8 线程争用、40000 价位完整保留、范围外更新不伪造覆盖、bootstrap 字节预算。
- [623 文件摘要](source.sha256)与[逐文件校验](source-check.log)一致；仅服务器隔离目录追加既有镜像配置，不改本地配置或 Cargo.lock。[工具链](toolchain.log)与[产物摘要](artifact-checksums.txt)。

## 性能对照

探针 [payload_bench.rs](../../../../../crates/titan-event-engine/examples/payload_bench.rs) 在 release 模式测量 reserve → 填充 → commit → clone → 读取切片 → 释放；全局分配器记录一次完整生命周期的分配数。每组 9 轮，表内为两次运行各自中位数的范围，单位 ns；这是载荷生命周期微基准，不是交易链路 P99。

| 载荷字节 | 旧版 ns | 本轮 ns | 本轮堆分配/次 |
| --- | ---: | ---: | ---: |
| 532 | 66–67 | 68 | 0 |
| 4852 | 91–93 | 92 | 0 |
| 48052 | 619–623 | 622–623 | 0 |
| 96052 | 拒绝 | 2336–2337 | 1 |
| 1572844 | 拒绝 | 37261–37596 | 1 |

[旧版输出](bench-baseline.log)、[本轮输出](bench-new.log)。常规消息维持零分配，耗时接近基线；不声称全面提速。原始深度/Tick 与 Top100 临时数组的删除是额外热路径优化，未将其收益计入上表。

基准初测发现共享 `CARGO_TARGET_DIR` 跨 checkout 复用新二进制，已舍弃初测对照；最终旧版使用独立 `/tmp/titan-payload-baseline-target` 构建，确认超块输入确实拒绝。产品回归与新实现测量使用 `/tmp/titan-deplugin-target`，最终表和日志只使用校正后的对照。

## 边界

ABI/schema 不变，V1 `u16 item_count` 单批最多 65535 条；这属于编码边界。没有宣称支持超过该协议的无限批次。HOST_DEPTH_100 仍要求可信两侧各 100 档；低于 100 的快照配置仍无法提供该能力。跨连接缺口和可信覆盖耗尽需要真实重同步。共享字节预算仅约束在途超块载荷，不是总进程内存配额；长期 book 内存由活跃价位数决定。
