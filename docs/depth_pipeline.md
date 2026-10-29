# 深度发布、Top100 与重同步契约

2026-10-28。适用 Binance USD-M 原生深度与显式启用 `HOST_DEPTH_100` 的 ABI V13 策略。初轮验证见 [深度验收](verification/2026-10-28/depth/README.md)，载荷重构见[本轮验收](verification/2026-10-28/payload/README.md)。

## 载荷承载

- 保留 52 字节 `MarketBatchHeaderV1` 和每项 24 字节 `DepthItemV1`，不修改现有 ABI、schema 或指纹。V1 的 `u16 item_count` 是协议编码边界（单批最多 65535 条），不额外叠加宿主条数上限。
- 默认 MarketBatch 仍为 65536 B × 2048 槽（128 MiB），正常消息使用预分配内存，发布和扇出不复制载荷、不进行堆分配。
- 超过池块的市场消息在同一槽内按实际字节申请存储，仍使用原有 handle、generation、引用计数与队列。最后一个消费者释放或预留取消时回收内存；不分片、不截断，也不因超过块大小而重新同步。
- `[event_engine.arena] market_overflow_bytes` 是超块消息共享的在途字节预算，默认 128 MiB；设为 0 可关闭超块存储。只有大消息走预算原子操作和一次按需分配。其他事件池保持原有预分配约束。
- 解码器按头部与实际字节长度验证，不再限制 2730 条；Rust book 不再限制 16384 个价位。原始深度与 Top100 数组复用容量，不再同时构建无用 Tick，也不在投影时分配临时价位数组。
- Binance `depth_snapshot_limit` 默认 1000，合法取值由 connector 单处验证；固定池块不再限定 REST 档数。使用 `HOST_DEPTH_100` 须配置至少 100，低于该覆盖仍拒绝运行。
- Bootstrap 缓存使用每品种 32 MiB 的估算内存预算，计入 owned 字符串与价位数组，增量记账；删除 4096 批/32768 条组合上限及每次接收扫描历史缓存的计数开销。这是 REST 等待期间的独立队列预算。
- 真正的内存/队列耗尽、分配失败和协议错误继续显式失效，不能将未收到的消息表示为完整。按需载荷不代表无界内存；长期运行的 book 内存还取决于活跃价位和订阅数量。

## 分层职责

1. Binance connector 缓存 diff，验证首批 `U <= lastUpdateId <= u` 和后续 `pu == previous.u`；成功后才交付快照及缓冲 diff。没有桥接 diff 时 REST 快照保持私有候选，不能提前表示 READY。
2. REST 失败交回连接任务，使用既有连接重试/退避；连接失败显式发布失效。恢复代次使用进程内单调计数，不能因重建 WebSocket 对象回到 1。同步缓存按上述估算字节预算限制，真实预算耗尽才结束该连接并重新同步。
3. 发布失败立即尝试发布该品种的失效事件，挡住后续 diff，直到新快照成功；无关 BBO/成交不能清除此状态。失效事件带品种路由，使用同一行情 source stream 的 publication sequence。
4. Rust 策略宿主按绑定品种维护较深 L2，并在完整批次应用后投影买卖各 100 档。删除前排会补回此前未变化的第 101 档；不让策略自己丢弃深部价位后猜补位。
5. 初始快照的最低买价/最高卖价限定可信覆盖范围。范围外未知深度不能补成完整证据；不足两侧各 100 档时视图无效，向 MarketService 请求新快照。初始快照本身不足 100 档时明确拒绝该能力的运行，避免持续请求小快照。
6. 所有已收到的价位更新都保存在宿主 book；快照覆盖范围外的零星更新不扩大可信范围。失效 publication 水位防止排队的旧快照重新开启 book。
7. `HOST_DEPTH_100` 强制 ReliableOrdered 深度订阅，并自动订阅失效事件。PRIMARY 异常时停止定时计算；源健康异常使定时回调前的 book 失效，新增风险前也检查 book 和源健康。失效期间允许撤单和经账户/品种/持仓侧、方向、可用数量及预占证明的 reduce-only 减仓；原有价格保护不放宽。在线和离线共用准入函数。

## V13 可选能力与 payload v1

不声明能力的策略保持原始 depth 批次语义，但 `TitanDepthView.is_snapshot` 现在反映真实头部标志。头部 kind/flags 矛盾会拒绝。

声明 `Capability.HOST_DEPTH_100` 后：

- `ctx.depth()` 是**完整替换视图**，有效时买侧 100 条在前、卖侧 100 条在后，每侧从最优排序；不是原始 diff。
- `is_snapshot` 仍只表示真正来源快照，不把每次 Top100 投影冒充来源快照。
- `ctx.depth_batch()` 返回一个只读的 48 字节 `DepthBatchInfoV1`，来自现有 `event_payload_ptr/len`。没有改变 V13 context/item 布局，扩展由能力名和 payload `version=1` 共同约束。旧宿主拒绝未知能力。
- 字段按序为 `version:u32`、`asset_no:u32`、`stream_epoch:u64`、`first_update_sequence:u64`、`last_update_sequence:u64`、`publication_sequence:u64`、`valid:u8`、`reset:u8`、6 字节保留区。
- `valid=0` 时条目为空，仍调用 `on_depth`；不能用“没有条目”忽略失效。`reset=1` 时丢弃依赖旧盘口的业务窗口。
- 原生 `pu` 仍在 connector 中校验，没有假装现有批量头已经携带它。publication sequence 跨该 source 的不同事件类型递增，数值间隔不等于深度丢包；交付保证来自 ReliableOrdered 和失效屏障。

## orderflow_events

策略版本 0.2.2、恢复 schema 3；旧 checkpoint 不直接加载。采用 100 档，删除 `snapshot_items` 与 `depth_levels` 参数。BBO 只供 OFI，不改写深度或延长深度新鲜度。

整批比较旧/新视图后替换；来源快照、恢复、失效和超时后的首批都清理 flow、refill/冰山、价格/量窗口及 EWMA 基线。新进入 Top100 的价位没有已知旧数量，不能当成 refill；离开 Top100 的价位不能当成撤单或被消耗。

`depth5` 固定累加真正的前 5 档。E07 过去视图只保存价位和两侧 Top5 数量合计，省去两条 100 项数量副本，保留真实所需事前深度。秒窗口增加当前秒所需一个槽，容纳原默认 15 分钟回看。

重同步后按事件各自的最长窗口预热：短窗事件在窗口填满后即恢复，E11 默认需要 15 分钟、E14 默认 5 分钟。观测 schema 3（0.2.2）包含 `book_valid`、`stream_epoch`、`book_resets`、`warmup_remaining_ms`；`book_valid` 表示深度覆盖有效，`book_ready` 表示盘口可用（不再等待最长预热），`warmup_remaining_ms` 给出最慢已启用事件的剩余预热。成交推断以“新最优价之外消失的价位”为穿价带；只有离开 Top100 的价位不算消耗。

## 离线输入

`OfflineV13Adapter::on_depth_batch` 使用同一 Rust 重建器；CLI 深度 trace 可提供 `depth_batch` 元数据。能力策略的旧 `on_depth` 输入明确拒绝，不能猜测 epoch 或快照。

回放方必须先按源协议检查原生连续性，再提供有序批次：正常输入 `valid=1`，快照另设 `reset=1`；已知缺口传 `valid=0`。元数据不是连续性证明的替代品。Tardis 25 档不能用于声称 100 档完整；原始 collector 的快照与 diff 还需按实际文件内容确认覆盖、排序和缺口，不能仅凭订阅了 `@depth` 认定完整。

## 审查修复

同代次已知删除的价位在可信范围内补回时继续记录 refill；首次进入窗口、范围外未知价位和旧代次删除记录均不作为补单证据。E04 的删空/补回/触发及在线、离线执行准入已补充真实原生回归，见[审查修复验收](verification/2026-10-28/depth-review/README.md)。
