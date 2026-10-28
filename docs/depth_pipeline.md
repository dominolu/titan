# 深度发布、Top100 与重同步契约

2026-10-28。适用 Binance USD-M 原生深度与显式启用 `HOST_DEPTH_100` 的 ABI V13 策略。验证记录见 [本轮验收](verification/2026-10-28/depth/README.md)。

## 容量

- 深度事件仍使用 52 字节 `MarketBatchHeaderV1` 和每项 24 字节 `DepthItemV1`；没有修改现有 ABI 结构或指纹。
- 默认 MarketBatch 块 65,536 B、2,048 槽、低水位 256，总块数据预算保持 128 MiB。相比旧配置，单批容量增加、可同时持有的块数减少；生产积压和延迟须另外测量。
- V13 深度输入独立上限 2,730 条，账户等 `PUBLIC_CAPACITY=1024` 保持原值。1000×2 档快照 48,052 B 可通过。
- Binance connector TOML 可配置 `depth_snapshot_limit`，默认 1000，接受 5/10/20/50/100/500/1000。应用配置加载检查快照是否能装入指定 arena 块；connector 构造检查合法取值。现有 `[event_engine.arena.market_batch]` 已能直接配置，并非需要另造透传。
- 超过块容量的 diff 不截断、不分片，关闭深度视图并走现有流恢复。增加任意深度分片协议仍属于后续工作，当前不宣称支持任意大小批次。

## 分层职责

1. Binance connector 缓存 diff，验证首批 `U <= lastUpdateId <= u` 和后续 `pu == previous.u`；成功后才交付快照及缓冲 diff。没有桥接 diff 时 REST 快照保持私有候选，不能提前表示 READY。
2. REST 失败交回连接任务，使用既有连接重试/退避；连接失败显式发布失效。恢复代次使用进程内单调计数，不能因重建 WebSocket 对象回到 1。同步缓存按最多 4096 批、32768 个价位条目限制，超限结束该连接并重新同步。
3. 发布失败立即尝试发布该品种的失效事件，挡住后续 diff，直到新快照成功；无关 BBO/成交不能清除此状态。失效事件带品种路由，使用同一行情 source stream 的 publication sequence。
4. Rust 策略宿主按绑定品种维护较深 L2，并在完整批次应用后投影买卖各 100 档。删除前排会补回此前未变化的第 101 档；不让策略自己丢弃深部价位后猜补位。
5. 初始快照的最低买价/最高卖价限定可信覆盖范围。范围外未知深度不能补成完整证据；不足两侧各 100 档时视图无效，向 MarketService 请求新快照。初始快照本身不足 100 档时明确拒绝该能力的运行，避免持续请求小快照。
6. 每个宿主 book 总计最多 16384 个价位，超限失败关闭。失效 publication 水位防止排队的旧快照重新开启 book。
7. `HOST_DEPTH_100` 强制 ReliableOrdered 深度订阅，并自动订阅失效事件。PRIMARY 异常时停止定时计算；源健康异常使定时回调前的 book 失效，提交新单前也检查 book 和源健康。撤单不受新单准入检查限制。

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

策略版本 0.2.0、恢复 schema 2；旧 checkpoint 不直接加载。采用 100 档，删除 `snapshot_items` 参数与批次大小启发式。BBO 只供 OFI，不改写深度或延长深度新鲜度。

整批比较旧/新视图后替换；来源快照、恢复、失效和超时后的首批都清理 flow、refill/冰山、价格/量窗口及 EWMA 基线。新进入 Top100 的价位没有已知旧数量，不能当成 refill；离开 Top100 的价位不能当成撤单或被消耗。

`depth5` 固定累加真正的前 5 档，不随 `depth_levels=100` 变成百档总量。E07 过去视图只保存价位和两侧 Top5 数量合计，省去两条 100 项数量副本，保留真实所需事前深度。秒窗口增加当前秒所需一个槽，容纳原默认 15 分钟回看。

重同步后按最长配置回看预热，默认 E11 为 15 分钟。观测 schema 2 新增 `book_valid`、`stream_epoch`、`book_resets`、`warmup_remaining_ms`，区分深度有效与事件可计算。`book_ready=0` 时禁止检测信号。

## 离线输入

`OfflineV13Adapter::on_depth_batch` 使用同一 Rust 重建器；CLI 深度 trace 可提供 `depth_batch` 元数据。能力策略的旧 `on_depth` 输入明确拒绝，不能猜测 epoch 或快照。

回放方必须先按源协议检查原生连续性，再提供有序批次：正常输入 `valid=1`，快照另设 `reset=1`；已知缺口传 `valid=0`。元数据不是连续性证明的替代品。Tardis 25 档不能用于声称 100 档完整；原始 collector 的快照与 diff 还需按实际文件内容确认覆盖、排序和缺口，不能仅凭订阅了 `@depth` 认定完整。
