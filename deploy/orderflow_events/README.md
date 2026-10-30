# orderflow_events 主网只读冒烟部署模板

`runtime.toml` 是 ABI V13 `orderflow_events` 0.2.2 的**只读行情**冒烟配置：单个 Binance USDⓈ-M
主网源（BTCUSDT），无账户、无下单能力（策略只声明 `MARKET_DATA | TIMER | HOST_DEPTH_100`）。
`allow_unsigned_artifacts = true` 只用于冒烟；正式部署前必须换成签名制品并配置
`trusted_ed25519_keys`。

## 目录

- `runtime.toml`：部署定义（源、绑定、timer 100 ms、初始 `expected_digest` 占位）。
- `artifacts/orderflow_events.titan`：不提交，由编译服务器产出后拷入本目录。

仓库中的 `runtime.toml` 已固定为编译服务器产出的 0.2.2 冒烟制品（bundle
`2405eb55…`，digest `a7be38b5…`），`artifacts/orderflow_events.titan` 随仓库提交，便于直接下发。
重编后必须重新执行下文的固定步骤。构建与测试证据见
[验收记录](../../docs/verification/2026-10-30/orderflow-0.2.2/README.md)。

## 1. 编译（在编译服务器 192.168.3.88，本机不编译）

```bash
cd <titan>
titan strategy compile \
  --strategy strategies/orderflow_events/strategy.py \
  --parameters strategies/orderflow_events/parameters.json \
  --target x86_64-unknown-linux-gnu --cpu-baseline x86-64-v2 \
  --strategy-format bundle \
  --output deploy/orderflow_events/artifacts/orderflow_events.titan --json
```

预期：`source_version=0.2.2`、`recovery_state_schema_version=3`、`state_len=62896`、
观测 schema `titan.orderflow_events/v3`。

## 2. 固定 `expected_digest`

```bash
titan strategy show deploy/orderflow_events/artifacts/orderflow_events.titan --json \
  > /tmp/orderflow_manifest.json
python3 - <<'PY'
import json, pathlib, re
manifest = json.load(open("/tmp/orderflow_manifest.json"))
digest = manifest.get("strategy", manifest)["compiled_strategy_digest"]
raw = bytes.fromhex(digest)
path = pathlib.Path("deploy/orderflow_events/runtime.toml")
text = path.read_text()
text = re.sub(r"expected_digest = \[[^\]]*\]",
              "expected_digest = [" + ", ".join(map(str, raw)) + "]", text)
path.write_text(text)
print("pinned", digest)
PY
```

重编策略后必须重新执行本步，否则启动会因 digest 不匹配被拒绝。

## 3. 配置校验（不连行情）

```bash
titan validate orderflow_events -e live -m tick -c deploy/orderflow_events/runtime.toml --json
```

## 4. 主网只读冒烟（建议 30–60 分钟）

```bash
titan run orderflow_events -e live -m tick -c deploy/orderflow_events/runtime.toml
# 或前台运行配置里全部 enabled 定义：
titan core-run -c deploy/orderflow_events/runtime.toml
```

另开终端查看运行记录：

```bash
titan ls --active
titan show <run_id> --json
titan logs <run_id> --json
```

策略没有账户与订单能力，冒烟不涉及资金；风险仅在行情连接与平台稳定性。

## 5. 冒烟观察项

| 观察 | 期望 |
| --- | --- |
| lifecycle | `Running`，无 `Failed`/反复重启 |
| `callback_count` | 每 100 ms 递增（timer 回调） |
| `book_valid` / `book_ready` | 首个快照后均为 1 |
| `warmup_remaining_ms` | 从约 901000 递减，约 15 分钟后到 0 |
| `stream_epoch` / `book_resets` | epoch 单调；`book_resets` 只在真实断线/重连时 +1 |
| `dropped` | 0 |
| `prints_seen` / `e*_count` | 随时间增长；预热门槛满足后 E01/E03/E07/E08/E13 先恢复，E11/E14 约 15/5 分钟后 |
| 日志 | 不应持续出现 `scheduling stream recovery`、`depth_snapshot_insufficient_coverage` |

常见失败：

- `depth_snapshot_insufficient_coverage`：`depth_snapshot_limit < 100`，或标的本身不足两侧各 100 档
  （Binance 测试网深度通常不满足，只能用主网验证）。
- `market_not_bound`：`[[strategies]].markets` 的 `source_key`/`asset_id`/`local_asset_no` 与
  `[[market_sources]]`、策略参数 `asset_no` 不一致。
- `strategy package ... is outside allowed_strategy_roots`：bundle 没放到 `artifacts/`。
- 制品 digest 不匹配：第 2 步未执行，或重编后未更新。

## 6. 停止

前台 `Ctrl-C`；或 `titan stop <run_id>`。

## 7. 正式部署前

- 去掉 `allow_unsigned_artifacts`，配置 `trusted_ed25519_keys`；编译时加
  `--signing-key <32B 或 64 hex seed> --key-id <id>`，并用
  `titan strategy validate ... --trusted-key <id>=<hex> --require-signature` 校验。
- 需要跨重启续算时增加 `strategy_service.checkpoint_root` 并把 `recovery` 改为
  `restore_latest_checkpoint`；当前模板是 `fresh`，重启后重新预热（短窗事件秒级恢复，E11 15 分钟）。
- 长期运行观察宿主 book 价位增长（RSS）与 `event_engine.arena.market_overflow_bytes` 预算。
- 策略参数在编译时烘焙；改 `parameters.json` 必须重编并重新固定 `expected_digest`。
