# OKX mainnet + Hyperliquid mainnet 联调部署

`runtime.toml` 当前是已经完成 shadow 验证的小规模主网 canary：`min_profitability_bps = 10`、
`order_amount_base = 0.0002 BTC`、`max_order_notional = 25 USDT`。该数量高于 Hyperliquid 的
`10 USDC` 最小订单金额，同时仍将单次名义风险限制在约 `16 USDT`。切换账户或品种前必须重新从两个
交易所的实时 instrument metadata 校验 `price_tick`、`quantity_lot` 与 OKX `ctVal`，并先恢复高盈利
门槛完成 shadow 验证。

该配置的 OKX 与 Hyperliquid 都是真实主网。默认 Shadow 门禁不得在账户资金、净仓位、API 权限和
最小下单量验证完成前降低；canary 阶段仍应使用能够在两边同时成交的最小可对冲订单。

Hyperliquid `l2Book` 是完整、幂等但非固定频率的快照流；主网探针观测到约 5.5 秒的正常包间隔，因此
canary 使用 `market_stale_ms = 12000`。修改此值前应重新运行 ignored 公共流探针并保留至少两倍的
实测最大间隔；超时后策略会撤掉 OKX maker 报价。

凭据只允许放在本目录的 `secrets/` 中，文件权限必须为 `0600`：

- `okx-mainnet.toml`：`api_key`、`secret`、`passphrase`；必须是权限受限的 OKX 主网 API key。
- `hyperliquid-mainnet.toml`：`private_key`、可选 `account_address`；必须是已获主账户授权的
  Hyperliquid mainnet agent key。

运行时通过 `secret://file/...` 引用凭据，配置和日志不会包含密钥。两个账户必须是专用联调账户；Account
connector 停机时会清理其注册品种上的挂单。

服务器执行以下命令会以单任务构建、生成带 SHA256 的 Connector packages，并完成不读取凭据的 live
配置校验：

```bash
./deploy/okx_hyperliquid_xemm_testnet/prepare_server.sh
```

服务文件按 user systemd 部署；首次运行保持 shadow，确认账户 READY、无现存挂单/仓位并观察稳定行情后再
进入 canary。

服务器需要一个原生 Linux Python 虚拟环境，且当前发布目录由稳定软链接指向：

```bash
python3 -m venv "$HOME/titan-xemm-venv"
"$HOME/.local/bin/uv" pip install \
  --python "$HOME/titan-xemm-venv/bin/python" \
  ./python/titan-strategy-sdk
ln -sfn "$PWD" "$HOME/titan-xemm-current"
```

将 `titan-xemm-testnet.service` 安装到 `~/.config/systemd/user/` 后，先保持未启用状态完成
Shadow 验证；只有两个主网账户均 READY、资金和风险限制均确认后才启用服务。
