# OKX mainnet + Hyperliquid mainnet 联调部署

`runtime.toml` 默认是 shadow 门禁：`min_profitability_bps = 10000`，能建立真实公共/私有订阅和账户
快照，但不会生成可成交报价。完成 shadow 验证后，必须先从两个交易所实时 instrument metadata 校验
`price_tick`、`quantity_lot` 与 OKX `ctVal`，再把盈利门槛降到 canary 值。

该配置的 OKX 与 Hyperliquid 都是真实主网。默认 Shadow 门禁不得在账户资金、净仓位、API 权限和
最小下单量验证完成前降低；canary 阶段仍应限制为交易所允许的最小订单。

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
