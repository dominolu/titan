# OKX demo + Hyperliquid testnet 部署

`runtime.toml` 默认是 shadow 门禁：`min_profitability_bps = 10000`，能建立真实公共/私有订阅和账户
快照，但不会生成可成交报价。完成 shadow 验证后，必须先从两个交易所实时 instrument metadata 校验
`price_tick`、`quantity_lot` 与 OKX `ctVal`，再把盈利门槛降到 canary 值。

凭据只允许放在本目录的 `secrets/` 中，文件权限必须为 `0600`：

- `okx-demo.toml`：`api_key`、`secret`、`passphrase`；必须是 OKX Demo Trading API key。
- `hyperliquid-testnet.toml`：`private_key`、可选 `account_address`；必须对应 Hyperliquid testnet。

运行时通过 `secret://file/...` 引用凭据，配置和日志不会包含密钥。两个账户必须是专用测试账户；Account
connector 停机时会清理其注册品种上的挂单。

服务器执行以下命令会以单任务构建、生成带 SHA256 的 Connector packages，并完成不读取凭据的 live
配置校验：

```bash
./deploy/okx_hyperliquid_xemm_testnet/prepare_server.sh
```

服务文件按 user systemd 部署；首次运行保持 shadow，确认账户 READY、无现存挂单/仓位并观察稳定行情后再
进入 canary。
