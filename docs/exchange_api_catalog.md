# 官方原生 API 覆盖目录

核对日期：2026-10-31。以下是已生成命名 Rust 接口的官方公开路由；编译、协议测试及实盘状态见 [接入说明](exchange_onboarding.md)。不把未公开的交易协议、内部接口或成功受理当作已完成实盘验收。

## Backpack（75 个 HTTP 操作）

| 操作 | HTTP | Rust 方法 | 官方来源 |
| --- | --- | --- | --- |
| `get_account` | `GET /api/v1/account` | `native_get_account` | [规范](https://docs.backpack.exchange/#operation/get_account) |
| `update_account_settings` | `PATCH /api/v1/account` | `native_update_account_settings` | [规范](https://docs.backpack.exchange/#operation/update_account_settings) |
| `convert_dust` | `POST /api/v1/account/convertDust` | `native_convert_dust` | [规范](https://docs.backpack.exchange/#operation/convert_dust) |
| `get_max_borrow_quantity` | `GET /api/v1/account/limits/borrow` | `native_get_max_borrow_quantity` | [规范](https://docs.backpack.exchange/#operation/get_max_borrow_quantity) |
| `get_max_order_quantity` | `GET /api/v1/account/limits/order` | `native_get_max_order_quantity` | [规范](https://docs.backpack.exchange/#operation/get_max_order_quantity) |
| `get_max_withdrawal_quantity` | `GET /api/v1/account/limits/withdrawal` | `native_get_max_withdrawal_quantity` | [规范](https://docs.backpack.exchange/#operation/get_max_withdrawal_quantity) |
| `get_assets` | `GET /api/v1/assets` | `native_get_assets` | [规范](https://docs.backpack.exchange/#operation/get_assets) |
| `get_collateral_parameters` | `GET /api/v1/collateral` | `native_get_collateral_parameters` | [规范](https://docs.backpack.exchange/#operation/get_collateral_parameters) |
| `get_borrow_lend_positions` | `GET /api/v1/borrowLend/positions` | `native_get_borrow_lend_positions` | [规范](https://docs.backpack.exchange/#operation/get_borrow_lend_positions) |
| `get_borrow_lend_estimated_liquidation_price` | `GET /api/v1/borrowLend/position/liquidationPrice` | `native_get_borrow_lend_estimated_liquidation_price` | [规范](https://docs.backpack.exchange/#operation/get_borrow_lend_estimated_liquidation_price) |
| `execute_borrow_lend` | `POST /api/v1/borrowLend` | `native_execute_borrow_lend` | [规范](https://docs.backpack.exchange/#operation/execute_borrow_lend) |
| `get_borrow_lend_markets` | `GET /api/v1/borrowLend/markets` | `native_get_borrow_lend_markets` | [规范](https://docs.backpack.exchange/#operation/get_borrow_lend_markets) |
| `get_borrow_lend_markets_history` | `GET /api/v1/borrowLend/markets/history` | `native_get_borrow_lend_markets_history` | [规范](https://docs.backpack.exchange/#operation/get_borrow_lend_markets_history) |
| `get_apy_rates` | `GET /api/v1/borrowLend/apy` | `native_get_apy_rates` | [规范](https://docs.backpack.exchange/#operation/get_apy_rates) |
| `get_balances` | `GET /api/v1/capital` | `native_get_balances` | [规范](https://docs.backpack.exchange/#operation/get_balances) |
| `get_collateral` | `GET /api/v1/capital/collateral` | `native_get_collateral` | [规范](https://docs.backpack.exchange/#operation/get_collateral) |
| `get_deposits` | `GET /wapi/v1/capital/deposits` | `native_get_deposits` | [规范](https://docs.backpack.exchange/#operation/get_deposits) |
| `get_deposit_address` | `GET /wapi/v1/capital/deposit/address` | `native_get_deposit_address` | [规范](https://docs.backpack.exchange/#operation/get_deposit_address) |
| `get_markets` | `GET /api/v1/markets` | `native_get_markets` | [规范](https://docs.backpack.exchange/#operation/get_markets) |
| `get_market` | `GET /api/v1/market` | `native_get_market` | [规范](https://docs.backpack.exchange/#operation/get_market) |
| `get_depth` | `GET /api/v1/depth` | `native_get_depth` | [规范](https://docs.backpack.exchange/#operation/get_depth) |
| `get_mark_prices` | `GET /api/v1/markPrices` | `native_get_mark_prices` | [规范](https://docs.backpack.exchange/#operation/get_mark_prices) |
| `get_open_interest` | `GET /api/v1/openInterest` | `native_get_open_interest` | [规范](https://docs.backpack.exchange/#operation/get_open_interest) |
| `get_market_sessions` | `GET /api/v1/stockSessions` | `native_get_market_sessions` | [规范](https://docs.backpack.exchange/#operation/get_market_sessions) |
| `get_market_holidays` | `GET /api/v1/stockHolidays` | `native_get_market_holidays` | [规范](https://docs.backpack.exchange/#operation/get_market_holidays) |
| `get_securities` | `GET /api/v1/securities` | `native_get_securities` | [规范](https://docs.backpack.exchange/#operation/get_securities) |
| `get_order` | `GET /api/v1/order` | `native_get_order` | [规范](https://docs.backpack.exchange/#operation/get_order) |
| `execute_order` | `POST /api/v1/order` | `native_execute_order` | [规范](https://docs.backpack.exchange/#operation/execute_order) |
| `cancel_order` | `DELETE /api/v1/order` | `native_cancel_order` | [规范](https://docs.backpack.exchange/#operation/cancel_order) |
| `execute_order_batch` | `POST /api/v1/orders` | `native_execute_order_batch` | [规范](https://docs.backpack.exchange/#operation/execute_order_batch) |
| `get_open_orders` | `GET /api/v1/orders` | `native_get_open_orders` | [规范](https://docs.backpack.exchange/#operation/get_open_orders) |
| `cancel_open_orders` | `DELETE /api/v1/orders` | `native_cancel_open_orders` | [规范](https://docs.backpack.exchange/#operation/cancel_open_orders) |
| `get_positions` | `GET /api/v1/position` | `native_get_positions` | [规范](https://docs.backpack.exchange/#operation/get_positions) |
| `submit_quote` | `POST /api/v1/rfq/quote` | `native_submit_quote` | [规范](https://docs.backpack.exchange/#operation/submit_quote) |
| `get_open_rfqs` | `GET /api/v1/rfqs` | `native_get_open_rfqs` | [规范](https://docs.backpack.exchange/#operation/get_open_rfqs) |
| `submit_rfq` | `POST /api/v1/rfq` | `native_submit_rfq` | [规范](https://docs.backpack.exchange/#operation/submit_rfq) |
| `accept_quote` | `POST /api/v1/rfq/accept` | `native_accept_quote` | [规范](https://docs.backpack.exchange/#operation/accept_quote) |
| `cancel_rfq` | `POST /api/v1/rfq/cancel` | `native_cancel_rfq` | [规范](https://docs.backpack.exchange/#operation/cancel_rfq) |
| `get_status` | `GET /api/v1/status` | `native_get_status` | [规范](https://docs.backpack.exchange/#operation/get_status) |
| `ping` | `GET /api/v1/ping` | `native_ping` | [规范](https://docs.backpack.exchange/#operation/ping) |
| `get_time` | `GET /api/v1/time` | `native_get_time` | [规范](https://docs.backpack.exchange/#operation/get_time) |
| `get_recent_trades` | `GET /api/v1/trades` | `native_get_recent_trades` | [规范](https://docs.backpack.exchange/#operation/get_recent_trades) |
| `get_vaults` | `GET /api/v1/vaults` | `native_get_vaults` | [规范](https://docs.backpack.exchange/#operation/get_vaults) |
| `vault_mint` | `POST /api/v1/vault/mint` | `native_vault_mint` | [规范](https://docs.backpack.exchange/#operation/vault_mint) |
| `vault_redeem` | `POST /api/v1/vault/redeem` | `native_vault_redeem` | [规范](https://docs.backpack.exchange/#operation/vault_redeem) |
| `vault_redeem_cancel` | `DELETE /api/v1/vault/redeem` | `native_vault_redeem_cancel` | [规范](https://docs.backpack.exchange/#operation/vault_redeem_cancel) |
| `get_vault_pending_redeems` | `GET /api/v1/vault/redeems/pending` | `native_get_vault_pending_redeems` | [规范](https://docs.backpack.exchange/#operation/get_vault_pending_redeems) |
| `get_vault_nav` | `GET /api/v1/vault/nav` | `native_get_vault_nav` | [规范](https://docs.backpack.exchange/#operation/get_vault_nav) |
| `get_vault_history` | `GET /api/v1/vaults/history` | `native_get_vault_history` | [规范](https://docs.backpack.exchange/#operation/get_vault_history) |
| `get_wallets` | `GET /api/v1/wallets` | `native_get_wallets` | [规范](https://docs.backpack.exchange/#operation/get_wallets) |
| `get_withdrawals` | `GET /wapi/v1/capital/withdrawals` | `native_get_withdrawals` | [规范](https://docs.backpack.exchange/#operation/get_withdrawals) |
| `request_withdrawal` | `POST /wapi/v1/capital/withdrawals` | `native_request_withdrawal` | [规范](https://docs.backpack.exchange/#operation/request_withdrawal) |
| `get_withdrawal_delay` | `GET /wapi/v1/capital/withdrawals/delay` | `native_get_withdrawal_delay` | [规范](https://docs.backpack.exchange/#operation/get_withdrawal_delay) |
| `create_withdrawal_delay` | `POST /wapi/v1/capital/withdrawals/delay` | `native_create_withdrawal_delay` | [规范](https://docs.backpack.exchange/#operation/create_withdrawal_delay) |
| `update_withdrawal_delay` | `PATCH /wapi/v1/capital/withdrawals/delay` | `native_update_withdrawal_delay` | [规范](https://docs.backpack.exchange/#operation/update_withdrawal_delay) |
| `get_borrow_lend_history` | `GET /wapi/v1/history/borrowLend` | `native_get_borrow_lend_history` | [规范](https://docs.backpack.exchange/#operation/get_borrow_lend_history) |
| `get_interest_history` | `GET /wapi/v1/history/interest` | `native_get_interest_history` | [规范](https://docs.backpack.exchange/#operation/get_interest_history) |
| `get_borrow_lend_position_history` | `GET /wapi/v1/history/borrowLend/positions` | `native_get_borrow_lend_position_history` | [规范](https://docs.backpack.exchange/#operation/get_borrow_lend_position_history) |
| `get_dust_history` | `GET /wapi/v1/history/dust` | `native_get_dust_history` | [规范](https://docs.backpack.exchange/#operation/get_dust_history) |
| `get_fills` | `GET /wapi/v1/history/fills` | `native_get_fills` | [规范](https://docs.backpack.exchange/#operation/get_fills) |
| `get_funding_payments` | `GET /wapi/v1/history/funding` | `native_get_funding_payments` | [规范](https://docs.backpack.exchange/#operation/get_funding_payments) |
| `get_order_history` | `GET /wapi/v1/history/orders` | `native_get_order_history` | [规范](https://docs.backpack.exchange/#operation/get_order_history) |
| `get_rfq_history` | `GET /wapi/v1/history/rfq` | `native_get_rfq_history` | [规范](https://docs.backpack.exchange/#operation/get_rfq_history) |
| `get_quote_history` | `GET /wapi/v1/history/quote` | `native_get_quote_history` | [规范](https://docs.backpack.exchange/#operation/get_quote_history) |
| `get_rfq_fill_history` | `GET /wapi/v1/history/rfq/fill` | `native_get_rfq_fill_history` | [规范](https://docs.backpack.exchange/#operation/get_rfq_fill_history) |
| `get_quote_fill_history` | `GET /wapi/v1/history/quote/fill` | `native_get_quote_fill_history` | [规范](https://docs.backpack.exchange/#operation/get_quote_fill_history) |
| `get_settlement_history` | `GET /wapi/v1/history/settlement` | `native_get_settlement_history` | [规范](https://docs.backpack.exchange/#operation/get_settlement_history) |
| `get_position_history` | `GET /wapi/v1/history/position` | `native_get_position_history` | [规范](https://docs.backpack.exchange/#operation/get_position_history) |
| `get_klines` | `GET /api/v1/klines` | `native_get_klines` | [规范](https://docs.backpack.exchange/#operation/get_klines) |
| `get_ticker` | `GET /api/v1/ticker` | `native_get_ticker` | [规范](https://docs.backpack.exchange/#operation/get_ticker) |
| `get_tickers` | `GET /api/v1/tickers` | `native_get_tickers` | [规范](https://docs.backpack.exchange/#operation/get_tickers) |
| `get_funding_interval_rates` | `GET /api/v1/fundingRates` | `native_get_funding_interval_rates` | [规范](https://docs.backpack.exchange/#operation/get_funding_interval_rates) |
| `get_historical_trades` | `GET /api/v1/trades/history` | `native_get_historical_trades` | [规范](https://docs.backpack.exchange/#operation/get_historical_trades) |
| `get_stock_indicative_quote` | `GET /api/v1/stockIndicativeQuote` | `native_get_stock_indicative_quote` | [规范](https://docs.backpack.exchange/#operation/get_stock_indicative_quote) |
| `get_stock_corporate_action_schedule` | `GET /api/v1/stocks/corporateActions` | `native_get_stock_corporate_action_schedule` | [规范](https://docs.backpack.exchange/#operation/get_stock_corporate_action_schedule) |

## Lighter（104 个 HTTP 操作）

| 操作 | HTTP | Rust 方法 | 官方来源 |
| --- | --- | --- | --- |
| `status` | `GET /` | `native_status` | [规范](https://docs.lighter.xyz/api/system/status.md) |
| `account` | `GET /api/v1/account` | `native_account` | [规范](https://docs.lighter.xyz/api/account/account.md) |
| `accountActiveOrders` | `GET /api/v1/accountActiveOrders` | `native_account_active_orders` | [规范](https://docs.lighter.xyz/api/orders/accountactiveorders.md) |
| `accountInactiveOrders` | `GET /api/v1/accountInactiveOrders` | `native_account_inactive_orders` | [规范](https://docs.lighter.xyz/api/orders/accountinactiveorders.md) |
| `accountLimits` | `GET /api/v1/accountLimits` | `native_account_limits` | [规范](https://docs.lighter.xyz/api/account/accountlimits.md) |
| `accountMetadata` | `GET /api/v1/accountMetadata` | `native_account_metadata` | [规范](https://docs.lighter.xyz/api/account/accountmetadata.md) |
| `accountOrders` | `GET /api/v1/accountOrders` | `native_account_orders` | [规范](https://docs.lighter.xyz/api/orders/accountorders.md) |
| `accountTxs` | `GET /api/v1/accountTxs` | `native_account_txs` | [规范](https://github.com/elliottech/lighter-python/blob/d30dc2296d27f1584faeb22a590486aded1f03b1/openapi.json) |
| `accountsByL1Address` | `GET /api/v1/accountsByL1Address` | `native_accounts_by_l1_address` | [规范](https://docs.lighter.xyz/api/account/accountsbyl1address.md) |
| `announcement` | `GET /api/v1/announcement` | `native_announcement` | [规范](https://docs.lighter.xyz/api/announcements/announcement.md) |
| `apikeys` | `GET /api/v1/apikeys` | `native_apikeys` | [规范](https://docs.lighter.xyz/api/api-keys/apikeys.md) |
| `assetDetails` | `GET /api/v1/assetDetails` | `native_asset_details` | [规范](https://docs.lighter.xyz/api/orders/assetdetails.md) |
| `block` | `GET /api/v1/block` | `native_block` | [规范](https://github.com/elliottech/lighter-python/blob/d30dc2296d27f1584faeb22a590486aded1f03b1/openapi.json) |
| `blockTxs` | `GET /api/v1/blockTxs` | `native_block_txs` | [规范](https://github.com/elliottech/lighter-python/blob/d30dc2296d27f1584faeb22a590486aded1f03b1/openapi.json) |
| `blocks` | `GET /api/v1/blocks` | `native_blocks` | [规范](https://github.com/elliottech/lighter-python/blob/d30dc2296d27f1584faeb22a590486aded1f03b1/openapi.json) |
| `candles` | `GET /api/v1/candles` | `native_candles` | [规范](https://docs.lighter.xyz/api/candlesticks/candles.md) |
| `changeAccountTier` | `POST /api/v1/changeAccountTier` | `native_change_account_tier` | [规范](https://docs.lighter.xyz/api/account/changeaccounttier.md) |
| `createIntentAddress` | `POST /api/v1/createIntentAddress` | `native_create_intent_address` | [规范](https://docs.lighter.xyz/api/bridge/createintentaddress.md) |
| `currentHeight` | `GET /api/v1/currentHeight` | `native_current_height` | [规范](https://github.com/elliottech/lighter-python/blob/d30dc2296d27f1584faeb22a590486aded1f03b1/openapi.json) |
| `deposit_history` | `GET /api/v1/deposit/history` | `native_deposit_history` | [规范](https://docs.lighter.xyz/api/transactions/deposit_history.md) |
| `deposit_latest` | `GET /api/v1/deposit/latest` | `native_deposit_latest` | [规范](https://docs.lighter.xyz/api/bridge/deposit_latest.md) |
| `deposit_networks` | `GET /api/v1/deposit/networks` | `native_deposit_networks` | [规范](https://docs.lighter.xyz/api/bridge/deposit_networks.md) |
| `events` | `GET /api/v1/events` | `native_events` | [规范](https://docs.lighter.xyz/api/binary-options/events.md) |
| `exchangeMetrics` | `GET /api/v1/exchangeMetrics` | `native_exchange_metrics` | [规范](https://docs.lighter.xyz/api/info/exchangemetrics.md) |
| `exchangeStats` | `GET /api/v1/exchangeStats` | `native_exchange_stats` | [规范](https://docs.lighter.xyz/api/info/exchangestats.md) |
| `executeStats` | `GET /api/v1/executeStats` | `native_execute_stats` | [规范](https://docs.lighter.xyz/api/info/executestats.md) |
| `export` | `GET /api/v1/export` | `native_export` | [规范](https://docs.lighter.xyz/api/orders/export.md) |
| `export_historicalTrades` | `GET /api/v1/export/historicalTrades` | `native_export_historical_trades` | [规范](https://docs.lighter.xyz/api/orders/export_historicaltrades.md) |
| `fastbridge_info` | `GET /api/v1/fastbridge/info` | `native_fastbridge_info` | [规范](https://docs.lighter.xyz/api/bridge/fastbridge_info.md) |
| `fastwithdraw` | `POST /api/v1/fastwithdraw` | `native_fastwithdraw` | [规范](https://docs.lighter.xyz/api/bridge/fastwithdraw.md) |
| `fastwithdraw_info` | `GET /api/v1/fastwithdraw/info` | `native_fastwithdraw_info` | [规范](https://docs.lighter.xyz/api/bridge/fastwithdraw_info.md) |
| `feeTiers` | `GET /api/v1/feeTiers` | `native_fee_tiers` | [规范](https://docs.lighter.xyz/api/binary-options/feetiers.md) |
| `funding-rates` | `GET /api/v1/funding-rates` | `native_funding_rates` | [规范](https://docs.lighter.xyz/api/funding/funding-rates.md) |
| `fundings` | `GET /api/v1/fundings` | `native_fundings` | [规范](https://docs.lighter.xyz/api/funding/fundings.md) |
| `getMakerOnlyApiKeys` | `GET /api/v1/getMakerOnlyApiKeys` | `native_get_maker_only_api_keys` | [规范](https://docs.lighter.xyz/api/api-keys/getmakeronlyapikeys.md) |
| `l1Metadata` | `GET /api/v1/l1Metadata` | `native_l1_metadata` | [规范](https://docs.lighter.xyz/api/account/l1metadata.md) |
| `layer1BasicInfo` | `GET /api/v1/layer1BasicInfo` | `native_layer1_basic_info` | [规范](https://docs.lighter.xyz/api/system/layer1basicinfo.md) |
| `leaderboard` | `GET /api/v1/leaderboard` | `native_leaderboard` | [规范](https://docs.lighter.xyz/api/points/leaderboard.md) |
| `leaseOptions` | `GET /api/v1/leaseOptions` | `native_lease_options` | [规范](https://docs.lighter.xyz/api/fee-credits/leaseoptions.md) |
| `leases` | `GET /api/v1/leases` | `native_leases` | [规范](https://docs.lighter.xyz/api/fee-credits/leases.md) |
| `liquidations` | `GET /api/v1/liquidations` | `native_liquidations` | [规范](https://docs.lighter.xyz/api/account/liquidations.md) |
| `litLease` | `POST /api/v1/litLease` | `native_lit_lease` | [规范](https://docs.lighter.xyz/api/fee-credits/litlease.md) |
| `livePoints_total` | `GET /api/v1/livePoints/total` | `native_live_points_total` | [规范](https://docs.lighter.xyz/api/points/livepoints_total.md) |
| `markPriceCandles` | `GET /api/v1/markPriceCandles` | `native_mark_price_candles` | [规范](https://docs.lighter.xyz/api/candlesticks/markpricecandles.md) |
| `marketPriceCharts` | `GET /api/v1/marketPriceCharts` | `native_market_price_charts` | [规范](https://docs.lighter.xyz/api/candlesticks/marketpricecharts.md) |
| `nextNonce` | `GET /api/v1/nextNonce` | `native_next_nonce` | [规范](https://docs.lighter.xyz/api/api-keys/nextnonce.md) |
| `notification_ack` | `POST /api/v1/notification/ack` | `native_notification_ack` | [规范](https://docs.lighter.xyz/api/notifications/notification_ack.md) |
| `orderBookDetails` | `GET /api/v1/orderBookDetails` | `native_order_book_details` | [规范](https://docs.lighter.xyz/api/orders/orderbookdetails.md) |
| `orderBookOrders` | `GET /api/v1/orderBookOrders` | `native_order_book_orders` | [规范](https://docs.lighter.xyz/api/orders/orderbookorders.md) |
| `orderBooks` | `GET /api/v1/orderBooks` | `native_order_books` | [规范](https://docs.lighter.xyz/api/orders/orderbooks.md) |
| `partnerStats` | `GET /api/v1/partnerStats` | `native_partner_stats` | [规范](https://docs.lighter.xyz/api/info/partnerstats.md) |
| `pnl` | `GET /api/v1/pnl` | `native_pnl` | [规范](https://docs.lighter.xyz/api/account/pnl.md) |
| `pnlLeaderboard` | `GET /api/v1/pnlLeaderboard` | `native_pnl_leaderboard` | [规范](https://docs.lighter.xyz/api/account/pnlleaderboard.md) |
| `portfolio_boPnl` | `GET /api/v1/portfolio/boPnl` | `native_portfolio_bo_pnl` | [规范](https://docs.lighter.xyz/api/binary-options/portfolio_bopnl.md) |
| `positionFunding` | `GET /api/v1/positionFunding` | `native_position_funding` | [规范](https://docs.lighter.xyz/api/account/positionfunding.md) |
| `priceOracleInfo` | `GET /api/v1/priceOracleInfo` | `native_price_oracle_info` | [规范](https://docs.lighter.xyz/api/info/priceoracleinfo.md) |
| `publicPoolsMetadata` | `GET /api/v1/publicPoolsMetadata` | `native_public_pools_metadata` | [规范](https://docs.lighter.xyz/api/account/publicpoolsmetadata.md) |
| `recentTrades` | `GET /api/v1/recentTrades` | `native_recent_trades` | [规范](https://docs.lighter.xyz/api/orders/recenttrades.md) |
| `referral_create` | `POST /api/v1/referral/create` | `native_referral_create` | [规范](https://docs.lighter.xyz/api/referral/referral_create.md) |
| `referral_get` | `GET /api/v1/referral/get` | `native_referral_get` | [规范](https://docs.lighter.xyz/api/referral/referral_get.md) |
| `referral_kickback_update` | `POST /api/v1/referral/kickback/update` | `native_referral_kickback_update` | [规范](https://docs.lighter.xyz/api/referral/referral_kickback_update.md) |
| `referral_points` | `GET /api/v1/referral/points` | `native_referral_points` | [规范](https://docs.lighter.xyz/api/referral/referral_points.md) |
| `referral_stats` | `GET /api/v1/referral/stats` | `native_referral_stats` | [规范](https://docs.lighter.xyz/api/referral/referral_stats.md) |
| `referral_update` | `POST /api/v1/referral/update` | `native_referral_update` | [规范](https://docs.lighter.xyz/api/referral/referral_update.md) |
| `referral_use` | `POST /api/v1/referral/use` | `native_referral_use` | [规范](https://docs.lighter.xyz/api/referral/referral_use.md) |
| `referral_userReferrals` | `GET /api/v1/referral/userReferrals` | `native_referral_user_referrals` | [规范](https://docs.lighter.xyz/api/referral/userreferrals.md) |
| `rfq_create` | `POST /api/v1/rfq/create` | `native_rfq_create` | [规范](https://docs.lighter.xyz/api/rfq/rfq_create.md) |
| `rfq_get` | `GET /api/v1/rfq/get` | `native_rfq_get` | [规范](https://docs.lighter.xyz/api/rfq/rfq_get.md) |
| `rfq_list` | `GET /api/v1/rfq/list` | `native_rfq_list` | [规范](https://docs.lighter.xyz/api/rfq/rfq_list.md) |
| `rfq_respond` | `POST /api/v1/rfq/respond` | `native_rfq_respond` | [规范](https://docs.lighter.xyz/api/rfq/rfq_respond.md) |
| `rfq_update` | `POST /api/v1/rfq/update` | `native_rfq_update` | [规范](https://docs.lighter.xyz/api/rfq/rfq_update.md) |
| `sendTx` | `POST /api/v1/sendTx` | `native_send_tx` | [规范](https://docs.lighter.xyz/api/transactions/sendtx.md) |
| `sendTxBatch` | `POST /api/v1/sendTxBatch` | `native_send_tx_batch` | [规范](https://docs.lighter.xyz/api/transactions/sendtxbatch.md) |
| `setAccountMetadata` | `POST /api/v1/setAccountMetadata` | `native_set_account_metadata` | [规范](https://github.com/elliottech/lighter-python/blob/d30dc2296d27f1584faeb22a590486aded1f03b1/openapi.json) |
| `setMakerOnlyApiKeys` | `POST /api/v1/setMakerOnlyApiKeys` | `native_set_maker_only_api_keys` | [规范](https://docs.lighter.xyz/api/api-keys/setmakeronlyapikeys.md) |
| `syntheticSpotInfo` | `GET /api/v1/syntheticSpotInfo` | `native_synthetic_spot_info` | [规范](https://docs.lighter.xyz/api/info/syntheticspotinfo.md) |
| `systemConfig` | `GET /api/v1/systemConfig` | `native_system_config` | [规范](https://docs.lighter.xyz/api/system/systemconfig.md) |
| `tokenlist` | `GET /api/v1/tokenlist` | `native_tokenlist` | [规范](https://docs.lighter.xyz/api/info/tokenlist.md) |
| `tokens` | `GET /api/v1/tokens` | `native_tokens` | [规范](https://docs.lighter.xyz/api/api-keys/tokens.md) |
| `tokens_create` | `POST /api/v1/tokens/create` | `native_tokens_create` | [规范](https://docs.lighter.xyz/api/api-keys/tokens_create.md) |
| `tokens_revoke` | `POST /api/v1/tokens/revoke` | `native_tokens_revoke` | [规范](https://docs.lighter.xyz/api/api-keys/tokens_revoke.md) |
| `trades` | `GET /api/v1/trades` | `native_trades` | [规范](https://docs.lighter.xyz/api/orders/trades.md) |
| `transfer_history` | `GET /api/v1/transfer/history` | `native_transfer_history` | [规范](https://docs.lighter.xyz/api/transactions/transfer_history.md) |
| `transferFeeInfo` | `GET /api/v1/transferFeeInfo` | `native_transfer_fee_info` | [规范](https://docs.lighter.xyz/api/info/transferfeeinfo.md) |
| `tx` | `GET /api/v1/tx` | `native_tx` | [规范](https://docs.lighter.xyz/api/transactions/tx.md) |
| `txFromL1TxHash` | `GET /api/v1/txFromL1TxHash` | `native_tx_from_l1_tx_hash` | [规范](https://docs.lighter.xyz/api/transactions/txfroml1txhash.md) |
| `txs` | `GET /api/v1/txs` | `native_txs` | [规范](https://github.com/elliottech/lighter-python/blob/d30dc2296d27f1584faeb22a590486aded1f03b1/openapi.json) |
| `upDownCandles` | `GET /api/v1/upDownCandles` | `native_up_down_candles` | [规范](https://docs.lighter.xyz/api/binary-options/updowncandles.md) |
| `withdraw_history` | `GET /api/v1/withdraw/history` | `native_withdraw_history` | [规范](https://docs.lighter.xyz/api/transactions/withdraw_history.md) |
| `withdrawalDelay` | `GET /api/v1/withdrawalDelay` | `native_withdrawal_delay` | [规范](https://docs.lighter.xyz/api/info/withdrawaldelay.md) |
| `info` | `GET /info` | `native_info` | [规范](https://docs.lighter.xyz/api/system/info.md) |
| `explorer_get_accounts_param_assets` | `GET /accounts/{param}/assets` | `native_explorer_get_accounts_param_assets` | [规范](https://docs.lighter.xyz/api/explorer-api/account/get-account-assets.md) |
| `explorer_get_accounts_param_logs` | `GET /accounts/{param}/logs` | `native_explorer_get_accounts_param_logs` | [规范](https://docs.lighter.xyz/api/explorer-api/account/get-account-by-address.md) |
| `explorer_get_accounts_param_positions` | `GET /accounts/{param}/positions` | `native_explorer_get_accounts_param_positions` | [规范](https://docs.lighter.xyz/api/explorer-api/account/get-account-by-address-1.md) |
| `explorer_get_batches` | `GET /batches` | `native_explorer_get_batches` | [规范](https://docs.lighter.xyz/api/explorer-api/batches/get-recent-batches.md) |
| `explorer_get_batches_batch_id_` | `GET /batches/{batchId}` | `native_explorer_get_batches_batch_id_` | [规范](https://docs.lighter.xyz/api/explorer-api/batches/get-batch-by-id.md) |
| `explorer_get_blocks` | `GET /blocks` | `native_explorer_get_blocks` | [规范](https://docs.lighter.xyz/api/explorer-api/blocks/get-recent-blocks.md) |
| `explorer_get_blocks_block_id_` | `GET /blocks/{blockId}` | `native_explorer_get_blocks_block_id_` | [规范](https://docs.lighter.xyz/api/explorer-api/blocks/get-block-by-id.md) |
| `explorer_get_logs_hash_` | `GET /logs/{hash}` | `native_explorer_get_logs_hash_` | [规范](https://docs.lighter.xyz/api/explorer-api/logs/get-log-by-hash.md) |
| `explorer_get_markets` | `GET /markets` | `native_explorer_get_markets` | [规范](https://docs.lighter.xyz/api/explorer-api/markets/list-all-markets.md) |
| `explorer_get_markets_symbol_logs` | `GET /markets/{symbol}/logs` | `native_explorer_get_markets_symbol_logs` | [规范](https://docs.lighter.xyz/api/explorer-api/markets/get-market-latest-logs-to-query-perpetual-markets-use-the-symbol-alone-for-spot-markets-use-symbol-q.md) |
| `explorer_get_search` | `GET /search` | `native_explorer_get_search` | [规范](https://docs.lighter.xyz/api/explorer-api/search/search-for-blocks-batches-transactions-or-accounts.md) |
| `explorer_get_stats_tx` | `GET /stats/tx` | `native_explorer_get_stats_tx` | [规范](https://docs.lighter.xyz/api/explorer-api/stats/get-tx-stats.md) |
| `explorer_get_total` | `GET /total` | `native_explorer_get_total` | [规范](https://docs.lighter.xyz/api/explorer-api/stats/list-total-values.md) |

## Variational（2 个公开 HTTP 操作）

| HTTP | Rust 方法 | 范围 |
| --- | --- | --- |
| `GET /metadata/v2/stats` | `stats` / `native_stats(StatsVersion::V2)` | 推荐 v2，原生入口保留全部字段 |
| `GET /metadata/stats` | `native_stats(StatsVersion::LegacyV1)` | 官方仍提供的废弃 v1，不转换为 v2 |

[官方 Trading API](https://docs.variational.io/technical-documentation/api#trading-api)尚未开放，不能补造交易接口。

## Lighter 原生签名（23 类交易）

固定官方提交 `aefa96848a105d82a6bbb55d3d05663293a71713`；`NativeTransaction` 对应全部 `Sign*` 导出。

| C 签名入口 | Rust 交易类型 |
| --- | --- |
| `SignChangePubKey` | `NativeTransaction::ChangePubKey` |
| `SignCreateOrder` | `NativeTransaction::CreateOrder` |
| `SignCreateGroupedOrders` | `NativeTransaction::CreateGroupedOrders` |
| `SignCancelOrder` | `NativeTransaction::CancelOrder` |
| `SignWithdraw` | `NativeTransaction::Withdraw` |
| `SignCreateSubAccount` | `NativeTransaction::CreateSubAccount` |
| `SignCancelAllOrders` | `NativeTransaction::CancelAllOrders` |
| `SignModifyOrder` | `NativeTransaction::ModifyOrder` |
| `SignTransfer` | `NativeTransaction::Transfer` |
| `SignCreatePublicPool` | `NativeTransaction::CreatePublicPool` |
| `SignUpdatePublicPool` | `NativeTransaction::UpdatePublicPool` |
| `SignMintShares` | `NativeTransaction::MintShares` |
| `SignBurnShares` | `NativeTransaction::BurnShares` |
| `SignUpdateLeverage` | `NativeTransaction::UpdateLeverage` |
| `SignUpdateMargin` | `NativeTransaction::UpdateMargin` |
| `SignStakeAssets` | `NativeTransaction::StakeAssets` |
| `SignUnstakeAssets` | `NativeTransaction::UnstakeAssets` |
| `SignApproveIntegrator` | `NativeTransaction::ApproveIntegrator` |
| `SignUpdateAccountConfig` | `NativeTransaction::UpdateAccountConfig` |
| `SignUpdateAccountAssetConfig` | `NativeTransaction::UpdateAccountAssetConfig` |
| `SignBinaryOptionsOperatorCreateMarket` | `NativeTransaction::BinaryOptionsOperatorCreateMarket` |
| `SignBinaryOptionsOperatorUpdateMarket` | `NativeTransaction::BinaryOptionsOperatorUpdateMarket` |
| `SignBinaryOptionsOperatorSettleMarket` | `NativeTransaction::BinaryOptionsOperatorSettleMarket` |

另提供 `LighterNativeSigner::new`、`generate_api_key`、`auth_token`、`check_client`，原生内存由内部 Free 回收。

## WebSocket

Backpack 的 20 个流模式见 `backpack::WEBSOCKET_STREAMS`；Lighter 的 25 个频道模式见 `lighter::WEBSOCKET_CHANNELS`。原生订阅保留完整字段、JSON/二进制消息及服务器错误，用户负责相关性和状态管理；统一运行时仍使用专门校验连续性与账户就绪的适配器。

Lighter 另提供 `send_transaction` / `send_transactions`，分别使用官方 `jsonapi/sendtx` / `jsonapi/sendtxbatch` 编码。写操作不自动重发。
