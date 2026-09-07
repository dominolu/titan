//! `BrokerApi` 的 EVM 实现：把统一 REST 语义映射到链上操作。
//!
//! 语义映射（CEX → AMM）：
//! - `submit_order`：Market / IOC / FOK → 带滑点保护的 swap；GTC/GTX 与条件单拒绝；
//! - `cancel_*`：已广播交易不可撤回，`cancel_order` 返回错误，`cancel_all_*` 幂等空操作；
//! - `get_order`：订单号即 tx hash，状态由收据驱动；
//! - `get_positions`：spot 无杠杆，恒为空；杠杆接口返回 1x 默认值；
//! - 余额：热钱包的 ERC20/native 余额，即 spot 的全部可用资金。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider as _;
use async_trait::async_trait;
use chrono::Utc;
use hftbacktest::types::Status;

use crate::api::{
    AccountInfo, AmendOrderRequest, ApiError, ApiOrderStatus, ApiOrderType, ApiPositionSide,
    ApiSide, ApiTimeInForce, Balance, BrokerApi, CancelOrderRequest, FeeRate, Fill, FundingRate,
    IncomeRecord, InstrumentInfo, Kline, LeverageInfo, OpenInterest, OrderInfo, PositionInfo,
    Ticker, Trade, UnifiedOrderRequest,
};
use crate::evm::config::EvmConfig;
use crate::evm::ordermanager::{OrderManager, SharedOrderManager, TrackedOrder};
use crate::evm::provider::EvmProvider;
use crate::evm::tx::{EXCHANGE_NAME, TxEngine};
use crate::evm::types::{PoolConfig, PoolState, u256_to_f64};

/// EVM venue 的统一 REST facade。
pub struct EvmBrokerApi {
    config: Arc<EvmConfig>,
    provider: EvmProvider,
    engine: Arc<TxEngine>,
    order_manager: SharedOrderManager,
    /// 已终态订单的历史（get_order_history / get_fills 的本地视图）。
    history: Arc<Mutex<VecDeque<OrderInfo>>>,
    local_order_id: Arc<AtomicU64>,
}

impl EvmBrokerApi {
    pub fn new(
        config: Arc<EvmConfig>,
        provider: EvmProvider,
        signer: Option<alloy_signer_local::PrivateKeySigner>,
        order_manager: SharedOrderManager,
    ) -> Self {
        let engine = Arc::new(TxEngine::new(
            config.clone(),
            provider.clone(),
            signer,
            order_manager.clone(),
        ));
        Self {
            config,
            provider,
            engine,
            order_manager,
            history: Arc::new(Mutex::new(VecDeque::new())),
            local_order_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// 暴露 TxEngine，让 connector 的 run_account 把 PublishSender 接进确认循环。
    pub fn engine(&self) -> &Arc<TxEngine> {
        &self.engine
    }

    fn record_history(&self, info: OrderInfo) {
        let mut history = self.history.lock().unwrap();
        if history.len() >= 1000 {
            history.pop_front();
        }
        history.push_back(info);
    }

    fn api_status(status: Status) -> ApiOrderStatus {
        match status {
            Status::New => ApiOrderStatus::New,
            Status::PartiallyFilled => ApiOrderStatus::PartiallyFilled,
            Status::Filled => ApiOrderStatus::Filled,
            Status::Canceled => ApiOrderStatus::Canceled,
            Status::Rejected => ApiOrderStatus::Rejected,
            _ => ApiOrderStatus::Unknown,
        }
    }

    fn tracked_to_order_info(key: &str, tracked: &TrackedOrder) -> OrderInfo {
        OrderInfo {
            symbol: tracked.symbol.clone(),
            order_id: tracked.tx_hash.clone().unwrap_or_default(),
            client_order_id: key.to_string(),
            side: match tracked.order.side {
                hftbacktest::types::Side::Buy => ApiSide::Buy,
                hftbacktest::types::Side::Sell => ApiSide::Sell,
                _ => ApiSide::Unknown,
            },
            order_type: ApiOrderType::Market,
            status: Self::api_status(tracked.order.status),
            price: 0.0,
            qty: tracked.order.qty,
            executed_qty: tracked.order.exec_qty,
            avg_price: tracked.order.exec_price_tick as f64 * tracked.order.tick_size,
            leaves_qty: tracked.order.leaves_qty,
            time_in_force: ApiTimeInForce::IOC,
            reduce_only: false,
            position_side: ApiPositionSide::Net,
            create_time: tracked.submitted_at_ms,
            update_time: tracked.order.exch_timestamp / 1_000_000,
            stop_price: None,
        }
    }

    fn synthetic_state(&self, pool: &PoolConfig) -> Result<PoolState, ApiError> {
        let reserves = {
            let market = self.config.market_state.lock().unwrap();
            market
                .pools
                .get(&pool.pair_address)
                .and_then(|s| s.effective())
        };
        reserves
            .map(|(r0, r1)| PoolState {
                confirmed: Some((r0, r1)),
                prechain: None,
                last_update_ns: 0,
            })
            .ok_or_else(|| ApiError::new(EXCHANGE_NAME, "NO_STATE", "pool reserves not loaded yet"))
    }

    fn unsupported(method: &'static str) -> ApiError {
        ApiError::new(
            EXCHANGE_NAME,
            "UNSUPPORTED",
            format!("{method} is not available on AMM venues"),
        )
    }
}

#[async_trait]
impl BrokerApi for EvmBrokerApi {
    async fn ping(&self) -> Result<(), ApiError> {
        self.provider
            .block_number()
            .await
            .map(|_| ())
            .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))
    }

    async fn get_server_time(&self) -> Result<i64, ApiError> {
        // 链没有墙上时钟；用本地时间与 hyperliquid/CEX 的语义对齐。
        Ok(Utc::now().timestamp_millis())
    }

    async fn get_instruments(&self) -> Result<Vec<InstrumentInfo>, ApiError> {
        Ok(self
            .config
            .pools
            .iter()
            .map(|pool| InstrumentInfo {
                symbol: pool.symbol.clone(),
                base_asset: format!("{:?}", pool.base_token),
                quote_asset: format!("{:?}", pool.quote_token),
                tick_size: 0.01,
                lot_size: 0.000_001,
                min_qty: 0.0,
                contract_size: 1.0,
                margin_asset: String::new(),
                price_precision: 6,
                qty_precision: 6,
                tradable: true,
            })
            .collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<Ticker, ApiError> {
        let pool = self.config.pool(symbol).ok_or_else(|| {
            ApiError::new(EXCHANGE_NAME, "SYMBOL", format!("unknown pool {symbol}"))
        })?;
        let state = self.synthetic_state(pool)?;
        let now = Utc::now().timestamp_millis();
        Ok(Ticker {
            symbol: symbol.to_string(),
            last_price: pool.mid_price(&state),
            mark_price: None,
            index_price: None,
            funding_rate: None,
            next_funding_time: None,
            open_24h: 0.0,
            high_24h: 0.0,
            low_24h: 0.0,
            volume_24h: 0.0,
            quote_volume_24h: 0.0,
            timestamp: now,
        })
    }

    async fn get_tickers(&self) -> Result<Vec<Ticker>, ApiError> {
        let mut tickers = Vec::with_capacity(self.config.pools.len());
        for pool in &self.config.pools {
            match self.synthetic_state(pool) {
                Ok(state) => {
                    let now = Utc::now().timestamp_millis();
                    tickers.push(Ticker {
                        symbol: pool.symbol.clone(),
                        last_price: pool.mid_price(&state),
                        mark_price: None,
                        index_price: None,
                        funding_rate: None,
                        next_funding_time: None,
                        open_24h: 0.0,
                        high_24h: 0.0,
                        low_24h: 0.0,
                        volume_24h: 0.0,
                        quote_volume_24h: 0.0,
                        timestamp: now,
                    });
                }
                Err(_) => continue,
            }
        }
        Ok(tickers)
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<crate::api::OrderBook, ApiError> {
        use crate::evm::dex::DexAdapter as _;
        let pool = self.config.pool(symbol).ok_or_else(|| {
            ApiError::new(EXCHANGE_NAME, "SYMBOL", format!("unknown pool {symbol}"))
        })?;
        let state = self.synthetic_state(pool)?;
        let levels = (limit as usize).clamp(1, self.config.book_levels);
        let adapter = crate::evm::dex::uniswap_v2::UniswapV2Adapter::new();
        let (bids, asks) = adapter.synthetic_book(pool, &state, levels);
        let now = Utc::now().timestamp_millis();
        Ok(crate::api::OrderBook {
            symbol: symbol.to_string(),
            bids: bids
                .into_iter()
                .map(|(price, qty)| crate::api::PriceLevel { price, qty })
                .collect(),
            asks: asks
                .into_iter()
                .map(|(price, qty)| crate::api::PriceLevel { price, qty })
                .collect(),
            timestamp: now,
        })
    }

    async fn get_trades(&self, symbol: &str, limit: u32) -> Result<Vec<Trade>, ApiError> {
        let market = self.config.market_state.lock().unwrap();
        Ok(market
            .trades
            .iter()
            .filter(|t| t.symbol == symbol)
            .rev()
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn get_klines(&self, _: &str, _: &str, _: u32) -> Result<Vec<Kline>, ApiError> {
        Err(Self::unsupported("get_klines"))
    }

    async fn get_funding_rate(&self, _: &str) -> Result<FundingRate, ApiError> {
        Err(Self::unsupported("get_funding_rate"))
    }

    async fn get_funding_rate_history(
        &self,
        _: &str,
        _: u32,
    ) -> Result<Vec<FundingRate>, ApiError> {
        Err(Self::unsupported("get_funding_rate_history"))
    }

    async fn get_open_interest(&self, _: &str) -> Result<OpenInterest, ApiError> {
        Err(Self::unsupported("get_open_interest"))
    }

    async fn submit_order(&self, req: &UnifiedOrderRequest) -> Result<OrderInfo, ApiError> {
        let local_id = self.local_order_id.fetch_add(1, Ordering::Relaxed);
        let info = self.engine.submit(req, local_id).await?;
        self.record_history(info.clone());
        Ok(info)
    }

    async fn submit_orders(
        &self,
        reqs: &[UnifiedOrderRequest],
    ) -> Result<Vec<OrderInfo>, ApiError> {
        let mut results = Vec::with_capacity(reqs.len());
        for req in reqs {
            results.push(self.submit_order(req).await?);
        }
        Ok(results)
    }

    async fn cancel_order(&self, _req: &CancelOrderRequest) -> Result<OrderInfo, ApiError> {
        Err(ApiError::new(
            EXCHANGE_NAME,
            "UNSUPPORTED",
            "broadcast transactions cannot be canceled on AMM venues",
        ))
    }

    async fn cancel_orders(&self, reqs: &[CancelOrderRequest]) -> Result<Vec<OrderInfo>, ApiError> {
        // 与 cancel_order 一致：全部拒绝（语义显式，避免调用方误以为撤单成功）。
        if reqs.is_empty() {
            return Ok(Vec::new());
        }
        self.cancel_order(&reqs[0]).await.map(|_| Vec::new())
    }

    async fn cancel_all_orders(&self, _symbol: &str) -> Result<(), ApiError> {
        // AMM 无挂单：pending swap 要么落块要么 revert，无需撤单。
        Ok(())
    }

    async fn cancel_all_after(&self, _timeout_ms: u64) -> Result<(), ApiError> {
        Ok(())
    }

    async fn amend_order(&self, _req: &AmendOrderRequest) -> Result<OrderInfo, ApiError> {
        Err(Self::unsupported("amend_order"))
    }

    async fn get_order(
        &self,
        _symbol: &str,
        order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> Result<OrderInfo, ApiError> {
        // 本地状态机查找（不跨越 await 持锁）。
        let local = {
            let mgr = self.order_manager.lock().unwrap();
            let from_cloid = client_order_id.and_then(|cloid| {
                mgr.by_client_order_id(cloid)
                    .map(|t| (cloid.to_string(), t.clone()))
            });
            let from_tx = order_id.and_then(|tx| {
                mgr.client_order_id_by_tx(tx).and_then(|cloid| {
                    mgr.by_client_order_id(cloid)
                        .map(|t| (cloid.clone(), t.clone()))
                })
            });
            from_cloid.or(from_tx)
        };
        if let Some((cloid, tracked)) = local {
            return Ok(Self::tracked_to_order_info(&cloid, &tracked));
        }
        // 不在本地状态机里：用 tx hash 直接查链上收据。
        if let Some(tx) = order_id {
            return self.order_info_from_receipt(tx).await;
        }
        Err(ApiError::new(
            EXCHANGE_NAME,
            "NOT_FOUND",
            "order not tracked and no tx hash provided",
        ))
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, ApiError> {
        let mgr = self.order_manager.lock().unwrap();
        Ok(mgr
            .pending_by_symbol(symbol)
            .into_iter()
            .map(|(key, tracked)| Self::tracked_to_order_info(&key, &tracked))
            .collect())
    }

    async fn get_order_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<OrderInfo>, ApiError> {
        let history = self.history.lock().unwrap();
        Ok(history
            .iter()
            .filter(|o| o.symbol == symbol)
            .rev()
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn get_fills(&self, symbol: &str, limit: u32) -> Result<Vec<Fill>, ApiError> {
        // 本地视图：由终态订单合成 fill（链上精确成交来自收据日志，见确认循环）。
        let history = self.history.lock().unwrap();
        Ok(history
            .iter()
            .filter(|o| o.symbol == symbol && o.status == ApiOrderStatus::Filled)
            .rev()
            .take(limit as usize)
            .map(|o| Fill {
                symbol: o.symbol.clone(),
                trade_id: o.order_id.clone(),
                order_id: o.order_id.clone(),
                client_order_id: o.client_order_id.clone(),
                price: o.avg_price,
                qty: o.executed_qty,
                side: o.side,
                fee: 0.0,
                fee_asset: String::new(),
                realized_pnl: 0.0,
                maker: false,
                timestamp: o.update_time,
            })
            .collect())
    }

    async fn get_account(&self) -> Result<AccountInfo, ApiError> {
        let wallet = self
            .engine
            .signer
            .as_ref()
            .map(|s| s.address())
            .ok_or_else(|| {
                ApiError::new(EXCHANGE_NAME, "NO_SIGNER", "private_key is not configured")
            })?;
        let mut balances = Vec::new();
        // 原生代币余额。
        let native = self
            .provider
            .rpc()
            .get_balance(wallet)
            .await
            .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))?;
        balances.push(Balance {
            asset: "ETH".to_string(),
            wallet_balance: u256_to_f64(native) / 1e18,
            available_balance: u256_to_f64(native) / 1e18,
            unrealized_pnl: 0.0,
            margin_balance: u256_to_f64(native) / 1e18,
        });
        // 配置里出现过的全部 ERC20。
        let mut tokens: Vec<Address> = Vec::new();
        for pool in &self.config.pools {
            for token in [pool.base_token, pool.quote_token] {
                if !tokens.contains(&token) {
                    tokens.push(token);
                }
            }
        }
        for token in tokens {
            let amount = match self.provider.token_balance(token, wallet).await {
                Ok(v) => v,
                Err(_) => U256::ZERO,
            };
            balances.push(Balance {
                asset: format!("{token:?}"),
                wallet_balance: u256_to_f64(amount) / 1e18,
                available_balance: u256_to_f64(amount) / 1e18,
                unrealized_pnl: 0.0,
                margin_balance: u256_to_f64(amount) / 1e18,
            });
        }
        let total: f64 = balances.iter().map(|b| b.wallet_balance).sum();
        Ok(AccountInfo {
            total_wallet_balance: total,
            total_margin_balance: total,
            total_unrealized_pnl: 0.0,
            available_balance: total,
            balances,
            timestamp: Utc::now().timestamp_millis(),
        })
    }

    async fn get_positions(&self, _symbol: Option<&str>) -> Result<Vec<PositionInfo>, ApiError> {
        // spot 无杠杆持仓模型。
        Ok(Vec::new())
    }

    async fn set_leverage(
        &self,
        symbol: &str,
        _leverage: f64,
        _position_side: Option<ApiPositionSide>,
    ) -> Result<LeverageInfo, ApiError> {
        Ok(LeverageInfo {
            symbol: symbol.to_string(),
            leverage: 1.0,
            margin_type: crate::api::ApiMarginType::Unknown,
            position_side: ApiPositionSide::Net,
        })
    }

    async fn get_leverage(&self, symbol: &str) -> Result<LeverageInfo, ApiError> {
        Ok(LeverageInfo {
            symbol: symbol.to_string(),
            leverage: 1.0,
            margin_type: crate::api::ApiMarginType::Unknown,
            position_side: ApiPositionSide::Net,
        })
    }

    async fn get_fee_rates(&self, symbol: &str) -> Result<FeeRate, ApiError> {
        Ok(FeeRate {
            symbol: symbol.to_string(),
            maker_fee: 0.0,
            taker_fee: self.config.fee_bps / 10_000.0,
            timestamp: Utc::now().timestamp_millis(),
        })
    }

    async fn get_income_history(&self, _: &str, _: u32) -> Result<Vec<IncomeRecord>, ApiError> {
        Err(Self::unsupported("get_income_history"))
    }
}

impl EvmBrokerApi {
    /// 用 tx hash 直接查收据构建订单信息（本地状态机之外的兜底路径）。
    async fn order_info_from_receipt(&self, tx_hash: &str) -> Result<OrderInfo, ApiError> {
        let hash: B256 = tx_hash.parse().map_err(|_| {
            ApiError::new(EXCHANGE_NAME, "INVALID_ARG", "order_id is not a tx hash")
        })?;
        let receipt = self
            .provider
            .transaction_receipt(hash)
            .await
            .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))?;
        let now = Utc::now().timestamp_millis();
        match receipt {
            Some(r) => Ok(OrderInfo {
                symbol: String::new(),
                order_id: tx_hash.to_string(),
                client_order_id: String::new(),
                side: ApiSide::Unknown,
                order_type: ApiOrderType::Market,
                status: if r.status() {
                    ApiOrderStatus::Filled
                } else {
                    ApiOrderStatus::Rejected
                },
                price: 0.0,
                qty: 0.0,
                executed_qty: 0.0,
                avg_price: 0.0,
                leaves_qty: 0.0,
                time_in_force: ApiTimeInForce::IOC,
                reduce_only: false,
                position_side: ApiPositionSide::Net,
                create_time: now,
                update_time: now,
                stop_price: None,
            }),
            None => Err(ApiError::new(
                EXCHANGE_NAME,
                "NOT_FOUND",
                format!("no receipt for {tx_hash}"),
            )),
        }
    }
}

/// OrderManager 的 brokerapi 侧辅助：按符号列 pending（含 client_order_id）。
trait PendingBySymbol {
    fn pending_by_symbol(&self, symbol: &str) -> Vec<(String, TrackedOrder)>;
}

impl PendingBySymbol for OrderManager {
    fn pending_by_symbol(&self, symbol: &str) -> Vec<(String, TrackedOrder)> {
        self.orders_snapshot()
            .into_iter()
            .filter(|(_, t)| t.symbol == symbol && t.order.active())
            .collect()
    }
}
