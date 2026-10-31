//! `BrokerApi` 的 EVM 公共行情 facade。
//!
//! 原生池行情、交易构造/签名/收据工具保留；统一账户与执行明确失败关闭。
//! 精确 swap 数量、手续费、代币库存和共同计价尚未接入平台，不能发布合成成交、
//! 把未知余额补零或将不同代币余额直接求和。配置校验阻止账户 READY；统一下单、
//! 历史成交、账户/持仓和杠杆接口返回 Unsupported。已广播交易不具备撤单语义。

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use hftbacktest::types::Status;

use crate::api::{
    AccountInfo, AmendOrderRequest, ApiError, ApiOrderStatus, ApiOrderType, ApiPositionSide,
    ApiSide, ApiTimeInForce, BrokerApi, CancelOrderRequest, FeeRate, Fill, FundingRate,
    IncomeRecord, InstrumentInfo, Kline, LeverageInfo, OpenInterest, OrderInfo, PositionInfo,
    SubmitGuard, Ticker, Trade, UnifiedOrderRequest,
};
use crate::evm::config::EvmConfig;
use crate::evm::ordermanager::{OrderManager, SharedOrderManager, TrackedOrder};
use crate::evm::provider::EvmProvider;
use crate::evm::tx::{EXCHANGE_NAME, TxEngine};
use crate::evm::types::{PoolConfig, PoolState};

/// EVM venue 的统一 REST facade。
pub struct EvmBrokerApi {
    config: Arc<EvmConfig>,
    provider: EvmProvider,
    engine: Arc<TxEngine>,
    order_manager: SharedOrderManager,
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
        }
    }

    /// 暴露 TxEngine，让 connector 的 run_account 把 PublishSender 接进确认循环。
    pub fn engine(&self) -> &Arc<TxEngine> {
        &self.engine
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
        self.submit_order_fenced(req, None).await
    }

    async fn submit_order_fenced(
        &self,
        req: &UnifiedOrderRequest,
        guard: Option<SubmitGuard>,
    ) -> Result<OrderInfo, ApiError> {
        crate::api::check_submit_guard(&guard)?;
        let _ = req;
        Err(Self::unsupported(
            "submit_order: exact swap quantities, execution fees and account inventory projection are not yet supported",
        ))
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

    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), ApiError> {
        if self
            .order_manager
            .lock()
            .unwrap()
            .pending_by_symbol(symbol)
            .is_empty()
        {
            Ok(())
        } else {
            Err(Self::unsupported(
                "cancel_all_orders: unresolved transactions cannot be canceled",
            ))
        }
    }

    async fn cancel_all_after(&self, _timeout_ms: u64) -> Result<(), ApiError> {
        Err(Self::unsupported("cancel_all_after"))
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
        if order_id.is_some() {
            return Err(Self::unsupported(
                "get_order: receipt alone cannot prove order identity, quantity and execution",
            ));
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

    async fn get_order_history(&self, _: &str, _: u32) -> Result<Vec<OrderInfo>, ApiError> {
        Err(Self::unsupported(
            "get_order_history: durable transaction history is unavailable",
        ))
    }

    async fn get_fills(&self, _: &str, _: u32) -> Result<Vec<Fill>, ApiError> {
        Err(Self::unsupported(
            "get_fills: exact native executions and fees are unavailable",
        ))
    }

    async fn validate_account_configuration(&self) -> Result<(), ApiError> {
        Err(Self::unsupported(
            "unified EVM account: exact inventory, currency valuation and execution fees are not yet supported",
        ))
    }

    async fn get_account(&self) -> Result<AccountInfo, ApiError> {
        // ERC20 amounts cannot be summed across currencies or guessed to use 18 decimals.
        Err(Self::unsupported(
            "get_account: authoritative multi-token account valuation is unavailable",
        ))
    }

    async fn get_positions(&self, _symbol: Option<&str>) -> Result<Vec<PositionInfo>, ApiError> {
        Err(Self::unsupported(
            "get_positions: spot token inventory projection is unavailable",
        ))
    }

    async fn set_leverage(
        &self,
        _symbol: &str,
        _leverage: f64,
        _position_side: Option<ApiPositionSide>,
    ) -> Result<LeverageInfo, ApiError> {
        Err(Self::unsupported("set_leverage"))
    }

    async fn get_leverage(&self, _symbol: &str) -> Result<LeverageInfo, ApiError> {
        Err(Self::unsupported("get_leverage"))
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
