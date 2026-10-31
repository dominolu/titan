//! `BrokerApi` 的 Solana 只读行情实现。
//!
//! 当前未实现真实 swap fill/手续费解析、spot 库存对账和共同币种账户估值。
//! 统一账户激活、下单、账户快照与成交查询返回 Unsupported；原生 RPC/签名工具保留。
//!
//! - `submit_order` / `submit_order_fenced`：在签名或广播前拒绝；
//! - `cancel_*`：已广播交易不可撤回；cancel-all 仅在该品种无未决订单时为空操作；
//! - `get_order`：可查询已跟踪身份；签名成功不能替代实际成交详情；
//! - 行情与原生 RPC 工具可用不代表统一账户就绪。

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;

use crate::api::{
    AccountInfo, AmendOrderRequest, ApiError, ApiMarginType, ApiOrderStatus, ApiOrderType,
    ApiPositionSide, ApiSide, ApiTimeInForce, BrokerApi, CancelOrderRequest, FeeRate, Fill,
    FundingRate, IncomeRecord, InstrumentInfo, Kline, LeverageInfo, OpenInterest, OrderInfo,
    PositionInfo, SubmitGuard, Ticker, Trade, UnifiedOrderRequest,
};
use crate::solana::config::SolanaConfig;
use crate::solana::ordermanager::{SharedOrderManager, TrackedOrder};
use crate::solana::raydium::UniswapV2StyleBook;
use crate::solana::rpc::SolanaRpc;
use crate::solana::tx::{EXCHANGE_NAME, TxEngine};
use crate::solana::types::PoolConfig;

/// Solana venue 的统一 REST facade。
pub struct SolanaBrokerApi {
    config: Arc<SolanaConfig>,
    rpc: SolanaRpc,
    engine: Option<Arc<TxEngine>>,
    order_manager: SharedOrderManager,
    history: Arc<Mutex<VecDeque<OrderInfo>>>,
}

impl SolanaBrokerApi {
    pub fn new(
        config: Arc<SolanaConfig>,
        rpc: SolanaRpc,
        signing: Option<ed25519_dalek::SigningKey>,
        order_manager: SharedOrderManager,
    ) -> Self {
        let engine = signing.map(|signing| {
            Arc::new(TxEngine::new(
                config.clone(),
                rpc.clone(),
                signing,
                order_manager.clone(),
            ))
        });
        Self {
            config,
            rpc,
            engine,
            order_manager,
            history: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn engine(&self) -> Option<&Arc<TxEngine>> {
        self.engine.as_ref()
    }

    fn api_status(status: hftbacktest::types::Status) -> ApiOrderStatus {
        match status {
            hftbacktest::types::Status::New => ApiOrderStatus::New,
            hftbacktest::types::Status::PartiallyFilled => ApiOrderStatus::PartiallyFilled,
            hftbacktest::types::Status::Filled => ApiOrderStatus::Filled,
            hftbacktest::types::Status::Canceled => ApiOrderStatus::Canceled,
            hftbacktest::types::Status::Rejected => ApiOrderStatus::Rejected,
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

    fn synthetic_state(&self, pool: &PoolConfig) -> Result<(u128, u128), ApiError> {
        self.config
            .market_state
            .lock()
            .unwrap()
            .reserves(pool)
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
impl BrokerApi for SolanaBrokerApi {
    async fn ping(&self) -> Result<(), ApiError> {
        self.rpc
            .slot()
            .await
            .map(|_| ())
            .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))
    }

    async fn get_server_time(&self) -> Result<i64, ApiError> {
        Ok(Utc::now().timestamp_millis())
    }

    async fn get_instruments(&self) -> Result<Vec<InstrumentInfo>, ApiError> {
        Ok(self
            .config
            .pools
            .iter()
            .map(|pool| InstrumentInfo {
                symbol: pool.symbol.clone(),
                base_asset: pool.base_mint.clone(),
                quote_asset: pool.quote_mint.clone(),
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
        let (r_base, r_quote) = self.synthetic_state(pool)?;
        let mid = pool.mid_price_from_reserves(r_base, r_quote);
        let now = Utc::now().timestamp_millis();
        Ok(Ticker {
            symbol: symbol.to_string(),
            last_price: mid,
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
            if let Ok((r_base, r_quote)) = self.synthetic_state(pool) {
                let now = Utc::now().timestamp_millis();
                tickers.push(Ticker {
                    symbol: pool.symbol.clone(),
                    last_price: pool.mid_price_from_reserves(r_base, r_quote),
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
        }
        Ok(tickers)
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<crate::api::OrderBook, ApiError> {
        let pool = self.config.pool(symbol).ok_or_else(|| {
            ApiError::new(EXCHANGE_NAME, "SYMBOL", format!("unknown pool {symbol}"))
        })?;
        let (r_base, r_quote) = self.synthetic_state(pool)?;
        let levels = (limit as usize).clamp(1, self.config.book_levels);
        let (bids, asks) = UniswapV2StyleBook.synthetic_book(pool, r_base, r_quote, levels);
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
        _req: &UnifiedOrderRequest,
        _guard: Option<SubmitGuard>,
    ) -> Result<OrderInfo, ApiError> {
        Err(Self::unsupported(
            "unified Solana execution: native swap fills and spot inventory reconciliation",
        ))
    }

    async fn validate_account_configuration(&self) -> Result<(), ApiError> {
        Err(Self::unsupported(
            "unified Solana account activation: native swap fills and spot inventory reconciliation",
        ))
    }

    async fn get_account_position_mode(&self) -> Result<crate::api::ApiPositionMode, ApiError> {
        Ok(crate::api::ApiPositionMode::Net)
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
        if reqs.is_empty() {
            return Ok(Vec::new());
        }
        self.cancel_order(&reqs[0]).await.map(|_| Vec::new())
    }

    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), ApiError> {
        let pending = self
            .order_manager
            .lock()
            .unwrap()
            .orders_snapshot()
            .into_iter()
            .any(|(_, tracked)| tracked.symbol == symbol && tracked.order.active());
        if pending {
            Err(Self::unsupported(
                "cancel_all_orders: unresolved transactions cannot be canceled",
            ))
        } else {
            Ok(())
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
        if let Some(sig) = order_id {
            return self.order_info_from_signature(sig).await;
        }
        Err(ApiError::new(
            EXCHANGE_NAME,
            "NOT_FOUND",
            "order not tracked and no signature provided",
        ))
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, ApiError> {
        let mgr = self.order_manager.lock().unwrap();
        Ok(mgr
            .orders_snapshot()
            .into_iter()
            .filter(|(_, t)| t.symbol == symbol && t.order.active())
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

    async fn get_fills(&self, _symbol: &str, _limit: u32) -> Result<Vec<Fill>, ApiError> {
        Err(Self::unsupported("native swap fills and fees"))
    }

    async fn get_account(&self) -> Result<AccountInfo, ApiError> {
        Err(Self::unsupported(
            "complete SPL account balances and account totals in a proven common currency",
        ))
    }

    async fn get_positions(&self, _symbol: Option<&str>) -> Result<Vec<PositionInfo>, ApiError> {
        Err(Self::unsupported("authoritative spot inventory projection"))
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
            margin_type: ApiMarginType::Unknown,
            position_side: ApiPositionSide::Net,
        })
    }

    async fn get_leverage(&self, symbol: &str) -> Result<LeverageInfo, ApiError> {
        Ok(LeverageInfo {
            symbol: symbol.to_string(),
            leverage: 1.0,
            margin_type: ApiMarginType::Unknown,
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

impl SolanaBrokerApi {
    /// 用签名直接查链上状态构建订单信息（本地状态机之外的兜底路径）。
    async fn order_info_from_signature(&self, signature: &str) -> Result<OrderInfo, ApiError> {
        let status = self
            .rpc
            .signature_status(signature)
            .await
            .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))?;
        let now = Utc::now().timestamp_millis();
        match status {
            Some(Ok(())) => Err(Self::unsupported(
                "confirmed swap execution details: signature status alone does not prove fill quantity or price",
            )),
            Some(Err(_)) => Ok(OrderInfo {
                symbol: String::new(),
                order_id: signature.to_string(),
                client_order_id: String::new(),
                side: ApiSide::Unknown,
                order_type: ApiOrderType::Market,
                status: ApiOrderStatus::Rejected,
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
                format!("no status for {signature}"),
            )),
        }
    }
}
