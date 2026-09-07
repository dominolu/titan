//! Solana 行情源：WS `accountSubscribe`（vault 储备）→ 合成行情 → `PublishEvent`。
//!
//! 订阅模型与 EVM venue 同构：订阅配置里全部 vault 的 SPL token 账户变更
//! （amount 固定在偏移 64），落块确认后更新 confirmed 储备并发布 BBO；
//! `Snapshot` 命令经 HTTP `getAccountInfo` 拉取快照（带单调 epoch）。
//! 储备变化同时推演为公共成交（Trade 事件）——恒定乘积模型下，单侧 vault
//! 的增减方向即 taker 方向。

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::prelude::{
    Event, LOCAL_ASK_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_SNAPSHOT_EVENT, LOCAL_BID_DEPTH_BBO_EVENT,
    LOCAL_BID_DEPTH_SNAPSHOT_EVENT, LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT,
};
use tokio::sync::broadcast::Receiver;
use tracing::{debug, error, warn};

use crate::api::{ApiSide, Trade};
use crate::connector::{MarketDataCommand, MarketStreamMetadata, PublishEvent, PublishSender};
use crate::solana::SolanaError;
use crate::solana::config::SolanaConfig;
use crate::solana::raydium::UniswapV2StyleBook;
use crate::solana::rpc::SolanaRpc;
use crate::solana::types::{SPL_TOKEN_ACCOUNT_AMOUNT_OFFSET, SharedMarketState};

/// 行情发布器：WS / 快照两个路径共用。
pub struct MarketPublisher {
    pub config: Arc<SolanaConfig>,
    pub ev_tx: PublishSender,
    pub market_state: SharedMarketState,
    epochs: Mutex<HashMap<String, u64>>,
    book: UniswapV2StyleBook,
}

impl MarketPublisher {
    pub fn new(
        config: Arc<SolanaConfig>,
        ev_tx: PublishSender,
        market_state: SharedMarketState,
    ) -> Self {
        Self {
            config,
            ev_tx,
            market_state,
            epochs: Mutex::new(HashMap::new()),
            book: UniswapV2StyleBook,
        }
    }

    fn now_ns() -> i64 {
        Utc::now().timestamp_nanos_opt().unwrap_or(0)
    }

    /// vault 储备更新（按 vault 地址路由到池），并发布 BBO 与推演 Trade。
    pub fn apply_vault_amount(&self, vault: &str, amount: u64) {
        let pools: Vec<_> = self
            .config
            .pools
            .iter()
            .filter(|p| p.base_vault == vault || p.quote_vault == vault)
            .cloned()
            .collect();
        if pools.is_empty() {
            return;
        }
        let now = Self::now_ns();
        {
            let mut market = self.market_state.lock().unwrap();
            market.vaults.insert(vault.to_string(), amount);
        }
        for pool in pools {
            let Some((r_base, r_quote)) = self.config.market_state.lock().unwrap().reserves(&pool)
            else {
                continue;
            };
            self.publish_bbo(&pool, r_base, r_quote, now);
        }
    }

    /// 发布最优买卖（单档 BBO 事件）。
    pub fn publish_bbo(
        &self,
        pool: &crate::solana::types::PoolConfig,
        r_base: u128,
        r_quote: u128,
        now_ns: i64,
    ) {
        let (bids, asks) = self.book.synthetic_book(pool, r_base, r_quote, 1);
        let mut events = Vec::with_capacity(2);
        if let Some((px, qty)) = bids.first() {
            events.push(Event {
                ev: LOCAL_BID_DEPTH_BBO_EVENT,
                exch_ts: now_ns,
                local_ts: now_ns,
                order_id: 0,
                px: *px,
                qty: *qty,
                ival: 0,
                fval: 0.0,
            });
        }
        if let Some((px, qty)) = asks.first() {
            events.push(Event {
                ev: LOCAL_ASK_DEPTH_BBO_EVENT,
                exch_ts: now_ns,
                local_ts: now_ns,
                order_id: 0,
                px: *px,
                qty: *qty,
                ival: 0,
                fval: 0.0,
            });
        }
        if !events.is_empty() {
            let _ = self.ev_tx.send(PublishEvent::FeedBatch {
                symbol: pool.symbol.clone(),
                events,
                stream: None,
            });
        }
    }

    /// 发布完整深度快照（替换语义 + 单调 epoch）。
    pub fn publish_snapshot(&self, symbol: &str) {
        let Some(pool) = self.config.pool(symbol).cloned() else {
            return;
        };
        let Some((r_base, r_quote)) = self.config.market_state.lock().unwrap().reserves(&pool)
        else {
            warn!(symbol, "snapshot requested before reserves are known");
            return;
        };
        let now = Self::now_ns();
        let (bids, asks) =
            self.book
                .synthetic_book(&pool, r_base, r_quote, self.config.book_levels);
        let epoch = {
            let mut epochs = self.epochs.lock().unwrap();
            let entry = epochs.entry(pool.symbol.clone()).or_insert(0);
            *entry = entry.saturating_add(1);
            *entry
        };
        let mut events = Vec::with_capacity(bids.len() + asks.len());
        for (px, qty) in &bids {
            events.push(Event {
                ev: LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
                exch_ts: now,
                local_ts: now,
                order_id: 0,
                px: *px,
                qty: *qty,
                ival: 0,
                fval: 0.0,
            });
        }
        for (px, qty) in &asks {
            events.push(Event {
                ev: LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
                exch_ts: now,
                local_ts: now,
                order_id: 0,
                px: *px,
                qty: *qty,
                ival: 0,
                fval: 0.0,
            });
        }
        let _ = self.ev_tx.send(PublishEvent::FeedBatch {
            symbol: pool.symbol.clone(),
            events,
            stream: Some(MarketStreamMetadata {
                epoch,
                first_update_sequence: epoch,
                last_update_sequence: epoch,
                snapshot: true,
            }),
        });
    }

    /// 记录并发布由储备变化推演的公共成交。
    pub fn record_implied_trade(
        &self,
        pool: &crate::solana::types::PoolConfig,
        prev_base: u128,
        new_base: u128,
        new_quote: u128,
    ) {
        let base_delta = new_base as i128 - prev_base as i128;
        if base_delta == 0 {
            return;
        }
        let side = if base_delta < 0 {
            ApiSide::Sell
        } else {
            ApiSide::Buy
        };
        let qty = base_delta.unsigned_abs() as f64 / 10f64.powi(pool.base_decimals as i32);
        let price = pool.mid_price_from_reserves(new_base, new_quote);
        if price <= 0.0 || qty <= 0.0 {
            return;
        }
        let now = Self::now_ns();
        let trade = Trade {
            symbol: pool.symbol.clone(),
            id: format!("{}-{now}", pool.amm_id),
            price,
            qty,
            side,
            timestamp: now / 1_000_000,
        };
        {
            let mut market = self.market_state.lock().unwrap();
            market.record_trade(trade.clone(), self.config.trade_buffer);
        }
        let event_ev = if side == ApiSide::Sell {
            LOCAL_SELL_TRADE_EVENT
        } else {
            LOCAL_BUY_TRADE_EVENT
        };
        let _ = self.ev_tx.send(PublishEvent::FeedBatch {
            symbol: pool.symbol.clone(),
            events: vec![Event {
                ev: event_ev,
                exch_ts: now,
                local_ts: now,
                order_id: 0,
                px: price,
                qty,
                ival: 0,
                fval: 0.0,
            }],
            stream: None,
        });
    }
}

/// WS 行情后端：accountSubscribe 全部 vault，断线由运行循环重建。
pub struct WsFeedBackend {
    pub config: Arc<SolanaConfig>,
    pub rpc: SolanaRpc,
    pub market_state: SharedMarketState,
}

impl WsFeedBackend {
    pub fn new(config: Arc<SolanaConfig>, rpc: SolanaRpc, market_state: SharedMarketState) -> Self {
        Self {
            config,
            rpc,
            market_state,
        }
    }

    /// 长期运行（调用方 spawn）。断线自动重建订阅。
    pub async fn run(&self, mut commands: Receiver<MarketDataCommand>, ev_tx: PublishSender) {
        let publisher = Arc::new(MarketPublisher::new(
            self.config.clone(),
            ev_tx,
            self.market_state.clone(),
        ));
        let active: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        loop {
            if let Err(error) = self.run_once(&active, &publisher, &mut commands).await {
                error!(?error, "Solana WS backend disconnected; retrying");
            }
            // 重连前用 HTTP 兜底刷新一次储备。
            self.refresh_reserves(&publisher).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn refresh_reserves(&self, publisher: &Arc<MarketPublisher>) {
        let vaults = self.config.vault_addresses();
        for vault in vaults {
            match self.rpc.token_account_amount(&vault).await {
                Ok(amount) => publisher.apply_vault_amount(&vault, amount),
                Err(err) => debug!(?err, %vault, "reserve refresh failed"),
            }
        }
    }

    async fn run_once(
        &self,
        active: &Arc<Mutex<HashSet<String>>>,
        publisher: &Arc<MarketPublisher>,
        commands: &mut Receiver<MarketDataCommand>,
    ) -> Result<(), SolanaError> {
        use tokio_tungstenite::tungstenite::Message;
        let (ws, _) = tokio_tungstenite::connect_async(self.rpc.ws_url())
            .await
            .map_err(|e| SolanaError::Ws(e.to_string()))?;
        let (mut write, mut read) = ws.split();

        // 订阅全部配置 vault。
        let vaults = self.config.vault_addresses();
        let mut subscription_to_vault: HashMap<u64, String> = HashMap::new();
        for (i, vault) in vaults.iter().enumerate() {
            let sub = serde_json::json!({
                "jsonrpc": "2.0", "id": i + 1, "method": "accountSubscribe",
                "params": [vault, {"encoding": "base64", "commitment": "confirmed"}]
            });
            write
                .send(Message::text(sub.to_string()))
                .await
                .map_err(|e| SolanaError::Ws(e.to_string()))?;
            subscription_to_vault.insert(i as u64 + 1, vault.clone());
        }
        // 连接建立后立即拉一次全部储备，让行情即刻可用。
        self.refresh_reserves(publisher).await;
        for pool in &self.config.pools {
            if self
                .config
                .market_state
                .lock()
                .unwrap()
                .reserves(pool)
                .is_some()
            {
                publisher.publish_snapshot(&pool.symbol);
            }
        }

        loop {
            tokio::select! {
                frame = read.next() => {
                    let Some(frame) = frame else {
                        return Err(SolanaError::ConnectionInterrupted);
                    };
                    let msg = frame.map_err(|e| SolanaError::Ws(e.to_string()))?;
                    let Message::Text(text) = msg else { continue };
                    let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else { continue };
                    if let Some(params) = value.get("params") {
                        let subscription = params["subscription"].as_u64().unwrap_or(0);
                        let slot = params["result"]["context"]["slot"].as_u64().unwrap_or(0) as i64;
                        let data_b64 = params["result"]["value"]["data"][0].as_str().unwrap_or("");
                        let Some(vault) = subscription_to_vault.get(&subscription) else { continue };
                        use base64::Engine;
                        let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(data_b64) else { continue };
                        if raw.len() < SPL_TOKEN_ACCOUNT_AMOUNT_OFFSET + 8 { continue; }
                        let amount = u64::from_le_bytes(
                            raw[SPL_TOKEN_ACCOUNT_AMOUNT_OFFSET..SPL_TOKEN_ACCOUNT_AMOUNT_OFFSET + 8].try_into().unwrap(),
                        );
                        let prev = self.config.market_state.lock().unwrap().vaults.get(vault).copied();
                        publisher.apply_vault_amount(vault, amount);
                        // 推演成交：base vault 的变化即 taker 方向。
                        if let Some(prev_amount) = prev {
                            for pool in self.config.pools.iter().filter(|p| &p.base_vault == vault) {
                                if let Some((r_base, r_quote)) = self.config.market_state.lock().unwrap().reserves(pool) {
                                    publisher.record_implied_trade(pool, prev_amount as u128, r_base, r_quote);
                                }
                            }
                        }
                        let _ = slot;
                    }
                }
                command = commands.recv() => {
                    match command {
                        Ok(MarketDataCommand::Subscribe { symbol, .. })
                        | Ok(MarketDataCommand::InitializeTrading { symbol }) => {
                            active.lock().unwrap().insert(symbol.clone());
                            if let Some(pool) = self.config.pool(&symbol) {
                                if let Some((r_base, r_quote)) = self.config.market_state.lock().unwrap().reserves(pool) {
                                    publisher.publish_bbo(pool, r_base, r_quote, Utc::now().timestamp_nanos_opt().unwrap_or(0));
                                    publisher.publish_snapshot(&pool.symbol);
                                }
                            }
                        }
                        Ok(MarketDataCommand::Unsubscribe { symbol, .. }) => {
                            active.lock().unwrap().remove(&symbol);
                        }
                        Ok(MarketDataCommand::Snapshot { symbol }) => {
                            if let Some(pool) = self.config.pool(&symbol) {
                                let _ = self.rpc.token_account_amount(&pool.base_vault).await
                                    .map(|v| publisher.apply_vault_amount(&pool.base_vault, v));
                                let _ = self.rpc.token_account_amount(&pool.quote_vault).await
                                    .map(|v| publisher.apply_vault_amount(&pool.quote_vault, v));
                            }
                            publisher.publish_snapshot(&symbol);
                        }
                        Err(_) => return Err(SolanaError::ConnectionInterrupted),
                    }
                }
            }
        }
    }
}
