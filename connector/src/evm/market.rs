//! EVM 行情源：WS log 订阅（RPC 后端）→ 合成行情 → `PublishEvent`。
//!
//! 两个后端共享 [`MarketPublisher`]：
//! - `RpcFeedBackend`（本文件）：订阅 Sync/Swap 日志，落块确认后更新 confirmed 状态；
//! - `LowLatencyFeedBackend`（feed.rs）：sequencer feed 预链推演，写 prechain 视图。
//!
//! 订阅模型：filter 始终覆盖配置里的全部 pair（池集合是静态配置），活跃订阅
//! 只控制状态更新与快照发布，因此 Subscribe/Unsubscribe 不需要重建连接。
//!
//! 合成订单簿说明：AMM 没有 order book，深度由当前 reserve 按常数乘积公式
//! 积分生成；`Sync` 触发 BBO，`Snapshot` 命令触发完整深度快照（快照以 epoch
//! 单调递增，下游按替换语义消费）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_primitives::{Address, U256};
use alloy_rpc_types_eth::Log;
use chrono::Utc;
use futures_util::StreamExt;
use hftbacktest::prelude::{
    Event, LOCAL_ASK_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_SNAPSHOT_EVENT, LOCAL_BID_DEPTH_BBO_EVENT,
    LOCAL_BID_DEPTH_SNAPSHOT_EVENT, LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT,
};
use tokio::sync::broadcast::Receiver;
use tracing::{debug, error, warn};

use crate::api::{ApiSide, Trade};
use crate::connector::{MarketDataCommand, MarketStreamMetadata, PublishEvent, PublishSender};
use crate::evm::config::EvmConfig;
use crate::evm::dex::DexAdapter;
use crate::evm::dex::uniswap_v2::{Swap, Sync};
use crate::evm::types::{PoolConfig, PoolState, SharedMarketState, u256_to_f64};
use crate::evm::{EvmError, provider::EvmProvider};
use alloy_sol_types::SolEvent;

/// 行情发布器：market / feed 两个后端共用。
pub struct MarketPublisher {
    pub config: Arc<EvmConfig>,
    pub ev_tx: PublishSender,
    pub market_state: SharedMarketState,
    /// symbol -> 快照 epoch（每次快照单调递增）。
    epochs: Mutex<HashMap<String, u64>>,
    adapter: Arc<dyn DexAdapter>,
}

impl MarketPublisher {
    pub fn new(
        config: Arc<EvmConfig>,
        ev_tx: PublishSender,
        market_state: SharedMarketState,
        adapter: Arc<dyn DexAdapter>,
    ) -> Self {
        Self {
            config,
            ev_tx,
            market_state,
            epochs: Mutex::new(HashMap::new()),
            adapter,
        }
    }

    fn now_ns() -> i64 {
        Utc::now().timestamp_nanos_opt().unwrap_or(0)
    }

    /// 更新 confirmed reserve（Sync 或 RPC 快照），并发布 BBO。
    pub fn confirm_reserves(&self, pool: &PoolConfig, reserve0: U256, reserve1: U256) {
        let now = Self::now_ns();
        {
            let mut market = self.market_state.lock().unwrap();
            market
                .pools
                .entry(pool.pair_address)
                .or_default()
                .confirm(reserve0, reserve1, now);
        }
        let state = PoolState {
            confirmed: Some((reserve0, reserve1)),
            prechain: None,
            last_update_ns: now,
        };
        self.publish_bbo(pool, &state, now);
    }

    /// 预链推演更新 prechain（feed 后端调用），并发布 BBO。
    pub fn apply_prechain(&self, pool: &PoolConfig, reserve0: U256, reserve1: U256) {
        let now = Self::now_ns();
        {
            let mut market = self.market_state.lock().unwrap();
            market
                .pools
                .entry(pool.pair_address)
                .or_default()
                .apply_prechain(reserve0, reserve1, now);
        }
        let state = PoolState {
            confirmed: None,
            prechain: Some((reserve0, reserve1)),
            last_update_ns: now,
        };
        self.publish_bbo(pool, &state, now);
    }

    /// 由当前 reserve 视图发布最优买卖（单档 BBO 事件）。
    pub fn publish_bbo(&self, pool: &PoolConfig, state: &PoolState, now_ns: i64) {
        let (bids, asks) = self.adapter.synthetic_book(pool, state, 1);
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
        let reserves = {
            let market = self.market_state.lock().unwrap();
            market
                .pools
                .get(&pool.pair_address)
                .and_then(|s| s.effective())
        };
        let Some((r0, r1)) = reserves else {
            warn!(symbol, "snapshot requested before reserves are known");
            return;
        };
        let now = Self::now_ns();
        let state = PoolState {
            confirmed: Some((r0, r1)),
            prechain: None,
            last_update_ns: now,
        };
        let (bids, asks) = self
            .adapter
            .synthetic_book(&pool, &state, self.config.book_levels);
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

    /// Swap 事件 → 统一 Trade 事件 + 环形缓冲记录。
    pub fn publish_swap(
        &self,
        pool: &PoolConfig,
        amount0_in: U256,
        amount1_in: U256,
        amount0_out: U256,
        amount1_out: U256,
    ) {
        let base_first = pool.token0_is_base();
        let (base_in, base_out, quote_in, quote_out) = if base_first {
            (amount0_in, amount0_out, amount1_in, amount1_out)
        } else {
            (amount1_in, amount1_out, amount0_in, amount0_out)
        };
        let base_dec = 10f64.powi(pool.base_decimals as i32);
        let quote_dec = 10f64.powi(pool.quote_decimals as i32);
        // 事件方向以 taker 视角：base 流入池 = 卖出。
        let (side, price, qty) = if base_in > U256::ZERO {
            let qty_h = u256_to_f64(base_in) / base_dec;
            let px = u256_to_f64(quote_out) / quote_dec / qty_h.max(f64::EPSILON);
            (ApiSide::Sell, px, qty_h)
        } else {
            let qty_h = u256_to_f64(base_out) / base_dec;
            let px = u256_to_f64(quote_in) / quote_dec / qty_h.max(f64::EPSILON);
            (ApiSide::Buy, px, qty_h)
        };
        if !price.is_finite() || price <= 0.0 || qty <= 0.0 {
            return;
        }
        let now = Self::now_ns();
        let trade = Trade {
            symbol: pool.symbol.clone(),
            id: format!("{}-{now}", pool.pair_address),
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

/// 活跃订阅状态：地址 -> 池配置（控制状态更新与快照，不影响 filter）。
type SharedActivePools = Arc<Mutex<HashMap<Address, PoolConfig>>>;

/// RPC 行情后端：WS 订阅 Sync/Swap，断线由运行循环重建。
pub struct RpcFeedBackend {
    pub config: Arc<EvmConfig>,
    pub provider: EvmProvider,
    pub market_state: SharedMarketState,
    pub adapter: Arc<dyn DexAdapter>,
}

impl RpcFeedBackend {
    pub fn new(
        config: Arc<EvmConfig>,
        provider: EvmProvider,
        market_state: SharedMarketState,
        adapter: Arc<dyn DexAdapter>,
    ) -> Self {
        Self {
            config,
            provider,
            market_state,
            adapter,
        }
    }

    /// 长期运行（调用方 spawn）。断线自动重建订阅。
    pub async fn run(&self, mut commands: Receiver<MarketDataCommand>, ev_tx: PublishSender) {
        let active: SharedActivePools = Arc::new(Mutex::new(HashMap::new()));
        let publisher = Arc::new(MarketPublisher::new(
            self.config.clone(),
            ev_tx,
            self.market_state.clone(),
            self.adapter.clone(),
        ));
        loop {
            if let Err(error) = self.run_once(&active, &publisher, &mut commands).await {
                error!(?error, "EVM market backend disconnected; retrying");
            }
            // 重连前用 HTTP 兜底刷新一次储备，尽量减小状态空洞。
            self.refresh_reserves(&active, &publisher).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn refresh_reserves(&self, active: &SharedActivePools, publisher: &Arc<MarketPublisher>) {
        let pairs: Vec<(PoolConfig, Address)> = {
            let guard = active.lock().unwrap();
            guard
                .iter()
                .map(|(addr, pool)| (pool.clone(), *addr))
                .collect()
        };
        for (pool, addr) in pairs {
            match self.provider.get_reserves(addr).await {
                Ok((r0, r1)) => publisher.confirm_reserves(&pool, r0, r1),
                Err(error) => debug!(?error, address = %addr, "reserve refresh failed"),
            }
        }
    }

    async fn run_once(
        &self,
        active: &SharedActivePools,
        publisher: &Arc<MarketPublisher>,
        commands: &mut Receiver<MarketDataCommand>,
    ) -> Result<(), EvmError> {
        let ws = self.provider.connect_ws().await?;
        let log_stream = self
            .provider
            .subscribe_logs(
                &ws,
                self.config.pair_addresses(),
                vec![Sync::SIGNATURE_HASH, Swap::SIGNATURE_HASH],
            )
            .await?
            .into_stream();
        // 连接建立后立即拉取全部已配置池的储备，让行情即刻可用。
        for pool in &self.config.pools {
            if let Ok((r0, r1)) = self.provider.get_reserves(pool.pair_address).await {
                publisher.confirm_reserves(pool, r0, r1);
            }
        }
        let mut log_stream = std::pin::pin!(log_stream);

        loop {
            tokio::select! {
                maybe_log = log_stream.next() => {
                    let Some(log) = maybe_log else {
                        return Err(EvmError::ConnectionInterrupted);
                    };
                    self.handle_log(log, active, publisher);
                }
                command = commands.recv() => {
                    match command {
                        Ok(MarketDataCommand::Subscribe { symbol, .. })
                        | Ok(MarketDataCommand::InitializeTrading { symbol }) => {
                            let Some(pool) = self.config.pool(&symbol).cloned() else {
                                continue;
                            };
                            let first = {
                                let mut guard = active.lock().unwrap();
                                guard.insert(pool.pair_address, pool.clone()).is_none()
                            };
                            if first {
                                // 新激活的池立即拉储备并发布快照。
                                if let Ok((r0, r1)) =
                                    self.provider.get_reserves(pool.pair_address).await
                                {
                                    publisher.confirm_reserves(&pool, r0, r1);
                                    publisher.publish_snapshot(&pool.symbol);
                                }
                            }
                        }
                        Ok(MarketDataCommand::Unsubscribe { symbol, .. }) => {
                            if let Some(pool) = self.config.pool(&symbol) {
                                active.lock().unwrap().remove(&pool.pair_address);
                            }
                        }
                        Ok(MarketDataCommand::Snapshot { symbol }) => {
                            if let Some(pool) = self.config.pool(&symbol) {
                                if let Ok((r0, r1)) =
                                    self.provider.get_reserves(pool.pair_address).await
                                {
                                    publisher.confirm_reserves(pool, r0, r1);
                                }
                            }
                            publisher.publish_snapshot(&symbol);
                        }
                        Err(_) => return Err(EvmError::ConnectionInterrupted),
                    }
                }
            }
        }
    }

    fn handle_log(&self, log: Log, active: &SharedActivePools, publisher: &Arc<MarketPublisher>) {
        let address = log.address();
        let pool = active.lock().unwrap().get(&address).cloned();
        let Some(pool) = pool else {
            return;
        };
        let Some(&topic) = log.topics().first() else {
            return;
        };
        if topic == Sync::SIGNATURE_HASH {
            if let Ok((r0, r1)) = Sync::abi_decode_data(log.data().data.as_ref()) {
                publisher.confirm_reserves(&pool, U256::from(r0), U256::from(r1));
            }
        } else if topic == Swap::SIGNATURE_HASH {
            if let Ok(swap) = Swap::abi_decode_data(log.data().data.as_ref()) {
                publisher.publish_swap(&pool, swap.0, swap.1, swap.2, swap.3);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evm::types::MarketState;

    #[tokio::test]
    async fn publisher_synthesizes_bbo_and_snapshot() {
        // 用独立配置验证发布逻辑（不连 RPC）。
        let (ev_tx, mut rx) = crate::connector::test_publish_channel();
        let config = std::sync::Arc::new(EvmConfig::test_config(
            vec![PoolConfig {
                symbol: "WETH/USDC".to_string(),
                pair_address: Address::repeat_byte(0x03),
                base_token: Address::repeat_byte(0x01),
                quote_token: Address::repeat_byte(0x02),
                base_decimals: 18,
                quote_decimals: 6,
            }],
            MarketState::default(),
        ));
        let publisher = MarketPublisher::new(
            config.clone(),
            ev_tx,
            config.market_state.clone(),
            Arc::new(crate::evm::dex::uniswap_v2::UniswapV2Adapter::new()),
        );
        let pool = &config.pools[0];
        publisher.confirm_reserves(
            pool,
            U256::from(10u128.pow(21)),
            U256::from(3_000_000_000_000u64),
        );

        // BBO：买单价低于卖价，无 stream 元数据。
        match rx.recv().await {
            Some(PublishEvent::FeedBatch {
                symbol,
                events,
                stream,
            }) => {
                assert_eq!(symbol, "WETH/USDC");
                assert!(stream.is_none());
                assert_eq!(events.len(), 2);
                assert!(events[0].px < events[1].px);
            }
            _other => panic!("expected BBO FeedBatch"),
        }

        // 快照：10 档，单调 epoch，snapshot 标记。
        publisher.publish_snapshot("WETH/USDC");
        match rx.recv().await {
            Some(PublishEvent::FeedBatch { events, stream, .. }) => {
                let stream = stream.unwrap();
                assert!(stream.snapshot);
                assert_eq!(stream.epoch, 1);
                assert_eq!(events.len(), 2 * config.book_levels);
            }
            _other => panic!("expected snapshot FeedBatch"),
        }

        // Swap → Trade 事件 + 缓冲记录。
        publisher.publish_swap(
            pool,
            U256::from(10u128.pow(17)),
            U256::ZERO,
            U256::ZERO,
            U256::from(299_000_000u64),
        );
        match rx.recv().await {
            Some(PublishEvent::FeedBatch { events, .. }) => {
                assert_eq!(events.len(), 1);
                assert!(events[0].px > 2980.0 && events[0].px < 3000.0);
            }
            _other => panic!("expected trade FeedBatch"),
        }
        let market = config.market_state.lock().unwrap();
        assert_eq!(market.trades.len(), 1);
    }
}
