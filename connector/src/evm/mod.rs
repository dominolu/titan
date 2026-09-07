//! EVM 链上 venue（方案 A）：Ethereum / Arbitrum / 任意 Arbitrum Orbit 链。
//!
//! 结构总览（设计文档 `docs/onchain_venue_broker_design.md` 方案 A）：
//! - [`config`]：TOML 配置与运行期解析；
//! - [`provider`]：JSON-RPC 读路径 + WS 订阅；
//! - [`dex`]：AMM 协议适配（行情推演 + swap calldata），首发 Uniswap V2 类；
//! - [`market`]：RPC WS 行情后端（confirmed 视图）；
//! - [`feed`]：Arbitrum sequencer feed 预链视图（prechain，可选）；
//! - [`tx`]：swap 交易构造/签名/广播 + 收据确认循环（私有流等价物）；
//! - [`ordermanager`]：tx hash 驱动的订单状态机；
//! - [`brokerapi`]：统一 `BrokerApi` 的 AMM 语义映射。
//!
//! `run_market_data` 只启动行情后端；`run_account` 启动确认循环与 feed，并发布
//! `PrivateStreamReady`（EVM 无持久私有订阅，就绪即事件）。

pub mod brokerapi;
pub mod config;
pub mod dex;
pub mod feed;
pub mod market;
pub mod ordermanager;
pub mod provider;
pub mod tx;
pub mod types;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use hftbacktest::types::Value;
use thiserror::Error;
use titan_market_plugin::MarketDataKind;
use tokio::sync::broadcast;
use tracing::warn;

use crate::connector::{
    Connector, ConnectorBuilder, GetOrders, MarketDataCommand, PublishEvent, PublishSender,
};
use crate::evm::brokerapi::EvmBrokerApi;
use crate::evm::config::{EvmConfig, RawEvmConfig};
use crate::evm::dex::uniswap_v2::UniswapV2Adapter;
use crate::evm::ordermanager::{OrderManager, SharedOrderManager};
use crate::evm::provider::EvmProvider;
use crate::evm::types::SharedMarketState;

#[derive(Error, Debug)]
pub enum EvmError {
    #[error("InvalidArg: {0}")]
    InvalidArg(&'static str),
    #[error("Decode: {0}")]
    Decode(&'static str),
    #[error("Transport: {0}")]
    Transport(#[from] alloy_transport::TransportError),
    #[error("Hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("Config: {0}")]
    Config(#[from] toml::de::Error),
    #[error("Signer: {0}")]
    Signer(String),
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
}

impl EvmError {
    pub fn to_value(&self) -> Value {
        Value::String(self.to_string())
    }
}

pub type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;
pub type SharedMarketSubscriptions = Arc<Mutex<HashMap<String, HashSet<MarketDataKind>>>>;

pub struct Evm {
    config: Arc<EvmConfig>,
    provider: EvmProvider,
    #[cfg_attr(not(test), allow(dead_code))]
    signer: Option<alloy_signer_local::PrivateKeySigner>,
    order_manager: SharedOrderManager,
    client: Arc<EvmBrokerApi>,
    market_tx: broadcast::Sender<MarketDataCommand>,
    market_subscriptions: SharedMarketSubscriptions,
    symbols: SharedSymbolSet,
}

fn market_kind_set() -> Vec<MarketDataKind> {
    vec![MarketDataKind::Depth, MarketDataKind::Trades]
}

impl Evm {
    fn from_raw(raw: RawEvmConfig, require_signer: bool) -> Result<Self, EvmError> {
        let signer = if raw.private_key.is_empty() {
            if require_signer {
                return Err(EvmError::InvalidArg(
                    "private_key is required for account connector",
                ));
            }
            None
        } else {
            let hex = raw.private_key.trim_start_matches("0x");
            let bytes = hex::decode(hex)?;
            if bytes.len() != 32 {
                return Err(EvmError::InvalidArg("private_key must be 32 bytes"));
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&bytes);
            Some(
                alloy_signer_local::PrivateKeySigner::from_bytes(&alloy_primitives::B256::from(
                    key,
                ))
                .map_err(|e| EvmError::Signer(e.to_string()))?,
            )
        };
        let market_state: SharedMarketState = Arc::new(Mutex::new(Default::default()));
        let config = Arc::new(EvmConfig::from_raw(raw, market_state.clone()));
        let provider = EvmProvider::connect(&config.rpc_url, &config.ws_url)?;
        let order_manager = Arc::new(Mutex::new(OrderManager::new()));
        let client = Arc::new(EvmBrokerApi::new(
            config.clone(),
            provider.clone(),
            signer.clone(),
            order_manager.clone(),
        ));
        let (market_tx, _) = broadcast::channel(500);
        Ok(Self {
            config,
            provider,
            signer,
            order_manager,
            client,
            market_tx,
            market_subscriptions: Default::default(),
            symbols: Default::default(),
        })
    }

    fn start_market_backend(&self, ev_tx: PublishSender) {
        let backend = market::RpcFeedBackend::new(
            self.config.clone(),
            self.provider.clone(),
            self.config.market_state.clone(),
            Arc::new(UniswapV2Adapter::new()),
        );
        let commands = self.market_tx.subscribe();
        tokio::spawn(async move {
            backend.run(commands, ev_tx).await;
        });
    }

    fn start_feed_backend(&self, ev_tx: PublishSender) {
        if self.config.feed_url.is_none() {
            return;
        }
        let backend = feed::LowLatencyFeedBackend::new(
            self.config.clone(),
            self.config.market_state.clone(),
            Arc::new(UniswapV2Adapter::new()),
        );
        tokio::spawn(async move {
            backend.run(ev_tx).await;
        });
    }

    fn start_order_gc(&self) {
        let order_manager = self.order_manager.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                order_manager.lock().unwrap().gc();
            }
        });
    }
}

impl ConnectorBuilder for Evm {
    type Error = EvmError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        Self::from_raw(RawEvmConfig::parse(config)?, true)
    }
}

impl Evm {
    /// 行情连接构建：不要求也不保留私钥。
    pub(crate) fn build_market_from(config: &str) -> Result<Self, EvmError> {
        let mut raw = RawEvmConfig::parse(config)?;
        raw.private_key.clear();
        Self::from_raw(raw, false)
    }
}

#[async_trait::async_trait]
impl Connector for Evm {
    fn register(&mut self, symbol: String) {
        if self.config.pool(&symbol).is_none() {
            warn!(%symbol, "EVM connector: symbol not present in pools config; ignored");
            return;
        }
        if self.symbols.lock().unwrap().insert(symbol.clone()) {
            let _ = self.market_tx.send(MarketDataCommand::InitializeTrading {
                symbol: symbol.clone(),
            });
        }
        self.subscribe_market_data(symbol, market_kind_set());
    }

    fn subscribe_market_data(&mut self, symbol: String, kinds: Vec<MarketDataKind>) {
        self.market_subscriptions
            .lock()
            .unwrap()
            .entry(symbol.clone())
            .or_default()
            .extend(kinds.iter().copied());
        let _ = self
            .market_tx
            .send(MarketDataCommand::Subscribe { symbol, kinds });
    }

    fn unregister(&mut self, symbol: String) {
        self.symbols.lock().unwrap().remove(&symbol);
        let kinds = self
            .market_subscriptions
            .lock()
            .unwrap()
            .get(&symbol)
            .map(|v| v.iter().copied().collect())
            .unwrap_or_default();
        self.unsubscribe_market_data(symbol, kinds);
    }

    fn unsubscribe_market_data(&mut self, symbol: String, kinds: Vec<MarketDataKind>) {
        {
            let mut subscriptions = self.market_subscriptions.lock().unwrap();
            if let Some(active) = subscriptions.get_mut(&symbol) {
                for kind in &kinds {
                    active.remove(kind);
                }
                if active.is_empty() {
                    subscriptions.remove(&symbol);
                }
            }
        }
        let _ = self
            .market_tx
            .send(MarketDataCommand::Unsubscribe { symbol, kinds });
    }

    fn request_snapshot(&mut self, symbol: String) {
        let _ = self.market_tx.send(MarketDataCommand::Snapshot { symbol });
    }

    fn recover_market_data(&mut self, symbols: Vec<String>) {
        for symbol in symbols {
            let _ = self.market_tx.send(MarketDataCommand::Snapshot { symbol });
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, ev_tx: PublishSender) {
        self.start_market_backend(ev_tx.clone());
        self.start_account_resources(ev_tx);
    }

    fn run_market_data(&mut self, ev_tx: PublishSender) {
        self.start_market_backend(ev_tx);
    }

    fn run_account(&mut self, ev_tx: PublishSender) {
        self.start_account_resources(ev_tx);
    }

    fn track_managed_order(
        &self,
        symbol: &str,
        client_order_id: &str,
        order: &hftbacktest::types::Order,
    ) {
        self.order_manager.lock().unwrap().track_managed_order(
            symbol,
            client_order_id,
            order.clone(),
        );
    }

    fn broker_api(&self) -> Option<Arc<dyn crate::api::BrokerApi>> {
        Some(self.client.clone())
    }

    async fn shutdown(&self) -> Result<(), String> {
        // AMM 无挂单可撤：把 pending 订单本地落为失败态并返回成功。
        let symbols: Vec<String> = self.symbols.lock().unwrap().iter().cloned().collect();
        let mut manager = self.order_manager.lock().unwrap();
        for symbol in symbols {
            manager.fail_pending(&symbol);
        }
        Ok(())
    }
}

impl Evm {
    /// 账户资源：确认循环的发布通道 + 就绪事件 + feed + 订单回收。
    fn start_account_resources(&self, ev_tx: PublishSender) {
        self.client.engine().set_account_publisher(ev_tx.clone());
        // EVM 没有需要握手的私有订阅；就绪即事件，AccountPlugin 以此为对账屏障。
        let _ = ev_tx.send(PublishEvent::PrivateStreamReady);
        self.start_feed_backend(ev_tx);
        self.start_order_gc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_str() -> String {
        format!(
            r#"rpc_url = "http://127.0.0.1:1"
ws_url = "ws://127.0.0.1:1"
chain_id = 42161
router_address = "0x{:040x}"
private_key = "{}"

[[pools]]
symbol = "WETH/USDC"
pair_address = "0x{:040x}"
base_token = "0x{:040x}"
quote_token = "0x{:040x}"
base_decimals = 18
quote_decimals = 6
"#,
            0xaa,
            hex::encode([7u8; 32]),
            0x01,
            0x02,
            0x03
        )
    }

    #[test]
    fn build_from_requires_and_parses_signer() {
        let evm = Evm::build_from(&config_str()).unwrap();
        assert!(evm.signer.is_some());
        assert!(evm.broker_api().is_some());
        // 行情构建不要求私钥。
        let mut raw_config = config_str();
        raw_config = raw_config.replace(&hex::encode([7u8; 32]), "");
        let market = Evm::build_market_from(&raw_config).unwrap();
        assert!(market.signer.is_none());
        // 缺私钥的账户构建必须失败。
        assert!(Evm::build_from(&raw_config).is_err());
    }

    #[test]
    fn register_ignores_unknown_symbols() {
        let mut evm = Evm::build_from(&config_str()).unwrap();
        evm.register("UNKNOWN/PAIR".to_string());
        assert!(evm.symbols.lock().unwrap().is_empty());
        evm.register("WETH/USDC".to_string());
        assert!(evm.symbols.lock().unwrap().contains("WETH/USDC"));
    }

    #[tokio::test]
    async fn shutdown_fails_pending_orders() {
        use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
        let mut evm = Evm::build_from(&config_str()).unwrap();
        evm.register("WETH/USDC".to_string());
        let mut order = hftbacktest::types::Order::new(
            1,
            0,
            1.0,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::IOC,
        );
        order.status = Status::New;
        evm.track_managed_order("WETH/USDC", "c1", &order);
        evm.shutdown().await.unwrap();
        let manager = evm.order_manager.lock().unwrap();
        let snapshot = manager.orders_snapshot();
        assert_eq!(snapshot[0].1.order.status, Status::Rejected);
    }
}
