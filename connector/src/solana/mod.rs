//! Solana 链上 venue（方案 B 的 JSON-RPC 全链路实现，gRPC 预链插槽待接）。
//!
//! 结构总览（设计文档 `docs/onchain_venue_broker_design.md` 方案 B）：
//! - [`config`]：TOML 配置与运行期解析；
//! - [`rpc`]：JSON-RPC 封装（HTTP 读路径 + 发单 + 状态轮询）；
//! - [`market`]：WS `accountSubscribe` 行情后端（confirmed 视图）；
//! - [`raydium`]：Raydium AMM V4 swap 指令构造 + 恒定乘积行情推演；
//! - [`tx`]：交易编码/签名（ed25519）+ WSOL wrap/unwrap + 确认监视循环；
//! - [`ordermanager`]：签名驱动的订单状态机；
//! - [`brokerapi`]：统一 `BrokerApi` 的 AMM 语义映射。
//!
//! `run_market_data` 只启动行情后端；`run_account` 启动确认通道并发布
//! `PrivateStreamReady`（Solana 无持久私有订阅，就绪即事件）。

pub mod brokerapi;
pub mod config;
pub mod market;
pub mod ordermanager;
pub mod raydium;
pub mod rpc;
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
use crate::solana::brokerapi::SolanaBrokerApi;
use crate::solana::config::{RawSolanaConfig, SolanaConfig};
use crate::solana::ordermanager::{OrderManager, SharedOrderManager};
use crate::solana::rpc::SolanaRpc;
use crate::solana::types::SharedMarketState;

#[derive(Error, Debug)]
pub enum SolanaError {
    #[error("InvalidArg: {0}")]
    InvalidArg(&'static str),
    #[error("Decode: {0}")]
    Decode(&'static str),
    #[error("Rpc: {0}")]
    Rpc(String),
    #[error("Http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Ws: {0}")]
    Ws(String),
    #[error("Keypair: {0}")]
    Keypair(String),
    #[error("Config: {0}")]
    Config(#[from] toml::de::Error),
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
}

impl SolanaError {
    pub fn to_value(&self) -> Value {
        Value::String(self.to_string())
    }
}

pub type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;
pub type SharedMarketSubscriptions = Arc<Mutex<HashMap<String, HashSet<MarketDataKind>>>>;

pub struct Solana {
    config: Arc<SolanaConfig>,
    rpc: SolanaRpc,
    #[cfg_attr(not(test), allow(dead_code))]
    signing: Option<ed25519_dalek::SigningKey>,
    order_manager: SharedOrderManager,
    client: Arc<SolanaBrokerApi>,
    market_tx: broadcast::Sender<MarketDataCommand>,
    market_subscriptions: SharedMarketSubscriptions,
    symbols: SharedSymbolSet,
}

fn market_kind_set() -> Vec<MarketDataKind> {
    vec![MarketDataKind::Depth, MarketDataKind::Trades]
}

/// 加载 solana-keygen 格式 keypair（64 字节数字数组），返回 ed25519 签名器。
fn load_signer(path: &str) -> Result<ed25519_dalek::SigningKey, SolanaError> {
    let bytes: Vec<u8> = serde_json::from_slice(
        &std::fs::read(path).map_err(|e| SolanaError::Keypair(format!("read {path}: {e}")))?,
    )
    .map_err(|e| SolanaError::Keypair(format!("parse {path}: {e}")))?;
    if bytes.len() != 64 {
        return Err(SolanaError::Keypair(format!(
            "keypair must be 64 bytes, got {}",
            bytes.len()
        )));
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes[..32]);
    let signing = ed25519_dalek::SigningKey::from_bytes(&seed);
    if signing.verifying_key().to_bytes()[..] != bytes[32..] {
        return Err(SolanaError::Keypair(
            "keypair file inconsistent: derived pubkey mismatch".to_string(),
        ));
    }
    Ok(signing)
}

impl Solana {
    fn from_raw(raw: RawSolanaConfig, require_signer: bool) -> Result<Self, SolanaError> {
        let signing = if raw.keypair_path.is_empty() {
            if require_signer {
                return Err(SolanaError::InvalidArg(
                    "keypair_path is required for account connector",
                ));
            }
            None
        } else {
            Some(load_signer(&raw.keypair_path)?)
        };
        let market_state: SharedMarketState = Arc::new(Mutex::new(Default::default()));
        let config = Arc::new(SolanaConfig::from_raw(raw, market_state.clone()));
        let rpc = SolanaRpc::new(&config.rpc_url, &config.ws_url);
        let order_manager = Arc::new(Mutex::new(OrderManager::new()));
        let client = Arc::new(SolanaBrokerApi::new(
            config.clone(),
            rpc.clone(),
            signing.clone(),
            order_manager.clone(),
        ));
        let (market_tx, _) = broadcast::channel(500);
        Ok(Self {
            config,
            rpc,
            signing,
            order_manager,
            client,
            market_tx,
            market_subscriptions: Default::default(),
            symbols: Default::default(),
        })
    }

    fn start_market_backend(&self, ev_tx: PublishSender) {
        let backend = market::WsFeedBackend::new(
            self.config.clone(),
            self.rpc.clone(),
            self.config.market_state.clone(),
        );
        let commands = self.market_tx.subscribe();
        tokio::spawn(async move {
            backend.run(commands, ev_tx).await;
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

impl ConnectorBuilder for Solana {
    type Error = SolanaError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        Self::from_raw(RawSolanaConfig::parse(config)?, true)
    }
}

impl Solana {
    /// 行情连接构建：不要求也不保留 keypair。
    pub(crate) fn build_market_from(config: &str) -> Result<Self, SolanaError> {
        let mut raw = RawSolanaConfig::parse(config)?;
        raw.keypair_path.clear();
        Self::from_raw(raw, false)
    }
}

#[async_trait::async_trait]
impl Connector for Solana {
    fn register(&mut self, symbol: String) {
        if self.config.pool(&symbol).is_none() {
            warn!(%symbol, "Solana connector: symbol not present in pools config; ignored");
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

impl Solana {
    /// 账户资源：确认循环的发布通道 + 就绪事件 + 订单回收。
    fn start_account_resources(&self, ev_tx: PublishSender) {
        if let Some(engine) = self.client.engine() {
            engine.set_account_publisher(ev_tx.clone());
        }
        // Solana 没有需要握手的私有订阅；就绪即事件，AccountPlugin 以此为对账屏障。
        let _ = ev_tx.send(PublishEvent::PrivateStreamReady);
        self.start_order_gc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_str() -> String {
        let keypair = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../.secrets/sol_test_keypair.json"
        );
        format!(
            r#"rpc_url = "https://127.0.0.1:1"
ws_url = "ws://127.0.0.1:1"
keypair_path = "{keypair}"

[[pools]]
symbol = "TST/WSOL"
amm_id = "AMMID"
base_mint = "MINTA"
quote_mint = "So11111111111111111111111111111111111111112"
base_vault = "VA"
quote_vault = "VQ"
base_decimals = 6
quote_decimals = 9
"#
        )
    }

    #[test]
    fn build_from_requires_and_loads_keypair() {
        let solana = Solana::build_from(&config_str()).unwrap();
        assert!(solana.signing.is_some());
        assert!(solana.broker_api().is_some());
        // 行情构建不要求 keypair。
        let market_config = config_str().replace(
            &concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../.secrets/sol_test_keypair.json"
            ),
            "",
        );
        let market = Solana::build_market_from(&market_config).unwrap();
        assert!(market.signing.is_none());
        // 缺 keypair 的账户构建必须失败。
        assert!(Solana::build_from(&market_config).is_err());
    }

    #[test]
    fn register_ignores_unknown_symbols() {
        let mut solana = Solana::build_from(&config_str()).unwrap();
        solana.register("UNKNOWN/PAIR".to_string());
        assert!(solana.symbols.lock().unwrap().is_empty());
        solana.register("TST/WSOL".to_string());
        assert!(solana.symbols.lock().unwrap().contains("TST/WSOL"));
    }

    #[tokio::test]
    async fn shutdown_fails_pending_orders() {
        use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
        let mut solana = Solana::build_from(&config_str()).unwrap();
        solana.register("TST/WSOL".to_string());
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
        solana.track_managed_order("TST/WSOL", "c1", &order);
        solana.shutdown().await.unwrap();
        let manager = solana.order_manager.lock().unwrap();
        let snapshot = manager.orders_snapshot();
        assert_eq!(snapshot[0].1.order.status, Status::Rejected);
    }
}
