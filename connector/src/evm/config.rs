//! EVM venue 配置：TOML 反序列化 + 运行期解析（含共享行情状态）。
//!
//! 账户连接与行情连接共用同一份 TOML；行情构建（`build_market_from`）不要求
//! private_key，账户构建必须提供。

use serde::Deserialize;

#[cfg(test)]
use crate::evm::types::MarketState;
use crate::evm::types::{PoolConfig, SharedMarketState};
use alloy_primitives::Address;

/// TOML 原始配置。
#[derive(Clone, Debug, Deserialize)]
pub struct RawEvmConfig {
    pub rpc_url: String,
    pub ws_url: String,
    /// Arbitrum sequencer feed 端点（可选；缺省不启用预链视图）。
    #[serde(default)]
    pub feed_url: Option<String>,
    /// Orbit 链的 chain id；缺省时从 RPC 探测（sequencer feed 必须显式提供）。
    #[serde(default)]
    pub chain_id: Option<u64>,
    /// Uniswap V2 风格 router 地址。
    pub router_address: Address,
    #[serde(default)]
    pub private_key: String,
    /// 市价单滑点保护（基点，双边）。
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: u64,
    /// 单笔交易 gas 上限价（gwei）。
    #[serde(default = "default_max_gas_price_gwei")]
    pub max_gas_price_gwei: f64,
    #[serde(default = "default_swap_gas_limit")]
    pub swap_gas_limit: u64,
    #[serde(default = "default_approve_gas_limit")]
    pub approve_gas_limit: u64,
    /// swap 交易的有效期（秒），过期后 router revert。
    #[serde(default = "default_deadline_secs")]
    pub deadline_secs: u64,
    /// 收据等待超时；超时视为 dropped（Canceled）。
    #[serde(default = "default_tx_timeout_ms")]
    pub tx_timeout_ms: u64,
    /// 合成订单簿档数。
    #[serde(default = "default_book_levels")]
    pub book_levels: usize,
    /// taker 手续费率（基点），用于 get_fee_rates。
    #[serde(default = "default_fee_bps")]
    pub fee_bps: f64,
    /// 公共成交环形缓冲长度。
    #[serde(default = "default_trade_buffer")]
    pub trade_buffer: usize,
    pub pools: Vec<PoolConfig>,
}

fn default_slippage_bps() -> u64 {
    50
}
fn default_max_gas_price_gwei() -> f64 {
    0.5
}
fn default_swap_gas_limit() -> u64 {
    600_000
}
fn default_approve_gas_limit() -> u64 {
    120_000
}
fn default_deadline_secs() -> u64 {
    300
}
fn default_tx_timeout_ms() -> u64 {
    120_000
}
fn default_book_levels() -> usize {
    10
}
fn default_fee_bps() -> f64 {
    30.0
}
fn default_trade_buffer() -> usize {
    1000
}

impl RawEvmConfig {
    pub fn parse(config: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(config)
    }
}

/// 运行期配置：原始字段 + 全 venue 共享的行情状态。
#[derive(Clone)]
pub struct EvmConfig {
    pub rpc_url: String,
    pub ws_url: String,
    pub feed_url: Option<String>,
    pub chain_id: Option<u64>,
    pub router_address: Address,
    pub slippage_bps: u64,
    pub max_gas_price_gwei: f64,
    pub swap_gas_limit: u64,
    pub approve_gas_limit: u64,
    pub deadline_secs: u64,
    pub tx_timeout_ms: u64,
    pub book_levels: usize,
    pub fee_bps: f64,
    pub trade_buffer: usize,
    pub pools: Vec<PoolConfig>,
    pub market_state: SharedMarketState,
}

impl EvmConfig {
    pub fn from_raw(raw: RawEvmConfig, market_state: SharedMarketState) -> Self {
        Self {
            rpc_url: raw.rpc_url,
            ws_url: raw.ws_url,
            feed_url: raw.feed_url,
            chain_id: raw.chain_id,
            router_address: raw.router_address,
            slippage_bps: raw.slippage_bps,
            max_gas_price_gwei: raw.max_gas_price_gwei,
            swap_gas_limit: raw.swap_gas_limit,
            approve_gas_limit: raw.approve_gas_limit,
            deadline_secs: raw.deadline_secs,
            tx_timeout_ms: raw.tx_timeout_ms,
            book_levels: raw.book_levels,
            fee_bps: raw.fee_bps,
            trade_buffer: raw.trade_buffer,
            pools: raw.pools,
            market_state,
        }
    }

    pub fn pool(&self, symbol: &str) -> Option<&PoolConfig> {
        self.pools.iter().find(|p| p.symbol == symbol)
    }

    pub fn pair_addresses(&self) -> Vec<Address> {
        self.pools.iter().map(|p| p.pair_address).collect()
    }

    /// 测试构造：直接给定池与行情状态。
    #[cfg(test)]
    pub(crate) fn test_config(pools: Vec<PoolConfig>, market_state: MarketState) -> Self {
        Self {
            rpc_url: "http://127.0.0.1:1".to_string(),
            ws_url: "ws://127.0.0.1:1".to_string(),
            feed_url: None,
            chain_id: Some(42161),
            router_address: Address::repeat_byte(0xaa),
            slippage_bps: 50,
            max_gas_price_gwei: 0.5,
            swap_gas_limit: 600_000,
            approve_gas_limit: 120_000,
            deadline_secs: 300,
            tx_timeout_ms: 120_000,
            book_levels: 10,
            fee_bps: 30.0,
            trade_buffer: 1000,
            pools,
            market_state: SharedMarketState::new(std::sync::Mutex::new(market_state)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    const SAMPLE: &str = r#"
rpc_url = "https://arb1.arbitrum.io/rpc"
ws_url = "wss://arb1.arbitrum.io/ws"
feed_url = "wss://arb1-feed.arbitrum.io/feed"
chain_id = 42161
router_address = "0xf164fC0Ec4E93095b804a4795bBe1e041497b92a"
private_key = ""
slippage_bps = 30

[[pools]]
symbol = "WETH/USDC"
pair_address = "0xc31e54faf074f5c3191d1f3a92849a4b7424e6e2"
base_token = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
quote_token = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
base_decimals = 18
quote_decimals = 6
"#;

    #[test]
    fn parses_sample_config() {
        let raw = RawEvmConfig::parse(SAMPLE).unwrap();
        assert_eq!(raw.slippage_bps, 30);
        assert_eq!(raw.chain_id, Some(42161));
        assert_eq!(raw.pools.len(), 1);
        assert_eq!(raw.pools[0].base_decimals, 18);
        assert_eq!(
            raw.router_address,
            address!("f164fC0Ec4E93095b804a4795bBe1e041497b92a")
        );
        // 未填字段落默认值。
        assert_eq!(raw.max_gas_price_gwei, 0.5);
        assert_eq!(raw.tx_timeout_ms, 120_000);
        assert_eq!(raw.book_levels, 10);
    }

    #[test]
    fn rejects_missing_required_fields() {
        assert!(RawEvmConfig::parse("rpc_url = \"x\"").is_err());
    }
}
