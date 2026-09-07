//! Solana venue 配置：TOML 反序列化 + 运行期解析。

use serde::Deserialize;

use crate::solana::types::{PoolConfig, SharedMarketState};

/// TOML 原始配置。
#[derive(Clone, Debug, Deserialize)]
pub struct RawSolanaConfig {
    pub rpc_url: String,
    pub ws_url: String,
    /// solana-keygen 格式的 keypair 文件（64 字节数字数组）。
    #[serde(default)]
    pub keypair_path: String,
    /// 优先费（micro-lamports/cu）。0 = 不加优先费指令。
    #[serde(default = "default_priority_fee_micro_lamports")]
    pub priority_fee_micro_lamports: u64,
    /// 单笔交易 compute unit limit。
    #[serde(default = "default_compute_unit_limit")]
    pub compute_unit_limit: u32,
    /// 市价单滑点保护（基点，双边）。
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: u64,
    /// AMM 手续费（基点），用于本地报价与滑点下限。
    #[serde(default = "default_fee_bps")]
    pub fee_bps: f64,
    /// 收据等待超时；超时视为 dropped。
    #[serde(default = "default_tx_timeout_ms")]
    pub tx_timeout_ms: u64,
    /// 合成订单簿档数。
    #[serde(default = "default_book_levels")]
    pub book_levels: usize,
    /// 公共成交环形缓冲长度。
    #[serde(default = "default_trade_buffer")]
    pub trade_buffer: usize,
    pub pools: Vec<PoolConfig>,
}

fn default_priority_fee_micro_lamports() -> u64 {
    50_000
}
fn default_compute_unit_limit() -> u32 {
    300_000
}
fn default_slippage_bps() -> u64 {
    100
}
fn default_fee_bps() -> f64 {
    25.0
}
fn default_tx_timeout_ms() -> u64 {
    60_000
}
fn default_book_levels() -> usize {
    10
}
fn default_trade_buffer() -> usize {
    1000
}

impl RawSolanaConfig {
    pub fn parse(config: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(config)
    }
}

/// 运行期配置。
#[derive(Clone)]
pub struct SolanaConfig {
    pub rpc_url: String,
    pub ws_url: String,
    pub keypair_path: String,
    pub priority_fee_micro_lamports: u64,
    pub compute_unit_limit: u32,
    pub slippage_bps: u64,
    pub fee_bps: f64,
    pub tx_timeout_ms: u64,
    pub book_levels: usize,
    pub trade_buffer: usize,
    pub pools: Vec<PoolConfig>,
    pub market_state: SharedMarketState,
}

impl SolanaConfig {
    pub fn from_raw(raw: RawSolanaConfig, market_state: SharedMarketState) -> Self {
        Self {
            rpc_url: raw.rpc_url,
            ws_url: raw.ws_url,
            keypair_path: raw.keypair_path,
            priority_fee_micro_lamports: raw.priority_fee_micro_lamports,
            compute_unit_limit: raw.compute_unit_limit,
            slippage_bps: raw.slippage_bps,
            fee_bps: raw.fee_bps,
            tx_timeout_ms: raw.tx_timeout_ms,
            book_levels: raw.book_levels,
            trade_buffer: raw.trade_buffer,
            pools: raw.pools,
            market_state,
        }
    }

    pub fn pool(&self, symbol: &str) -> Option<&PoolConfig> {
        self.pools.iter().find(|p| p.symbol == symbol)
    }

    pub fn vault_addresses(&self) -> Vec<String> {
        let mut v = Vec::new();
        for pool in &self.pools {
            for vault in [&pool.base_vault, &pool.quote_vault] {
                if !v.contains(vault) {
                    v.push(vault.clone());
                }
            }
        }
        v
    }

    #[cfg(test)]
    pub(crate) fn test_config(pools: Vec<PoolConfig>, market_state: SharedMarketState) -> Self {
        Self {
            rpc_url: "https://127.0.0.1:1".to_string(),
            ws_url: "ws://127.0.0.1:1".to_string(),
            keypair_path: String::new(),
            priority_fee_micro_lamports: 50_000,
            compute_unit_limit: 300_000,
            slippage_bps: 100,
            fee_bps: 25.0,
            tx_timeout_ms: 60_000,
            book_levels: 10,
            trade_buffer: 1000,
            pools,
            market_state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
rpc_url = "https://solana-rpc.publicnode.com"
ws_url = "wss://solana-rpc.publicnode.com"
keypair_path = "/path/keypair.json"
priority_fee_micro_lamports = 100000
slippage_bps = 50

[[pools]]
symbol = "TST/WSOL"
amm_id = "AMMID"
base_mint = "MINTA"
quote_mint = "So11111111111111111111111111111111111111112"
base_vault = "VA"
quote_vault = "VQ"
base_decimals = 6
quote_decimals = 9
"#;

    #[test]
    fn parses_sample_config() {
        let raw = RawSolanaConfig::parse(SAMPLE).unwrap();
        assert_eq!(raw.slippage_bps, 50);
        assert_eq!(raw.priority_fee_micro_lamports, 100_000);
        assert_eq!(raw.pools.len(), 1);
        assert_eq!(raw.fee_bps, 25.0);
        let config = SolanaConfig::from_raw(raw, SharedMarketState::default());
        assert_eq!(
            config.vault_addresses(),
            vec!["VA".to_string(), "VQ".to_string()]
        );
        assert!(config.pool("TST/WSOL").is_some());
        assert!(config.pool("NOPE").is_none());
    }

    #[test]
    fn rejects_missing_required_fields() {
        assert!(RawSolanaConfig::parse("rpc_url = \"x\"").is_err());
    }
}
