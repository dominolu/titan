//! Solana venue 共享类型：池配置与链上池状态。
//!
//! 行情模型与 EVM 方案的 Uniswap V2 适配同构：订阅 AMM 两个 vault 的 SPL token
//! 账户（amount 固定在偏移 64，u64 LE），按常数乘积公式合成深度。
//! 所有 symbol 使用 `BASE/QUOTE` 形式。

use serde::Deserialize;
use sha2::Digest;

/// SPL token 账户中 amount 字段的偏移（u64 LE）。
pub const SPL_TOKEN_ACCOUNT_AMOUNT_OFFSET: usize = 64;

/// base58 编码/解码（Solana 全部公钥与签名的编码）。
pub fn b58_encode(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}

pub fn b58_decode(encoded: &str) -> Vec<u8> {
    bs58::decode(encoded).into_vec().unwrap_or_default()
}

/// ed25519 曲线点解压校验（PDA 推导的核心判定）。
pub fn is_on_curve(bytes: &[u8; 32]) -> bool {
    curve25519_dalek::edwards::CompressedEdwardsY::from_slice(bytes)
        .ok()
        .and_then(|c| c.decompress())
        .is_some()
}

/// find_program_address：从高 bump 向下找第一个"不在曲线上"的哈希。
pub fn find_program_address(seeds: &[&[u8]], program_id: &[u8; 32]) -> ([u8; 32], u8) {
    let mut seeds_flat = Vec::new();
    for seed in seeds {
        seeds_flat.extend_from_slice(seed);
    }
    for bump in (0..=255u8).rev() {
        let mut hasher = sha2::Sha256::new();
        hasher.update(&seeds_flat);
        hasher.update([bump]);
        hasher.update(program_id);
        hasher.update(b"ProgramDerivedAddress");
        let hash: [u8; 32] = hasher.finalize().into();
        if !is_on_curve(&hash) {
            return (hash, bump);
        }
    }
    panic!("no valid program-derived address found");
}

/// 用户 SPL token 关联账户（ATA）地址：find_pda([owner, token_program, mint], ATA 程序)。
pub fn ata_address(owner: &[u8; 32], mint: &str) -> String {
    let token_program: [u8; 32] = b58_decode(TOKEN_PROGRAM).try_into().unwrap();
    let mint_bytes: [u8; 32] = b58_decode(mint).try_into().unwrap();
    let ata_program: [u8; 32] = b58_decode(ATA_PROGRAM).try_into().unwrap();
    let (addr, _bump) = find_program_address(&[owner, &token_program, &mint_bytes], &ata_program);
    b58_encode(&addr)
}

/// Wrapped SOL mint。
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
/// Raydium AMM V4 程序。
pub const RAYDIUM_V4_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
/// Raydium AMM V4 权限（vault 的 owner）。
pub const RAYDIUM_V4_AUTHORITY: &str = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1";
/// Associated Token Account 程序。
pub const ATA_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
/// 系统程序。
pub const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
/// SPL Token 程序。
pub const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// 单个 AMM 交易池的静态配置。
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct PoolConfig {
    /// Venue 原生 symbol，`BASE/QUOTE` 形式。
    pub symbol: String,
    /// Raydium AMM V4 池账户（swap 指令的 ammId；同时充当 targetOrders 与
    /// 全部 serum 占位账户——按 2026-09 实盘 CPI 模板，新版程序接受该占位）。
    pub amm_id: String,
    /// 基础代币 mint（被交易的那个）。
    pub base_mint: String,
    /// 计价代币 mint。
    pub quote_mint: String,
    /// base 储备 vault（SPL token 账户）。
    pub base_vault: String,
    /// quote 储备 vault。
    pub quote_vault: String,
    pub base_decimals: u32,
    pub quote_decimals: u32,
}

impl PoolConfig {
    pub fn base_is_wsol(&self) -> bool {
        self.base_mint == WSOL_MINT
    }

    pub fn quote_is_wsol(&self) -> bool {
        self.quote_mint == WSOL_MINT
    }

    /// 用原始储备直接计算中间价。
    pub fn mid_price_from_reserves(&self, r_base: u128, r_quote: u128) -> f64 {
        let base = r_base as f64 / 10f64.powi(self.base_decimals as i32);
        let quote = r_quote as f64 / 10f64.powi(self.quote_decimals as i32);
        if base <= 0.0 { 0.0 } else { quote / base }
    }
}

/// 单个池的储备状态：confirmed 是落块确认值，prechain 预留（gRPC 预链视图），
/// 消费方取 freshest。
#[derive(Clone, Copy, Debug, Default)]
pub struct PoolReserves {
    pub confirmed: Option<(u128, u128)>,
    pub prechain: Option<(u128, u128)>,
    pub last_update_ns: i64,
}

impl PoolReserves {
    pub fn effective(&self) -> Option<(u128, u128)> {
        self.prechain.or(self.confirmed)
    }

    pub fn confirm(&mut self, base: u128, quote: u128, now_ns: i64) {
        self.confirmed = Some((base, quote));
        self.prechain = None;
        self.last_update_ns = now_ns;
    }
}

/// 全部已配置池的共享状态（WS 后端写入，brokerapi/tx 读取）。
#[derive(Default)]
pub struct SolanaMarketState {
    /// vault -> 当前储备量（raw u64）。
    pub vaults: std::collections::HashMap<String, u64>,
    /// 最近公共成交（由 vault 变化推演），供 `get_trades` 消费。
    pub trades: std::collections::VecDeque<crate::api::Trade>,
}

pub type SharedMarketState = std::sync::Arc<std::sync::Mutex<SolanaMarketState>>;

impl SolanaMarketState {
    pub fn record_trade(&mut self, trade: crate::api::Trade, capacity: usize) {
        if self.trades.len() >= capacity {
            self.trades.pop_front();
        }
        self.trades.push_back(trade);
    }

    /// 池的当前 (base_reserve, quote_reserve)；任一 vault 未加载时返回 None。
    pub fn reserves(&self, pool: &PoolConfig) -> Option<(u128, u128)> {
        let base = *self.vaults.get(&pool.base_vault)?;
        let quote = *self.vaults.get(&pool.quote_vault)?;
        Some((base as u128, quote as u128))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> PoolConfig {
        PoolConfig {
            symbol: "TST/WSOL".to_string(),
            amm_id: "AMM".to_string(),
            base_mint: "MINTA".to_string(),
            quote_mint: WSOL_MINT.to_string(),
            base_vault: "VA".to_string(),
            quote_vault: "VQ".to_string(),
            base_decimals: 6,
            quote_decimals: 9,
        }
    }

    #[test]
    fn wsol_detection() {
        let p = pool();
        assert!(p.quote_is_wsol());
        assert!(!p.base_is_wsol());
    }

    #[test]
    fn reserves_require_both_vaults() {
        let p = pool();
        let mut market = SolanaMarketState::default();
        assert!(market.reserves(&p).is_none());
        market.vaults.insert("VA".to_string(), 100);
        assert!(market.reserves(&p).is_none());
        market.vaults.insert("VQ".to_string(), 200);
        assert_eq!(market.reserves(&p), Some((100, 200)));
    }
}
