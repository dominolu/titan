//! Raydium AMM V4 swap 指令构造 + 恒定乘积报价。
//!
//! 账户模板取自 2026-09 主网实盘 CPI（17 账户；Serum/OpenBook 账户全部以
//! ammId 占位，新版程序在 Serum 退役后接受该形式）：
//! [tokenProgram, ammId, authority, targetOrders(=ammId), coinVault, pcVault,
//!  ammId ×8 (serum 占位), userSource, userDest, userOwner]
//! 指令数据：tag(9 = swapBaseIn) + amount_in u64 LE + min_amount_out u64 LE。

use crate::api::ApiSide;
use crate::solana::types::{PoolConfig, RAYDIUM_V4_AUTHORITY};

/// swapBaseIn 的指令 tag 与数据长度。
pub const SWAP_BASE_IN_TAG: u8 = 9;
pub const SWAP_BASE_IN_DATA_LEN: usize = 1 + 8 + 8;

/// 常数乘积报价（含手续费，基点）：amountOut = in*(10000-fee)*rOut / (rIn*10000 + in*(10000-fee))。
pub fn get_amount_out(amount_in: u128, reserve_in: u128, reserve_out: u128, fee_bps: u64) -> u128 {
    if reserve_in == 0 || reserve_out == 0 {
        return 0;
    }
    let fee = (10000 - fee_bps.min(9999)) as u128;
    let amount_in_with_fee = amount_in.saturating_mul(fee);
    let numerator = amount_in_with_fee.saturating_mul(reserve_out);
    let denominator = reserve_in
        .saturating_mul(10_000)
        .saturating_add(amount_in_with_fee);
    if denominator == 0 {
        0
    } else {
        numerator / denominator
    }
}

/// 反解 getAmountOut：给定输出量，求所需输入量。
pub fn get_amount_in(amount_out: u128, reserve_in: u128, reserve_out: u128, fee_bps: u64) -> u128 {
    if reserve_out <= amount_out {
        return u128::MAX;
    }
    let fee = (10000 - fee_bps.min(9999)) as u128;
    let numerator = reserve_in.saturating_mul(amount_out).saturating_mul(10_000);
    let denominator = (reserve_out - amount_out).saturating_mul(fee);
    if denominator == 0 {
        u128::MAX
    } else {
        numerator / denominator + 1
    }
}

/// 构造 Raydium V4 swapBaseIn 指令数据。
pub fn encode_swap_base_in_data(amount_in: u64, min_amount_out: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(SWAP_BASE_IN_DATA_LEN);
    data.push(SWAP_BASE_IN_TAG);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());
    data
}

/// 构造 swap 指令的账户列表（顺序与主网实盘 CPI 完全一致）。
///
/// - `amm_id`：池账户，同时充当 targetOrders 与 serum 占位；
/// - `coin_vault` / `pc_vault`：池的两个储备 vault（AMM 账户 @336/@368）；
/// - `user_source` / `user_dest`：用户的输入/输出 token 账户；
/// - `user_owner`：签名者。
pub fn swap_base_in_accounts(
    amm_id: &str,
    coin_vault: &str,
    pc_vault: &str,
    user_source: &str,
    user_dest: &str,
    user_owner: &str,
) -> Vec<String> {
    vec![
        crate::solana::types::TOKEN_PROGRAM.to_string(),
        amm_id.to_string(),
        RAYDIUM_V4_AUTHORITY.to_string(),
        amm_id.to_string(), // targetOrders 占位
        coin_vault.to_string(),
        pc_vault.to_string(),
        amm_id.to_string(), // serumProgram 占位
        amm_id.to_string(), // serumMarket 占位
        amm_id.to_string(), // serumBids 占位
        amm_id.to_string(), // serumAsks 占位
        amm_id.to_string(), // serumEventQueue 占位
        amm_id.to_string(), // serumCoinVault 占位
        amm_id.to_string(), // serumPcVault 占位
        amm_id.to_string(), // serumVaultSigner 占位
        user_source.to_string(),
        user_dest.to_string(),
        user_owner.to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amount_out_math() {
        // 1 SOL in, 100 SOL in / 10_000 token out 储备，25bps fee
        let out = get_amount_out(10u128.pow(9), 100 * 10u128.pow(9), 10_000_000_000, 25);
        let expected = (10u128.pow(9) * 9975 * 10_000_000_000)
            / (100 * 10u128.pow(9) * 10_000 + 10u128.pow(9) * 9975);
        assert_eq!(out, expected);
        // 储备 100 SOL : 10_000 token，卖 1 SOL（约 1% 池深，扣 0.25% fee）≈ 98.77 token
        assert!(out > 98_000_000 && out < 99_000_000);
    }

    #[test]
    fn amount_in_round_trips() {
        let (ri, ro) = (100 * 10u128.pow(9), 10_000_000_000);
        let out = get_amount_out(10u128.pow(9), ri, ro, 25);
        let back = get_amount_in(out, ri, ro, 25);
        let out2 = get_amount_out(back, ri, ro, 25);
        assert!(out2 >= out && out2 - out <= 2);
    }

    #[test]
    fn swap_data_layout() {
        let data = encode_swap_base_in_data(1_000_000, 999_999);
        assert_eq!(data.len(), 17);
        assert_eq!(data[0], 9);
        assert_eq!(
            u64::from_le_bytes(data[1..9].try_into().unwrap()),
            1_000_000
        );
        assert_eq!(u64::from_le_bytes(data[9..17].try_into().unwrap()), 999_999);
    }

    #[test]
    fn swap_account_template() {
        let accts = swap_base_in_accounts("AMM", "COINV", "PCV", "SRC", "DST", "OWNER");
        assert_eq!(accts.len(), 17);
        assert_eq!(accts[0], crate::solana::types::TOKEN_PROGRAM);
        assert_eq!(accts[1], "AMM");
        assert_eq!(accts[2], RAYDIUM_V4_AUTHORITY);
        assert_eq!(accts[3], "AMM", "targetOrders 占位");
        assert_eq!(accts[4], "COINV");
        assert_eq!(accts[5], "PCV");
        // [6..14] 全部为 ammId 占位（serum 账户组）
        assert!(accts[6..14].iter().all(|a| a == "AMM"));
        assert_eq!(accts[14], "SRC");
        assert_eq!(accts[15], "DST");
        assert_eq!(accts[16], "OWNER");
    }
}

/// 恒定乘积行情推演（与 EVM venue 的 UniswapV2 适配同构）。
///
/// `quote_out` 统一返回 quote 单位：Sell = 得到的 quote，Buy = 买下 base_qty
/// 所需付出的 quote。
#[derive(Debug, Default, Clone, Copy)]
pub struct UniswapV2StyleBook;

impl UniswapV2StyleBook {
    pub fn quote_out(
        &self,
        pool: &PoolConfig,
        r_base: u128,
        r_quote: u128,
        side: ApiSide,
        base_qty: f64,
    ) -> Option<f64> {
        let scale = |q: f64, d: u32| -> Option<u128> {
            let raw = q * 10f64.powi(d as i32);
            if !raw.is_finite() || raw <= 0.0 {
                return None;
            }
            Some(raw as u128)
        };
        let quote_raw = match side {
            ApiSide::Sell => {
                let amount_in = scale(base_qty, pool.base_decimals)?;
                get_amount_out(amount_in, r_base, r_quote, 0)
            }
            ApiSide::Buy => {
                let base_out = scale(base_qty, pool.base_decimals)?;
                let amount_in = get_amount_in(base_out, r_quote, r_base, 0);
                if amount_in == u128::MAX {
                    return None;
                }
                amount_in
            }
            ApiSide::Unknown => return None,
        };
        Some(quote_raw as f64 / 10f64.powi(pool.quote_decimals as i32))
    }

    /// 由当前储备合成深度：返回 (bids, asks)，每档 (price, base_qty)。
    pub fn synthetic_book(
        &self,
        pool: &PoolConfig,
        r_base: u128,
        r_quote: u128,
        levels: usize,
    ) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        let base_reserve = r_base as f64 / 10f64.powi(pool.base_decimals as i32);
        let mut bids = Vec::with_capacity(levels);
        let mut asks = Vec::with_capacity(levels);
        let mut size = base_reserve * 1e-4;
        if size <= 0.0 {
            return (bids, asks);
        }
        for _ in 0..levels {
            if let Some(bid) = self.quote_out(pool, r_base, r_quote, ApiSide::Sell, size) {
                if bid > 0.0 {
                    bids.push((bid / size, size));
                }
            }
            if let Some(ask) = self.quote_out(pool, r_base, r_quote, ApiSide::Buy, size) {
                if ask > 0.0 {
                    asks.push((ask / size, size));
                }
            }
            size *= 2.5;
        }
        (bids, asks)
    }
}
