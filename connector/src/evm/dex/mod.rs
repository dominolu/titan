//! EVM DEX adapter abstraction.
//!
//! The connector core depends on this trait for quoting, synthetic books,
//! reserve projection, and swap encoding. Additional DEX implementations can
//! be introduced without changing the market/feed/transaction orchestration.

pub mod uniswap_v2;

use alloy_primitives::{Address, U256};

use crate::api::ApiSide;
use crate::evm::types::{PoolConfig, PoolState};

/// Protocol-specific pricing and calldata implementation.
pub trait DexAdapter: Send + Sync {
    /// 以 quote 计价的成本/收益：Sell = 得到的 quote，Buy = 买下 base_qty 所需的 quote。
    fn quote_out(
        &self,
        pool: &PoolConfig,
        state: &PoolState,
        side: ApiSide,
        base_qty: f64,
    ) -> Option<f64>;

    /// 由当前观测合成深度：返回 (bids, asks)，每档 (price, base_qty)。
    fn synthetic_book(
        &self,
        pool: &PoolConfig,
        state: &PoolState,
        levels: usize,
    ) -> (Vec<(f64, f64)>, Vec<(f64, f64)>);

    /// 用一笔预链 swap 的 (in_amount, out_amount)（最小单位）推演新 reserve。
    fn apply_swap(
        &self,
        pool: &PoolConfig,
        state: &PoolState,
        side: ApiSide,
        in_raw: U256,
        out_raw: U256,
    ) -> Option<(U256, U256)>;

    /// 构建一笔 swap 的 calldata（最小单位输入 + 滑点保护）。
    fn encode_swap(
        &self,
        pool: &PoolConfig,
        side: ApiSide,
        amount_in_raw: U256,
        amount_out_min_raw: U256,
        recipient: Address,
        deadline: u64,
    ) -> Vec<u8>;

    /// 该家族要求的 router 授权额度（最大值）。
    fn max_allowance(&self) -> U256;
}
