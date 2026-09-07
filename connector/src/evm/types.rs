//! EVM venue 共享类型：池配置与链上池状态。
//!
//! 所有 symbol 使用 `BASE/QUOTE` 形式（如 `WETH/USDC`），与统一 API 的其它 venue
//! 一样只在本 venue 内解释。价格/数量在统一 API 边界上使用 `f64`，链上精确值用
//! `U256` 表示，换算由各 DEX adapter 负责。

use std::collections::{HashMap, VecDeque};

use alloy_primitives::{Address, U256};
use serde::Deserialize;

use crate::api::Trade;

/// 单个 AMM 交易池的静态配置（从 TOML 读取，启动时经 RPC 校验）。
#[derive(Clone, Debug, Deserialize)]
pub struct PoolConfig {
    /// Venue 原生 symbol，`BASE/QUOTE` 形式（如 `WETH/USDC`）。
    pub symbol: String,
    /// Uniswap V2 Pair（或等价 AMM）合约地址。
    pub pair_address: Address,
    /// 基础代币（被交易的那个）。
    pub base_token: Address,
    /// 计价代币。
    pub quote_token: Address,
    pub base_decimals: u32,
    pub quote_decimals: u32,
}

impl PoolConfig {
    /// Uniswap V2 约定 token0 是地址较小的那个；两个地址都来自配置，可本地推导。
    pub fn token0_is_base(&self) -> bool {
        self.base_token <= self.quote_token
    }

    /// 给定 side（以 base 为交易对象）返回 (reserveIn, reserveOut)。
    /// Buy = 用 quote 买 base；Sell = 卖 base 换 quote。池状态未知时返回 None。
    pub fn in_out_reserves(&self, state: &PoolState, buy: bool) -> Option<(U256, U256)> {
        let (r0, r1) = state.effective()?;
        let (r_base, r_quote) = if self.token0_is_base() {
            (r0, r1)
        } else {
            (r1, r0)
        };
        Some(if buy {
            (r_quote, r_base)
        } else {
            (r_base, r_quote)
        })
    }

    /// 用 token0/token1 reserve 计算以 quote 计价的 base 中间价。
    pub fn mid_price(&self, state: &PoolState) -> f64 {
        let Some((r0, r1)) = state.effective() else {
            return 0.0;
        };
        let (r_base, r_quote) = if self.token0_is_base() {
            (r0, r1)
        } else {
            (r1, r0)
        };
        let base = u256_to_f64(r_base) / 10f64.powi(self.base_decimals as i32);
        let quote = u256_to_f64(r_quote) / 10f64.powi(self.quote_decimals as i32);
        if base <= 0.0 { 0.0 } else { quote / base }
    }
}

/// 单个池的链上状态：confirmed 是落块确认的 reserve，prechain 是 sequencer feed
/// 预链推演出的最新估计（`effective()` 消费方取 freshest，Sync 事件到达时收敛清空）。
#[derive(Clone, Debug, Default)]
pub struct PoolState {
    pub confirmed: Option<(U256, U256)>,
    pub prechain: Option<(U256, U256)>,
    pub last_update_ns: i64,
}

impl PoolState {
    /// 当前最可信的 reserve 视图：优先预链估计，其次落块确认值。
    pub fn effective(&self) -> Option<(U256, U256)> {
        self.prechain.or(self.confirmed)
    }

    pub fn confirm(&mut self, reserve0: U256, reserve1: U256, now_ns: i64) {
        self.confirmed = Some((reserve0, reserve1));
        // 预链估计已被链上事实覆盖：收敛。
        self.prechain = None;
        self.last_update_ns = now_ns;
    }

    pub fn apply_prechain(&mut self, reserve0: U256, reserve1: U256, now_ns: i64) {
        self.prechain = Some((reserve0, reserve1));
        self.last_update_ns = now_ns;
    }
}

/// 所有已配置池的共享状态（market 循环写入，brokerapi/feed 读取）。
#[derive(Default)]
pub struct MarketState {
    /// pair_address -> 最新状态。
    pub pools: HashMap<Address, PoolState>,
    /// 最近公共成交（Swap 事件），供 `get_trades`/`get_fills` 消费。
    pub trades: VecDeque<Trade>,
}

pub type SharedMarketState = std::sync::Arc<std::sync::Mutex<MarketState>>;

impl MarketState {
    pub fn record_trade(&mut self, trade: Trade, capacity: usize) {
        if self.trades.len() >= capacity {
            self.trades.pop_front();
        }
        self.trades.push_back(trade);
    }
}

/// `U256` 到 `f64` 的有损换算（仅在统一 API 的 f64 边界上使用）。
pub fn u256_to_f64(value: U256) -> f64 {
    // 常规储备值都在 u128 内；溢出时退化为字符串解析，宁可慢不可错。
    match u128::try_from(value) {
        Ok(v) => v as f64,
        Err(_) => value.to_string().parse::<f64>().unwrap_or(f64::INFINITY),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(token0_is_base: bool) -> PoolConfig {
        let (base, quote) = if token0_is_base {
            (Address::repeat_byte(0x01), Address::repeat_byte(0x02))
        } else {
            (Address::repeat_byte(0x02), Address::repeat_byte(0x01))
        };
        PoolConfig {
            symbol: "TST/USD".to_string(),
            pair_address: Address::repeat_byte(0x03),
            base_token: base,
            quote_token: quote,
            base_decimals: 18,
            quote_decimals: 6,
        }
    }

    fn state(reserve0: U256, reserve1: U256) -> PoolState {
        PoolState {
            confirmed: Some((reserve0, reserve1)),
            prechain: None,
            last_update_ns: 0,
        }
    }

    #[test]
    fn token0_is_derived_from_address_order() {
        assert!(pool(true).token0_is_base());
        assert!(!pool(false).token0_is_base());
    }

    #[test]
    fn mid_price_uses_decimals() {
        // 1 base token (18dp) against 3000 quote (6dp) => 3000.0
        let p = pool(true).mid_price(&state(
            U256::from(10u128.pow(18)),
            U256::from(3_000_000_000u64),
        ));
        assert!((p - 3000.0).abs() < 1e-9);
        // token1 是 base 的镜像配置，结果一致。
        let p = pool(false).mid_price(&state(
            U256::from(3_000_000_000u64),
            U256::from(10u128.pow(18)),
        ));
        assert!((p - 3000.0).abs() < 1e-9);
    }

    #[test]
    fn in_out_reserves_follows_side() {
        let cfg = pool(true);
        let st = state(U256::from(100u64), U256::from(200u64));
        // Buy: in=quote(token1), out=base(token0)
        assert_eq!(
            cfg.in_out_reserves(&st, true),
            Some((U256::from(200u64), U256::from(100u64)))
        );
        // Sell: in=base(token0), out=quote(token1)
        assert_eq!(
            cfg.in_out_reserves(&st, false),
            Some((U256::from(100u64), U256::from(200u64)))
        );
    }

    #[test]
    fn prechain_overrides_until_confirmed() {
        let mut st = state(U256::from(100u64), U256::from(200u64));
        st.apply_prechain(U256::from(90u64), U256::from(210u64), 1);
        assert_eq!(
            st.effective(),
            Some((U256::from(90u64), U256::from(210u64)))
        );
        st.confirm(U256::from(101u64), U256::from(201u64), 2);
        assert_eq!(
            st.effective(),
            Some((U256::from(101u64), U256::from(201u64)))
        );
        assert!(st.prechain.is_none());
    }

    #[test]
    fn u256_to_f64_handles_large_values() {
        assert_eq!(u256_to_f64(U256::from(123u64)), 123.0);
        let big = U256::from(2).pow(U256::from(200));
        assert!(u256_to_f64(big).is_finite() && u256_to_f64(big) > 0.0);
    }
}
