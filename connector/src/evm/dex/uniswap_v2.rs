//! Uniswap V2（及所有 x*y=k 类 AMM）适配。
//!
//! 行情：由 Sync 维护 reserve，常数乘积公式推演深度；预链视图由 sequencer feed
//! 解码出的 (in, out) 直接平移 reserve 得到。
//! 交易：`swapExactTokensForTokens`（quote→base 与 base→quote 两个方向）。

use alloy_primitives::{Address, U256};
use alloy_sol_types::{SolCall, sol};

use crate::api::ApiSide;
use crate::evm::dex::DexAdapter;
use crate::evm::types::{PoolConfig, PoolState, u256_to_f64};

sol! {
    interface IUniswapV2Router02 {
        function swapExactTokensForTokens(
            uint amountIn,
            uint amountOutMin,
            address[] calldata path,
            address to,
            uint deadline
        ) external returns (uint[] memory amounts);
        function swapTokensForExactTokens(
            uint amountOut,
            uint amountInMax,
            address[] calldata path,
            address to,
            uint deadline
        ) external returns (uint[] memory amounts);
    }

    interface IUniswapV2Pair {
        function getReserves()
            external
            view
            returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
    }

    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
    }
}

sol! {
    event Sync(uint112 reserve0, uint112 reserve1);
    event Swap(
        address indexed sender,
        uint amount0In,
        uint amount1In,
        uint amount0Out,
        uint amount1Out,
        address indexed to
    );
}

/// Uniswap V2 手续费分母：`amountOut = (in * 997 * rOut) / (rIn * 1000 + in * 997)`。
const FEE_NUMERATOR: u128 = 997;
const FEE_DENOMINATOR: u128 = 1000;

/// 反解 getAmountOut：给定输出量，求所需输入量（含手续费）。
pub fn get_amount_in(amount_out: u128, reserve_in: u128, reserve_out: u128) -> u128 {
    if reserve_out <= amount_out {
        return u128::MAX;
    }
    let numerator = reserve_in
        .saturating_mul(amount_out)
        .saturating_mul(FEE_DENOMINATOR);
    let denominator = (reserve_out - amount_out).saturating_mul(FEE_NUMERATOR);
    if denominator == 0 {
        return u128::MAX;
    }
    numerator / denominator + 1
}

/// 标准常乘积报价（含 0.3% 手续费）。
pub fn get_amount_out(amount_in: u128, reserve_in: u128, reserve_out: u128) -> u128 {
    if reserve_in == 0 || reserve_out == 0 {
        return 0;
    }
    let amount_in_with_fee = amount_in.saturating_mul(FEE_NUMERATOR);
    let numerator = amount_in_with_fee.saturating_mul(reserve_out);
    let denominator = reserve_in
        .saturating_mul(FEE_DENOMINATOR)
        .saturating_add(amount_in_with_fee);
    if denominator == 0 {
        0
    } else {
        numerator / denominator
    }
}

fn to_u128(v: U256) -> Option<u128> {
    u128::try_from(v).ok()
}

fn scale(quantity: f64, decimals: u32) -> Option<U256> {
    if !quantity.is_finite() || quantity <= 0.0 {
        return None;
    }
    let scaled = quantity * 10f64.powi(decimals as i32);
    if !scaled.is_finite() || scaled >= 1e39 {
        return None;
    }
    Some(U256::from(scaled as u128))
}

fn unscale(value: U256, decimals: u32) -> f64 {
    u256_to_f64(value) / 10f64.powi(decimals as i32)
}

/// Uniswap V2 适配器。`pool_levels` 控制合成深度的档数基准。
#[derive(Debug, Default)]
pub struct UniswapV2Adapter {
    pub max_allowance_value: U256,
}

impl UniswapV2Adapter {
    pub fn new() -> Self {
        Self {
            max_allowance_value: U256::MAX,
        }
    }
}

impl DexAdapter for UniswapV2Adapter {
    fn quote_out(
        &self,
        pool: &PoolConfig,
        state: &PoolState,
        side: ApiSide,
        base_qty: f64,
    ) -> Option<f64> {
        // 统一语义：返回以 quote 计价的成本/收益 —— Sell = 得到的 quote，
        // Buy = 买下 base_qty 所需付出的 quote。两种方向都换算到 quote 单位，
        // 调用方（plan_swap / synthetic_book）据此计算价格与滑点保护。
        let Some((reserve_in, reserve_out)) = pool.in_out_reserves(state, side == ApiSide::Buy)
        else {
            return None;
        };
        let (ri, ro) = (to_u128(reserve_in)?, to_u128(reserve_out)?);
        let quote_raw = match side {
            ApiSide::Sell => {
                let amount_in_raw = scale(base_qty, pool.base_decimals)?;
                get_amount_out(to_u128(amount_in_raw)?, ri, ro)
            }
            ApiSide::Buy => {
                let base_out_raw = scale(base_qty, pool.base_decimals)?;
                let amount_in = get_amount_in(to_u128(base_out_raw)?, ri, ro);
                if amount_in == u128::MAX {
                    return None;
                }
                amount_in
            }
            ApiSide::Unknown => return None,
        };
        Some(unscale(U256::from(quote_raw), pool.quote_decimals))
    }

    fn synthetic_book(
        &self,
        pool: &PoolConfig,
        state: &PoolState,
        levels: usize,
    ) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        let base_reserve = pool
            .in_out_reserves(state, false)
            .map(|(r_in_base, _)| unscale(r_in_base, pool.base_decimals))
            .unwrap_or(0.0);
        let mut bids = Vec::with_capacity(levels);
        let mut asks = Vec::with_capacity(levels);
        // 每档用上一档累计量的 1.5 倍递增，覆盖从微小到显著的成交规模。
        let mut size = base_reserve * 1e-4;
        if size <= 0.0 {
            return (bids, asks);
        }
        for _ in 0..levels {
            if let Some(bid) = self.quote_out(pool, state, ApiSide::Sell, size) {
                if bid > 0.0 {
                    bids.push((bid / size, size));
                }
            }
            if let Some(ask) = self.quote_out(pool, state, ApiSide::Buy, size) {
                if ask > 0.0 {
                    asks.push((ask / size, size));
                }
            }
            size *= 2.5;
        }
        (bids, asks)
    }

    fn apply_swap(
        &self,
        pool: &PoolConfig,
        state: &PoolState,
        side: ApiSide,
        in_raw: U256,
        out_raw: U256,
    ) -> Option<(U256, U256)> {
        let (reserve_in, reserve_out) = pool.in_out_reserves(state, side == ApiSide::Buy)?;
        let new_in = reserve_in.checked_add(in_raw)?;
        let new_out = reserve_out.checked_sub(out_raw)?;
        let (r_base, r_quote) = if side == ApiSide::Buy {
            (new_out, new_in)
        } else {
            (new_in, new_out)
        };
        let base_first = pool.token0_is_base();
        Some(if base_first {
            (r_base, r_quote)
        } else {
            (r_quote, r_base)
        })
    }

    fn encode_swap(
        &self,
        pool: &PoolConfig,
        side: ApiSide,
        amount_in_raw: U256,
        amount_out_min_raw: U256,
        recipient: Address,
        deadline: u64,
    ) -> Vec<u8> {
        encode_v2_router_swap(
            pool,
            side,
            amount_in_raw,
            amount_out_min_raw,
            recipient,
            deadline,
        )
    }

    fn max_allowance(&self) -> U256 {
        self.max_allowance_value
    }
}

/// 由 pool 配置直接构建 router swap calldata（V2 的 path 就是 [in_token, out_token]）。
pub fn encode_v2_router_swap(
    pool: &PoolConfig,
    side: ApiSide,
    amount_in_raw: U256,
    amount_out_min_raw: U256,
    recipient: Address,
    deadline: u64,
) -> Vec<u8> {
    let (in_token, out_token) = if side == ApiSide::Buy {
        (pool.quote_token, pool.base_token)
    } else {
        (pool.base_token, pool.quote_token)
    };
    let call = IUniswapV2Router02::swapExactTokensForTokensCall {
        amountIn: amount_in_raw,
        amountOutMin: amount_out_min_raw,
        path: vec![in_token, out_token],
        to: recipient,
        deadline: U256::from(deadline),
    };
    call.abi_encode()
}

/// V2 getReserves 返回值解码（Sync 事件与 eth_call 共用）。
pub fn decode_reserves(data: &[u8]) -> Option<(U256, U256)> {
    let ret = IUniswapV2Pair::getReservesCall::abi_decode_returns(data).ok()?;
    Some((U256::from(ret.reserve0), U256::from(ret.reserve1)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evm::types::PoolState;

    fn pool() -> PoolConfig {
        PoolConfig {
            symbol: "WETH/USDC".to_string(),
            pair_address: Address::repeat_byte(0x03),
            base_token: Address::repeat_byte(0x01),
            quote_token: Address::repeat_byte(0x02),
            base_decimals: 18,
            quote_decimals: 6,
        }
    }

    fn state(r_base: u128, r_quote: u128) -> PoolState {
        PoolState {
            confirmed: Some((U256::from(r_base), U256::from(r_quote))),
            prechain: None,
            last_update_ns: 0,
        }
    }

    #[test]
    fn amount_out_matches_constant_product() {
        // 1 ETH in, 3000 ETH reserve in, 9_000_000 USDC reserve out
        let out = get_amount_out(10u128.pow(18), 3000 * 10u128.pow(18), 9_000_000_000_000);
        // 期望约 2991 USDC（扣 0.3% 后按常数乘积）
        let expected = (10u128.pow(18) * 997 * 9_000_000_000_000)
            / (3000 * 10u128.pow(18) * 1000 + 10u128.pow(18) * 997);
        assert_eq!(out, expected);
        assert!(out > 2_990_000_000 && out < 2_991_500_000);
    }

    #[test]
    fn amount_in_round_trips_amount_out() {
        let (ri, ro) = (3_000_000_000_000u128, 9_000_000_000_000u128);
        let out = get_amount_out(10u128.pow(18), ri, ro);
        let back = get_amount_in(out, ri, ro);
        let out2 = get_amount_out(back, ri, ro);
        assert!(out2 >= out && out2 - out <= 2, "round trip drift too large");
    }

    #[test]
    fn quote_out_sell_base() {
        let adapter = UniswapV2Adapter::new();
        let out = adapter
            .quote_out(
                &pool(),
                &state(10u128.pow(21), 3_000_000_000_000),
                ApiSide::Sell,
                0.1,
            )
            .unwrap();
        // 0.1 ETH ≈ 299 USDC
        assert!(out > 298.0 && out < 300.0);
    }

    #[test]
    fn quote_out_buy_base_inverts() {
        let adapter = UniswapV2Adapter::new();
        // 买 0.1 ETH 需要的 USDC，与卖 0.1 ETH 得到的 USDC 接近（方向差仅手续费非线性）
        let sell = adapter
            .quote_out(
                &pool(),
                &state(10u128.pow(21), 3_000_000_000_000),
                ApiSide::Sell,
                0.1,
            )
            .unwrap();
        let buy = adapter
            .quote_out(
                &pool(),
                &state(10u128.pow(21), 3_000_000_000_000),
                ApiSide::Buy,
                0.1,
            )
            .unwrap();
        assert!((sell - buy).abs() / sell < 0.02);
    }

    #[test]
    fn synthetic_book_is_monotone() {
        let adapter = UniswapV2Adapter::new();
        let (bids, asks) =
            adapter.synthetic_book(&pool(), &state(10u128.pow(21), 3_000_000_000_000), 5);
        assert_eq!(bids.len(), 5);
        assert_eq!(asks.len(), 5);
        // 卖得越多价格越差：bids 单调递减；买得越多价格越贵：asks 单调递增。
        for w in bids.windows(2) {
            assert!(w[0].0 >= w[1].0);
        }
        for w in asks.windows(2) {
            assert!(w[0].0 <= w[1].0);
        }
        // 两侧围绕中间价对称展开。
        assert!(bids[0].0 < asks[0].0);
    }

    #[test]
    fn apply_swap_moves_reserves_in_the_right_direction() {
        let adapter = UniswapV2Adapter::new();
        let cfg = pool();
        let st = state(10u128.pow(21), 3_000_000_000_000);
        // Sell: in=base out=quote
        let (r0, r1) = adapter
            .apply_swap(
                &cfg,
                &st,
                ApiSide::Sell,
                U256::from(10u128.pow(17)),
                U256::from(299_000_000u64),
            )
            .unwrap();
        assert!(r0 > U256::from(10u128.pow(21)));
        assert!(r1 < U256::from(3_000_000_000_000u64));
        // Buy: in=quote out=base
        let (r0, r1) = adapter
            .apply_swap(
                &cfg,
                &st,
                ApiSide::Buy,
                U256::from(300_000_000u64),
                U256::from(10u128.pow(17)),
            )
            .unwrap();
        assert!(r0 < U256::from(10u128.pow(21)));
        assert!(r1 > U256::from(3_000_000_000_000u64));
    }

    #[test]
    fn encode_swap_has_router_selector_and_path() {
        let data = encode_v2_router_swap(
            &pool(),
            ApiSide::Buy,
            U256::from(1_000_000u64),
            U256::from(1u64),
            Address::repeat_byte(0x09),
            12345,
        );
        // swapExactTokensForTokens selector = keccak("swapExactTokensForTokens(uint256,uint256,address[],address,uint256)")[..4]
        assert_eq!(&data[..4], &[0x38, 0xed, 0x17, 0x39]);
        assert!(data.len() > 4 + 5 * 32);
    }
}
