//! Arbitrum sequencer feed 低延迟行情后端（预链视图）。
//!
//! 订阅 `feed_url` 的 L2 消息流，在交易**执行前**解码目标池/路由器的 swap，
//! 用常数乘积公式推演新的 reserve 并写入 `prechain` 视图；落块的 `Sync` 事件
//! 随后覆盖 confirmed 并收敛预链估计（见 `PoolState::confirm`）。
//!
//! 支持两类 calldata：
//! - Pair 直连 `swap(uint256,uint256,address,bytes)`（0x022c0d9f）：从参数直接
//!   读输出量，输入量反解；
//! - Router `swapExactTokensForTokens(...)`（0x38ed1739）：读 amountIn 与 path
//!   首地址判方向，输出量本地推演。

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use arb_sequencer_consensus::transactions::ArbTxEnvelope;
use futures_util::StreamExt;
use tracing::{debug, warn};

use crate::api::ApiSide;
use crate::connector::PublishSender;
use crate::evm::config::EvmConfig;
use crate::evm::dex::DexAdapter;
use crate::evm::dex::uniswap_v2::UniswapV2Adapter;
use crate::evm::dex::uniswap_v2::get_amount_out;
use crate::evm::market::MarketPublisher;
use crate::evm::types::PoolState;

/// `swap(uint256,uint256,address,bytes)` 的 selector。
const PAIR_SWAP_SELECTOR: [u8; 4] = [0x02, 0x2c, 0x0d, 0x9f];
/// `swapExactTokensForTokens(uint256,uint256,address[],address,uint256)` 的 selector。
const ROUTER_SWAP_SELECTOR: [u8; 4] = [0x38, 0xed, 0x17, 0x39];

pub struct LowLatencyFeedBackend {
    pub config: Arc<EvmConfig>,
    pub market_state: crate::evm::types::SharedMarketState,
    pub adapter: Arc<dyn DexAdapter>,
}

impl LowLatencyFeedBackend {
    pub fn new(
        config: Arc<EvmConfig>,
        market_state: crate::evm::types::SharedMarketState,
        adapter: Arc<dyn DexAdapter>,
    ) -> Self {
        Self {
            config,
            market_state,
            adapter,
        }
    }

    /// 长期运行（调用方 spawn）。`SequencerReader` 自带重连；这里兜底整体重建。
    pub async fn run(&self, ev_tx: PublishSender) {
        let Some(feed_url) = self.config.feed_url.clone() else {
            return;
        };
        let Some(chain_id) = self.config.chain_id else {
            warn!("feed_url configured but chain_id missing; low-latency feed disabled");
            return;
        };
        let publisher = Arc::new(MarketPublisher::new(
            self.config.clone(),
            ev_tx,
            self.market_state.clone(),
            self.adapter.clone(),
        ));
        loop {
            // SequencerReader::new 连接失败会 panic（其内部实现），spawn 内 panic 只
            // 终止本任务，因此重连交给外层循环；用独立任务隔离。
            let publisher_for_task = publisher.clone();
            let url = feed_url.clone();
            let handle = tokio::spawn(async move {
                let reader = sequencer_client::SequencerReader::new(&url, chain_id, 1).await;
                let mut stream = reader.into_stream();
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(message) => {
                            handle_txs(
                                &message.txs,
                                &publisher_for_task,
                                &monitored_addresses(&publisher_for_task.config),
                                &UniswapV2Adapter::new(),
                            );
                        }
                        Err(error) => debug!(?error, "sequencer feed message error"),
                    }
                }
            });
            let _ = handle.await;
            warn!("sequencer feed stream ended; reconnecting");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

fn monitored_addresses(config: &EvmConfig) -> HashSet<Address> {
    let mut set: HashSet<Address> = config.pair_addresses().into_iter().collect();
    set.insert(config.router_address);
    set
}

/// 预链推演一批 L2 交易。
pub fn handle_txs(
    txs: &[ArbTxEnvelope],
    publisher: &MarketPublisher,
    monitored: &HashSet<Address>,
    adapter: &UniswapV2Adapter,
) {
    for tx in txs {
        let Some((to, input)) = tx_target_and_input(tx) else {
            continue;
        };
        if !monitored.contains(&to) {
            continue;
        }
        let pool = publisher.config.pools.iter().find(|p| {
            p.pair_address == to
                || to == publisher.config.router_address && {
                    // router 级交易需要 path 指向本池的 base/quote。
                    let path = router_swap_path(input);
                    path.as_slice() == [p.quote_token, p.base_token]
                        || path.as_slice() == [p.base_token, p.quote_token]
                }
        });
        let Some(pool) = pool.cloned() else {
            continue;
        };
        apply_prechain_swap(publisher, adapter, &pool, &to, input);
    }
}

/// 提取交易目标地址与 calldata（不支持的信封类型返回 None）。
fn tx_target_and_input(tx: &ArbTxEnvelope) -> Option<(Address, &[u8])> {
    // alloy 2.x 的各 typed tx 通过字段暴露目标与 calldata（to: TxKind / input: Bytes）。
    match tx {
        ArbTxEnvelope::Legacy(signed) => {
            let t = signed.tx();
            Some((*t.to.to()?, &t.input))
        }
        ArbTxEnvelope::Eip2930(signed) => {
            let t = signed.tx();
            Some((*t.to.to()?, &t.input))
        }
        ArbTxEnvelope::Eip1559(signed) => {
            let t = signed.tx();
            Some((*t.to.to()?, &t.input))
        }
        ArbTxEnvelope::Eip7702(signed) => {
            let t = signed.tx();
            Some((t.to, &t.input))
        }
        _ => None,
    }
}

/// 解析 router swap calldata 里的 path（方向判定用）。
fn router_swap_path(input: &[u8]) -> Vec<Address> {
    if input.len() < 4 + 5 * 32 {
        return Vec::new();
    }
    let path_offset = U256::from_be_bytes::<32>(
        input[4 + 2 * 32..4 + 3 * 32]
            .try_into()
            .expect("slice length checked"),
    );
    // ABI 动态区偏移相对参数区（selector 之后）计算。
    let Ok(args_offset) = usize::try_from(path_offset) else {
        return Vec::new();
    };
    let offset = 4 + args_offset;
    if offset + 32 > input.len() {
        return Vec::new();
    }
    let Ok(len) = usize::try_from(U256::from_be_bytes::<32>(
        input[offset..offset + 32]
            .try_into()
            .expect("slice length checked"),
    )) else {
        return Vec::new();
    };
    let mut path = Vec::with_capacity(len.min(4));
    for i in 0..len.min(4) {
        let start = offset + 32 + i * 32;
        if start + 32 > input.len() {
            break;
        }
        path.push(Address::from_word(
            input[start..start + 32]
                .try_into()
                .expect("slice length checked"),
        ));
    }
    path
}

/// 用当前 effective reserve 推演一笔 swap 并写入 prechain 视图。
fn apply_prechain_swap(
    publisher: &MarketPublisher,
    adapter: &UniswapV2Adapter,
    pool: &crate::evm::types::PoolConfig,
    to: &Address,
    input: &[u8],
) {
    let reserves = {
        let market = publisher.market_state.lock().unwrap();
        market
            .pools
            .get(&pool.pair_address)
            .and_then(|s| s.effective())
    };
    let Some((r0, r1)) = reserves else {
        return;
    };
    let state = PoolState {
        confirmed: Some((r0, r1)),
        prechain: None,
        last_update_ns: 0,
    };
    let (side, in_raw, out_raw) = if *to == pool.pair_address {
        // Pair 直连 swap：输出量在参数里，输入量反解。
        if input.len() < 4 + 2 * 32 || input[..4] != PAIR_SWAP_SELECTOR {
            return;
        }
        let amount0_out =
            U256::from_be_bytes::<32>(input[4..36].try_into().expect("slice length checked"));
        let amount1_out =
            U256::from_be_bytes::<32>(input[36..68].try_into().expect("slice length checked"));
        let base_first = pool.token0_is_base();
        let (out_raw, side) = if (base_first && amount1_out > U256::ZERO)
            || (!base_first && amount0_out > U256::ZERO)
        {
            // quote 流出 = 卖 base
            (
                if base_first { amount1_out } else { amount0_out },
                ApiSide::Sell,
            )
        } else {
            (
                if base_first { amount0_out } else { amount1_out },
                ApiSide::Buy,
            )
        };
        // 反解输入量：get_amount_in 的镜像（apply_swap 需要 in_raw）。
        let Some((reserve_in, reserve_out)) = pool.in_out_reserves(&state, side == ApiSide::Buy)
        else {
            return;
        };
        let (Ok(ri), Ok(ro)) = (u128::try_from(reserve_in), u128::try_from(reserve_out)) else {
            return;
        };
        let Ok(out_u128) = u128::try_from(out_raw) else {
            return;
        };
        let in_u128 = crate::evm::dex::uniswap_v2::get_amount_in(out_u128, ri, ro);
        if in_u128 == u128::MAX {
            return;
        }
        (side, U256::from(in_u128), out_raw)
    } else {
        // Router swap：amountIn 在头部，方向由 path[0] 决定，输出量本地推演。
        if input.len() < 4 + 32 || input[..4] != ROUTER_SWAP_SELECTOR {
            return;
        }
        let amount_in =
            U256::from_be_bytes::<32>(input[4..36].try_into().expect("slice length checked"));
        let path = router_swap_path(input);
        if path.len() < 2 {
            return;
        }
        let side = if path[0] == pool.base_token {
            ApiSide::Sell
        } else {
            ApiSide::Buy
        };
        let Some((reserve_in, reserve_out)) = pool.in_out_reserves(&state, side == ApiSide::Buy)
        else {
            return;
        };
        let (Ok(ri), Ok(ro)) = (u128::try_from(reserve_in), u128::try_from(reserve_out)) else {
            return;
        };
        let Ok(in_u128) = u128::try_from(amount_in) else {
            return;
        };
        let out = get_amount_out(in_u128, ri, ro);
        (side, amount_in, U256::from(out))
    };
    let Some((r0_new, r1_new)) = adapter.apply_swap(pool, &state, side, in_raw, out_raw) else {
        return;
    };
    debug!(symbol = %pool.symbol, "prechain reserve update applied");
    publisher.apply_prechain(pool, r0_new, r1_new);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evm::types::{MarketState, PoolConfig, PoolState};
    use alloy_primitives::Bytes;

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

    fn config() -> Arc<EvmConfig> {
        let mut market = MarketState::default();
        market.pools.insert(
            pool().pair_address,
            PoolState {
                confirmed: Some((U256::from(10u128.pow(18)), U256::from(3_000_000_000_000u64))),
                prechain: None,
                last_update_ns: 0,
            },
        );
        Arc::new(EvmConfig::test_config(vec![pool()], market))
    }

    /// 用 2.4.1 consensus 签名出完整 legacy 交易，再经 RLP 在 arb 树的
    /// consensus 1.8 实例里解码回 `Signed<TxLegacy>`，保证与 ArbTxEnvelope
    /// 的类型身份一致。
    fn signed_legacy(to: alloy_primitives::Address, input: Vec<u8>) -> ArbTxEnvelope {
        use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
        use alloy_eips::eip2718::Encodable2718;
        use alloy_network::TxSignerSync;
        let mut tx = TxLegacy {
            chain_id: Some(42161),
            nonce: 0,
            gas_price: 100_000_000,
            gas_limit: 300_000,
            to: alloy_primitives::TxKind::Call(to),
            value: U256::ZERO,
            input: Bytes::from(input),
        };
        let signer = alloy_signer_local::PrivateKeySigner::from_slice(&[7u8; 32]).unwrap();
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let envelope: TxEnvelope = tx.into_signed(sig).into();
        let wire = envelope.encoded_2718();
        use alloy_consensus_arb::transaction::RlpEcdsaDecodableTx;
        let decoded = alloy_consensus_arb::TxLegacy::rlp_decode_signed(&mut &wire[..])
            .expect("decode signed legacy tx");
        ArbTxEnvelope::Legacy(decoded)
    }

    fn publisher(config: Arc<EvmConfig>) -> MarketPublisher {
        // 测试通道不消费事件，仅验证状态。
        let sender = crate::connector::direct_publish_sender(move |_| {});
        MarketPublisher::new(
            config.clone(),
            sender,
            config.market_state.clone(),
            Arc::new(UniswapV2Adapter::new()),
        )
    }

    #[test]
    fn pair_direct_swap_updates_prechain() {
        let config = config();
        let publ = publisher(config.clone());
        let monitored = monitored_addresses(&config);
        let adapter = UniswapV2Adapter::new();
        // 卖 0.1 WETH 的 pair swap：amount0Out=0, amount1Out=299_000e6（quote out）。
        let mut calldata = PAIR_SWAP_SELECTOR.to_vec();
        calldata.extend_from_slice(&U256::ZERO.to_be_bytes::<32>()); // amount0Out
        calldata.extend_from_slice(&U256::from(299_000_000u64).to_be_bytes::<32>()); // amount1Out
        let tx = signed_legacy(pool().pair_address, calldata);
        handle_txs(std::slice::from_ref(&tx), &publ, &monitored, &adapter);
        let market = config.market_state.lock().unwrap();
        let state = market.pools.get(&pool().pair_address).unwrap();
        // prechain 被写入且 reserve0（base）增加、reserve1（quote）减少。
        let (r0, r1) = state.effective().unwrap();
        assert!(r0 > U256::from(10u128.pow(18)));
        assert!(r1 < U256::from(3_000_000_000_000u64));
    }

    #[test]
    fn unrelated_tx_is_ignored() {
        let config = config();
        let publ = publisher(config.clone());
        let monitored = monitored_addresses(&config);
        let adapter = UniswapV2Adapter::new();
        let tx = signed_legacy(Address::repeat_byte(0xff), Vec::new());
        handle_txs(std::slice::from_ref(&tx), &publ, &monitored, &adapter);
        let market = config.market_state.lock().unwrap();
        let state = market.pools.get(&pool().pair_address).unwrap();
        assert!(state.prechain.is_none());
    }

    #[test]
    fn router_swap_selector_and_path_shape() {
        // path 解码用的偏移逻辑与 encode_v2_router_swap 的 abi 编码一致。
        let data = crate::evm::dex::uniswap_v2::encode_v2_router_swap(
            &pool(),
            ApiSide::Buy,
            U256::from(1_000_000u64),
            U256::from(1u64),
            Address::repeat_byte(0x09),
            12345,
        );
        let path = router_swap_path(&data);
        assert_eq!(path, vec![pool().quote_token, pool().base_token]);
    }
}
