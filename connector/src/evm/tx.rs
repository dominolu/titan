//! 交易构造与签名：EIP-1559 swap 交易 + nonce 管理 + 授权保障 + 收据确认。
//!
//! EVM venue 的"私有流"由确认循环承担：`submit` 广播交易后 spawn 轮询任务，
//! 落块（成功/回滚）或超时（dropped）后回填订单状态机并发布
//! `AccountPublication::Order`，等价于 CEX 的 orderUpdates 通道。
//!
//! AMM 语义映射：没有挂单，`Market` 与 `IOC/FOK` 限价单映射为带滑点/限价保护的
//! swap（预期价差超出保护线时本地拒绝，不消耗 gas）；`GTC/GTX` 与条件单不支持。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxHash, TxKind, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::TransactionReceipt;
use alloy_signer_local::PrivateKeySigner;
use chrono::Utc;
use hftbacktest::types::{
    ErrorKind, LiveError, OrdType, Order as HbOrder, Side, Status, TimeInForce, Value,
};

use crate::api::{
    ApiError, ApiOrderStatus, ApiOrderType, ApiSide, ApiTimeInForce, OrderInfo, UnifiedOrderRequest,
};
use crate::connector::{AccountPublication, PublishSender};
use crate::evm::config::EvmConfig;
use crate::evm::dex::DexAdapter;
use crate::evm::dex::uniswap_v2::UniswapV2Adapter;
#[cfg(test)]
use crate::evm::ordermanager::OrderManager;
use crate::evm::ordermanager::SharedOrderManager;
use crate::evm::provider::IERC20Metadata;
use crate::evm::provider::{EvmProvider, TRANSFER_EVENT_SIGNATURE};
use crate::evm::types::{PoolConfig, PoolState};
use alloy_sol_types::{SolCall, SolValue};

pub const EXCHANGE_NAME: &str = "evm";

/// 市价单输入量的缓冲系数：quote 反解存在手续费非线性，留 1% 上浮避免 revert。
const INPUT_BUFFER: f64 = 1.01;

/// 一个已解析到具体池的发单意图。
#[derive(Debug, Clone)]
pub struct SwapIntent {
    pub pool: PoolConfig,
    pub side: ApiSide,
    pub base_qty: f64,
    pub client_order_id: String,
    /// 限价（quote/base）；市价单为 None。
    pub limit_price: Option<f64>,
}

/// 广播前定型的交易参数（最小单位）。
#[derive(Debug, Clone)]
pub struct SwapPlan {
    pub input_token: Address,
    pub amount_in_raw: U256,
    pub amount_out_min_raw: U256,
    pub output_token: Address,
}

pub struct TxEngine {
    pub config: Arc<EvmConfig>,
    pub provider: EvmProvider,
    pub signer: Option<PrivateKeySigner>,
    pub order_manager: SharedOrderManager,
    adapter: UniswapV2Adapter,
    /// 本地维护的 pending nonce：首次从 RPC 取，之后本地自增，避免并发读取竞态。
    nonce: Arc<Mutex<Option<u64>>>,
    account_tx: Arc<Mutex<Option<PublishSender>>>,
}

impl TxEngine {
    pub fn new(
        config: Arc<EvmConfig>,
        provider: EvmProvider,
        signer: Option<PrivateKeySigner>,
        order_manager: SharedOrderManager,
    ) -> Self {
        Self {
            config,
            provider,
            signer,
            order_manager,
            adapter: UniswapV2Adapter::new(),
            nonce: Arc::new(Mutex::new(None)),
            account_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub fn set_account_publisher(&self, sender: PublishSender) {
        *self.account_tx.lock().unwrap() = Some(sender);
    }

    fn require_signer(&self) -> Result<&PrivateKeySigner, ApiError> {
        self.signer.as_ref().ok_or_else(|| {
            ApiError::new(EXCHANGE_NAME, "NO_SIGNER", "private_key is not configured")
        })
    }

    fn wallet(&self) -> Result<Address, ApiError> {
        Ok(self.require_signer()?.address())
    }

    fn effective_state(&self, pool: &PoolConfig) -> Result<PoolState, ApiError> {
        let reserves = {
            let market = self.config.market_state.lock().unwrap();
            market
                .pools
                .get(&pool.pair_address)
                .and_then(|state| state.effective())
        };
        reserves
            .map(|(r0, r1)| PoolState {
                confirmed: Some((r0, r1)),
                prechain: None,
                last_update_ns: 0,
            })
            .ok_or_else(|| ApiError::new(EXCHANGE_NAME, "NO_STATE", "pool reserves not loaded yet"))
    }

    /// 把统一订单请求解析为 swap 意图。
    pub fn resolve_intent(&self, req: &UnifiedOrderRequest) -> Result<SwapIntent, ApiError> {
        let pool = self
            .config
            .pool(&req.symbol)
            .ok_or_else(|| {
                ApiError::new(
                    EXCHANGE_NAME,
                    "SYMBOL",
                    format!("unknown pool {}", req.symbol),
                )
            })?
            .clone();
        match req.order_type {
            ApiOrderType::StopMarket
            | ApiOrderType::StopLimit
            | ApiOrderType::TakeProfitMarket
            | ApiOrderType::TakeProfitLimit
            | ApiOrderType::TrailingStopMarket => {
                return Err(ApiError::new(
                    EXCHANGE_NAME,
                    "UNSUPPORTED",
                    "conditional orders are not supported on AMM venues",
                ));
            }
            ApiOrderType::Unknown => {
                return Err(ApiError::new(
                    EXCHANGE_NAME,
                    "INVALID_ARG",
                    "unknown order type",
                ));
            }
            _ => {}
        }
        // GTC/GTX 需要撮合队列；IOC/FOK 等价于带价格保护的即时 swap。
        if matches!(req.time_in_force, ApiTimeInForce::GTC | ApiTimeInForce::GTX)
            && req.order_type == ApiOrderType::Limit
        {
            return Err(ApiError::new(
                EXCHANGE_NAME,
                "UNSUPPORTED",
                "resting limit orders are not supported on AMM venues; use IOC/FOK",
            ));
        }
        if req.order_type == ApiOrderType::Limit && req.price.is_none() {
            return Err(ApiError::new(
                EXCHANGE_NAME,
                "INVALID_ARG",
                "limit orders require a price for slippage protection",
            ));
        }
        if req.side == ApiSide::Unknown {
            return Err(ApiError::new(
                EXCHANGE_NAME,
                "INVALID_ARG",
                "side is required",
            ));
        }
        if !(req.qty.is_finite() && req.qty > 0.0) {
            return Err(ApiError::new(
                EXCHANGE_NAME,
                "INVALID_ARG",
                "qty must be positive",
            ));
        }
        Ok(SwapIntent {
            pool,
            side: req.side,
            base_qty: req.qty,
            client_order_id: req.client_order_id.clone().unwrap_or_default(),
            limit_price: req.price,
        })
    }

    /// 由当前 reserve 视图定型交易参数。预期成交劣于保护线时直接拒绝（省 gas）。
    pub fn plan_swap(&self, intent: &SwapIntent) -> Result<SwapPlan, ApiError> {
        let state = self.effective_state(&intent.pool)?;
        let slippage = self.config.slippage_bps as f64 / 10_000.0;
        let scale = |value: f64, decimals: u32| -> Result<U256, ApiError> {
            let raw = value * 10f64.powi(decimals as i32);
            if !raw.is_finite() || raw <= 0.0 || raw >= 1.2e38 {
                return Err(ApiError::new(
                    EXCHANGE_NAME,
                    "INVALID_ARG",
                    "amount out of representable range",
                ));
            }
            Ok(U256::from(raw as u128))
        };
        match intent.side {
            ApiSide::Sell => {
                let expected_quote = self
                    .adapter
                    .quote_out(&intent.pool, &state, ApiSide::Sell, intent.base_qty)
                    .ok_or_else(|| {
                        ApiError::new(EXCHANGE_NAME, "INVALID_ARG", "qty out of range for pool")
                    })?;
                let min_out_quote = intent
                    .limit_price
                    .map(|price| price * intent.base_qty)
                    .unwrap_or_else(|| expected_quote * (1.0 - slippage));
                if expected_quote < min_out_quote {
                    return Err(ApiError::new(
                        EXCHANGE_NAME,
                        "PRICE_PROTECTION",
                        format!(
                            "expected out {expected_quote:.6} below protection {}",
                            min_out_quote
                        ),
                    ));
                }
                Ok(SwapPlan {
                    input_token: intent.pool.base_token,
                    amount_in_raw: scale(intent.base_qty, intent.pool.base_decimals)?,
                    amount_out_min_raw: scale(min_out_quote, intent.pool.quote_decimals)?,
                    output_token: intent.pool.quote_token,
                })
            }
            ApiSide::Buy => {
                let required_quote = self
                    .adapter
                    .quote_out(&intent.pool, &state, ApiSide::Buy, intent.base_qty)
                    .ok_or_else(|| {
                        ApiError::new(EXCHANGE_NAME, "INVALID_ARG", "qty out of range for pool")
                    })?;
                let max_in_quote = intent
                    .limit_price
                    .map(|price| price * intent.base_qty)
                    .unwrap_or_else(|| required_quote * (1.0 + slippage));
                if required_quote > max_in_quote {
                    return Err(ApiError::new(
                        EXCHANGE_NAME,
                        "PRICE_PROTECTION",
                        format!("required in {required_quote:.6} above cap {max_in_quote:.6}"),
                    ));
                }
                // 输入上限取保护线，输出下限按滑点收缩（router 语义保证双向保护）。
                let min_base = intent.base_qty * (1.0 - slippage);
                Ok(SwapPlan {
                    input_token: intent.pool.quote_token,
                    amount_in_raw: scale(max_in_quote * INPUT_BUFFER, intent.pool.quote_decimals)?,
                    amount_out_min_raw: scale(min_base, intent.pool.base_decimals)?,
                    output_token: intent.pool.base_token,
                })
            }
            ApiSide::Unknown => Err(ApiError::new(EXCHANGE_NAME, "INVALID_ARG", "side")),
        }
    }

    async fn next_nonce(&self) -> Result<u64, ApiError> {
        // 先在锁外完成 RPC（std MutexGuard 不可跨越 await），锁内只做单调递增。
        let cached = *self.nonce.lock().unwrap();
        let fetched = match cached {
            Some(v) => Some(v + 1),
            None => Some(
                self.provider
                    .pending_nonce(self.wallet()?)
                    .await
                    .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))?,
            ),
        };
        let mut nonce = self.nonce.lock().unwrap();
        let mut value = fetched.unwrap_or(0);
        if let Some(prev) = *nonce {
            if value <= prev {
                value = prev + 1;
            }
        }
        *nonce = Some(value);
        Ok(value)
    }

    /// 广播前保障 router 对输入代币的授权额度；不足时先发一笔 approve 并等它落块。
    async fn ensure_allowance(&self, token: Address, amount: U256) -> Result<(), ApiError> {
        let wallet = self.wallet()?;
        let allowance = self
            .provider
            .token_allowance(token, wallet, self.config.router_address)
            .await
            .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))?;
        if allowance >= amount {
            return Ok(());
        }
        tracing::warn!(%token, "router allowance insufficient; submitting approve first");
        let approve = IERC20Metadata::approveCall {
            spender: self.config.router_address,
            amount: self.adapter.max_allowance(),
        }
        .abi_encode();
        let tx_hash = self
            .send_eip1559(token, approve, U256::ZERO, self.config.approve_gas_limit)
            .await?;
        let receipt = self
            .provider
            .poll_receipts()
            .wait(tx_hash, Duration::from_secs(90), Duration::from_millis(300))
            .await
            .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))?;
        match receipt {
            Some(r) if r.status() => Ok(()),
            Some(_) => Err(ApiError::new(
                EXCHANGE_NAME,
                "APPROVE_REVERTED",
                token.to_string(),
            )),
            None => Err(ApiError::new(
                EXCHANGE_NAME,
                "APPROVE_TIMEOUT",
                token.to_string(),
            )),
        }
    }

    async fn send_eip1559(
        &self,
        to: Address,
        calldata: Vec<u8>,
        value: U256,
        gas_limit: u64,
    ) -> Result<TxHash, ApiError> {
        let signer = self.require_signer()?;
        let chain_id = self
            .provider
            .chain_id()
            .await
            .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))?;
        let gas_price = self
            .provider
            .gas_price()
            .await
            .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))?;
        // L2（Arbitrum）gas 价低且稳定；封顶交给配置，下限 1 wei 防零价。
        let max_fee = ((gas_price as f64)
            .min(self.config.max_gas_price_gwei * 1_000_000_000.0)
            .max(1.0)) as u128;
        let mut tx = TxEip1559 {
            chain_id,
            nonce: self.next_nonce().await?,
            gas_limit,
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(to),
            value,
            access_list: Default::default(),
            input: Bytes::from(calldata),
        };
        let signature = signer
            .sign_transaction_sync(&mut tx)
            .map_err(|e| ApiError::new(EXCHANGE_NAME, "SIGNING", e.to_string()))?;
        let envelope: TxEnvelope = tx.into_signed(signature).into();
        let pending = self
            .provider
            .rpc()
            .send_raw_transaction(&envelope.encoded_2718())
            .await
            .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))?;
        Ok(*pending.tx_hash())
    }

    /// 统一入口：注册状态机 -> 保障授权 -> 编码/签名/广播 -> 启动确认监视。
    pub async fn submit(
        &self,
        req: &UnifiedOrderRequest,
        local_order_id: u64,
    ) -> Result<OrderInfo, ApiError> {
        let intent = self.resolve_intent(req)?;
        let plan = self.plan_swap(&intent)?;
        let wallet = self.wallet()?;
        let now_ms = Utc::now().timestamp_millis();

        {
            // AccountRuntime 发单前已 track；此处兜底注册（重复注册被拒绝，无副作用）。
            let mut mgr = self.order_manager.lock().unwrap();
            mgr.track_managed_order(&req.symbol, &intent.client_order_id, {
                let mut hb_order = HbOrder::new(
                    local_order_id,
                    0,
                    1.0,
                    req.qty,
                    if intent.side == ApiSide::Buy {
                        Side::Buy
                    } else {
                        Side::Sell
                    },
                    OrdType::Market,
                    TimeInForce::IOC,
                );
                hb_order.status = Status::New;
                hb_order
            });
        }

        self.ensure_allowance(plan.input_token, plan.amount_in_raw)
            .await?;

        let deadline = (Utc::now().timestamp() as u64).saturating_add(self.config.deadline_secs);
        let calldata = self.adapter.encode_swap(
            &intent.pool,
            intent.side,
            plan.amount_in_raw,
            plan.amount_out_min_raw,
            wallet,
            deadline,
        );
        let tx_hash = self
            .send_eip1559(
                self.config.router_address,
                calldata,
                U256::ZERO,
                self.config.swap_gas_limit,
            )
            .await?;
        let tx_hash_str = format!("{tx_hash:#x}");
        {
            let mut mgr = self.order_manager.lock().unwrap();
            mgr.attach_tx(&intent.client_order_id, &tx_hash_str);
        }

        let info = OrderInfo {
            symbol: req.symbol.clone(),
            order_id: tx_hash_str.clone(),
            client_order_id: intent.client_order_id.clone(),
            side: req.side,
            order_type: req.order_type,
            status: ApiOrderStatus::New,
            price: req.price.unwrap_or(0.0),
            qty: req.qty,
            executed_qty: 0.0,
            avg_price: 0.0,
            leaves_qty: req.qty,
            time_in_force: req.time_in_force,
            reduce_only: req.reduce_only,
            position_side: req
                .position_side
                .unwrap_or(crate::api::ApiPositionSide::Net),
            create_time: now_ms,
            update_time: now_ms,
            stop_price: req.stop_price,
        };

        let watcher = SwapWatcher {
            config: self.config.clone(),
            provider: self.provider.clone(),
            order_manager: self.order_manager.clone(),
            account_tx: Arc::clone(&self.account_tx),
            wallet,
            pool: intent.pool.clone(),
            side: intent.side,
            qty: req.qty,
            client_order_id: intent.client_order_id.clone(),
        };
        tokio::spawn(async move {
            watcher.watch(tx_hash).await;
        });
        Ok(info)
    }
}

/// 单笔 swap 的收据确认任务（私有流等价物）。
struct SwapWatcher {
    config: Arc<EvmConfig>,
    provider: EvmProvider,
    order_manager: SharedOrderManager,
    account_tx: Arc<Mutex<Option<PublishSender>>>,
    wallet: Address,
    pool: PoolConfig,
    side: ApiSide,
    qty: f64,
    client_order_id: String,
}

impl SwapWatcher {
    fn publish_error(&self, client_order_id: &str, message: String) {
        if let Some(sender) = self.account_tx.lock().unwrap().as_ref() {
            let _ = sender.send_account(AccountPublication::Error(LiveError::with(
                ErrorKind::OrderError,
                Value::Map({
                    let mut map = std::collections::HashMap::new();
                    map.insert(
                        "client_order_id".to_string(),
                        Value::String(client_order_id.to_string()),
                    );
                    map.insert("msg".to_string(), Value::String(message));
                    map
                }),
            )));
        }
    }

    fn publish_account(&self, publication: AccountPublication) {
        if let Some(sender) = self.account_tx.lock().unwrap().as_ref() {
            let _ = sender.send_account(publication);
        }
    }

    /// 从收据日志解析实际成交：
    /// - 输入：`Transfer(wallet -> *, input_token)` 的数额；
    /// - 输出：`Transfer(* -> wallet, output_token)` 的数额。
    /// 返回 (输入原始值, 输出原始值)。
    fn parse_fill(&self, receipt: &TransactionReceipt) -> (U256, U256) {
        let (input_token, output_token) = if self.side == ApiSide::Buy {
            (self.pool.quote_token, self.pool.base_token)
        } else {
            (self.pool.base_token, self.pool.quote_token)
        };
        let mut amount_in = U256::ZERO;
        let mut amount_out = U256::ZERO;
        for log in receipt.inner.logs() {
            if log.topics().first() != Some(&TRANSFER_EVENT_SIGNATURE) || log.topics().len() < 3 {
                continue;
            }
            let token = log.address();
            let from = Address::from_word(log.topics()[1]);
            let to = Address::from_word(log.topics()[2]);
            let Ok(value) = U256::abi_decode(log.data().data.as_ref()) else {
                continue;
            };
            if token == input_token && from == self.wallet {
                amount_in = value;
            }
            if token == output_token && to == self.wallet {
                amount_out = value;
            }
        }
        (amount_in, amount_out)
    }

    async fn watch(self, tx_hash: TxHash) {
        let receipt = self
            .provider
            .poll_receipts()
            .wait(
                tx_hash,
                Duration::from_millis(self.config.tx_timeout_ms),
                Duration::from_millis(300),
            )
            .await;
        let (status, exec_qty, avg_price) = match receipt {
            Ok(Some(r)) if r.status() => {
                let (amount_in, amount_out) = self.parse_fill(&r);
                let (in_dec, out_dec) = if self.side == ApiSide::Buy {
                    (self.pool.quote_decimals, self.pool.base_decimals)
                } else {
                    (self.pool.base_decimals, self.pool.quote_decimals)
                };
                let in_human =
                    crate::evm::types::u256_to_f64(amount_in) / 10f64.powi(in_dec as i32);
                let out_human =
                    crate::evm::types::u256_to_f64(amount_out) / 10f64.powi(out_dec as i32);
                let avg = if in_human > 0.0 && out_human > 0.0 {
                    in_human / out_human
                } else {
                    0.0
                };
                (Status::Filled, self.qty, avg)
            }
            Ok(Some(_)) => (Status::Rejected, 0.0, 0.0),
            Ok(None) => (Status::Canceled, 0.0, 0.0),
            Err(error) => {
                tracing::error!(?error, %tx_hash, "receipt polling failed");
                (Status::Canceled, 0.0, 0.0)
            }
        };
        let finalized = self.order_manager.lock().unwrap().finalize(
            &self.client_order_id,
            status,
            exec_qty,
            avg_price,
        );
        match finalized {
            Some(order) => self.publish_account(AccountPublication::Order {
                symbol: self.pool.symbol.clone(),
                client_order_id: Some(self.client_order_id.clone()),
                venue_order_id: Some(format!("{tx_hash:#x}")),
                order,
            }),
            None => self.publish_error(
                &self.client_order_id,
                format!("swap confirmation lost: {tx_hash:#x}"),
            ),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evm::types::{MarketState, PoolConfig};
    use std::sync::Arc;

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

    fn engine_with_state(r_base: u128, r_quote: u64) -> TxEngine {
        let mut market = MarketState::default();
        market.pools.insert(
            pool().pair_address,
            crate::evm::types::PoolState {
                confirmed: Some((U256::from(r_base), U256::from(r_quote))),
                prechain: None,
                last_update_ns: 0,
            },
        );
        let config = Arc::new(EvmConfig::test_config(vec![pool()], market));
        TxEngine::new(
            config,
            EvmProvider::connect("http://127.0.0.1:1", "ws://127.0.0.1:1").unwrap(),
            None,
            Arc::new(Mutex::new(OrderManager::new())),
        )
    }

    fn request(
        side: ApiSide,
        order_type: ApiOrderType,
        price: Option<f64>,
        qty: f64,
    ) -> UnifiedOrderRequest {
        UnifiedOrderRequest {
            symbol: "WETH/USDC".to_string(),
            side,
            order_type,
            price,
            qty,
            time_in_force: if order_type == ApiOrderType::Limit {
                ApiTimeInForce::IOC
            } else {
                ApiTimeInForce::GTC
            },
            reduce_only: false,
            position_side: None,
            client_order_id: Some("c1".to_string()),
            stop_price: None,
        }
    }

    #[tokio::test]
    async fn resolve_rejects_resting_and_conditional_orders() {
        let engine = engine_with_state(10u128.pow(21), 3_000_000_000_000);
        assert!(
            engine
                .resolve_intent(&request(
                    ApiSide::Buy,
                    ApiOrderType::Limit,
                    Some(3000.0),
                    1.0
                ))
                .is_ok()
        );
        let mut gtc = request(ApiSide::Buy, ApiOrderType::Limit, Some(3000.0), 1.0);
        gtc.time_in_force = ApiTimeInForce::GTC;
        assert!(engine.resolve_intent(&gtc).is_err());
        let mut stop = request(ApiSide::Buy, ApiOrderType::StopMarket, None, 1.0);
        stop.order_type = ApiOrderType::StopMarket;
        assert!(engine.resolve_intent(&stop).is_err());
        let mut no_price = request(ApiSide::Buy, ApiOrderType::Limit, None, 1.0);
        no_price.order_type = ApiOrderType::Limit;
        assert!(engine.resolve_intent(&no_price).is_err());
    }

    #[tokio::test]
    async fn plan_sell_uses_limit_price_as_floor() {
        let engine = engine_with_state(10u128.pow(21), 3_000_000_000_000);
        // 市价卖：预期 ~2991，滑点 50bps => 下限约 2976
        let plan = engine
            .plan_swap(&SwapIntent {
                pool: pool(),
                side: ApiSide::Sell,
                base_qty: 1.0,
                client_order_id: "c".into(),
                limit_price: None,
            })
            .unwrap();
        let min_out = crate::evm::types::u256_to_f64(plan.amount_out_min_raw) / 1e6;
        assert!(min_out > 2970.0 && min_out < 2991.0);
        // 限价高于市价（卖单要求更高价）：本地拒绝，不烧 gas。
        let rejected = engine.plan_swap(&SwapIntent {
            pool: pool(),
            side: ApiSide::Sell,
            base_qty: 1.0,
            client_order_id: "c".into(),
            limit_price: Some(3500.0),
        });
        assert_eq!(rejected.unwrap_err().code, "PRICE_PROTECTION");
        // 限价低于市价：以限价为下限。
        let aggressive = engine
            .plan_swap(&SwapIntent {
                pool: pool(),
                side: ApiSide::Sell,
                base_qty: 1.0,
                client_order_id: "c".into(),
                limit_price: Some(2800.0),
            })
            .unwrap();
        let min_out = crate::evm::types::u256_to_f64(aggressive.amount_out_min_raw) / 1e6;
        assert!((min_out - 2800.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn plan_buy_caps_input_at_limit_price() {
        let engine = engine_with_state(10u128.pow(21), 3_000_000_000_000);
        // 买价上限过低（市价 ~2991）：本地拒绝。
        let rejected = engine.plan_swap(&SwapIntent {
            pool: pool(),
            side: ApiSide::Buy,
            base_qty: 1.0,
            client_order_id: "c".into(),
            limit_price: Some(2500.0),
        });
        assert_eq!(rejected.unwrap_err().code, "PRICE_PROTECTION");
        // 宽松上限：输入量被 cap 在限价 * (1+缓冲)。
        let plan = engine
            .plan_swap(&SwapIntent {
                pool: pool(),
                side: ApiSide::Buy,
                base_qty: 1.0,
                client_order_id: "c".into(),
                limit_price: Some(4000.0),
            })
            .unwrap();
        let max_in = crate::evm::types::u256_to_f64(plan.amount_in_raw) / 1e6;
        assert!((max_in - 4000.0 * 1.01).abs() < 1.0);
        assert_eq!(plan.input_token, pool().quote_token);
        assert_eq!(plan.output_token, pool().base_token);
    }

    #[test]
    fn submit_requires_signer() {
        let engine = engine_with_state(10u128.pow(21), 3_000_000_000_000);
        let err = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(engine.submit(&request(ApiSide::Buy, ApiOrderType::Market, None, 1.0), 1));
        assert_eq!(err.unwrap_err().code, "NO_SIGNER");
    }
}
