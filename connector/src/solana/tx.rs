//! Solana 交易编码 / 签名 / 广播 / 确认监视。
//!
//! 全部用本仓库自带原语实现（ed25519-dalek 签名、sha2 PDA 推导、base58 编码），
//! 已由 `solana_rest_probe`（主网实盘）与 Raydium CPI 模板验证：
//! - legacy 消息：header(3) + 账户 + blockhash + 指令（compact-u16 长度）；
//! - 签名载荷 = 完整消息字节，签名者必须是消息 key 列表的第一项；
//! - ATA 地址 = find_pda([owner, token_program, mint], ATA 程序)。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use ed25519_dalek::{Signer, SigningKey};
use hftbacktest::types::{
    ErrorKind, LiveError, OrdType, Order as HbOrder, Side, Status, TimeInForce, Value,
};

use crate::api::{
    ApiError, ApiOrderStatus, ApiOrderType, ApiPositionSide, ApiSide, ApiTimeInForce, OrderInfo,
    UnifiedOrderRequest,
};
use crate::connector::{AccountPublication, PublishSender};
use crate::solana::SolanaError;
use crate::solana::config::SolanaConfig;
#[cfg(test)]
use crate::solana::ordermanager::OrderManager;
use crate::solana::ordermanager::SharedOrderManager;
use crate::solana::raydium::swap_base_in_accounts;
use crate::solana::rpc::SolanaRpc;
use crate::solana::types::{
    ATA_PROGRAM, PoolConfig, RAYDIUM_V4_PROGRAM, SYSTEM_PROGRAM, TOKEN_PROGRAM, ata_address,
    b58_decode, b58_encode,
};

pub const EXCHANGE_NAME: &str = "solana";
const COMPUTE_BUDGET_PROGRAM: &str = "ComputeBudget111111111111111111111111111111";

// ---------------------------------------------------------------------
// 基础编码原语
// ---------------------------------------------------------------------

/// Solana compact-u16 编码。
pub fn compact_u16(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

// ---------------------------------------------------------------------
// 消息组装
// ---------------------------------------------------------------------

struct MessageBuilder {
    /// key[0] 必须是签名者（Solana header 规则）。
    keys: Vec<String>,
    key_index: HashMap<String, u8>,
    readonly: Vec<bool>,
    instructions: Vec<(u8, Vec<u8>, Vec<u8>)>,
}

impl MessageBuilder {
    /// `signer` 注册为 key 0（可写），程序账户注册为只读。
    fn new(signer: &str) -> Self {
        let mut builder = Self {
            keys: Vec::new(),
            key_index: HashMap::new(),
            readonly: Vec::new(),
            instructions: Vec::new(),
        };
        builder.push_key(signer, false);
        builder
    }

    fn push_key(&mut self, pubkey: &str, readonly: bool) -> u8 {
        if let Some(&idx) = self.key_index.get(pubkey) {
            // 已有 key 若被标记只读但后续需要可写，升格为可写（保守方向）。
            if !readonly {
                self.readonly[idx as usize] = false;
            }
            return idx;
        }
        let idx = self.keys.len() as u8;
        self.keys.push(pubkey.to_string());
        self.key_index.insert(pubkey.to_string(), idx);
        self.readonly.push(readonly);
        idx
    }

    fn instruction(&mut self, program: &str, accounts: &[&str], data: &[u8]) {
        let program_index = self.push_key(program, true);
        let account_indexes = accounts.iter().map(|a| self.push_key(a, false)).collect();
        self.instructions
            .push((program_index, account_indexes, data.to_vec()));
    }

    /// Solana 消息要求账户按 [可写签名者, 只读签名者, 可写未签名者, 只读未签名者]
    /// 连续分区排列 —— 只读集合是"末尾 N 个 key"的位置语义，因此必须重排：
    /// key0（签名者）→ 可写未签名者 → 只读，并同步重映射指令中的索引。
    fn encode(self, blockhash: &[u8; 32]) -> Vec<u8> {
        let num_signed = 1usize; // 仅 payer 签名
        let mut order: Vec<u8> = Vec::with_capacity(self.keys.len());
        order.push(0); // 签名者
        for (idx, ro) in self.readonly.iter().enumerate().skip(1) {
            if !ro {
                order.push(idx as u8);
            }
        }
        for (idx, ro) in self.readonly.iter().enumerate().skip(1) {
            if *ro {
                order.push(idx as u8);
            }
        }
        let mut remap = vec![0u8; self.keys.len()];
        for (new, old) in order.iter().enumerate() {
            remap[*old as usize] = new as u8;
        }
        let readonly_unsigned = self
            .readonly
            .iter()
            .enumerate()
            .skip(1)
            .filter(|(_, ro)| **ro)
            .count();

        let mut msg = Vec::new();
        msg.push(num_signed as u8);
        msg.push(0u8); // 只读签名账户数
        msg.push(readonly_unsigned as u8);
        compact_u16(self.keys.len(), &mut msg);
        for old in &order {
            msg.extend_from_slice(&b58_decode(&self.keys[*old as usize]));
        }
        msg.extend_from_slice(blockhash);
        compact_u16(self.instructions.len(), &mut msg);
        for (program, accounts, data) in self.instructions {
            msg.push(remap[program as usize]);
            compact_u16(accounts.len(), &mut msg);
            for acc in &accounts {
                msg.push(remap[*acc as usize]);
            }
            compact_u16(data.len(), &mut msg);
            msg.extend_from_slice(&data);
        }
        msg
    }
}

// ---------------------------------------------------------------------
// 交易引擎
// ---------------------------------------------------------------------

/// 一个已解析到具体池的 swap 意图。
#[derive(Debug, Clone)]
pub struct SwapIntent {
    pub pool: PoolConfig,
    pub side: ApiSide,
    pub base_qty: f64,
    pub client_order_id: String,
    pub limit_price: Option<f64>,
}

pub struct SignedTx {
    pub signature: String,
    pub wire_base58: String,
}

pub struct TxEngine {
    pub config: Arc<SolanaConfig>,
    pub rpc: SolanaRpc,
    pub signing: SigningKey,
    pub order_manager: SharedOrderManager,
    account_tx: Arc<Mutex<Option<PublishSender>>>,
}

impl TxEngine {
    pub fn new(
        config: Arc<SolanaConfig>,
        rpc: SolanaRpc,
        signing: SigningKey,
        order_manager: SharedOrderManager,
    ) -> Self {
        Self {
            config,
            rpc,
            signing,
            order_manager,
            account_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub fn set_account_publisher(&self, sender: PublishSender) {
        *self.account_tx.lock().unwrap() = Some(sender);
    }

    pub fn wallet_bytes(&self) -> [u8; 32] {
        self.signing.verifying_key().to_bytes()
    }

    pub fn wallet(&self) -> String {
        b58_encode(&self.wallet_bytes())
    }

    /// 把统一订单请求解析为 swap 意图（AMM 语义映射与 EVM venue 一致）。
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
            | ApiOrderType::TrailingStopMarket
            | ApiOrderType::Unknown => {
                return Err(ApiError::new(
                    EXCHANGE_NAME,
                    "UNSUPPORTED",
                    "only Market / IOC / FOK orders are supported on AMM venues",
                ));
            }
            _ => {}
        }
        if req.order_type == ApiOrderType::Limit
            && matches!(req.time_in_force, ApiTimeInForce::GTC | ApiTimeInForce::GTX)
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

    fn effective_reserves(&self, pool: &PoolConfig) -> Result<(u128, u128), ApiError> {
        self.config
            .market_state
            .lock()
            .unwrap()
            .reserves(pool)
            .ok_or_else(|| ApiError::new(EXCHANGE_NAME, "NO_STATE", "pool reserves not loaded yet"))
    }

    /// 计算 (输入 raw, 最小输出 raw)。方向与滑点保护语义与 EVM venue 一致：
    /// Sell 的预期输出劣于限价/滑点下限、Buy 的所需输入超上限时本地拒绝（省费）。
    pub fn plan_swap(&self, intent: &SwapIntent) -> Result<(u64, u64), ApiError> {
        let (r_base, r_quote) = self.effective_reserves(&intent.pool)?;
        let slippage = self.config.slippage_bps as f64 / 10_000.0;
        let scale = |value: f64, decimals: u32| -> Result<u128, ApiError> {
            let raw = value * 10f64.powi(decimals as i32);
            if !raw.is_finite() || raw <= 0.0 || raw >= 1.2e38 {
                return Err(ApiError::new(
                    EXCHANGE_NAME,
                    "INVALID_ARG",
                    "amount out of representable range",
                ));
            }
            Ok(raw as u128)
        };
        match intent.side {
            ApiSide::Sell => {
                let amount_in = scale(intent.base_qty, intent.pool.base_decimals)?;
                let expected_out = crate::solana::raydium::get_amount_out(
                    amount_in,
                    r_base,
                    r_quote,
                    self.config.fee_bps as u64,
                );
                let floor = match intent.limit_price {
                    Some(p) => scale(p * intent.base_qty, intent.pool.quote_decimals)?,
                    None => (expected_out as f64 * (1.0 - slippage)) as u128,
                };
                if expected_out < floor {
                    return Err(ApiError::new(
                        EXCHANGE_NAME,
                        "PRICE_PROTECTION",
                        format!("expected out {expected_out} below floor {floor}"),
                    ));
                }
                // 限价单以限价为硬下限；市价单按滑点收缩（与 EVM venue 语义一致）。
                let min_out = match intent.limit_price {
                    Some(_) => floor,
                    None => (expected_out as f64 * (1.0 - slippage)) as u128,
                };
                Ok((amount_in as u64, min_out as u64))
            }
            ApiSide::Buy => {
                let base_out = scale(intent.base_qty, intent.pool.base_decimals)?;
                let required_quote_in = crate::solana::raydium::get_amount_in(
                    base_out,
                    r_quote,
                    r_base,
                    self.config.fee_bps as u64,
                );
                if required_quote_in == u128::MAX {
                    return Err(ApiError::new(
                        EXCHANGE_NAME,
                        "INVALID_ARG",
                        "qty out of range",
                    ));
                }
                let cap = match intent.limit_price {
                    Some(p) => scale(p * intent.base_qty, intent.pool.quote_decimals)?,
                    None => (required_quote_in as f64 * (1.0 + slippage)) as u128,
                };
                if required_quote_in > cap {
                    return Err(ApiError::new(
                        EXCHANGE_NAME,
                        "PRICE_PROTECTION",
                        format!("required in {required_quote_in} above cap {cap}"),
                    ));
                }
                // 输入取保护线上浮 1%（手续费非线性缓冲），输出下限按滑点收缩。
                let amount_in = ((cap as f64 * 1.01) as u128) as u64;
                let min_out = ((base_out as f64 * (1.0 - slippage)) as u128) as u64;
                Ok((amount_in, min_out))
            }
            ApiSide::Unknown => Err(ApiError::new(EXCHANGE_NAME, "INVALID_ARG", "side")),
        }
    }

    /// 组装并签名 swap 交易（含 WSOL wrap/unwrap 与 ATA 幂等创建）。
    pub async fn build_signed_swap(
        &self,
        intent: &SwapIntent,
        amount_in: u64,
        min_out: u64,
    ) -> Result<SignedTx, ApiError> {
        let wallet = self.wallet();
        let mut builder = MessageBuilder::new(&wallet);

        // compute budget：limit + 优先费
        let mut limit_data = vec![2u8]; // SetComputeUnitLimit（实盘交易验证的 tag）
        limit_data.extend_from_slice(&self.config.compute_unit_limit.to_le_bytes());
        builder.instruction(COMPUTE_BUDGET_PROGRAM, &[], &limit_data);
        if self.config.priority_fee_micro_lamports > 0 {
            let mut price_data = vec![3u8]; // SetComputeUnitPrice (u64)
            price_data.extend_from_slice(&self.config.priority_fee_micro_lamports.to_le_bytes());
            builder.instruction(COMPUTE_BUDGET_PROGRAM, &[], &price_data);
        }

        let (input_mint, output_mint) = if intent.side == ApiSide::Sell {
            (
                intent.pool.base_mint.clone(),
                intent.pool.quote_mint.clone(),
            )
        } else {
            (
                intent.pool.quote_mint.clone(),
                intent.pool.base_mint.clone(),
            )
        };
        let input_is_wsol = input_mint == crate::solana::types::WSOL_MINT;
        let output_is_wsol = output_mint == crate::solana::types::WSOL_MINT;

        let user_source = ata_address(&self.wallet_bytes(), &input_mint);
        let user_dest = ata_address(&self.wallet_bytes(), &output_mint);

        // 输出 ATA 幂等创建（WSOL 输出也创建，随后 close 解包回收租金）。
        builder.instruction(
            ATA_PROGRAM,
            &[
                wallet.as_str(),
                user_dest.as_str(),
                wallet.as_str(),
                output_mint.as_str(),
                SYSTEM_PROGRAM,
                TOKEN_PROGRAM,
            ],
            &[1u8], // create_idempotent
        );

        // WSOL 输入：创建 ATA + 转入 SOL + syncNative。
        if input_is_wsol {
            builder.instruction(
                ATA_PROGRAM,
                &[
                    wallet.as_str(),
                    user_source.as_str(),
                    wallet.as_str(),
                    input_mint.as_str(),
                    SYSTEM_PROGRAM,
                    TOKEN_PROGRAM,
                ],
                &[1u8],
            );
            // 系统程序的指令 tag 是 u32（与 Token/Raydium 的 u8 tag 不同）。
            let mut transfer = vec![2u8, 0, 0, 0];
            transfer.extend_from_slice(&amount_in.to_le_bytes());
            builder.instruction(
                SYSTEM_PROGRAM,
                &[wallet.as_str(), user_source.as_str()],
                &transfer,
            );
            builder.instruction(TOKEN_PROGRAM, &[user_source.as_str()], &[17u8]); // syncNative
        }

        // swap（coinVault/pcVault 是池的固定账户，方向由用户 source/dest 决定）
        let (coin_vault, pc_vault) = (
            intent.pool.base_vault.clone(),
            intent.pool.quote_vault.clone(),
        );
        let accounts = swap_base_in_accounts(
            &intent.pool.amm_id,
            &coin_vault,
            &pc_vault,
            &user_source,
            &user_dest,
            &wallet,
        );
        let account_refs: Vec<&str> = accounts.iter().map(|a| a.as_str()).collect();
        builder.instruction(
            RAYDIUM_V4_PROGRAM,
            &account_refs,
            &crate::solana::raydium::encode_swap_base_in_data(amount_in, min_out),
        );

        // WSOL 输出：closeAccount 自动解包回 SOL（回收租金）。
        if output_is_wsol {
            builder.instruction(
                TOKEN_PROGRAM,
                &[user_dest.as_str(), wallet.as_str(), wallet.as_str()],
                &[9u8],
            );
        }

        let blockhash_b58 = self
            .rpc
            .latest_blockhash()
            .await
            .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))?;
        let blockhash: [u8; 32] = b58_decode(&blockhash_b58)
            .try_into()
            .map_err(|_| ApiError::new(EXCHANGE_NAME, "DECODE", "bad blockhash"))?;
        let message = builder.encode(&blockhash);
        let signature = self.signing.sign(&message).to_bytes();
        let mut wire = Vec::with_capacity(1 + 64 + message.len());
        wire.push(1);
        wire.extend_from_slice(&signature);
        wire.extend_from_slice(&message);
        Ok(SignedTx {
            signature: b58_encode(&signature),
            wire_base58: b58_encode(&wire),
        })
    }

    /// 统一入口：注册状态机 -> 编码/签名 -> 广播 -> 确认监视。
    #[allow(dead_code)]
    pub async fn submit(
        &self,
        req: &UnifiedOrderRequest,
        local_order_id: u64,
    ) -> Result<OrderInfo, ApiError> {
        let intent = self.resolve_intent(req)?;
        let (amount_in, min_out) = self.plan_swap(&intent)?;
        let now_ms = Utc::now().timestamp_millis();
        {
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
        let signed = self.build_signed_swap(&intent, amount_in, min_out).await?;
        {
            let mut mgr = self.order_manager.lock().unwrap();
            mgr.attach_tx(&intent.client_order_id, &signed.signature);
        }
        // 公共 RPC 的预检偶发误报（如错误的 accounts-size cap），先带预检，
        // 预检被拒时降级 skipPreflight 直接上链，真实结果由确认循环回报。
        let landed = match self.rpc.send_transaction(&signed.wire_base58).await {
            Ok(sig) => sig,
            Err(SolanaError::Rpc(msg)) => {
                tracing::warn!(%msg, "preflight rejected; retrying with skipPreflight");
                self.rpc
                    .send_transaction_with(&signed.wire_base58, true)
                    .await
                    .map_err(|e| ApiError::transport(EXCHANGE_NAME, e))?
            }
            Err(e) => return Err(ApiError::transport(EXCHANGE_NAME, e)),
        };

        let info = OrderInfo {
            symbol: req.symbol.clone(),
            order_id: landed.clone(),
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
            position_side: req.position_side.unwrap_or(ApiPositionSide::Net),
            create_time: now_ms,
            update_time: now_ms,
            stop_price: req.stop_price,
        };

        let watcher = SwapWatcher {
            config: self.config.clone(),
            rpc: self.rpc.clone(),
            order_manager: self.order_manager.clone(),
            account_tx: Arc::clone(&self.account_tx),
            signature: landed,
            symbol: req.symbol.clone(),
            qty: req.qty,
            client_order_id: intent.client_order_id.clone(),
        };
        tokio::spawn(async move {
            watcher.watch().await;
        });
        Ok(info)
    }
}

/// 单笔 swap 的确认监视任务（私有流等价物）。
struct SwapWatcher {
    config: Arc<SolanaConfig>,
    rpc: SolanaRpc,
    order_manager: SharedOrderManager,
    account_tx: Arc<Mutex<Option<PublishSender>>>,
    signature: String,
    symbol: String,
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

    async fn watch(self) {
        let started = tokio::time::Instant::now();
        let status = loop {
            match self.rpc.signature_status(&self.signature).await {
                Ok(Some(Ok(()))) => break Status::Filled,
                Ok(Some(Err(err))) => {
                    tracing::warn!(signature = %self.signature, %err, "swap failed on chain");
                    break Status::Rejected;
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(?error, signature = %self.signature, "status polling failed");
                    break Status::Canceled;
                }
            }
            if started.elapsed() > Duration::from_millis(self.config.tx_timeout_ms) {
                tracing::warn!(signature = %self.signature, "confirmation timeout; treating as dropped");
                break Status::Canceled;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        let exec_qty = if status == Status::Filled {
            self.qty
        } else {
            0.0
        };
        let finalized = self.order_manager.lock().unwrap().finalize(
            &self.client_order_id,
            status,
            exec_qty,
            0.0,
        );
        match finalized {
            Some(order) => self.publish_account(AccountPublication::Order {
                symbol: self.symbol.clone(),
                client_order_id: Some(self.client_order_id.clone()),
                venue_order_id: Some(self.signature.clone()),
                order,
            }),
            None => self.publish_error(
                &self.client_order_id,
                format!("swap confirmation lost: {}", self.signature),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{ApiOrderType, ApiTimeInForce};
    use crate::solana::types::{PoolConfig, WSOL_MINT};

    fn pool() -> PoolConfig {
        PoolConfig {
            symbol: "TST/WSOL".to_string(),
            amm_id: "AMMID".to_string(),
            base_mint: "MINTA".to_string(),
            quote_mint: WSOL_MINT.to_string(),
            base_vault: "VA".to_string(),
            quote_vault: "VQ".to_string(),
            base_decimals: 6,
            quote_decimals: 9,
        }
    }

    fn engine_with_state(r_base: u128, r_quote: u128) -> TxEngine {
        let mut market = crate::solana::types::SolanaMarketState::default();
        market.vaults.insert("VA".to_string(), r_base as u64);
        market.vaults.insert("VQ".to_string(), r_quote as u64);
        let config = Arc::new(SolanaConfig::test_config(
            vec![pool()],
            crate::solana::types::SharedMarketState::new(Mutex::new(market)),
        ));
        TxEngine::new(
            config,
            SolanaRpc::new("https://127.0.0.1:1", "ws://127.0.0.1:1"),
            ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]),
            Arc::new(Mutex::new(OrderManager::new())),
        )
    }

    fn intent(side: ApiSide, base_qty: f64, limit_price: Option<f64>) -> SwapIntent {
        SwapIntent {
            pool: pool(),
            side,
            base_qty,
            client_order_id: "c1".to_string(),
            limit_price,
        }
    }

    #[test]
    fn plan_sell_uses_limit_price_as_floor() {
        // 储备 1000 SOL : 3_000_000 TST，价格 3000；卖 1 TST ≈ 2999.25 lamports
        let engine = engine_with_state(1_000_000_000_000, 3_000_000_000_000);
        let (amount_in, min_out) = engine.plan_swap(&intent(ApiSide::Sell, 1.0, None)).unwrap();
        assert_eq!(amount_in, 1_000_000); // 1 TST (6dp)
        // 滑点 100bps：预期 ~0.00299 SOL，min_out ≈ 0.00296 SOL (lamports)
        assert!(min_out > 2_900_000 && min_out < 2_990_000);
        // 限价高于市价（卖单要求更高价，0.0035 SOL/TST > 市价 0.003）：本地拒绝。
        let rejected = engine.plan_swap(&intent(ApiSide::Sell, 1.0, Some(0.0035)));
        assert_eq!(rejected.unwrap_err().code, "PRICE_PROTECTION");
        // 限价低于市价（0.0028）：以限价为下限。
        let (amount_in, min_out) = engine
            .plan_swap(&intent(ApiSide::Sell, 1.0, Some(0.0028)))
            .unwrap();
        assert_eq!(amount_in, 1_000_000);
        assert!((min_out as f64 / 1e9 - 0.0028).abs() < 1e-6);
    }

    #[test]
    fn plan_buy_caps_input_at_limit_price() {
        let engine = engine_with_state(1_000_000_000_000, 3_000_000_000_000);
        // 买价上限过低（0.0025 SOL/TST < 市价 ~0.003）：本地拒绝。
        let rejected = engine.plan_swap(&intent(ApiSide::Buy, 1.0, Some(0.0025)));
        assert_eq!(rejected.unwrap_err().code, "PRICE_PROTECTION");
        // 宽松上限（0.004）：输入量被 cap 在限价 * (1+缓冲)。
        let (amount_in, min_out) = engine
            .plan_swap(&intent(ApiSide::Buy, 1.0, Some(0.004)))
            .unwrap();
        assert!((amount_in as f64 / 1e9 - 0.00404).abs() < 1e-8);
        // 输出下限按滑点收缩：1 TST * 0.99 = 990_000 raw (6dp)。
        assert_eq!(min_out, 990_000);
    }

    #[tokio::test]
    async fn resolve_rejects_resting_orders() {
        let engine = engine_with_state(1_000_000_000_000, 3_000_000_000_000);
        let mut req = UnifiedOrderRequest {
            symbol: "TST/WSOL".to_string(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Limit,
            price: Some(3000.0),
            qty: 1.0,
            time_in_force: ApiTimeInForce::GTC,
            reduce_only: false,
            position_side: None,
            client_order_id: Some("c1".to_string()),
            stop_price: None,
        };
        assert!(engine.resolve_intent(&req).is_err());
        req.time_in_force = ApiTimeInForce::IOC;
        assert!(engine.resolve_intent(&req).is_ok());
        req.order_type = ApiOrderType::StopMarket;
        assert!(engine.resolve_intent(&req).is_err());
    }
}
