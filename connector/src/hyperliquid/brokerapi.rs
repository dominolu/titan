#![allow(dead_code)]
//! Hyperliquid 全量 API + 统一 [`BrokerApi`] 实现。
//!
//! 对照官方 gitbook 补齐：
//! - info：allMids/clearinghouseState/openOrders/frontendOpenOrders/userFills/userFillsByTime/
//!   userRateLimit/orderStatus/l2Book/candleSnapshot/maxBuilderFee/historicalOrders/
//!   userTwapSliceFills/subAccounts/userVaultEquities/vaultDetails/userRole/portfolio/referral/
//!   userFees/meta/metaAndAssetCtxs/allPerpMetas/activeAssetData/fundingHistory/predictedFundings/
//!   userFunding/spotMeta/spotMetaAndAssetCtxs/spotClearinghouseState/spotDeployState/tokenDetails
//! - exchange：order/cancel/cancelByCloid/scheduleCancel/modify/batchModify/updateLeverage/
//!   updateIsolatedMargin/twapOrder/twapCancel/approveAgent/approveBuilderFee/usdClassTransfer/
//!   spotSend/withdraw3
//! - WebSocket 订阅频道见 ws.rs。

use std::collections::HashMap;
use std::error::Error;

use serde::Serialize;

use crate::{
    api::{
        AccountInfo, AlgoOrderRequest, AmendOrderRequest, ApiError, ApiMarginType, ApiOrderStatus,
        ApiOrderType, ApiPositionSide, ApiSide, ApiTimeInForce, Balance, BrokerApi,
        CancelOrderRequest, FeeRate, Fill, FundingRate, IncomeRecord, InstrumentInfo, Kline,
        LeverageInfo, OpenInterest, OrderBook, OrderInfo, PositionInfo, PriceLevel, Ticker, Trade,
        UnifiedOrderRequest,
    },
    hyperliquid::{HyperliquidError, client::HyperliquidClient, msg as m},
};

impl From<HyperliquidError> for ApiError {
    fn from(e: HyperliquidError) -> Self {
        match e {
            HyperliquidError::OrderError(msg) => ApiError::new("hyperliquid", "ORDER", msg),
            HyperliquidError::Reqwest(err) => {
                let mut msg = err.to_string();
                let mut cause = err.source();
                while let Some(c) = cause {
                    msg.push_str(&format!("; caused by: {c}"));
                    cause = c.source();
                }
                ApiError::new("hyperliquid", "TRANSPORT", msg)
            }
            other => ApiError::new("hyperliquid", "ERR", other.to_string()),
        }
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        ApiError::new("hyperliquid", "SERDE", e.to_string())
    }
}

// ------------------------------------------------------------------
// 转换辅助
// ------------------------------------------------------------------

fn f(s: &str) -> f64 {
    s.parse::<f64>().unwrap_or(0.0)
}

/// HL 方向：A = ask 侧（卖出），B = bid 侧（买入）。
fn side_from_hl(s: &str) -> ApiSide {
    match s {
        "A" => ApiSide::Sell,
        "B" => ApiSide::Buy,
        _ => ApiSide::from_str(s),
    }
}

fn side_to_hl(side: ApiSide) -> Result<bool, ApiError> {
    match side {
        ApiSide::Buy => Ok(true),
        ApiSide::Sell => Ok(false),
        _ => Err(ApiError::new(
            "hyperliquid",
            "INVALID",
            "side must be Buy or Sell",
        )),
    }
}

fn tif_to_hl(tif: ApiTimeInForce) -> &'static str {
    match tif {
        ApiTimeInForce::GTC => "Gtc",
        ApiTimeInForce::IOC => "Ioc",
        ApiTimeInForce::GTX => "Alo",
        _ => "Gtc",
    }
}

fn status_from_hl(s: &str) -> ApiOrderStatus {
    match s {
        "filled" => ApiOrderStatus::Filled,
        "open" => ApiOrderStatus::New,
        "canceled" | "marginCanceled" => ApiOrderStatus::Canceled,
        "triggered" => ApiOrderStatus::Triggered,
        "rejected" => ApiOrderStatus::Rejected,
        _ => ApiOrderStatus::from_str(s),
    }
}

fn order_info_from_historical(o: &m::HistoricalOrder) -> OrderInfo {
    let qty = f(&o.orig_sz);
    let executed = f(&o.filled);
    OrderInfo {
        symbol: o.coin.clone(),
        order_id: o.oid.to_string(),
        client_order_id: o.cloid.clone().unwrap_or_default(),
        side: side_from_hl(&o.side),
        order_type: ApiOrderType::from_str(&o.order_type),
        status: if executed >= qty && qty > 0.0 {
            ApiOrderStatus::Filled
        } else if executed > 0.0 {
            ApiOrderStatus::PartiallyFilled
        } else {
            ApiOrderStatus::New
        },
        price: f(&o.limit_px),
        qty,
        executed_qty: executed,
        avg_price: f(&o.avg_px),
        leaves_qty: (qty - executed).max(0.0),
        time_in_force: ApiTimeInForce::GTC,
        reduce_only: o.reduce_only,
        position_side: ApiPositionSide::Unknown,
        create_time: o.timestamp as i64,
        update_time: o.timestamp as i64,
        stop_price: if o.trigger_px.parse::<f64>().unwrap_or(0.0) > 0.0 {
            Some(f(&o.trigger_px))
        } else {
            None
        },
    }
}

/// 解析 metaAndAssetCtxs 响应 → (universe, ctxs)。提取为纯函数便于离线单测。
pub(crate) fn parse_meta_and_ctxs(
    resp: &serde_json::Value,
) -> Result<(Vec<m::AssetMeta>, Vec<m::AssetCtx>), HyperliquidError> {
    let arr = resp
        .as_array()
        .ok_or(HyperliquidError::InvalidArg("metaAndAssetCtxs not array"))?;
    if arr.len() < 2 {
        return Err(HyperliquidError::InvalidArg(
            "metaAndAssetCtxs missing ctxs",
        ));
    }
    let universe: Vec<m::AssetMeta> = serde_json::from_value(
        arr[0]
            .get("universe")
            .cloned()
            .unwrap_or(serde_json::Value::Array(vec![])),
    )
    .map_err(|_| HyperliquidError::InvalidArg("metaAndAssetCtxs universe parse failed"))?;
    let ctxs: Vec<m::AssetCtx> = serde_json::from_value(arr[1].clone())
        .map_err(|_| HyperliquidError::InvalidArg("metaAndAssetCtxs ctxs parse failed"))?;
    Ok((universe, ctxs))
}

// ------------------------------------------------------------------
// 统一 API 的 wire 类型（字段顺序对齐官方 msgpack 哈希）
// ------------------------------------------------------------------

#[derive(Serialize)]
pub struct ApiTif {
    pub tif: String,
}

#[derive(Serialize)]
pub struct ApiLimitOrderType {
    pub limit: ApiTif,
}

#[derive(Serialize)]
pub struct ApiMarketOrderType {
    pub market: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiTriggerWire {
    pub is_market: bool,
    pub trigger_px: String,
    pub tpsl: String,
}

#[derive(Serialize)]
pub struct ApiTriggerOrderType {
    pub trigger: ApiTriggerWire,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum ApiOrderTypeWire {
    Limit(ApiLimitOrderType),
    Market(ApiMarketOrderType),
    Trigger(ApiTriggerOrderType),
}

#[derive(Serialize)]
pub struct ApiOrderWire {
    pub a: u32,
    pub b: bool,
    pub p: String,
    pub s: String,
    pub r: bool,
    pub t: ApiOrderTypeWire,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
}

#[derive(Serialize)]
pub struct ApiOrderAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub orders: Vec<ApiOrderWire>,
    pub grouping: String,
}

#[derive(Serialize)]
pub struct ApiCancelWire {
    pub a: u32,
    pub o: u64,
}

#[derive(Serialize)]
pub struct ApiCancelAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub cancels: Vec<ApiCancelWire>,
}

#[derive(Serialize)]
pub struct ApiCancelCloidWire {
    pub asset: u32,
    pub cloid: String,
}

#[derive(Serialize)]
pub struct ApiCancelByCloidAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub cancels: Vec<ApiCancelCloidWire>,
}

#[derive(Serialize)]
pub struct ApiScheduleCancelAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub time: u64,
}

#[derive(Serialize)]
pub struct ApiModifyWire {
    pub a: u32,
    pub o: u64,
    pub p: String,
    pub s: String,
}

#[derive(Serialize)]
pub struct ApiModifyAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub modifies: Vec<ApiModifyWire>,
}

#[derive(Serialize)]
pub struct ApiUpdateLeverageAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub coin: String,
    pub is_cross: bool,
    pub leverage: u64,
}

#[derive(Serialize)]
pub struct ApiUpdateIsolatedMarginAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub coin: String,
    pub is_buy: bool,
    pub ntli: String,
}

#[derive(Serialize)]
pub struct ApiTwapOrderAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub order: ApiTwapOrderWire,
}

#[derive(Serialize)]
pub struct ApiTwapOrderWire {
    pub a: u32,
    pub b: bool,
    pub p: String,
    pub s: String,
    pub r: bool,
    pub t: ApiOrderTypeWire,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
    pub duration: u64,
}

#[derive(Serialize)]
pub struct ApiTwapCancelAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub twap_id: u64,
}

#[derive(Serialize)]
pub struct ApiApproveAgentAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub hyperliquid_chain: String,
    pub signature_chain_id: String,
    pub agent_address: String,
    pub agent_name: String,
    pub nonce: u64,
}

// ------------------------------------------------------------------
// 原始 info 端点（对照官方文档全量）
// ------------------------------------------------------------------

impl HyperliquidClient {
    async fn parse_info<T: for<'de> serde::Deserialize<'de>>(
        &self,
        body: serde_json::Value,
    ) -> Result<T, HyperliquidError> {
        let resp = self.post_info(body).await?;
        Ok(serde_json::from_value(resp)?)
    }

    pub async fn get_all_mids(&self) -> Result<HashMap<String, String>, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "allMids"}))
            .await
    }

    pub async fn get_frontend_open_orders(
        &self,
        user: &str,
    ) -> Result<Vec<serde_json::Value>, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "frontendOpenOrders", "user": user}))
            .await
    }

    pub async fn get_user_fills(
        &self,
        user: &str,
    ) -> Result<Vec<serde_json::Value>, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "userFills", "user": user}))
            .await
    }

    pub async fn get_user_fills_by_time(
        &self,
        user: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<serde_json::Value>, HyperliquidError> {
        self.parse_info(serde_json::json!({
            "type": "userFillsByTime",
            "user": user,
            "startTime": start_time,
            "endTime": end_time,
        }))
        .await
    }

    pub async fn get_user_rate_limit(
        &self,
        user: &str,
    ) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "userRateLimit", "user": user}))
            .await
    }

    pub async fn get_order_status(
        &self,
        oid: Option<u64>,
        cloid: Option<&str>,
    ) -> Result<m::OrderStatusResponse, HyperliquidError> {
        let mut body = serde_json::Map::new();
        body.insert(
            "type".to_string(),
            serde_json::Value::String("orderStatus".into()),
        );
        if let Some(oid) = oid {
            body.insert("oid".to_string(), serde_json::Value::Number(oid.into()));
        }
        if let Some(cloid) = cloid {
            body.insert("cloid".to_string(), serde_json::Value::String(cloid.into()));
        }
        self.parse_info(serde_json::Value::Object(body)).await
    }

    pub async fn get_l2_book(&self, coin: &str) -> Result<m::L2BookData, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "l2Book", "coin": coin}))
            .await
    }

    pub async fn get_candle_snapshot(
        &self,
        coin: &str,
        interval: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<m::Candle>, HyperliquidError> {
        self.parse_info(serde_json::json!({
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": start_time,
                "endTime": end_time,
            },
        }))
        .await
    }

    pub async fn get_max_builder_fee(&self) -> Result<i64, HyperliquidError> {
        let resp = self
            .post_info(serde_json::json!({"type": "maxBuilderFee"}))
            .await?;
        Ok(resp.as_i64().unwrap_or(0))
    }

    pub async fn get_historical_orders(
        &self,
        user: &str,
    ) -> Result<Vec<m::HistoricalOrder>, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "historicalOrders", "user": user}))
            .await
    }

    pub async fn get_user_twap_slice_fills(
        &self,
        user: &str,
    ) -> Result<Vec<serde_json::Value>, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "userTwapSliceFills", "user": user}))
            .await
    }

    pub async fn get_sub_accounts(
        &self,
        user: &str,
    ) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "subAccounts", "user": user}))
            .await
    }

    pub async fn get_user_vault_equities(
        &self,
        user: &str,
    ) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "userVaultEquities", "user": user}))
            .await
    }

    pub async fn get_vault_details(
        &self,
        vault_address: &str,
    ) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "vaultDetails", "vaultAddress": vault_address}))
            .await
    }

    pub async fn get_user_role(&self, user: &str) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "userRole", "user": user}))
            .await
    }

    pub async fn get_portfolio(&self, user: &str) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "portfolio", "user": user}))
            .await
    }

    pub async fn get_referral(&self, user: &str) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "referral", "user": user}))
            .await
    }

    pub async fn get_user_fees(&self, user: &str) -> Result<m::UserFees, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "userFees", "user": user}))
            .await
    }

    pub async fn get_all_perp_metas(&self) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "allPerpMetas"}))
            .await
    }

    pub async fn get_active_asset_data(&self) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "activeAssetData"}))
            .await
    }

    pub async fn get_funding_history(
        &self,
        coin: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<m::FundingHistoryRecord>, HyperliquidError> {
        self.parse_info(serde_json::json!({
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_time,
            "endTime": end_time,
        }))
        .await
    }

    pub async fn get_predicted_fundings(&self) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "predictedFundings"}))
            .await
    }

    pub async fn get_user_funding(
        &self,
        user: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<m::UserFundingRecord>, HyperliquidError> {
        self.parse_info(serde_json::json!({
            "type": "userFunding",
            "user": user,
            "startTime": start_time,
            "endTime": end_time,
        }))
        .await
    }

    pub async fn get_user_non_funding_ledger_updates(
        &self,
        user: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<serde_json::Value>, HyperliquidError> {
        self.parse_info(serde_json::json!({
            "type": "userNonFundingLedgerUpdates",
            "user": user,
            "startTime": start_time,
            "endTime": end_time,
        }))
        .await
    }

    pub async fn get_spot_meta(&self) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "spotMeta"}))
            .await
    }

    pub async fn get_spot_meta_and_asset_ctxs(
        &self,
    ) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "spotMetaAndAssetCtxs"}))
            .await
    }

    pub async fn get_spot_clearinghouse_state(
        &self,
        user: &str,
    ) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "spotClearinghouseState", "user": user}))
            .await
    }

    pub async fn get_spot_deploy_state(&self) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "spotDeployState"}))
            .await
    }

    pub async fn get_token_details(
        &self,
        token: &str,
    ) -> Result<serde_json::Value, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "tokenDetails", "token": token}))
            .await
    }

    pub async fn get_exchange_status(&self) -> Result<m::ExchangeStatus, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "exchangeStatus"}))
            .await
    }

    /// 详细账户状态（marginSummary + 完整持仓字段）。
    pub async fn get_clearinghouse_state_detail(
        &self,
        user: &str,
    ) -> Result<m::ClearinghouseStateDetail, HyperliquidError> {
        self.parse_info(serde_json::json!({"type": "clearinghouseState", "user": user}))
            .await
    }

    /// metaAndAssetCtxs → (universe, ctxs)。
    pub async fn get_meta_and_ctxs(
        &self,
    ) -> Result<(Vec<m::AssetMeta>, Vec<m::AssetCtx>), HyperliquidError> {
        let resp = self
            .post_info(serde_json::json!({"type": "metaAndAssetCtxs"}))
            .await?;
        parse_meta_and_ctxs(&resp)
    }

    /// 查询 coin 的资产索引（universe 下标），用于下单/撤单 wire。
    pub async fn asset_index(&self, coin: &str) -> Result<u32, HyperliquidError> {
        let meta = self.get_meta().await?;
        meta.universe
            .iter()
            .position(|a| a.name == coin)
            .map(|i| i as u32)
            .ok_or_else(|| HyperliquidError::AssetNotFound(coin.to_string()))
    }

    /// 从 order 响应中解析首个订单状态。
    fn parse_order_response(&self, resp: &m::ExchangeResponse) -> Result<OrderInfo, ApiError> {
        if resp.status != "ok" {
            return Err(ApiError::new(
                "hyperliquid",
                resp.status.clone(),
                "exchange error",
            ));
        }
        let data = resp
            .response
            .as_ref()
            .and_then(|r| r.data.as_ref())
            .ok_or_else(|| ApiError::new("hyperliquid", "EMPTY", "empty exchange response"))?;
        let statuses = data
            .get("statuses")
            .and_then(|s| s.as_array())
            .ok_or_else(|| ApiError::new("hyperliquid", "EMPTY", "missing statuses"))?;
        let first = statuses
            .first()
            .ok_or_else(|| ApiError::new("hyperliquid", "EMPTY", "empty statuses"))?;
        if let Some(e) = first.get("error").and_then(|e| e.as_str()) {
            return Err(ApiError::new("hyperliquid", "ORDER", e));
        }
        if let Some(resting) = first.get("resting") {
            let oid = resting.get("oid").and_then(|o| o.as_u64()).unwrap_or(0);
            return Ok(OrderInfo {
                symbol: String::new(),
                order_id: oid.to_string(),
                client_order_id: String::new(),
                side: ApiSide::Unknown,
                order_type: ApiOrderType::Unknown,
                status: ApiOrderStatus::New,
                price: 0.0,
                qty: 0.0,
                executed_qty: 0.0,
                avg_price: 0.0,
                leaves_qty: 0.0,
                time_in_force: ApiTimeInForce::Unknown,
                reduce_only: false,
                position_side: ApiPositionSide::Unknown,
                create_time: 0,
                update_time: 0,
                stop_price: None,
            });
        }
        if let Some(filled) = first.get("filled") {
            let oid = filled.get("oid").and_then(|o| o.as_u64()).unwrap_or(0);
            return Ok(OrderInfo {
                symbol: String::new(),
                order_id: oid.to_string(),
                client_order_id: String::new(),
                side: ApiSide::Unknown,
                order_type: ApiOrderType::Unknown,
                status: ApiOrderStatus::Filled,
                price: 0.0,
                qty: 0.0,
                executed_qty: 0.0,
                avg_price: 0.0,
                leaves_qty: 0.0,
                time_in_force: ApiTimeInForce::Unknown,
                reduce_only: false,
                position_side: ApiPositionSide::Unknown,
                create_time: 0,
                update_time: 0,
                stop_price: None,
            });
        }
        Err(ApiError::new(
            "hyperliquid",
            "UNKNOWN",
            "unrecognized order status",
        ))
    }
}

// ------------------------------------------------------------------
// 统一 BrokerApi 实现
// ------------------------------------------------------------------

#[async_trait::async_trait]
impl BrokerApi for HyperliquidClient {
    async fn ping(&self) -> Result<(), ApiError> {
        let _ = self.get_exchange_status().await?;
        Ok(())
    }

    async fn get_server_time(&self) -> Result<i64, ApiError> {
        let status = self.get_exchange_status().await?;
        Ok(status.time as i64)
    }

    async fn get_instruments(&self) -> Result<Vec<InstrumentInfo>, ApiError> {
        let meta = self.get_meta().await?;
        Ok(meta
            .universe
            .iter()
            .map(|a| {
                let lot = 10f64.powi(-(a.sz_decimals as i32));
                InstrumentInfo {
                    symbol: a.name.clone(),
                    base_asset: a.name.clone(),
                    quote_asset: "USDC".to_string(),
                    // HL 官方 API 不直接提供 tick size，由策略按价格档位自行处理。
                    tick_size: 0.0,
                    lot_size: lot,
                    min_qty: lot,
                    contract_size: 1.0,
                    margin_asset: "USDC".to_string(),
                    price_precision: 0,
                    qty_precision: a.sz_decimals,
                    tradable: true,
                }
            })
            .collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<Ticker, ApiError> {
        let (universe, ctxs) = self.get_meta_and_ctxs().await?;
        let pos = universe
            .iter()
            .position(|a| a.name == symbol)
            .ok_or_else(|| ApiError::new("hyperliquid", "NOT_FOUND", "asset not found"))?;
        let ctx = ctxs
            .get(pos)
            .ok_or_else(|| ApiError::new("hyperliquid", "NOT_FOUND", "ctx not found"))?;
        Ok(Ticker {
            symbol: symbol.to_string(),
            last_price: f(&ctx.mark_px),
            mark_price: Some(f(&ctx.mark_px)),
            index_price: Some(f(&ctx.oracle_px)),
            funding_rate: Some(f(&ctx.funding)),
            next_funding_time: None,
            open_24h: f(&ctx.prev_day_px),
            high_24h: 0.0,
            low_24h: 0.0,
            volume_24h: f(&ctx.day_base_vlm),
            quote_volume_24h: f(&ctx.day_ntl_vlm),
            timestamp: 0,
        })
    }

    async fn get_tickers(&self) -> Result<Vec<Ticker>, ApiError> {
        let (universe, ctxs) = self.get_meta_and_ctxs().await?;
        Ok(universe
            .iter()
            .zip(ctxs.iter())
            .map(|(a, ctx)| Ticker {
                symbol: a.name.clone(),
                last_price: f(&ctx.mark_px),
                mark_price: Some(f(&ctx.mark_px)),
                index_price: Some(f(&ctx.oracle_px)),
                funding_rate: Some(f(&ctx.funding)),
                next_funding_time: None,
                open_24h: f(&ctx.prev_day_px),
                high_24h: 0.0,
                low_24h: 0.0,
                volume_24h: f(&ctx.day_base_vlm),
                quote_volume_24h: f(&ctx.day_ntl_vlm),
                timestamp: 0,
            })
            .collect())
    }

    async fn get_order_book(&self, symbol: &str, _limit: u32) -> Result<OrderBook, ApiError> {
        let book = self.get_l2_book(symbol).await?;
        let levels = |rows: &[Vec<m::Level>]| -> Vec<PriceLevel> {
            rows.first()
                .map(|ls| {
                    ls.iter()
                        .map(|l| PriceLevel {
                            price: f(&l.px),
                            qty: f(&l.sz),
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        Ok(OrderBook {
            symbol: symbol.to_string(),
            bids: levels(&book.levels),
            asks: if book.levels.len() > 1 {
                levels(&book.levels[1..])
            } else {
                vec![]
            },
            timestamp: book.time as i64,
        })
    }

    async fn get_trades(&self, symbol: &str, limit: u32) -> Result<Vec<Trade>, ApiError> {
        let resp = self
            .post_info(serde_json::json!({"type": "recentTrades", "coin": symbol}))
            .await?;
        let trades: Vec<m::RecentTrade> = serde_json::from_value(resp)?;
        Ok(trades
            .into_iter()
            .take(limit.min(100) as usize)
            .map(|t| Trade {
                symbol: t.coin,
                id: t.tid.to_string(),
                price: f(&t.px),
                qty: f(&t.sz),
                side: side_from_hl(&t.side),
                timestamp: t.time as i64,
            })
            .collect())
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
    ) -> Result<Vec<Kline>, ApiError> {
        let interval_ms = match interval {
            "1m" => 60_000,
            "3m" => 180_000,
            "5m" => 300_000,
            "15m" => 900_000,
            "30m" => 1_800_000,
            "1h" => 3_600_000,
            "2h" => 7_200_000,
            "4h" => 14_400_000,
            "8h" => 28_800_000,
            "12h" => 43_200_000,
            "1d" => 86_400_000,
            "3d" => 259_200_000,
            "1w" => 604_800_000,
            "1M" => 2_592_000_000,
            _ => return Err(ApiError::new("hyperliquid", "INVALID", "bad interval")),
        };
        let end = chrono::Utc::now().timestamp_millis();
        let start = end - interval_ms * (limit.min(500) as i64);
        let candles = self
            .get_candle_snapshot(symbol, interval, start, end)
            .await?;
        Ok(candles
            .iter()
            .map(|c| Kline {
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                open_time: c.t as i64,
                close_time: c.t_close as i64,
                open: f(&c.o),
                high: f(&c.h),
                low: f(&c.l),
                close: f(&c.c),
                volume: f(&c.v),
                quote_volume: 0.0,
            })
            .collect())
    }

    async fn get_funding_rate(&self, symbol: &str) -> Result<FundingRate, ApiError> {
        let (universe, ctxs) = self.get_meta_and_ctxs().await?;
        let pos = universe
            .iter()
            .position(|a| a.name == symbol)
            .ok_or_else(|| ApiError::new("hyperliquid", "NOT_FOUND", "asset not found"))?;
        let ctx = ctxs
            .get(pos)
            .ok_or_else(|| ApiError::new("hyperliquid", "NOT_FOUND", "ctx not found"))?;
        Ok(FundingRate {
            symbol: symbol.to_string(),
            funding_rate: f(&ctx.funding),
            next_funding_time: 0,
            timestamp: 0,
        })
    }

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRate>, ApiError> {
        let end = chrono::Utc::now().timestamp_millis();
        let start = end - 3_600_000 * (limit.min(100) as i64);
        let records = self.get_funding_history(symbol, start, end).await?;
        Ok(records
            .iter()
            .map(|r| FundingRate {
                symbol: r.coin.clone(),
                funding_rate: f(&r.funding_rate),
                next_funding_time: 0,
                timestamp: r.time as i64,
            })
            .collect())
    }

    async fn get_open_interest(&self, symbol: &str) -> Result<OpenInterest, ApiError> {
        let (universe, ctxs) = self.get_meta_and_ctxs().await?;
        let pos = universe
            .iter()
            .position(|a| a.name == symbol)
            .ok_or_else(|| ApiError::new("hyperliquid", "NOT_FOUND", "asset not found"))?;
        let ctx = ctxs
            .get(pos)
            .ok_or_else(|| ApiError::new("hyperliquid", "NOT_FOUND", "ctx not found"))?;
        Ok(OpenInterest {
            symbol: symbol.to_string(),
            open_interest: f(&ctx.open_interest),
            timestamp: 0,
        })
    }

    async fn submit_order(&self, req: &UnifiedOrderRequest) -> Result<OrderInfo, ApiError> {
        let wires = vec![build_wire(self, req).await?];
        let action = ApiOrderAction {
            type_: "order".to_string(),
            orders: wires,
            grouping: "na".to_string(),
        };
        let resp = self.sign_and_post(&action).await?;
        let mut info = self.parse_order_response(&resp)?;
        info.symbol = req.symbol.clone();
        info.client_order_id = req.client_order_id.clone().unwrap_or_default();
        info.side = req.side;
        info.order_type = req.order_type;
        info.price = req.price.unwrap_or(0.0);
        info.qty = req.qty;
        info.leaves_qty = req.qty;
        info.time_in_force = req.time_in_force;
        info.reduce_only = req.reduce_only;
        info.stop_price = req.stop_price;
        Ok(info)
    }

    async fn submit_orders(
        &self,
        reqs: &[UnifiedOrderRequest],
    ) -> Result<Vec<OrderInfo>, ApiError> {
        let mut wires = Vec::with_capacity(reqs.len());
        for req in reqs {
            wires.push(build_wire(self, req).await?);
        }
        let action = ApiOrderAction {
            type_: "order".to_string(),
            orders: wires,
            grouping: "na".to_string(),
        };
        let resp = self.sign_and_post(&action).await?;
        if resp.status != "ok" {
            return Err(ApiError::new(
                "hyperliquid",
                resp.status.clone(),
                "exchange error",
            ));
        }
        let data = resp
            .response
            .as_ref()
            .and_then(|r| r.data.as_ref())
            .ok_or_else(|| ApiError::new("hyperliquid", "EMPTY", "empty response"))?;
        let statuses = data
            .get("statuses")
            .and_then(|s| s.as_array())
            .ok_or_else(|| ApiError::new("hyperliquid", "EMPTY", "missing statuses"))?;
        let mut infos = Vec::with_capacity(statuses.len());
        for (i, st) in statuses.iter().enumerate() {
            let req = reqs.get(i).cloned().unwrap_or_else(|| UnifiedOrderRequest {
                symbol: String::new(),
                side: ApiSide::Unknown,
                order_type: ApiOrderType::Unknown,
                price: None,
                qty: 0.0,
                time_in_force: ApiTimeInForce::Unknown,
                reduce_only: false,
                position_side: None,
                client_order_id: None,
                stop_price: None,
            });
            if let Some(e) = st.get("error").and_then(|e| e.as_str()) {
                return Err(ApiError::new("hyperliquid", "ORDER", e));
            }
            let oid = st
                .get("resting")
                .or_else(|| st.get("filled"))
                .and_then(|o| o.get("oid"))
                .and_then(|o| o.as_u64())
                .unwrap_or(0);
            infos.push(OrderInfo {
                symbol: req.symbol,
                order_id: oid.to_string(),
                client_order_id: req.client_order_id.clone().unwrap_or_default(),
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
                position_side: ApiPositionSide::Unknown,
                create_time: 0,
                update_time: 0,
                stop_price: req.stop_price,
            });
        }
        Ok(infos)
    }

    async fn cancel_order(&self, req: &CancelOrderRequest) -> Result<OrderInfo, ApiError> {
        let user = self.require_user()?;
        let asset = self.asset_index(&req.symbol).await?;
        let resp = if let Some(cloid) = &req.client_order_id {
            let action = ApiCancelByCloidAction {
                type_: "cancelByCloid".to_string(),
                cancels: vec![ApiCancelCloidWire {
                    asset,
                    cloid: cloid.clone(),
                }],
            };
            self.sign_and_post(&action).await?
        } else {
            let oid = match &req.order_id {
                Some(id) => id
                    .parse::<u64>()
                    .map_err(|_| ApiError::new("hyperliquid", "INVALID", "bad order id"))?,
                None => {
                    self.resolve_oid(&req.symbol, &req.client_order_id, user)
                        .await?
                }
            };
            let action = ApiCancelAction {
                type_: "cancel".to_string(),
                cancels: vec![ApiCancelWire { a: asset, o: oid }],
            };
            self.sign_and_post(&action).await?
        };
        let _ = resp;
        Ok(OrderInfo {
            symbol: req.symbol.clone(),
            order_id: req.order_id.clone().unwrap_or_default(),
            client_order_id: req.client_order_id.clone().unwrap_or_default(),
            side: ApiSide::Unknown,
            order_type: ApiOrderType::Unknown,
            status: ApiOrderStatus::Canceled,
            price: 0.0,
            qty: 0.0,
            executed_qty: 0.0,
            avg_price: 0.0,
            leaves_qty: 0.0,
            time_in_force: ApiTimeInForce::Unknown,
            reduce_only: false,
            position_side: ApiPositionSide::Unknown,
            create_time: 0,
            update_time: 0,
            stop_price: None,
        })
    }

    async fn cancel_orders(&self, reqs: &[CancelOrderRequest]) -> Result<Vec<OrderInfo>, ApiError> {
        let mut infos = Vec::with_capacity(reqs.len());
        for req in reqs {
            infos.push(self.cancel_order(req).await?);
        }
        Ok(infos)
    }

    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), ApiError> {
        let user = self.require_user()?;
        let asset = self.asset_index(symbol).await?;
        let orders = self.get_open_orders(user).await?;
        let cancels: Vec<ApiCancelWire> = orders
            .iter()
            .filter(|o| o.coin == symbol)
            .map(|o| ApiCancelWire { a: asset, o: o.oid })
            .collect();
        if cancels.is_empty() {
            return Ok(());
        }
        let action = ApiCancelAction {
            type_: "cancel".to_string(),
            cancels,
        };
        let _ = self.sign_and_post(&action).await?;
        Ok(())
    }

    async fn cancel_all_after(&self, timeout_ms: u64) -> Result<(), ApiError> {
        let time = chrono::Utc::now().timestamp_millis() as u64 + timeout_ms;
        let action = ApiScheduleCancelAction {
            type_: "scheduleCancel".to_string(),
            time,
        };
        let _ = self.sign_and_post(&action).await?;
        Ok(())
    }

    async fn amend_order(&self, req: &AmendOrderRequest) -> Result<OrderInfo, ApiError> {
        let user = self.require_user()?;
        let asset = self.asset_index(&req.symbol).await?;
        let oid = match &req.order_id {
            Some(id) => id
                .parse::<u64>()
                .map_err(|_| ApiError::new("hyperliquid", "INVALID", "bad order id"))?,
            None => {
                self.resolve_oid(&req.symbol, &req.client_order_id, user)
                    .await?
            }
        };
        let price = req
            .new_price
            .ok_or_else(|| ApiError::new("hyperliquid", "INVALID", "new_price required"))?;
        let qty = req
            .new_qty
            .ok_or_else(|| ApiError::new("hyperliquid", "INVALID", "new_qty required"))?;
        let action = ApiModifyAction {
            type_: "modify".to_string(),
            modifies: vec![ApiModifyWire {
                a: asset,
                o: oid,
                p: price.to_string(),
                s: qty.to_string(),
            }],
        };
        let _ = self.sign_and_post(&action).await?;
        Ok(OrderInfo {
            symbol: req.symbol.clone(),
            order_id: oid.to_string(),
            client_order_id: req.client_order_id.clone().unwrap_or_default(),
            side: ApiSide::Unknown,
            order_type: ApiOrderType::Unknown,
            status: ApiOrderStatus::New,
            price,
            qty,
            executed_qty: 0.0,
            avg_price: 0.0,
            leaves_qty: qty,
            time_in_force: ApiTimeInForce::Unknown,
            reduce_only: false,
            position_side: ApiPositionSide::Unknown,
            create_time: 0,
            update_time: 0,
            stop_price: None,
        })
    }

    async fn get_order(
        &self,
        symbol: &str,
        order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> Result<OrderInfo, ApiError> {
        let resp = self
            .get_order_status(order_id.and_then(|id| id.parse().ok()), client_order_id)
            .await?;
        if resp.status == "unknownOid" {
            return Err(ApiError::new(
                "hyperliquid",
                "NOT_FOUND",
                format!("unknown order: {}", resp.status),
            ));
        }
        let order = resp
            .order
            .ok_or_else(|| ApiError::new("hyperliquid", "EMPTY", "missing order in response"))?;
        let mut info = order_info_from_historical(&order);
        info.symbol = symbol.to_lowercase();
        info.status = status_from_hl(&resp.status);
        Ok(info)
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, ApiError> {
        let user = self.require_user()?;
        let orders = self.get_open_orders(user).await?;
        Ok(orders
            .iter()
            .filter(|o| o.coin == symbol)
            .map(|o| OrderInfo {
                symbol: o.coin.clone(),
                order_id: o.oid.to_string(),
                client_order_id: o.cloid.clone().unwrap_or_default(),
                side: side_from_hl(&o.side),
                order_type: ApiOrderType::Limit,
                status: ApiOrderStatus::New,
                price: f(&o.px),
                qty: f(&o.sz),
                executed_qty: 0.0,
                avg_price: 0.0,
                leaves_qty: f(&o.sz),
                time_in_force: ApiTimeInForce::GTC,
                reduce_only: false,
                position_side: ApiPositionSide::Unknown,
                create_time: 0,
                update_time: 0,
                stop_price: None,
            })
            .collect())
    }

    async fn get_order_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<OrderInfo>, ApiError> {
        let user = self.require_user()?;
        let orders = self.get_historical_orders(user).await?;
        Ok(orders
            .iter()
            .filter(|o| o.coin == symbol)
            .take(limit.min(500) as usize)
            .map(order_info_from_historical)
            .collect())
    }

    async fn get_fills(&self, symbol: &str, limit: u32) -> Result<Vec<Fill>, ApiError> {
        let user = self.require_user()?;
        let fills = self.get_user_fills(user).await?;
        Ok(fills
            .iter()
            .filter(|f| f.get("coin").and_then(|c| c.as_str()) == Some(symbol))
            .take(limit.min(500) as usize)
            .map(|fill| Fill {
                symbol: fill
                    .get("coin")
                    .and_then(|c| c.as_str())
                    .unwrap_or("")
                    .to_string(),
                trade_id: fill
                    .get("tid")
                    .and_then(|t| t.as_u64())
                    .unwrap_or(0)
                    .to_string(),
                order_id: fill
                    .get("oid")
                    .and_then(|t| t.as_u64())
                    .unwrap_or(0)
                    .to_string(),
                client_order_id: String::new(),
                price: fill
                    .get("px")
                    .and_then(|p| p.as_str())
                    .map(f)
                    .unwrap_or(0.0),
                qty: fill
                    .get("sz")
                    .and_then(|p| p.as_str())
                    .map(f)
                    .unwrap_or(0.0),
                side: fill
                    .get("side")
                    .and_then(|s| s.as_str())
                    .map(side_from_hl)
                    .unwrap_or(ApiSide::Unknown),
                fee: fill
                    .get("fee")
                    .and_then(|p| p.as_str())
                    .map(f)
                    .unwrap_or(0.0),
                fee_asset: fill
                    .get("feeToken")
                    .and_then(|p| p.as_str())
                    .unwrap_or("USDC")
                    .to_string(),
                realized_pnl: fill
                    .get("closedPnl")
                    .and_then(|p| p.as_str())
                    .map(f)
                    .unwrap_or(0.0),
                maker: false,
                timestamp: fill.get("time").and_then(|t| t.as_u64()).unwrap_or(0) as i64,
            })
            .collect())
    }

    async fn get_account(&self) -> Result<AccountInfo, ApiError> {
        let user = self.require_user()?;
        let state = self.get_clearinghouse_state_detail(user).await?;
        let summary = state.margin_summary;
        let account_value = f(&summary.account_value);
        Ok(AccountInfo {
            total_wallet_balance: account_value,
            total_margin_balance: f(&summary.total_margin_used) + account_value,
            total_unrealized_pnl: f(&summary.total_ntl_pos),
            available_balance: f(&state.withdrawable),
            balances: vec![Balance {
                asset: "USDC".to_string(),
                wallet_balance: account_value,
                available_balance: f(&state.withdrawable),
                unrealized_pnl: f(&summary.total_ntl_pos),
                margin_balance: account_value,
            }],
            timestamp: state.time as i64,
        })
    }

    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<PositionInfo>, ApiError> {
        let user = self.require_user()?;
        let state = self.get_clearinghouse_state_detail(user).await?;
        Ok(state
            .asset_positions
            .iter()
            .filter(|p| symbol.map(|s| p.position.coin == s).unwrap_or(true))
            .map(|p| PositionInfo {
                symbol: p.position.coin.clone(),
                position_side: ApiPositionSide::from_qty(f(&p.position.szi)),
                qty: f(&p.position.szi),
                entry_price: f(&p.position.entry_px),
                mark_price: 0.0,
                liquidation_price: p.position.liquidation_px.as_deref().map(f).unwrap_or(0.0),
                leverage: p
                    .position
                    .leverage
                    .as_ref()
                    .and_then(|l| l.value)
                    .unwrap_or(0) as f64,
                margin_type: p
                    .position
                    .leverage
                    .as_ref()
                    .map(|l| ApiMarginType::from_str(&l.type_))
                    .unwrap_or(ApiMarginType::Unknown),
                unrealized_pnl: f(&p.position.unrealized_pnl),
                realized_pnl: 0.0,
                notional: f(&p.position.position_value),
                update_time: p.position.update_time as i64,
            })
            .collect())
    }

    async fn set_leverage(
        &self,
        symbol: &str,
        leverage: f64,
        _position_side: Option<ApiPositionSide>,
    ) -> Result<LeverageInfo, ApiError> {
        let action = ApiUpdateLeverageAction {
            type_: "updateLeverage".to_string(),
            coin: symbol.to_string(),
            is_cross: true,
            leverage: leverage.round() as u64,
        };
        let _ = self.sign_and_post(&action).await?;
        Ok(LeverageInfo {
            symbol: symbol.to_string(),
            leverage,
            margin_type: ApiMarginType::Cross,
            position_side: ApiPositionSide::Unknown,
        })
    }

    async fn get_leverage(&self, symbol: &str) -> Result<LeverageInfo, ApiError> {
        let user = self.require_user()?;
        let state = self.get_clearinghouse_state_detail(user).await?;
        let pos = state
            .asset_positions
            .iter()
            .find(|p| p.position.coin == symbol);
        if let Some(p) = pos {
            return Ok(LeverageInfo {
                symbol: symbol.to_string(),
                leverage: p
                    .position
                    .leverage
                    .as_ref()
                    .and_then(|l| l.value)
                    .unwrap_or(0) as f64,
                margin_type: p
                    .position
                    .leverage
                    .as_ref()
                    .map(|l| ApiMarginType::from_str(&l.type_))
                    .unwrap_or(ApiMarginType::Unknown),
                position_side: ApiPositionSide::from_qty(f(&p.position.szi)),
            });
        }
        let meta = self.get_meta().await?;
        let max = meta
            .universe
            .iter()
            .find(|a| a.name == symbol)
            .map(|a| a.max_leverage as f64)
            .unwrap_or(0.0);
        Ok(LeverageInfo {
            symbol: symbol.to_string(),
            leverage: max,
            margin_type: ApiMarginType::Unknown,
            position_side: ApiPositionSide::Unknown,
        })
    }

    async fn get_fee_rates(&self, symbol: &str) -> Result<FeeRate, ApiError> {
        let user = self.require_user()?;
        let fees = self.get_user_fees(user).await?;
        Ok(FeeRate {
            symbol: symbol.to_string(),
            maker_fee: f(&fees.user_add_rate),
            taker_fee: f(&fees.user_cross_rate),
            timestamp: 0,
        })
    }

    async fn get_income_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<IncomeRecord>, ApiError> {
        let user = self.require_user()?;
        let end = chrono::Utc::now().timestamp_millis();
        let start = end - 3_600_000 * 24 * (limit.min(30) as i64);
        let funding = self.get_user_funding(user, start, end).await?;
        let mut records: Vec<IncomeRecord> = funding
            .into_iter()
            .filter_map(|r| {
                let amount = r
                    .delta
                    .as_ref()
                    .and_then(|d| d.get("amount"))
                    .and_then(|a| a.as_str())
                    .map(f)
                    .unwrap_or(0.0);
                Some(IncomeRecord {
                    symbol: symbol.to_string(),
                    income_type: "FUNDING".to_string(),
                    income: amount,
                    asset: "USDC".to_string(),
                    timestamp: r.time as i64,
                })
            })
            .collect();
        let ledger = self
            .get_user_non_funding_ledger_updates(user, start, end)
            .await?;
        records.extend(ledger.into_iter().filter_map(|r| {
            let typ = r.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let delta = r.get("delta")?;
            let amount = delta
                .get("amount")
                .or_else(|| delta.get("usdc"))
                .and_then(|a| a.as_str())
                .map(f)
                .unwrap_or(0.0);
            Some(IncomeRecord {
                symbol: symbol.to_string(),
                income_type: typ.to_string(),
                income: amount,
                asset: "USDC".to_string(),
                timestamp: r.get("time").and_then(|t| t.as_u64()).unwrap_or(0) as i64,
            })
        }));
        Ok(records)
    }
}

impl HyperliquidClient {
    fn require_user(&self) -> Result<&str, ApiError> {
        self.account_address().ok_or_else(|| {
            ApiError::new(
                "hyperliquid",
                "NO_SIGNER",
                "signer not configured; account operations unavailable",
            )
        })
    }

    /// 通过 orderStatusByCloid（或 openOrders 匹配）解析 cloid → oid。
    async fn resolve_oid(
        &self,
        _symbol: &str,
        client_order_id: &Option<String>,
        user: &str,
    ) -> Result<u64, ApiError> {
        if let Some(cloid) = client_order_id {
            if let Ok(resp) = self.get_order_status(None, Some(cloid)).await {
                if let Some(o) = resp.order {
                    return Ok(o.oid);
                }
            }
        }
        let orders = self.get_open_orders(user).await?;
        let cloid = client_order_id.as_deref();
        orders
            .iter()
            .find(|o| {
                cloid
                    .map(|c| o.cloid.as_deref() == Some(c))
                    .unwrap_or(false)
            })
            .map(|o| o.oid)
            .ok_or_else(|| ApiError::new("hyperliquid", "NOT_FOUND", "order not found"))
    }
}

/// 统一订单请求 → HL order wire。
async fn build_wire(
    client: &HyperliquidClient,
    req: &UnifiedOrderRequest,
) -> Result<ApiOrderWire, ApiError> {
    let asset = client.asset_index(&req.symbol).await?;
    build_wire_with_index(asset, req)
}

/// 统一订单请求 → HL order wire（资产索引由调用方提供，便于离线单测）。
pub(crate) fn build_wire_with_index(
    asset: u32,
    req: &UnifiedOrderRequest,
) -> Result<ApiOrderWire, ApiError> {
    let is_buy = side_to_hl(req.side)?;
    let (p, t) = match req.order_type {
        ApiOrderType::Limit => (
            req.price
                .ok_or_else(|| ApiError::new("hyperliquid", "INVALID", "limit price required"))?
                .to_string(),
            ApiOrderTypeWire::Limit(ApiLimitOrderType {
                limit: ApiTif {
                    tif: tif_to_hl(req.time_in_force).to_string(),
                },
            }),
        ),
        ApiOrderType::Market => (
            "0".to_string(),
            ApiOrderTypeWire::Market(ApiMarketOrderType {
                market: serde_json::json!({}),
            }),
        ),
        other => {
            let trigger_px = req
                .stop_price
                .or(req.price)
                .ok_or_else(|| ApiError::new("hyperliquid", "INVALID", "trigger price required"))?;
            let (is_market, tpsl) = match other {
                ApiOrderType::StopMarket => (true, "sl"),
                ApiOrderType::StopLimit => (false, "sl"),
                ApiOrderType::TakeProfitMarket => (true, "tp"),
                ApiOrderType::TakeProfitLimit => (false, "tp"),
                _ => {
                    return Err(ApiError::new(
                        "hyperliquid",
                        "INVALID",
                        "unsupported order type",
                    ));
                }
            };
            let p = if is_market {
                "0".to_string()
            } else {
                req.price
                    .ok_or_else(|| ApiError::new("hyperliquid", "INVALID", "limit price required"))?
                    .to_string()
            };
            (
                p,
                ApiOrderTypeWire::Trigger(ApiTriggerOrderType {
                    trigger: ApiTriggerWire {
                        is_market,
                        trigger_px: trigger_px.to_string(),
                        tpsl: tpsl.to_string(),
                    },
                }),
            )
        }
    };
    Ok(ApiOrderWire {
        a: asset,
        b: is_buy,
        p,
        s: req.qty.to_string(),
        r: req.reduce_only,
        t,
        c: req.client_order_id.clone(),
    })
}

/// AlgoOrderRequest（条件单）→ HL trigger order wire。
pub async fn build_algo_wire(
    client: &HyperliquidClient,
    req: &AlgoOrderRequest,
) -> Result<ApiOrderWire, ApiError> {
    let asset = client.asset_index(&req.symbol).await?;
    build_algo_wire_with_index(asset, req)
}

pub(crate) fn build_algo_wire_with_index(
    asset: u32,
    req: &AlgoOrderRequest,
) -> Result<ApiOrderWire, ApiError> {
    let is_buy = side_to_hl(req.side)?;
    let (is_market, tpsl) = match req.order_type {
        ApiOrderType::StopMarket => (true, "sl"),
        ApiOrderType::StopLimit => (false, "sl"),
        ApiOrderType::TakeProfitMarket => (true, "tp"),
        ApiOrderType::TakeProfitLimit => (false, "tp"),
        _ => {
            return Err(ApiError::new(
                "hyperliquid",
                "INVALID",
                "unsupported algo type",
            ));
        }
    };
    let p = if is_market {
        "0".to_string()
    } else {
        req.price
            .ok_or_else(|| ApiError::new("hyperliquid", "INVALID", "limit price required"))?
            .to_string()
    };
    Ok(ApiOrderWire {
        a: asset,
        b: is_buy,
        p,
        s: req.qty.to_string(),
        r: req.reduce_only.unwrap_or(false),
        t: ApiOrderTypeWire::Trigger(ApiTriggerOrderType {
            trigger: ApiTriggerWire {
                is_market,
                trigger_px: req.trigger_price.to_string(),
                tpsl: tpsl.to_string(),
            },
        }),
        c: req.client_order_id.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        AlgoOrderRequest, ApiOrderStatus, ApiOrderType, ApiPositionSide, ApiSide, ApiTimeInForce,
        UnifiedOrderRequest,
    };

    fn limit_req() -> UnifiedOrderRequest {
        UnifiedOrderRequest {
            symbol: "BTC".to_string(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Limit,
            price: Some(50000.0),
            qty: 1.0,
            time_in_force: ApiTimeInForce::GTC,
            reduce_only: false,
            position_side: Some(ApiPositionSide::Long),
            client_order_id: Some("c1".to_string()),
            stop_price: None,
        }
    }

    #[test]
    fn test_build_wire_limit() {
        let wire = build_wire_with_index(0, &limit_req()).unwrap();
        assert_eq!(wire.a, 0);
        assert!(wire.b);
        assert_eq!(wire.p, "50000");
        assert_eq!(wire.s, "1");
        assert_eq!(wire.c.as_deref(), Some("c1"));
        // 序列化检查字段结构与官方 wire 一致
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["t"]["limit"]["tif"], "Gtc");
        assert!(json.get("trigger_px").is_none());
    }

    #[test]
    fn test_build_wire_market() {
        let mut req = limit_req();
        req.order_type = ApiOrderType::Market;
        req.price = None;
        let wire = build_wire_with_index(0, &req).unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["t"]["market"], serde_json::json!({}));
        assert_eq!(json["p"], "0");
    }

    #[test]
    fn test_build_wire_stop() {
        let mut req = limit_req();
        req.order_type = ApiOrderType::StopMarket;
        req.stop_price = Some(49000.0);
        req.price = None;
        let wire = build_wire_with_index(0, &req).unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["t"]["trigger"]["isMarket"], true);
        assert_eq!(json["t"]["trigger"]["triggerPx"], "49000");
        assert_eq!(json["t"]["trigger"]["tpsl"], "sl");

        let mut req = limit_req();
        req.order_type = ApiOrderType::TakeProfitLimit;
        req.stop_price = Some(51000.0);
        let wire = build_wire_with_index(1, &req).unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["t"]["trigger"]["isMarket"], false);
        assert_eq!(json["t"]["trigger"]["tpsl"], "tp");
        assert_eq!(json["a"], 1);
    }

    #[test]
    fn test_build_algo_wire() {
        let req = AlgoOrderRequest {
            symbol: "BTC".to_string(),
            side: ApiSide::Sell,
            order_type: ApiOrderType::StopLimit,
            qty: 1.0,
            price: Some(50000.0),
            trigger_price: 51000.0,
            stop_price: None,
            reduce_only: Some(true),
            client_order_id: Some("a1".to_string()),
        };
        let wire = build_algo_wire_with_index(2, &req).unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["a"], 2);
        assert_eq!(json["b"], false);
        assert_eq!(json["t"]["trigger"]["isMarket"], false);
        assert_eq!(json["t"]["trigger"]["tpsl"], "sl");
    }

    #[test]
    fn test_side_mapping_hl() {
        assert_eq!(side_from_hl("A"), ApiSide::Sell);
        assert_eq!(side_from_hl("B"), ApiSide::Buy);
        assert_eq!(side_to_hl(ApiSide::Buy).unwrap(), true);
        assert_eq!(side_to_hl(ApiSide::Sell).unwrap(), false);
    }

    #[test]
    fn test_status_mapping_hl() {
        assert_eq!(status_from_hl("filled"), ApiOrderStatus::Filled);
        assert_eq!(status_from_hl("open"), ApiOrderStatus::New);
        assert_eq!(status_from_hl("canceled"), ApiOrderStatus::Canceled);
        assert_eq!(status_from_hl("triggered"), ApiOrderStatus::Triggered);
    }

    #[test]
    fn test_parse_meta_and_ctxs() {
        let resp = serde_json::json!([
            {"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 50}]},
            [{
                "funding": "0.0001",
                "openInterest": "123.4",
                "prevDayPx": "49900.0",
                "dayNtlVlm": "1000000.0",
                "premium": "0.00001",
                "markPx": "50000.0",
                "midPx": "50000.5",
                "oraclePx": "49990.0",
                "dayBaseVlm": "20.0"
            }]
        ]);
        let (universe, ctxs) = parse_meta_and_ctxs(&resp).unwrap();
        assert_eq!(universe[0].name, "BTC");
        assert_eq!(universe[0].sz_decimals, 5);
        assert_eq!(ctxs[0].funding, "0.0001");
        assert_eq!(ctxs[0].mark_px, "50000.0");
    }

    #[test]
    fn test_parse_asset_ctx_real_shape() {
        // 与实盘 metaAndAssetCtxs 中单个 ctx 完全一致的字段形状
        let json = r#"{"funding":"0.0000110336","openInterest":"39772.6187","prevDayPx":"64207.0","dayNtlVlm":"1391387868.7834703922","premium":"-0.0004033697","oraclePx":"64457.0","markPx":"64429.0","midPx":"64430.5","impactPxs":["64423.2","64431.0"],"dayBaseVlm":"21577.79143"}"#;
        let ctx: m::AssetCtx = serde_json::from_str(json).unwrap();
        assert_eq!(ctx.mark_px, "64429.0");
    }

    #[test]
    fn test_parse_clearinghouse_state_detail() {
        let json = r#"{
            "marginSummary": {
                "accountValue": "10000.0",
                "totalNtlPos": "100.0",
                "totalRawUsd": "9900.0",
                "totalMarginUsed": "100.0"
            },
            "crossMarginSummary": {
                "accountValue": "10000.0",
                "totalNtlPos": "100.0",
                "totalRawUsd": "9900.0",
                "totalMarginUsed": "100.0"
            },
            "withdrawable": "5000.0",
            "assetPositions": [{
                "position": {
                    "coin": "BTC",
                    "szi": "1.5",
                    "entryPx": "50000.0",
                    "positionValue": "75000.0",
                    "unrealizedPnl": "150.0",
                    "returnOnEquity": "0.01",
                    "liquidationPx": "45000.0",
                    "marginUsed": "100.0",
                    "maxLeverage": 50,
                    "leverage": {"type": "isolated", "value": 10},
                    "isPositionTpsl": false,
                    "cumulativeFunding": {"allTime": "1.0"},
                    "updateTime": 1700000000000
                }
            }],
            "time": 1700000000000
        }"#;
        let state: m::ClearinghouseStateDetail = serde_json::from_str(json).unwrap();
        assert_eq!(state.margin_summary.account_value, "10000.0");
        assert_eq!(state.withdrawable, "5000.0");
        assert_eq!(state.asset_positions.len(), 1);
        let p = &state.asset_positions[0].position;
        assert_eq!(p.coin, "BTC");
        assert_eq!(p.szi, "1.5");
        assert_eq!(p.entry_px, "50000.0");
        assert_eq!(p.leverage.as_ref().unwrap().value, Some(10));
    }

    #[test]
    fn test_parse_order_status_response() {
        let resp: m::OrderStatusResponse =
            serde_json::from_str(r#"{"status":"unknownOid"}"#).unwrap();
        assert_eq!(resp.status, "unknownOid");
        assert!(resp.order.is_none());

        let resp: m::OrderStatusResponse = serde_json::from_str(
            r#"{"status":"filled","order":{"coin":"BTC","oid":123,"side":"B","limitPx":"50000.0","sz":"1.0","origSz":"1.0","orderType":"Limit","filled":"1.0","avgPx":"50001.0","timestamp":1700000000000}}"#,
        )
        .unwrap();
        assert_eq!(resp.status, "filled");
        let info = order_info_from_historical(&resp.order.unwrap());
        assert_eq!(info.order_id, "123");
        assert_eq!(info.status, ApiOrderStatus::Filled);
        assert_eq!(info.side, ApiSide::Buy);
    }

    #[test]
    fn test_parse_l2_book() {
        let json = r#"{
            "coin": "BTC",
            "time": 1700000000000,
            "levels": [
                [{"px": "50000.0", "sz": "1.5", "n": 2}],
                [{"px": "50001.0", "sz": "2.0", "n": 1}]
            ]
        }"#;
        let book: m::L2BookData = serde_json::from_str(json).unwrap();
        assert_eq!(book.coin, "BTC");
        assert_eq!(book.levels.len(), 2);
        assert_eq!(book.levels[0][0].px, "50000.0");
    }

    #[test]
    fn test_parse_funding_history_and_candle() {
        let record: m::FundingHistoryRecord = serde_json::from_str(
            r#"{"coin":"BTC","fundingRate":"0.0001","premium":"0.00001","time":1700000000000}"#,
        )
        .unwrap();
        assert_eq!(record.funding_rate, "0.0001");

        let candle: m::Candle = serde_json::from_str(
            r#"{"t":1700000000000,"T":1700000060000,"s":"BTC","i":"1m","o":"50000.0","c":"50050.0","h":"50100.0","l":"49900.0","v":"10.0","n":100}"#,
        )
        .unwrap();
        assert_eq!(candle.i, "1m");
        assert_eq!(candle.c, "50050.0");
    }

    #[test]
    fn test_parse_recent_trades_and_bbo() {
        let trade: m::RecentTrade = serde_json::from_str(
            r#"{"coin":"BTC","side":"B","px":"50000.0","sz":"0.5","time":1700000000000,"tid":123,"hash":"0x0"}"#,
        )
        .unwrap();
        assert_eq!(trade.side, "B");
        assert_eq!(side_from_hl(&trade.side), ApiSide::Buy);

        let bbo: m::BboData = serde_json::from_str(
            r#"{"coin":"BTC","bbo":{"bids":[{"px":"50000.0","sz":"1.5"}],"asks":[{"px":"50001.0","sz":"2.0"}]},"time":1700000000000}"#,
        )
        .unwrap();
        assert_eq!(bbo.bbo.as_ref().unwrap().bids[0].px, "50000.0");
    }

    #[test]
    fn test_parse_exchange_status_and_fees() {
        let status: m::ExchangeStatus =
            serde_json::from_str(r#"{"specialStatuses":null,"time":1700000000000}"#).unwrap();
        assert_eq!(status.time, 1_700_000_000_000);

        let fees: m::UserFees = serde_json::from_str(
            r#"{"dailyUserVlm":[],"userCrossRate":"0.00045","userAddRate":"0.00015","userSpotCrossRate":"0.0007","userSpotAddRate":"0.0004"}"#,
        )
        .unwrap();
        assert_eq!(fees.user_cross_rate, "0.00045");
        assert_eq!(fees.user_add_rate, "0.00015");
    }

    #[test]
    fn test_parse_order_response() {
        // resting
        let resp: m::ExchangeResponse = serde_json::from_str(
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":77738308}}]}}}"#,
        )
        .unwrap();
        let client = HyperliquidClient::new("http://x", "http://y");
        let info = client.parse_order_response(&resp).unwrap();
        assert_eq!(info.order_id, "77738308");
        assert_eq!(info.status, ApiOrderStatus::New);

        // error
        let resp: m::ExchangeResponse = serde_json::from_str(
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order must have minimum value of $10."}]}}}"#,
        )
        .unwrap();
        let err = client.parse_order_response(&resp).unwrap_err();
        assert!(err.message.contains("minimum value"));
    }

    #[test]
    fn test_api_error_from_hl() {
        let err = ApiError::from(HyperliquidError::OrderError("rejected".to_string()));
        assert_eq!(err.exchange, "hyperliquid");
        assert!(err.message.contains("rejected"));
    }

    /// 实盘冒烟：Hyperliquid 公共接口（无需签名）。运行：
    /// `cargo test --all-features hyperliquid::brokerapi::tests::live -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn live_public_api_smoke() {
        /// 环境网络偶发 TLS 抖动/响应截断，最多重试 3 次。
        async fn retry<T, F, Fut>(mut f: F) -> T
        where
            F: FnMut() -> Fut,
            Fut: std::future::Future<Output = Result<T, ApiError>>,
        {
            let mut last = None;
            for attempt in 0..3 {
                match f().await {
                    Ok(v) => return v,
                    Err(e) => {
                        eprintln!("attempt {attempt} failed: {e:?}; retrying");
                        last = Some(e);
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
            }
            panic!("retry exhausted: {last:?}");
        }

        let client = HyperliquidClient::new(
            "https://api.hyperliquid.xyz/info",
            "https://api.hyperliquid.xyz/exchange",
        );
        let api: &dyn BrokerApi = &client;
        let time = retry(|| api.get_server_time()).await;
        assert!(time > 1_700_000_000_000);
        println!("server_time={time}");

        let instruments = retry(|| api.get_instruments()).await;
        println!("instruments={}", instruments.len());
        assert!(!instruments.is_empty());

        let ticker = retry(|| api.get_ticker("BTC")).await;
        println!("ticker={ticker:?}");
        assert!(ticker.last_price > 0.0);

        let book = retry(|| api.get_order_book("BTC", 20)).await;
        println!("book bids={} asks={}", book.bids.len(), book.asks.len());
        assert!(!book.bids.is_empty());
        assert!(!book.asks.is_empty());

        let funding = retry(|| api.get_funding_rate("BTC")).await;
        println!("funding={funding:?}");

        let oi = retry(|| api.get_open_interest("BTC")).await;
        println!("open_interest={oi:?}");
        assert!(oi.open_interest > 0.0);

        let trades = retry(|| api.get_trades("BTC", 5)).await;
        println!("trades={}", trades.len());
        assert!(!trades.is_empty());

        let klines = retry(|| api.get_klines("BTC", "1m", 5)).await;
        println!("klines={}", klines.len());
    }
}
