use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::Deserialize;

use super::{from_str_to_side, from_str_to_status, from_str_to_tif, from_str_to_type};
use crate::utils::{from_str_to_f64, from_str_to_f64_opt, from_str_to_i64, to_lowercase};

// ------------------------------------------------------------------
// 全量 REST 端点响应结构（对照官方文档）
// ------------------------------------------------------------------

/// GET /fapi/v1/ping 与 /fapi/v1/time 共用。
#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServerTime {
    pub server_time: i64,
}

/// GET /fapi/v1/exchangeInfo
#[derive(Deserialize, Debug)]
pub struct ExchangeInfo {
    #[serde(default)]
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SymbolInfo {
    pub symbol: String,
    #[serde(default)]
    pub pair: String,
    #[serde(default)]
    pub contract_type: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub base_asset: String,
    #[serde(default)]
    pub quote_asset: String,
    #[serde(default)]
    pub margin_asset: String,
    #[serde(default)]
    pub price_precision: u32,
    #[serde(default)]
    pub quantity_precision: u32,
    #[serde(default)]
    pub contract_size: String,
    #[serde(default)]
    pub filters: Vec<SymbolFilter>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SymbolFilter {
    pub filter_type: String,
    #[serde(default)]
    pub tick_size: Option<String>,
    #[serde(default)]
    pub step_size: Option<String>,
    #[serde(default)]
    pub min_qty: Option<String>,
    #[serde(default)]
    pub min_notional: Option<String>,
}

/// GET /fapi/v1/ticker/24hr
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Ticker24h {
    pub symbol: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub price_change: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub price_change_percent: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub last_price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub high_price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub low_price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub volume: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub quote_volume: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub open_price: f64,
    #[serde(default)]
    pub close_time: i64,
    #[serde(default)]
    pub count: i64,
}

/// GET /fapi/v1/premiumIndex
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PremiumIndex {
    pub symbol: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub mark_price: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub index_price: f64,
    #[serde(default)]
    pub last_funding_rate: Option<String>,
    #[serde(default)]
    pub next_funding_time: i64,
    #[serde(default)]
    pub time: i64,
}

/// GET /fapi/v1/ticker/price
#[derive(Deserialize, Debug, Clone)]
pub struct TickerPrice {
    pub symbol: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(default)]
    pub time: i64,
}

/// GET /fapi/v1/ticker/bookTicker
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BookTicker {
    pub symbol: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub bid_price: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub bid_qty: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub ask_price: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub ask_qty: f64,
    #[serde(default)]
    pub time: i64,
}

/// GET /fapi/v1/trades 与 /fapi/v1/historicalTrades
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PublicTrade {
    pub id: i64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub qty: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub quote_qty: f64,
    pub time: i64,
    pub is_buyer_maker: bool,
}

/// GET /fapi/v1/aggTrades
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AggTrade {
    #[serde(rename = "a")]
    pub agg_id: i64,
    #[serde(rename = "p")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "q")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub qty: f64,
    #[serde(rename = "f", default)]
    pub first_id: i64,
    #[serde(rename = "l", default)]
    pub last_id: i64,
    #[serde(rename = "T")]
    pub time: i64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

/// GET /fapi/v1/klines（数组格式）
pub type KlineResponse = Vec<Vec<serde_json::Value>>;

/// GET /fapi/v1/fundingRate
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FundingRateRecord {
    pub symbol: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub funding_rate: f64,
    #[serde(default)]
    pub funding_time: i64,
    #[serde(default)]
    pub mark_price: String,
}

/// GET /fapi/v1/fundingInfo
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FundingInfo {
    pub symbol: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub adjusted_funding_rate_cap: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub adjusted_funding_rate_floor: f64,
    #[serde(default)]
    pub funding_interval_hours: i64,
}

/// GET /fapi/v1/openInterest
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OpenInterest {
    pub symbol: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub open_interest: f64,
    #[serde(default)]
    pub time: i64,
}

/// GET /fapi/v1/order、/fapi/v1/openOrders、/fapi/v1/allOrders、/fapi/v1/openOrder
///
/// 注意：状态/方向/类型字段保留原始字符串，统一映射在 brokerapi 层完成，
/// 避免 REST 查询遇到 REJECTED 等状态时反序列化失败。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    #[serde(default)]
    pub avg_price: String,
    #[serde(default)]
    pub client_order_id: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub cum_quote: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub executed_qty: f64,
    #[serde(default)]
    pub order_id: i64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub orig_qty: f64,
    #[serde(default)]
    pub orig_type: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(default)]
    pub reduce_only: bool,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub position_side: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub stop_price: f64,
    #[serde(default)]
    pub close_position: bool,
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub time: i64,
    #[serde(default)]
    pub time_in_force: String,
    #[serde(rename = "type", default)]
    pub ty: String,
    #[serde(default)]
    pub update_time: i64,
    #[serde(default)]
    pub working_type: String,
    #[serde(default)]
    pub price_protect: bool,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub leaves_qty: f64,
}

/// GET /fapi/v1/userTrades
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserTrade {
    #[serde(default)]
    pub buyer: bool,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub commission: f64,
    #[serde(default)]
    pub commission_asset: String,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub maker: bool,
    #[serde(default)]
    pub order_id: i64,
    #[serde(default)]
    pub position_side: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub qty: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub quote_qty: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub realized_pnl: f64,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(default, deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(default)]
    pub time: i64,
}

/// GET /fapi/v2/balance 与 /fapi/v3/balance
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Balance {
    pub account_alias: String,
    pub asset: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub balance: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub cross_wallet_balance: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub cross_un_pnl: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub available_balance: f64,
    #[serde(default)]
    pub margin_available: bool,
    #[serde(default)]
    pub update_time: i64,
}

/// GET /fapi/v2/account 与 /fapi/v3/account
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub total_wallet_balance: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub total_unrealized_profit: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub total_margin_balance: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub available_balance: f64,
    #[serde(default)]
    pub update_time: i64,
    #[serde(default)]
    pub assets: Vec<AccountAsset>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AccountAsset {
    pub asset: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub wallet_balance: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub unrealized_profit: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub margin_balance: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub available_balance: f64,
    #[serde(default)]
    pub update_time: i64,
}

/// GET /fapi/v1/income
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Income {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub income_type: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub income: f64,
    #[serde(default)]
    pub asset: String,
    #[serde(default)]
    pub time: i64,
}

/// GET /fapi/v1/commissionRate
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CommissionRate {
    pub symbol: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub maker_commission_rate: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub taker_commission_rate: f64,
}

/// GET /fapi/v1/leverageBracket
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LeverageBracket {
    pub symbol: String,
    #[serde(default)]
    pub brackets: Vec<Bracket>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Bracket {
    #[serde(default)]
    pub bracket: i64,
    #[serde(default)]
    pub initial_leverage: i64,
    #[serde(default)]
    pub notional_cap: i64,
    #[serde(default)]
    pub notional_floor: i64,
    #[serde(default)]
    pub maint_margin_ratio: f64,
    #[serde(default)]
    pub cum: f64,
}

/// GET /fapi/v1/countdownCancelAll 响应
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CountdownCancelAll {
    pub symbol: String,
    #[serde(deserialize_with = "from_str_to_i64")]
    pub countdown_time: i64,
}

/// GET /fapi/v1/positionSide/dual、/fapi/v1/multiAssetsMargin 共用结构
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BoolSetting {
    pub dual_side_position: Option<bool>,
    pub multi_assets_margin: Option<bool>,
}

/// GET /fapi/v1/adlQuantile
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AdlQuantile {
    pub symbol: String,
    #[serde(default)]
    pub adl_quantile: serde_json::Value,
}

/// GET /futures/data/openInterestHist 等数据接口的通用行
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DataRow {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub sum_open_interest: String,
    #[serde(default)]
    pub sum_open_interest_value: String,
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default)]
    pub long_short_ratio: String,
    #[serde(default)]
    pub long_account: String,
    #[serde(default)]
    pub short_account: String,
    #[serde(default)]
    pub long_position: String,
    #[serde(default)]
    pub short_position: String,
    #[serde(default)]
    pub buy_sell_ratio: String,
    #[serde(default)]
    pub buy_vol: String,
    #[serde(default)]
    pub sell_vol: String,
}

/// GET /fapi/v1/income/asyn 与 /fapi/v1/order/asyn、/fapi/v1/trade/asyn
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AsyncDownloadId {
    #[serde(rename = "downloadId", alias = "id")]
    pub id: String,
    #[serde(default)]
    pub expired_timestamp: i64,
    #[serde(default)]
    pub status: String,
}

/// GET /fapi/v1/income/asyn/id 等
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AsyncDownloadLink {
    #[serde(default)]
    pub download_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub notified: bool,
    #[serde(default)]
    pub expiration_timestamp: i64,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum OrderResponseResult {
    Ok(OrderResponse),
    Err(ErrorResponse),
}

#[derive(Deserialize, Debug)]
pub struct OrderResponse {
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    #[serde(rename = "cumQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub cum_qty: f64,
    /// New Order and Cancel Order responses only field
    #[serde(rename = "cumQuote")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub cum_quote: Option<f64>,
    /// Modify Order response only field
    #[serde(rename = "cumBase")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub cum_base: Option<f64>,
    #[serde(rename = "executedQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub executed_qty: f64,
    #[serde(rename = "orderId")]
    pub order_id: i64,
    /// New Order and Modify Order responses only field
    #[serde(rename = "avgPrice")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub avg_price: Option<f64>,
    #[serde(rename = "origQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub orig_qty: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "reduceOnly")]
    pub reduce_only: bool,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "positionSide")]
    pub position_side: String,
    #[serde(deserialize_with = "from_str_to_status")]
    pub status: Status,
    #[serde(rename = "stopPrice")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub stop_price: f64,
    #[serde(rename = "closePosition")]
    pub close_position: bool,
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    // for Coin-M futures
    // pub pair: String,
    /// Modify Order response only field
    #[serde(default)]
    pub pair: Option<String>,
    #[serde(rename = "timeInForce")]
    #[serde(deserialize_with = "from_str_to_tif")]
    pub time_in_force: TimeInForce,
    #[serde(rename = "type")]
    #[serde(deserialize_with = "from_str_to_type")]
    pub ty: OrdType,
    #[serde(rename = "origType")]
    #[serde(deserialize_with = "from_str_to_type")]
    pub orig_type: OrdType,
    /// New Order and Cancel Order responses only field
    #[serde(rename = "activatePrice")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub activate_price: Option<f64>,
    /// New Order and Cancel Order responses only field
    #[serde(rename = "priceRate")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub price_rate: Option<f64>,
    #[serde(rename = "updateTime")]
    pub update_time: i64,
    #[serde(rename = "workingType")]
    pub working_type: String,
    #[serde(rename = "priceProtect")]
    pub price_protect: bool,
    #[serde(rename = "priceMatch")]
    pub price_match: String,
    #[serde(rename = "selfTradePreventionMode")]
    pub self_trade_prevention_mode: String,
    #[serde(rename = "goodTillDate")]
    pub good_till_date: i64,
}

#[derive(Deserialize, Debug)]
pub struct ErrorResponse {
    pub code: i64,
    pub msg: String,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum ApiResponse<T> {
    Error(ErrorResponse),
    Success(T),
}

#[derive(Deserialize, Debug)]
pub struct PositionInformationV2 {
    #[serde(rename = "entryPrice")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub entry_price: f64,
    #[serde(rename = "breakEvenPrice")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub breakeven_price: f64,
    #[serde(rename = "marginType")]
    pub margin_type: String,
    #[serde(rename = "isAutoAddMargin")]
    pub is_auto_add_margin: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub leverage: f64,
    #[serde(rename = "liquidationPrice")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub liquidation_price: f64,
    #[serde(rename = "markPrice")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub mark_price: f64,
    #[serde(rename = "maxNotionalValue")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub max_notional_value: f64,
    #[serde(rename = "positionAmt")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub position_amount: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub notional: f64,
    #[serde(rename = "isolatedWallet")]
    pub isolated_wallet: String,
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "unRealizedProfit")]
    pub unrealized_pnl: String,
    #[serde(rename = "positionSide")]
    pub position_side: String,
    #[serde(rename = "updateTime")]
    pub update_time: i64,
}

#[derive(Deserialize, Debug)]
pub struct Depth {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: i64,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
}
