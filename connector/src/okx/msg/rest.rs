#![allow(dead_code)]
use crate::utils::{from_lenient_bool, from_str_to_f64};
use serde::{Deserialize, Serialize};

/// OKX 统一响应包装：`{code, msg, data: [...]}`。
///
/// 说明：OKX V5 所有 REST 响应都包含 `data` 数组（出错时为空数组），
/// 因此不标注 serde(default)，避免对 `T: Default` 的额外约束。
#[derive(Deserialize, Debug)]
pub struct OkxResponse<T> {
    pub code: String,
    #[serde(default)]
    pub msg: String,
    pub data: Vec<T>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OrderRequest {
    pub inst_id: String,
    pub td_mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cl_ord_id: Option<String>,
    pub side: String,
    pub ord_type: String,
    pub sz: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub px: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct OrderResponse {
    pub code: String,
    #[serde(default)]
    pub msg: String,
    #[serde(default)]
    pub data: Vec<OrderResult>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OrderResult {
    #[serde(default)]
    pub cl_ord_id: String,
    #[serde(default)]
    pub ord_id: String,
    #[serde(default)]
    pub s_code: String,
    #[serde(default)]
    pub s_msg: String,
}

#[derive(Deserialize, Debug)]
pub struct CancelResponse {
    pub code: String,
    #[serde(default)]
    pub msg: String,
    #[serde(default)]
    pub data: Vec<CancelResult>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CancelResult {
    #[serde(default)]
    pub cl_ord_id: String,
    #[serde(default)]
    pub ord_id: String,
    #[serde(default)]
    pub s_code: String,
    #[serde(default)]
    pub s_msg: String,
    /// 撤单完成时间（毫秒 epoch 字符串），REST 撤单事实 exchange_ts 的来源。
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug)]
pub struct BooksResponse {
    pub code: String,
    #[serde(default)]
    pub msg: String,
    #[serde(default)]
    pub data: Vec<Books>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Books {
    pub asks: Vec<Vec<String>>,
    pub bids: Vec<Vec<String>>,
    #[serde(default)]
    pub ts: String,
    #[serde(default, rename = "seqId")]
    pub seq_id: i64,
    #[serde(default, rename = "prevSeqId")]
    pub prev_seq_id: i64,
    #[serde(default)]
    pub checksum: i64,
}

#[derive(Deserialize, Debug)]
pub struct PositionsResponse {
    pub code: String,
    #[serde(default)]
    pub msg: String,
    #[serde(default)]
    pub data: Vec<Position>,
}

#[derive(Default, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub inst_id: String,
    #[serde(default)]
    pub pos_side: String,
    #[serde(default)]
    pub pos: String,
    #[serde(default)]
    pub u_time: String,
    #[serde(default)]
    pub avail_pos: String,
    #[serde(default)]
    pub avg_px: String,
    #[serde(default)]
    pub mark_px: String,
    #[serde(default)]
    pub liq_px: String,
    #[serde(default)]
    pub lever: String,
    #[serde(default)]
    pub mgn_mode: String,
    #[serde(default)]
    pub upl: String,
    #[serde(default)]
    pub realized_pnl: String,
    #[serde(default)]
    pub notional_usd: String,
    #[serde(default)]
    pub c_time: String,
    #[serde(default)]
    pub mgn_ratio: String,
}

#[derive(Deserialize, Debug)]
pub struct InstrumentsResponse {
    pub code: String,
    #[serde(default)]
    pub msg: String,
    #[serde(default)]
    pub data: Vec<Instrument>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Instrument {
    pub inst_id: String,
    #[serde(default)]
    pub lot_sz: String,
    #[serde(default)]
    pub tick_sz: String,
    #[serde(default)]
    pub min_sz: String,
    #[serde(default)]
    pub ct_val: String,
    #[serde(default)]
    pub ct_val_ccy: String,
    #[serde(default)]
    pub settle_ccy: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub ct_type: String,
    #[serde(default)]
    pub lever: String,
}

// ------------------------------------------------------------------
// 账户（Account）
// ------------------------------------------------------------------

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Balance {
    #[serde(default)]
    pub ccy: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub total_eq: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub adj_eq: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub avail_eq: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub cash_bal: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub u_pnl: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub iso_eq: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub ord_frozen: f64,
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AccountBalance {
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub total_eq: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub adj_eq: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub iso_eq: f64,
    #[serde(default)]
    pub imr: String,
    #[serde(default)]
    pub mmr: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub notional_usd: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub mgn_ratio: f64,
    #[serde(default)]
    pub details: Vec<Balance>,
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AccountConfig {
    #[serde(default)]
    pub uid: String,
    #[serde(default)]
    pub acct_lv: String,
    #[serde(default)]
    pub pos_mode: String,
    #[serde(default)]
    pub auto_loan: bool,
    #[serde(default)]
    pub mgn_iso_mode: String,
    #[serde(default)]
    pub level: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Leverage {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub lever: String,
    #[serde(default)]
    pub mgn_mode: String,
    #[serde(default)]
    pub pos_side: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MaxSize {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub max_buy: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub max_sell: f64,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MaxAvailSize {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub avail_buy: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub avail_sell: f64,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeFee {
    #[serde(default)]
    pub level: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub taker: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub maker: f64,
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MaxWithdrawal {
    #[serde(default)]
    pub ccy: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub max_wd: f64,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RiskState {
    #[serde(default)]
    pub mgn_ratio: String,
    #[serde(default)]
    pub total_avail_bal: String,
    #[serde(default)]
    pub adj_eq: String,
    #[serde(default)]
    pub ts: String,
}

// ------------------------------------------------------------------
// 交易（Trade）
// ------------------------------------------------------------------

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub ord_id: String,
    #[serde(default)]
    pub cl_ord_id: String,
    #[serde(default)]
    pub tag: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub sz: String,
    #[serde(default)]
    pub ord_type: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub pos_side: String,
    #[serde(default)]
    pub td_mode: String,
    #[serde(default)]
    pub fill_px: String,
    #[serde(default)]
    pub trade_id: String,
    #[serde(default)]
    pub fill_sz: String,
    #[serde(default)]
    pub fill_pnl: String,
    #[serde(default)]
    pub avg_px: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub lever: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub fee: f64,
    #[serde(default)]
    pub fee_ccy: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub rebate: f64,
    #[serde(default)]
    pub rebate_ccy: String,
    #[serde(default, deserialize_with = "from_lenient_bool")]
    pub reduce_only: bool,
    #[serde(default)]
    pub sl_trigger_px: String,
    #[serde(default)]
    pub tp_trigger_px: String,
    #[serde(default)]
    pub c_time: String,
    #[serde(default)]
    pub u_time: String,
    #[serde(default)]
    pub tgt_ccy: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Fill {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub trade_id: String,
    #[serde(default)]
    pub ord_id: String,
    #[serde(default)]
    pub cl_ord_id: String,
    #[serde(default)]
    pub fill_px: String,
    #[serde(default)]
    pub fill_sz: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub pos_side: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub fee: f64,
    #[serde(default)]
    pub fee_ccy: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub rebate: f64,
    #[serde(default)]
    pub rebate_ccy: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub pnl: f64,
    #[serde(default)]
    pub fill_time: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AmendResult {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub ord_id: String,
    #[serde(default)]
    pub cl_ord_id: String,
    #[serde(default)]
    pub s_code: String,
    #[serde(default)]
    pub s_msg: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ClosePositionResult {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub pos_side: String,
    #[serde(default)]
    pub cl_ord_id: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MassCancelResult {
    #[serde(default)]
    pub result: bool,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CancelAllAfter {
    #[serde(default)]
    pub trigger_time: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlgoOrder {
    #[serde(default)]
    pub algo_id: String,
    #[serde(default)]
    pub cl_algo_id: String,
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub ord_type: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub sz: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub trigger_px: String,
    #[serde(default)]
    pub sl_trigger_px: String,
    #[serde(default)]
    pub tp_trigger_px: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub c_time: String,
    #[serde(default)]
    pub u_time: String,
}

// ------------------------------------------------------------------
// 行情（Market Data）
// ------------------------------------------------------------------

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Ticker {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub last: String,
    #[serde(default)]
    pub last_sz: String,
    #[serde(default)]
    pub ask_px: String,
    #[serde(default)]
    pub ask_sz: String,
    #[serde(default)]
    pub bid_px: String,
    #[serde(default)]
    pub bid_sz: String,
    #[serde(default)]
    pub open24h: String,
    #[serde(default)]
    pub high24h: String,
    #[serde(default)]
    pub low24h: String,
    #[serde(default)]
    pub vol24h: String,
    #[serde(default)]
    pub vol_ccy24h: String,
    #[serde(default)]
    pub ts: String,
}

/// GET /api/v5/market/candles（数组格式）
pub type CandleResponse = Vec<Vec<String>>;

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PublicTrade {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub trade_id: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub sz: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FundingRate {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub funding_rate: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub next_funding_rate: f64,
    #[serde(default)]
    pub funding_time: String,
    #[serde(default)]
    pub next_funding_time: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FundingRateHistory {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub funding_rate: f64,
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub realized_rate: f64,
    #[serde(default)]
    pub funding_time: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OpenInterest {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub oi: String,
    #[serde(default)]
    pub oi_ccy: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PriceLimit {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub buy_lmt: String,
    #[serde(default)]
    pub sell_lmt: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarkPrice {
    #[serde(default)]
    pub inst_type: String,
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub mark_px: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SystemTime {
    #[serde(default)]
    pub ts: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SystemStatus {
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub begin: String,
    #[serde(default)]
    pub end: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub service_type: String,
}
