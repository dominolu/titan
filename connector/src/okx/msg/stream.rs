use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug)]
pub struct WsRequest {
    pub op: String,
    pub args: Vec<WsArg>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WsArg {
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inst_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inst_type: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct LoginRequest {
    pub op: String,
    pub args: Vec<LoginArg>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LoginArg {
    pub api_key: String,
    pub passphrase: String,
    pub timestamp: String,
    pub sign: String,
}

/// A message received on the public or private WebSocket stream.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum StreamMsg {
    Data(DataMsg),
    Ack(AckMsg),
}

#[derive(Deserialize, Debug)]
pub struct AckMsg {
    #[serde(default)]
    pub event: Option<String>,
    #[serde(default)]
    pub op: Option<String>,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub msg: Option<String>,
    #[serde(default)]
    pub conn_id: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct DataMsg {
    pub arg: WsArg,
    pub data: Vec<serde_json::Value>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub ts: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderUpdate {
    pub inst_id: String,
    #[serde(default)]
    pub ord_id: String,
    #[serde(default)]
    pub cl_ord_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub ord_type: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub sz: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub acc_fill_sz: String,
    #[serde(default)]
    pub fill_px: String,
    #[serde(default)]
    pub avg_px: String,
    #[serde(default)]
    pub u_time: String,
    #[serde(default)]
    pub c_time: String,
    #[serde(default)]
    pub pos_side: String,
    #[serde(default)]
    pub td_mode: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
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
    pub inst_id: String,
    #[serde(default)]
    pub funding_rate: String,
    #[serde(default)]
    pub funding_time: String,
    #[serde(default)]
    pub next_funding_rate: String,
    #[serde(default)]
    pub next_funding_time: String,
}

// ------------------------------------------------------------------
// 补充公共频道
// ------------------------------------------------------------------

/// `books5` 频道的精简订单簿。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Books5 {
    pub asks: Vec<Vec<String>>,
    pub bids: Vec<Vec<String>>,
    #[serde(default)]
    pub ts: String,
}

/// `bbo-tbt` 频道的最优买卖价。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BboTbt {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub ask_px: String,
    #[serde(default)]
    pub ask_sz: String,
    #[serde(default)]
    pub bid_px: String,
    #[serde(default)]
    pub bid_sz: String,
    #[serde(default)]
    pub ts: String,
}

/// `tickers` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TickerWs {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub last: String,
    #[serde(default)]
    pub ask_px: String,
    #[serde(default)]
    pub bid_px: String,
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

/// `open-interest` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OpenInterestWs {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub oi: String,
    #[serde(default)]
    pub oi_ccy: String,
    #[serde(default)]
    pub ts: String,
}

/// `mark-price` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarkPriceWs {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub mark_px: String,
    #[serde(default)]
    pub ts: String,
}

/// `index-tickers` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexTickerWs {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub idx_px: String,
    #[serde(default)]
    pub ts: String,
}

/// `estimated-price` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EstimatedPriceWs {
    #[serde(default)]
    pub inst_type: String,
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub est_px: String,
    #[serde(default)]
    pub ts: String,
}

/// `liquidation-orders` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LiquidationOrderWs {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub sz: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub ts: String,
}

/// `adl-warning` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AdlWarningWs {
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub pos_side: String,
    #[serde(default)]
    pub adl: String,
    #[serde(default)]
    pub ts: String,
}

/// `status` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StatusWs {
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub begin: String,
    #[serde(default)]
    pub end: String,
    #[serde(default)]
    pub service_type: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub ts: String,
}

/// `candles` 频道（数组格式）。
pub type CandleWs = Vec<Vec<String>>;

// ------------------------------------------------------------------
// 补充私有频道
// ------------------------------------------------------------------

/// `account` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AccountWs {
    #[serde(default)]
    pub u_time: String,
    #[serde(default)]
    pub total_eq: String,
    #[serde(default)]
    pub adj_eq: String,
    #[serde(default)]
    pub iso_eq: String,
    #[serde(default)]
    pub avail_eq: String,
    #[serde(default)]
    pub mgn_ratio: String,
    #[serde(default)]
    pub details: Vec<serde_json::Value>,
}

/// `balance-and-position` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BalanceAndPositionWs {
    #[serde(default)]
    pub u_time: String,
    #[serde(default)]
    pub p_time: String,
    #[serde(default)]
    pub event_type: String,
    #[serde(default)]
    pub bal_data: Vec<serde_json::Value>,
    #[serde(default)]
    pub pos_data: Vec<serde_json::Value>,
}

/// `orders-algo` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrdersAlgoWs {
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
    pub state: String,
    #[serde(default)]
    pub trigger_px: String,
    #[serde(default)]
    pub ts: String,
}

/// `algo-advance` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlgoAdvanceWs {
    #[serde(default)]
    pub algo_id: String,
    #[serde(default)]
    pub inst_id: String,
    #[serde(default)]
    pub ord_type: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub ts: String,
}
