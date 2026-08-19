use serde::{Deserialize, Deserializer, Serialize};

/// HL 部分字段可能为 null（如 `premium`/`midPx`），统一容错为默认字符串。
pub fn from_str_or_default<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let v = Option::<String>::deserialize(deserializer)?;
    Ok(v.unwrap_or_default())
}

// ---------------------------------------------------------------------------
// Wire models
// ---------------------------------------------------------------------------

#[derive(Serialize, Debug, Clone)]
pub struct Tif {
    pub tif: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct OrderTypeWire {
    pub limit: Tif,
}

/// A single order in the wire format. Field order matters for the msgpack hash.
#[derive(Serialize, Debug, Clone)]
pub struct OrderWire {
    pub a: u32,
    pub b: bool,
    pub p: String,
    pub s: String,
    pub r: bool,
    pub t: OrderTypeWire,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
}

/// The `order` L1 action. Field order matters for the msgpack hash.
#[derive(Serialize, Debug, Clone)]
pub struct OrderAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub orders: Vec<OrderWire>,
    pub grouping: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct CancelWire {
    pub a: u32,
    pub o: u64,
}

/// The `cancel` L1 action.
#[derive(Serialize, Debug, Clone)]
pub struct CancelAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub cancels: Vec<CancelWire>,
}

#[derive(Serialize, Debug, Clone)]
pub struct CancelByCloidWire {
    pub asset: u32,
    pub cloid: String,
}

/// The `cancelByCloid` L1 action.
#[derive(Serialize, Debug, Clone)]
pub struct CancelByCloidAction {
    #[serde(rename = "type")]
    pub type_: String,
    pub cancels: Vec<CancelByCloidWire>,
}

/// Either cancel flavor, serialized untagged so the field order of the inner action
/// is preserved both in the msgpack hash and in the JSON request body.
#[derive(Serialize, Debug)]
#[serde(untagged)]
pub enum CancelActionWire {
    Oid(CancelAction),
    Cloid(CancelByCloidAction),
}

/// Body of `POST /exchange`.
#[derive(Serialize, Debug)]
pub struct ExchangeRequest<A> {
    pub action: A,
    pub nonce: u64,
    pub signature: crate::hyperliquid::signing::L1Signature,
}

#[derive(Deserialize, Debug)]
pub struct ExchangeResponse {
    pub status: String,
    #[serde(default)]
    pub response: Option<ExchangeResponseData>,
}

#[derive(Deserialize, Debug)]
pub struct ExchangeResponseData {
    #[serde(rename = "type")]
    #[serde(default)]
    pub type_: String,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum OrderStatus {
    Resting { resting: Resting },
    Filled { filled: Filled },
    Error { error: String },
}

#[derive(Deserialize, Debug)]
pub struct Resting {
    pub oid: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Filled {
    #[serde(default)]
    pub total_sz: String,
    #[serde(default)]
    pub avg_px: String,
    pub oid: u64,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum CancelStatus {
    Success(String),
    Error { error: String },
}

// ---------------------------------------------------------------------------
// Info endpoint models
// ---------------------------------------------------------------------------

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AssetMeta {
    pub name: String,
    #[serde(default)]
    pub sz_decimals: u32,
    #[serde(default)]
    pub max_leverage: u32,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Meta {
    pub universe: Vec<AssetMeta>,
}

#[derive(Deserialize, Debug)]
pub struct ClearinghouseState {
    #[serde(default)]
    pub asset_positions: Vec<AssetPosition>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AssetPosition {
    pub position: Position,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub coin: String,
    #[serde(default)]
    pub szi: String,
    #[serde(default)]
    pub update_time: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OpenOrder {
    pub coin: String,
    pub oid: u64,
    #[serde(default)]
    pub cloid: Option<String>,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub sz: String,
}

// ---------------------------------------------------------------------------
// WebSocket models
// ---------------------------------------------------------------------------

#[derive(Deserialize, Debug)]
pub struct WsMsg {
    pub channel: String,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
    #[serde(default)]
    pub subscription: Option<serde_json::Value>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub res: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct WsSubscribe {
    pub method: String,
    pub subscription: serde_json::Value,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct L2BookData {
    pub coin: String,
    pub time: u64,
    pub levels: Vec<Vec<Level>>,
}

#[derive(Deserialize, Debug)]
pub struct Level {
    pub px: String,
    pub sz: String,
    pub n: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub coin: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub sz: String,
    pub time: u64,
    pub tid: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OrderUpdate {
    pub order: OrderState,
    pub status: String,
    #[serde(default)]
    pub status_timestamp: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OrderState {
    pub coin: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub limit_px: String,
    #[serde(default)]
    pub sz: String,
    pub oid: u64,
    #[serde(default)]
    pub timestamp: u64,
    #[serde(default)]
    pub orig_sz: String,
    #[serde(default)]
    pub filled: String,
    #[serde(default)]
    pub avg_px: String,
    #[serde(default)]
    pub cloid: Option<String>,
    #[serde(default)]
    pub reduce_only: bool,
    #[serde(default)]
    pub tif: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserEvent {
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(default)]
    pub fill: Option<Fill>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Fill {
    pub coin: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub sz: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub time: u64,
    #[serde(default)]
    pub start_position: String,
}

// ---------------------------------------------------------------------------
// Broker API 模型（官方 info/exchange 响应）
// ---------------------------------------------------------------------------

#[derive(Default, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarginSummary {
    #[serde(default)]
    pub account_value: String,
    #[serde(default)]
    pub total_ntl_pos: String,
    #[serde(default)]
    pub total_raw_usd: String,
    #[serde(default)]
    pub total_margin_used: String,
}

#[derive(Default, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CrossMarginSummary {
    #[serde(default)]
    pub account_value: String,
    #[serde(default)]
    pub total_ntl_pos: String,
    #[serde(default)]
    pub total_raw_usd: String,
    #[serde(default)]
    pub total_margin_used: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Leverage {
    #[serde(rename = "type", default)]
    pub type_: String,
    #[serde(default)]
    pub value: Option<u64>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PositionDetail {
    #[serde(default)]
    pub coin: String,
    #[serde(default)]
    pub szi: String,
    #[serde(default)]
    pub entry_px: String,
    #[serde(default)]
    pub position_value: String,
    #[serde(default)]
    pub unrealized_pnl: String,
    #[serde(default)]
    pub return_on_equity: String,
    #[serde(default)]
    pub liquidation_px: Option<String>,
    #[serde(default)]
    pub margin_used: String,
    #[serde(default)]
    pub max_leverage: u64,
    #[serde(default)]
    pub leverage: Option<Leverage>,
    #[serde(default)]
    pub is_position_tpsl: bool,
    #[serde(default)]
    pub cumulative_funding: Option<serde_json::Value>,
    #[serde(default)]
    pub update_time: u64,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ClearinghouseStateDetail {
    #[serde(default)]
    pub margin_summary: MarginSummary,
    #[serde(default)]
    pub cross_margin_summary: CrossMarginSummary,
    #[serde(default)]
    pub withdrawable: String,
    #[serde(default)]
    pub asset_positions: Vec<AssetPositionDetail>,
    #[serde(default)]
    pub time: u64,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AssetPositionDetail {
    pub position: PositionDetail,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AssetCtx {
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub funding: String,
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub open_interest: String,
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub prev_day_px: String,
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub day_ntl_vlm: String,
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub premium: String,
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub mark_px: String,
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub mid_px: String,
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub oracle_px: String,
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub day_base_vlm: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    #[serde(default)]
    pub t: u64,
    #[serde(default)]
    pub t_close: u64,
    #[serde(default)]
    pub s: String,
    #[serde(default)]
    pub i: String,
    #[serde(default)]
    pub o: String,
    #[serde(default)]
    pub c: String,
    #[serde(default)]
    pub h: String,
    #[serde(default)]
    pub l: String,
    #[serde(default)]
    pub v: String,
    #[serde(default)]
    pub n: u64,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FundingHistoryRecord {
    #[serde(default)]
    pub coin: String,
    #[serde(default)]
    pub funding_rate: String,
    #[serde(default)]
    pub premium: String,
    #[serde(default)]
    pub time: u64,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RecentTrade {
    #[serde(default)]
    pub coin: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub sz: String,
    #[serde(default)]
    pub time: u64,
    #[serde(default)]
    pub tid: u64,
    #[serde(default)]
    pub hash: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalOrder {
    #[serde(default)]
    pub coin: String,
    #[serde(default)]
    pub oid: u64,
    #[serde(default)]
    pub cloid: Option<String>,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub limit_px: String,
    #[serde(default)]
    pub sz: String,
    #[serde(default)]
    pub orig_sz: String,
    #[serde(default)]
    pub order_type: String,
    #[serde(default)]
    pub reduce_only: bool,
    #[serde(default)]
    pub timestamp: u64,
    #[serde(default)]
    pub trigger_px: String,
    #[serde(default)]
    pub filled: String,
    #[serde(default)]
    pub avg_px: String,
}

/// orderStatus 响应：`{"status": "unknownOid"}` 或 `{"status": "...", "order": {...}}`。
#[derive(Deserialize, Debug, Clone)]
pub struct OrderStatusResponse {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub order: Option<HistoricalOrder>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserFees {
    #[serde(default)]
    pub daily_user_vlm: Vec<serde_json::Value>,
    #[serde(default)]
    pub user_cross_rate: String,
    #[serde(default)]
    pub user_add_rate: String,
    #[serde(default)]
    pub user_spot_cross_rate: String,
    #[serde(default)]
    pub user_spot_add_rate: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeStatus {
    #[serde(default)]
    pub special_statuses: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub time: u64,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserFundingRecord {
    #[serde(default)]
    pub time: u64,
    #[serde(default)]
    pub delta: Option<serde_json::Value>,
}

/// `bbo` 频道。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BboData {
    #[serde(default)]
    pub coin: String,
    #[serde(default)]
    pub bbo: Option<BboLevels>,
    #[serde(default)]
    pub time: u64,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BboLevels {
    #[serde(default)]
    pub bids: Vec<BboLevel>,
    #[serde(default)]
    pub asks: Vec<BboLevel>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BboLevel {
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub sz: String,
}

/// `userFundings` 频道：资金费快照 + 每小时结算推送。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WsUserFundings {
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub fundings: Vec<WsUserFunding>,
    #[serde(default)]
    pub is_snapshot: bool,
}

/// 单笔资金费记录。
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WsUserFunding {
    #[serde(default)]
    pub time: u64,
    #[serde(default)]
    pub coin: String,
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub usdc: String,
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub szi: String,
    #[serde(default, deserialize_with = "from_str_or_default")]
    pub funding_rate: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_order_status_variants() {
        let resting: OrderStatus = serde_json::from_str(r#"{"resting":{"oid":42}}"#).unwrap();
        assert!(matches!(
            resting,
            OrderStatus::Resting {
                resting: Resting { oid: 42 }
            }
        ));

        let filled: OrderStatus =
            serde_json::from_str(r#"{"filled":{"totalSz":"0.001","avgPx":"64200","oid":43}}"#)
                .unwrap();
        assert!(matches!(
            filled,
            OrderStatus::Filled {
                filled: Filled { oid: 43, .. }
            }
        ));

        let error: OrderStatus = serde_json::from_str(r#"{"error":"bad"}"#).unwrap();
        assert!(matches!(
            error,
            OrderStatus::Error { error } if error == "bad"
        ));
    }

    #[test]
    fn test_deserialize_cancel_status_variants() {
        let success: CancelStatus = serde_json::from_str(r#""success""#).unwrap();
        assert!(matches!(success, CancelStatus::Success(_)));

        let error: CancelStatus = serde_json::from_str(r#"{"error":"not found"}"#).unwrap();
        assert!(matches!(
            error,
            CancelStatus::Error { error } if error == "not found"
        ));
    }

    #[test]
    fn test_deserialize_ws_msg() {
        let data: WsMsg = serde_json::from_str(
            r#"{"channel":"l2Book:btc","data":{"coin":"BTC","time":1,"levels":[]}}"#,
        )
        .unwrap();
        assert_eq!(data.channel, "l2Book:btc");
        assert!(data.data.is_some());

        let sub: WsMsg =
            serde_json::from_str(r#"{"channel":"subscriptionResponse","data":{"success":true}}"#)
                .unwrap();
        assert_eq!(sub.channel, "subscriptionResponse");

        let error: WsMsg =
            serde_json::from_str(r#"{"channel":"error","error":"sub failed"}"#).unwrap();
        assert_eq!(error.error.as_deref(), Some("sub failed"));
    }

    #[test]
    fn test_deserialize_l2_book_levels() {
        let book: L2BookData = serde_json::from_str(
            r#"{
                "coin":"BTC",
                "time":1787000000000,
                "levels":[
                    [{"px":"64200.0","sz":"12.5","n":51}],
                    [{"px":"64201.0","sz":"7.5","n":18}]
                ]
            }"#,
        )
        .unwrap();
        assert_eq!(book.coin, "BTC");
        assert_eq!(book.levels.len(), 2);
        assert_eq!(book.levels[0][0].px, "64200.0");
        assert_eq!(book.levels[0][0].n, 51);
        assert_eq!(book.levels[1][0].px, "64201.0");
    }

    #[test]
    fn test_deserialize_order_state_camel_case() {
        let state: OrderState = serde_json::from_str(
            r#"{
                "coin":"BTC",
                "oid":42,
                "cloid":"0xabcd",
                "sz":"0.001",
                "filled":"0",
                "avgPx":"64200",
                "limitPx":"63000",
                "timestamp":1787000000000,
                "origSz":"0.001",
                "reduceOnly":false,
                "tif":"Gtc",
                "side":"B"
            }"#,
        )
        .unwrap();
        assert_eq!(state.coin, "BTC");
        assert_eq!(state.oid, 42);
        assert_eq!(state.cloid.as_deref(), Some("0xabcd"));
        assert_eq!(state.avg_px, "64200");
        assert_eq!(state.limit_px, "63000");
    }

    #[test]
    fn test_deserialize_user_event_fill() {
        let event: UserEvent = serde_json::from_str(
            r#"{
                "type":"fill",
                "fill":{
                    "coin":"BTC",
                    "px":"64200",
                    "sz":"0.001",
                    "side":"B",
                    "time":1787000000000,
                    "startPosition":"0.5"
                }
            }"#,
        )
        .unwrap();
        assert_eq!(event.type_, "fill");
        let fill = event.fill.unwrap();
        assert_eq!(fill.coin, "BTC");
        assert_eq!(fill.side, "B");
        assert_eq!(fill.sz, "0.001");
    }

    #[test]
    fn test_deserialize_meta_ignores_unknown_fields() {
        let meta: Meta = serde_json::from_str(
            r#"{"universe":[{"szDecimals":5,"name":"BTC","maxLeverage":40,"marginTableId":56,"isDelisted":true}]}"#,
        )
        .unwrap();
        assert_eq!(meta.universe.len(), 1);
        assert_eq!(meta.universe[0].name, "BTC");
        assert_eq!(meta.universe[0].sz_decimals, 5);
        assert_eq!(meta.universe[0].max_leverage, 40);
    }
}
