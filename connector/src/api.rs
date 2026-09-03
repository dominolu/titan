//! 统一 Broker API 层。
//!
//! 定义三家交易所（Binance USD-M / OKX V5 / Hyperliquid）共用的统一数据结构和
//! [`BrokerApi`] trait，保证策略代码只依赖本模块即可在任意 broker 之间自由切换。
//!
//! 统一规范：
//! - 所有 symbol 统一使用交易所原生字符串（如 `BTCUSDT`、`BTC-USDT-SWAP`、`BTC`）；
//! - 所有价格/数量统一使用 `f64`（精度问题由 connector 内部按交易所规则处理）；
//! - 订单 ID 统一使用 `String`；
//! - 时间戳统一使用毫秒（i64）；
//! - 状态/方向/类型使用本模块定义的枚举，无法映射时落入 `Unknown` 变体。

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 统一 API 错误。
#[derive(Error, Debug, Clone)]
#[error("[{exchange}] {code}: {message}")]
pub struct ApiError {
    /// 交易所名称，用于日志定位。
    pub exchange: &'static str,
    /// 交易所返回的错误码（或 HTTP 状态码）。
    pub code: String,
    /// 错误描述。
    pub message: String,
    /// HTTP 状态码（有则填）。
    pub status: Option<u16>,
}

impl ApiError {
    pub fn new(
        exchange: &'static str,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            exchange,
            code: code.into(),
            message: message.into(),
            status: None,
        }
    }

    pub fn http(exchange: &'static str, status: u16, message: impl Into<String>) -> Self {
        Self {
            exchange,
            code: status.to_string(),
            message: message.into(),
            status: Some(status),
        }
    }

    pub fn transport(exchange: &'static str, err: impl std::fmt::Display) -> Self {
        Self {
            exchange,
            code: "TRANSPORT".to_string(),
            message: err.to_string(),
            status: None,
        }
    }

    /// Whether the request may have reached the exchange despite the observed error.
    pub fn outcome_unknown(&self) -> bool {
        self.code == "TRANSPORT" || matches!(self.status, Some(408 | 425 | 429 | 500..=599))
    }
}

/// 统一成交方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ApiSide {
    Buy,
    Sell,
    Unknown,
}

impl ApiSide {
    pub fn from_str(side: &str) -> Self {
        match side.to_uppercase().as_str() {
            "BUY" | "B" | "b" => ApiSide::Buy,
            "SELL" | "S" | "s" => ApiSide::Sell,
            _ => ApiSide::Unknown,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ApiSide::Buy => "BUY",
            ApiSide::Sell => "SELL",
            ApiSide::Unknown => "UNKNOWN",
        }
    }
}

/// 统一订单类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApiOrderType {
    Limit,
    Market,
    StopMarket,
    StopLimit,
    TakeProfitMarket,
    TakeProfitLimit,
    TrailingStopMarket,
    Unknown,
}

impl ApiOrderType {
    pub fn from_str(t: &str) -> Self {
        match t.to_uppercase().as_str() {
            "LIMIT" | "L" => ApiOrderType::Limit,
            "MARKET" | "M" | "IOC" => ApiOrderType::Market,
            "STOP_MARKET" | "STOP" | "S" => ApiOrderType::StopMarket,
            "STOP_LIMIT" => ApiOrderType::StopLimit,
            "TAKE_PROFIT_MARKET" | "TP" => ApiOrderType::TakeProfitMarket,
            "TAKE_PROFIT_LIMIT" => ApiOrderType::TakeProfitLimit,
            "TRAILING_STOP_MARKET" => ApiOrderType::TrailingStopMarket,
            _ => ApiOrderType::Unknown,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ApiOrderType::Limit => "LIMIT",
            ApiOrderType::Market => "MARKET",
            ApiOrderType::StopMarket => "STOP_MARKET",
            ApiOrderType::StopLimit => "STOP_LIMIT",
            ApiOrderType::TakeProfitMarket => "TAKE_PROFIT_MARKET",
            ApiOrderType::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
            ApiOrderType::TrailingStopMarket => "TRAILING_STOP_MARKET",
            ApiOrderType::Unknown => "UNKNOWN",
        }
    }
}

/// 统一有效时间。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ApiTimeInForce {
    GTC,
    IOC,
    FOK,
    GTX,
    Unknown,
}

impl ApiTimeInForce {
    pub fn from_str(tif: &str) -> Self {
        match tif.to_uppercase().as_str() {
            "GTC" => ApiTimeInForce::GTC,
            "IOC" => ApiTimeInForce::IOC,
            "FOK" => ApiTimeInForce::FOK,
            "GTX" | "POST_ONLY" => ApiTimeInForce::GTX,
            _ => ApiTimeInForce::Unknown,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ApiTimeInForce::GTC => "GTC",
            ApiTimeInForce::IOC => "IOC",
            ApiTimeInForce::FOK => "FOK",
            ApiTimeInForce::GTX => "GTX",
            ApiTimeInForce::Unknown => "UNKNOWN",
        }
    }
}

/// 统一订单状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApiOrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
    Untriggered,
    Triggered,
    Unknown,
}

impl ApiOrderStatus {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "NEW" | "LIVE" | "OPEN" | "PENDING_NEW" | "PLACED" => ApiOrderStatus::New,
            "PARTIALLY_FILLED" | "PARTIALLYFILLED" => ApiOrderStatus::PartiallyFilled,
            "FILLED" => ApiOrderStatus::Filled,
            "CANCELED" | "CANCELLED" | "CANCELED_PENDING" | "CANCELLING" | "CANCELING" => {
                ApiOrderStatus::Canceled
            }
            "REJECTED" | "FAILED" => ApiOrderStatus::Rejected,
            "EXPIRED" | "EXPIRED_IN_MATCH" => ApiOrderStatus::Expired,
            "UNTRIGGERED" | "INACTIVE" | "PENDING" => ApiOrderStatus::Untriggered,
            "TRIGGERED" | "ACTIVE" | "WORKING" => ApiOrderStatus::Triggered,
            _ => ApiOrderStatus::Unknown,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ApiOrderStatus::New => "NEW",
            ApiOrderStatus::PartiallyFilled => "PARTIALLY_FILLED",
            ApiOrderStatus::Filled => "FILLED",
            ApiOrderStatus::Canceled => "CANCELED",
            ApiOrderStatus::Rejected => "REJECTED",
            ApiOrderStatus::Expired => "EXPIRED",
            ApiOrderStatus::Untriggered => "UNTRIGGERED",
            ApiOrderStatus::Triggered => "TRIGGERED",
            ApiOrderStatus::Unknown => "UNKNOWN",
        }
    }
}

/// 统一持仓方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ApiPositionSide {
    Long,
    Short,
    Net,
    Unknown,
}

impl ApiPositionSide {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "LONG" | "LONG_SIDE" | "BOTH" => {
                // BOTH 是双向持仓模式下持仓量为正/负时由 qty 判定，此处先归 Long
                ApiPositionSide::Long
            }
            "SHORT" | "SHORT_SIDE" => ApiPositionSide::Short,
            "NET" => ApiPositionSide::Net,
            _ => ApiPositionSide::Unknown,
        }
    }

    pub fn from_qty(qty: f64) -> Self {
        if qty > 0.0 {
            ApiPositionSide::Long
        } else if qty < 0.0 {
            ApiPositionSide::Short
        } else {
            ApiPositionSide::Net
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ApiPositionSide::Long => "LONG",
            ApiPositionSide::Short => "SHORT",
            ApiPositionSide::Net => "NET",
            ApiPositionSide::Unknown => "UNKNOWN",
        }
    }
}

/// 统一保证金类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ApiMarginType {
    Isolated,
    Cross,
    Unknown,
}

impl ApiMarginType {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "ISOLATED" => ApiMarginType::Isolated,
            "CROSS" | "CROSSED" => ApiMarginType::Cross,
            _ => ApiMarginType::Unknown,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ApiMarginType::Isolated => "ISOLATED",
            ApiMarginType::Cross => "CROSS",
            ApiMarginType::Unknown => "UNKNOWN",
        }
    }
}

/// 统一合约/标的元数据。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstrumentInfo {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub tick_size: f64,
    pub lot_size: f64,
    pub min_qty: f64,
    pub contract_size: f64,
    pub margin_asset: String,
    /// 价格精度（小数位）。
    pub price_precision: u32,
    /// 数量精度（小数位）。
    pub qty_precision: u32,
    /// 是否可交易。
    pub tradable: bool,
}

/// 统一行情快照。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ticker {
    pub symbol: String,
    pub last_price: f64,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub funding_rate: Option<f64>,
    pub next_funding_time: Option<i64>,
    pub open_24h: f64,
    pub high_24h: f64,
    pub low_24h: f64,
    pub volume_24h: f64,
    pub quote_volume_24h: f64,
    pub timestamp: i64,
}

/// 统一盘口档位。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceLevel {
    pub price: f64,
    pub qty: f64,
}

/// 统一订单簿。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBook {
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: i64,
}

/// 统一成交（公共行情）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub symbol: String,
    pub id: String,
    pub price: f64,
    pub qty: f64,
    pub side: ApiSide,
    pub timestamp: i64,
}

/// 统一 K 线。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Kline {
    pub symbol: String,
    pub interval: String,
    pub open_time: i64,
    pub close_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
}

/// 统一资金费率。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FundingRate {
    pub symbol: String,
    pub funding_rate: f64,
    pub next_funding_time: i64,
    pub timestamp: i64,
}

/// 统一持仓量。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenInterest {
    pub symbol: String,
    pub open_interest: f64,
    pub timestamp: i64,
}

/// 统一订单信息（下单/撤单/改单/查询返回值）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderInfo {
    pub symbol: String,
    /// 交易所订单 ID（String 统一）。
    pub order_id: String,
    /// 客户端订单 ID（无则空串）。
    pub client_order_id: String,
    pub side: ApiSide,
    pub order_type: ApiOrderType,
    pub status: ApiOrderStatus,
    pub price: f64,
    pub qty: f64,
    pub executed_qty: f64,
    pub avg_price: f64,
    pub leaves_qty: f64,
    pub time_in_force: ApiTimeInForce,
    pub reduce_only: bool,
    pub position_side: ApiPositionSide,
    pub create_time: i64,
    pub update_time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<f64>,
}

/// 统一持仓信息。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionInfo {
    pub symbol: String,
    pub position_side: ApiPositionSide,
    pub qty: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub liquidation_price: f64,
    pub leverage: f64,
    pub margin_type: ApiMarginType,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub notional: f64,
    pub update_time: i64,
}

/// 统一币种余额。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Balance {
    pub asset: String,
    pub wallet_balance: f64,
    pub available_balance: f64,
    pub unrealized_pnl: f64,
    pub margin_balance: f64,
}

/// 统一账户信息。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    pub total_wallet_balance: f64,
    pub total_margin_balance: f64,
    pub total_unrealized_pnl: f64,
    pub available_balance: f64,
    pub balances: Vec<Balance>,
    pub timestamp: i64,
}

/// 统一手续费率。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeeRate {
    pub symbol: String,
    pub maker_fee: f64,
    pub taker_fee: f64,
    pub timestamp: i64,
}

/// 统一杠杆信息。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeverageInfo {
    pub symbol: String,
    pub leverage: f64,
    pub margin_type: ApiMarginType,
    pub position_side: ApiPositionSide,
}

/// 统一成交明细（账户成交）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fill {
    pub symbol: String,
    pub trade_id: String,
    pub order_id: String,
    pub client_order_id: String,
    pub price: f64,
    pub qty: f64,
    pub side: ApiSide,
    pub fee: f64,
    pub fee_asset: String,
    pub realized_pnl: f64,
    pub maker: bool,
    pub timestamp: i64,
}

/// 统一收益流水。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncomeRecord {
    pub symbol: String,
    pub income_type: String,
    pub income: f64,
    pub asset: String,
    pub timestamp: i64,
}

/// 统一下单请求。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedOrderRequest {
    pub symbol: String,
    pub side: ApiSide,
    pub order_type: ApiOrderType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    pub qty: f64,
    pub time_in_force: ApiTimeInForce,
    pub reduce_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_side: Option<ApiPositionSide>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<f64>,
}

/// 统一改单请求。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AmendOrderRequest {
    pub symbol: String,
    /// 交易所订单 ID（与 client_order_id 二选一）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_qty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_stop_price: Option<f64>,
}

/// 统一撤单请求。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrderRequest {
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
}

/// 统一条件单请求（OKX 算法单 / Binance 条件单 / HL TP/SL）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlgoOrderRequest {
    pub symbol: String,
    pub side: ApiSide,
    pub order_type: ApiOrderType,
    pub qty: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    /// 触发价（必填）。
    pub trigger_price: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
}

/// 统一 Broker REST API。
///
/// 策略只依赖本 trait，通过 `Box<dyn BrokerApi>` 即可在 Binance / OKX / Hyperliquid
/// 之间自由切换。
#[async_trait::async_trait]
pub trait BrokerApi: Send + Sync {
    // ------------------------------------------------------------------
    // 基础
    // ------------------------------------------------------------------

    /// 连通性测试。
    async fn ping(&self) -> Result<(), ApiError>;

    /// 服务器时间（毫秒）。
    async fn get_server_time(&self) -> Result<i64, ApiError>;

    /// 合约/标的元数据。
    async fn get_instruments(&self) -> Result<Vec<InstrumentInfo>, ApiError>;

    // ------------------------------------------------------------------
    // 行情
    // ------------------------------------------------------------------

    /// 单个标的行情快照。
    async fn get_ticker(&self, symbol: &str) -> Result<Ticker, ApiError>;

    /// 全部标的行情快照。
    async fn get_tickers(&self) -> Result<Vec<Ticker>, ApiError>;

    /// 订单簿。
    async fn get_order_book(&self, symbol: &str, limit: u32) -> Result<OrderBook, ApiError>;

    /// 最近成交。
    async fn get_trades(&self, symbol: &str, limit: u32) -> Result<Vec<Trade>, ApiError>;

    /// K 线。
    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
    ) -> Result<Vec<Kline>, ApiError>;

    /// 当前资金费率。
    async fn get_funding_rate(&self, symbol: &str) -> Result<FundingRate, ApiError>;

    /// 资金费历史。
    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRate>, ApiError>;

    /// 持仓量。
    async fn get_open_interest(&self, symbol: &str) -> Result<OpenInterest, ApiError>;

    // ------------------------------------------------------------------
    // 交易
    // ------------------------------------------------------------------

    /// 下单。
    async fn submit_order(&self, req: &UnifiedOrderRequest) -> Result<OrderInfo, ApiError>;

    /// 批量下单。
    async fn submit_orders(&self, reqs: &[UnifiedOrderRequest])
    -> Result<Vec<OrderInfo>, ApiError>;

    /// 撤单。
    async fn cancel_order(&self, req: &CancelOrderRequest) -> Result<OrderInfo, ApiError>;

    /// 批量撤单。
    async fn cancel_orders(&self, reqs: &[CancelOrderRequest]) -> Result<Vec<OrderInfo>, ApiError>;

    /// 全部撤单（按标的）。
    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), ApiError>;

    /// 倒计时自动撤单（防失控安全网，timeout_ms=0 表示取消）。
    async fn cancel_all_after(&self, timeout_ms: u64) -> Result<(), ApiError>;

    /// 改单。
    async fn amend_order(&self, req: &AmendOrderRequest) -> Result<OrderInfo, ApiError>;

    /// 查单。
    async fn get_order(
        &self,
        symbol: &str,
        order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> Result<OrderInfo, ApiError>;

    /// 未成交订单。
    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, ApiError>;

    /// 历史订单。
    async fn get_order_history(&self, symbol: &str, limit: u32)
    -> Result<Vec<OrderInfo>, ApiError>;

    /// 成交明细。
    async fn get_fills(&self, symbol: &str, limit: u32) -> Result<Vec<Fill>, ApiError>;

    // ------------------------------------------------------------------
    // 账户
    // ------------------------------------------------------------------

    /// 账户信息（余额）。
    async fn get_account(&self) -> Result<AccountInfo, ApiError>;

    /// 持仓（symbol=None 时返回全部）。
    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<PositionInfo>, ApiError>;

    /// 设置杠杆。
    async fn set_leverage(
        &self,
        symbol: &str,
        leverage: f64,
        position_side: Option<ApiPositionSide>,
    ) -> Result<LeverageInfo, ApiError>;

    /// 查询杠杆。
    async fn get_leverage(&self, symbol: &str) -> Result<LeverageInfo, ApiError>;

    /// 手续费率。
    async fn get_fee_rates(&self, symbol: &str) -> Result<FeeRate, ApiError>;

    /// 收益流水。
    async fn get_income_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<IncomeRecord>, ApiError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_side_mapping() {
        assert_eq!(ApiSide::from_str("BUY"), ApiSide::Buy);
        assert_eq!(ApiSide::from_str("buy"), ApiSide::Buy);
        assert_eq!(ApiSide::from_str("SELL"), ApiSide::Sell);
        assert_eq!(ApiSide::from_str("B"), ApiSide::Buy);
        assert_eq!(ApiSide::from_str("S"), ApiSide::Sell);
        assert_eq!(ApiSide::from_str("xxx"), ApiSide::Unknown);
        assert_eq!(ApiSide::Buy.as_str(), "BUY");
        assert_eq!(ApiSide::Sell.as_str(), "SELL");
    }

    #[test]
    fn test_order_type_mapping() {
        assert_eq!(ApiOrderType::from_str("LIMIT"), ApiOrderType::Limit);
        assert_eq!(ApiOrderType::from_str("MARKET"), ApiOrderType::Market);
        assert_eq!(
            ApiOrderType::from_str("STOP_MARKET"),
            ApiOrderType::StopMarket
        );
        assert_eq!(
            ApiOrderType::from_str("stop_limit"),
            ApiOrderType::StopLimit
        );
        assert_eq!(
            ApiOrderType::from_str("TAKE_PROFIT_MARKET"),
            ApiOrderType::TakeProfitMarket
        );
        assert_eq!(
            ApiOrderType::from_str("TRAILING_STOP_MARKET"),
            ApiOrderType::TrailingStopMarket
        );
        assert_eq!(ApiOrderType::from_str("???"), ApiOrderType::Unknown);
    }

    #[test]
    fn test_tif_mapping() {
        assert_eq!(ApiTimeInForce::from_str("GTC"), ApiTimeInForce::GTC);
        assert_eq!(ApiTimeInForce::from_str("ioc"), ApiTimeInForce::IOC);
        assert_eq!(ApiTimeInForce::from_str("FOK"), ApiTimeInForce::FOK);
        assert_eq!(ApiTimeInForce::from_str("POST_ONLY"), ApiTimeInForce::GTX);
        assert_eq!(ApiTimeInForce::from_str("GTX"), ApiTimeInForce::GTX);
    }

    #[test]
    fn test_order_status_mapping() {
        assert_eq!(ApiOrderStatus::from_str("NEW"), ApiOrderStatus::New);
        assert_eq!(ApiOrderStatus::from_str("live"), ApiOrderStatus::New);
        assert_eq!(
            ApiOrderStatus::from_str("PARTIALLY_FILLED"),
            ApiOrderStatus::PartiallyFilled
        );
        assert_eq!(ApiOrderStatus::from_str("FILLED"), ApiOrderStatus::Filled);
        assert_eq!(
            ApiOrderStatus::from_str("CANCELED"),
            ApiOrderStatus::Canceled
        );
        assert_eq!(
            ApiOrderStatus::from_str("cancelled"),
            ApiOrderStatus::Canceled
        );
        assert_eq!(
            ApiOrderStatus::from_str("REJECTED"),
            ApiOrderStatus::Rejected
        );
        assert_eq!(ApiOrderStatus::from_str("EXPIRED"), ApiOrderStatus::Expired);
        assert_eq!(
            ApiOrderStatus::from_str("UNTRIGGERED"),
            ApiOrderStatus::Untriggered
        );
        assert_eq!(
            ApiOrderStatus::from_str("TRIGGERED"),
            ApiOrderStatus::Triggered
        );
    }

    #[test]
    fn test_position_side_from_qty() {
        assert_eq!(ApiPositionSide::from_qty(1.5), ApiPositionSide::Long);
        assert_eq!(ApiPositionSide::from_qty(-1.5), ApiPositionSide::Short);
        assert_eq!(ApiPositionSide::from_qty(0.0), ApiPositionSide::Net);
    }

    #[test]
    fn test_margin_type_mapping() {
        assert_eq!(ApiMarginType::from_str("ISOLATED"), ApiMarginType::Isolated);
        assert_eq!(ApiMarginType::from_str("cross"), ApiMarginType::Cross);
    }

    #[test]
    fn test_api_error_format() {
        let err = ApiError::new("binance", "1234", "order rejected");
        assert_eq!(err.exchange, "binance");
        assert_eq!(err.code, "1234");
        assert_eq!(err.to_string(), "[binance] 1234: order rejected");
        assert!(!err.outcome_unknown());
        assert!(ApiError::transport("binance", "timeout").outcome_unknown());
        assert!(ApiError::http("okx", 503, "unavailable").outcome_unknown());
        assert!(ApiError::http("okx", 429, "limited").outcome_unknown());
        assert!(!ApiError::http("okx", 400, "bad request").outcome_unknown());
    }

    #[test]
    fn test_unified_types_serde_roundtrip() {
        let order = OrderInfo {
            symbol: "BTCUSDT".to_string(),
            order_id: "123".to_string(),
            client_order_id: "c1".to_string(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Limit,
            status: ApiOrderStatus::PartiallyFilled,
            price: 100.5,
            qty: 1.0,
            executed_qty: 0.5,
            avg_price: 100.4,
            leaves_qty: 0.5,
            time_in_force: ApiTimeInForce::GTC,
            reduce_only: false,
            position_side: ApiPositionSide::Long,
            create_time: 1_700_000_000_000,
            update_time: 1_700_000_000_100,
            stop_price: None,
        };
        let json = serde_json::to_string(&order).unwrap();
        let back: OrderInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(order, back);
    }

    #[test]
    fn test_unified_request_serde() {
        let req = UnifiedOrderRequest {
            symbol: "BTC-USDT-SWAP".to_string(),
            side: ApiSide::Sell,
            order_type: ApiOrderType::StopMarket,
            price: None,
            qty: 2.0,
            time_in_force: ApiTimeInForce::GTC,
            reduce_only: true,
            position_side: Some(ApiPositionSide::Short),
            client_order_id: Some("x".to_string()),
            stop_price: Some(50000.0),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: UnifiedOrderRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[tokio::test]
    async fn test_trait_object_usable() {
        // 保证 BrokerApi 可作为 trait object 使用（策略自由切换的前提）。
        fn _assert_object_safe(_: &dyn BrokerApi) {}
        let _ = _assert_object_safe;
    }
}
