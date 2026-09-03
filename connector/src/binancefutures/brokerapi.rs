#![allow(dead_code)]
//! Binance USD-M Futures 全量 REST 接口 + 统一 [`BrokerApi`] 实现。
//!
//! 对照官方文档补齐以下端点组：
//! - 行情：ping/time/exchangeInfo/depth/trades/historicalTrades/aggTrades/klines/
//!   premiumIndex/fundingRate/fundingInfo/ticker(24hr,price,bookTicker)/openInterest/
//!   insuranceBalance/indexInfo/assetIndex/constituents/tradingSchedule/data 系列
//! - 交易：order/batchOrders(下单/撤单)/order(PUT 改单)/orderAmendment/countdownCancelAll/
//!   order(GET)/allOrders/openOrders/openOrder/forceOrders/userTrades/order(test)/algoOrder
//! - 账户：positionRisk(v2/v3)/balance(v2/v3)/account(v2/v3)/marginType/positionSide/leverage/
//!   multiAssetsMargin/positionMargin/positionMargin(history)/leverageBracket/commissionRate/
//!   accountConfig/symbolConfig/rateLimit/adlQuantile/symbolAdlRisk/income/apiTradingStatus
//! - 用户数据流：listenKey POST/PUT/DELETE

use hftbacktest::types::{OrdType, Side, TimeInForce};

use super::{BinanceFuturesError, msg::rest as m, rest::BinanceFuturesClient};
use crate::api::{
    AccountInfo, AlgoOrderRequest, AmendOrderRequest, ApiError, ApiMarginType, ApiOrderStatus,
    ApiOrderType, ApiPositionSide, ApiSide, ApiTimeInForce, Balance, BrokerApi, CancelOrderRequest,
    FeeRate, Fill, FundingRate, IncomeRecord, InstrumentInfo, Kline, LeverageInfo, OpenInterest,
    OrderBook, OrderInfo, PositionInfo, PriceLevel, Ticker, Trade, UnifiedOrderRequest,
};

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum EmptyObjectOrMany<T> {
    Many(Vec<T>),
    Empty(std::collections::HashMap<String, serde_json::Value>),
}

impl<T> OneOrMany<T> {
    fn into_vec(self) -> Vec<T> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
}

fn checked_exchange_value(
    value: serde_json::Value,
) -> Result<serde_json::Value, BinanceFuturesError> {
    let code = value.get("code").and_then(|code| {
        code.as_i64()
            .or_else(|| code.as_str().and_then(|code| code.parse().ok()))
    });
    if let Some(code) = code.filter(|code| *code != 200) {
        let message = value
            .get("msg")
            .and_then(|msg| msg.as_str())
            .unwrap_or("Binance request failed");
        if message.starts_with("No need to change") {
            return Ok(value);
        }
        return Err(BinanceFuturesError::OrderError {
            code,
            msg: message.to_owned(),
        });
    }
    Ok(value)
}

impl From<BinanceFuturesError> for ApiError {
    fn from(e: BinanceFuturesError) -> Self {
        match e {
            BinanceFuturesError::OrderError { code, msg } => {
                ApiError::new("binance", code.to_string(), msg)
            }
            BinanceFuturesError::ReqError(err) => ApiError::transport("binance", err),
            other => ApiError::new("binance", "ERR", other.to_string()),
        }
    }
}

impl From<reqwest::Error> for ApiError {
    fn from(e: reqwest::Error) -> Self {
        ApiError::transport("binance", e)
    }
}

// ------------------------------------------------------------------
// 原始响应 → 统一结构 转换
// ------------------------------------------------------------------

fn f64_or_zero(s: &str) -> f64 {
    s.parse::<f64>().unwrap_or(0.0)
}

fn symbol_matches(requested: Option<&str>, actual: &str) -> bool {
    requested
        .map(|symbol| actual.eq_ignore_ascii_case(symbol))
        .unwrap_or(true)
}

fn instrument_from_symbol(si: &m::SymbolInfo) -> InstrumentInfo {
    let mut tick_size = 0.0;
    let mut lot_size = 0.0;
    let mut min_qty = 0.0;
    for f in &si.filters {
        match f.filter_type.as_str() {
            "PRICE_FILTER" => {
                if let Some(t) = &f.tick_size {
                    tick_size = f64_or_zero(t);
                }
            }
            "LOT_SIZE" => {
                if let Some(s) = &f.step_size {
                    lot_size = f64_or_zero(s);
                }
                if let Some(q) = &f.min_qty {
                    min_qty = f64_or_zero(q);
                }
            }
            _ => {}
        }
    }
    InstrumentInfo {
        symbol: si.symbol.to_lowercase(),
        base_asset: si.base_asset.clone(),
        quote_asset: si.quote_asset.clone(),
        tick_size,
        lot_size,
        min_qty,
        contract_size: f64_or_zero(&si.contract_size),
        margin_asset: si.margin_asset.clone(),
        price_precision: si.price_precision,
        qty_precision: si.quantity_precision,
        tradable: si.status == "TRADING",
    }
}

fn ticker_from(t24: &m::Ticker24h, p: Option<&m::PremiumIndex>) -> Ticker {
    let funding = p.map(|p| f64_or_zero(p.last_funding_rate.as_deref().unwrap_or("0")));
    Ticker {
        symbol: t24.symbol.to_lowercase(),
        last_price: t24.last_price,
        mark_price: p.map(|p| p.mark_price),
        index_price: p.map(|p| p.index_price),
        funding_rate: funding,
        next_funding_time: p.map(|p| p.next_funding_time),
        open_24h: t24.open_price,
        high_24h: t24.high_price,
        low_24h: t24.low_price,
        volume_24h: t24.volume,
        quote_volume_24h: t24.quote_volume,
        timestamp: t24.close_time,
    }
}

fn order_info_from(o: &m::Order) -> OrderInfo {
    let executed = o.executed_qty;
    let qty = o.orig_qty;
    let leaves = if o.leaves_qty > 0.0 {
        o.leaves_qty
    } else {
        qty - executed
    };
    let pos_side = match o.position_side.to_uppercase().as_str() {
        "LONG" => ApiPositionSide::Long,
        "SHORT" => ApiPositionSide::Short,
        "BOTH" => ApiPositionSide::from_qty(qty),
        _ => ApiPositionSide::Unknown,
    };
    OrderInfo {
        symbol: o.symbol.to_lowercase(),
        order_id: o.order_id.to_string(),
        client_order_id: o.client_order_id.clone(),
        side: ApiSide::from_str(&o.side),
        order_type: ApiOrderType::from_str(&o.orig_type),
        status: ApiOrderStatus::from_str(&o.status),
        price: o.price,
        qty,
        executed_qty: executed,
        avg_price: f64_or_zero(&o.avg_price),
        leaves_qty: leaves,
        time_in_force: ApiTimeInForce::from_str(&o.time_in_force),
        reduce_only: o.reduce_only,
        position_side: pos_side,
        create_time: o.time,
        update_time: o.update_time,
        stop_price: if o.stop_price > 0.0 {
            Some(o.stop_price)
        } else {
            None
        },
    }
}

fn order_info_from_response(o: &m::OrderResponse) -> OrderInfo {
    let pos_side = match o.position_side.to_uppercase().as_str() {
        "LONG" => ApiPositionSide::Long,
        "SHORT" => ApiPositionSide::Short,
        "BOTH" => ApiPositionSide::from_qty(o.orig_qty),
        _ => ApiPositionSide::Unknown,
    };
    OrderInfo {
        symbol: o.symbol.clone(),
        order_id: o.order_id.to_string(),
        client_order_id: o.client_order_id.clone(),
        side: o.side.into(),
        order_type: o.ty.into(),
        status: o.status.into(),
        price: o.price,
        qty: o.orig_qty,
        executed_qty: o.executed_qty,
        avg_price: o.avg_price.unwrap_or(0.0),
        leaves_qty: o.orig_qty - o.executed_qty,
        time_in_force: o.time_in_force.into(),
        reduce_only: o.reduce_only,
        position_side: pos_side,
        create_time: o.update_time,
        update_time: o.update_time,
        stop_price: if o.stop_price > 0.0 {
            Some(o.stop_price)
        } else {
            None
        },
    }
}

fn position_from(o: &m::PositionInformationV2) -> PositionInfo {
    let side = if o.position_amount > 0.0 {
        ApiPositionSide::Long
    } else if o.position_amount < 0.0 {
        ApiPositionSide::Short
    } else {
        ApiPositionSide::from_str(&o.position_side)
    };
    PositionInfo {
        symbol: o.symbol.to_lowercase(),
        position_side: side,
        qty: o.position_amount,
        entry_price: o.entry_price,
        mark_price: o.mark_price,
        liquidation_price: o.liquidation_price,
        leverage: o.leverage,
        margin_type: ApiMarginType::from_str(&o.margin_type),
        unrealized_pnl: o.unrealized_pnl.parse().unwrap_or(0.0),
        realized_pnl: 0.0,
        notional: o.notional,
        update_time: o.update_time,
    }
}

fn account_from(a: &m::Account) -> AccountInfo {
    AccountInfo {
        total_wallet_balance: a.total_wallet_balance,
        total_margin_balance: a.total_margin_balance,
        total_unrealized_pnl: a.total_unrealized_profit,
        available_balance: a.available_balance,
        balances: a
            .assets
            .iter()
            .map(|asset| Balance {
                asset: asset.asset.clone(),
                wallet_balance: asset.wallet_balance,
                available_balance: asset.available_balance,
                unrealized_pnl: asset.unrealized_profit,
                margin_balance: asset.margin_balance,
            })
            .collect(),
        timestamp: a.update_time,
    }
}

// ------------------------------------------------------------------
// 原始端点（对照官方文档全量）
// ------------------------------------------------------------------

impl BinanceFuturesClient {
    // ---------------- 基础 ----------------

    pub async fn ping(&self) -> Result<(), BinanceFuturesError> {
        let _: serde_json::Value = self.get_noauth("/fapi/v1/ping", String::new()).await?;
        Ok(())
    }

    pub async fn get_server_time(&self) -> Result<i64, BinanceFuturesError> {
        let resp: m::ServerTime = self.get_noauth("/fapi/v1/time", String::new()).await?;
        Ok(resp.server_time)
    }

    pub async fn get_exchange_info(&self) -> Result<Vec<m::SymbolInfo>, BinanceFuturesError> {
        let resp: m::ExchangeInfo = self
            .get_noauth("/fapi/v1/exchangeInfo", String::new())
            .await?;
        Ok(resp.symbols)
    }

    // ---------------- 行情 ----------------

    pub async fn get_ticker_24h(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<m::Ticker24h>, BinanceFuturesError> {
        let query = symbol.map(|s| format!("symbol={s}")).unwrap_or_default();
        let response: OneOrMany<m::Ticker24h> =
            self.get_noauth("/fapi/v1/ticker/24hr", query).await?;
        Ok(response.into_vec())
    }

    pub async fn get_premium_index(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<m::PremiumIndex>, BinanceFuturesError> {
        let query = symbol.map(|s| format!("symbol={s}")).unwrap_or_default();
        let response: OneOrMany<m::PremiumIndex> =
            self.get_noauth("/fapi/v1/premiumIndex", query).await?;
        Ok(response.into_vec())
    }

    pub async fn get_ticker_price(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<m::TickerPrice>, BinanceFuturesError> {
        let query = symbol.map(|s| format!("symbol={s}")).unwrap_or_default();
        let response: OneOrMany<m::TickerPrice> =
            self.get_noauth("/fapi/v1/ticker/price", query).await?;
        Ok(response.into_vec())
    }

    pub async fn get_book_ticker(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<m::BookTicker>, BinanceFuturesError> {
        let query = symbol.map(|s| format!("symbol={s}")).unwrap_or_default();
        let response: OneOrMany<m::BookTicker> =
            self.get_noauth("/fapi/v1/ticker/bookTicker", query).await?;
        Ok(response.into_vec())
    }

    pub async fn get_public_trades(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<m::PublicTrade>, BinanceFuturesError> {
        let query = format!("symbol={symbol}&limit={limit}");
        Ok(self.get_noauth("/fapi/v1/trades", query).await?)
    }

    pub async fn get_historical_trades(
        &self,
        symbol: &str,
        limit: u32,
        from_id: Option<i64>,
    ) -> Result<Vec<m::PublicTrade>, BinanceFuturesError> {
        let mut query = format!("symbol={symbol}&limit={limit}");
        if let Some(id) = from_id {
            query.push_str(&format!("&fromId={id}"));
        }
        Ok(self.get_apikey("/fapi/v1/historicalTrades", query).await?)
    }

    pub async fn get_agg_trades(
        &self,
        symbol: &str,
        limit: u32,
        from_id: Option<i64>,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Vec<m::AggTrade>, BinanceFuturesError> {
        let mut query = format!("symbol={symbol}&limit={limit}");
        if let Some(id) = from_id {
            query.push_str(&format!("&fromId={id}"));
        }
        if let Some(t) = start_time {
            query.push_str(&format!("&startTime={t}"));
        }
        if let Some(t) = end_time {
            query.push_str(&format!("&endTime={t}"));
        }
        Ok(self.get_noauth("/fapi/v1/aggTrades", query).await?)
    }

    pub async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<m::KlineResponse, BinanceFuturesError> {
        let mut query = format!("symbol={symbol}&interval={interval}&limit={limit}");
        if let Some(t) = start_time {
            query.push_str(&format!("&startTime={t}"));
        }
        if let Some(t) = end_time {
            query.push_str(&format!("&endTime={t}"));
        }
        Ok(self.get_noauth("/fapi/v1/klines", query).await?)
    }

    pub async fn get_funding_rate_records(
        &self,
        symbol: &str,
        limit: u32,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Vec<m::FundingRateRecord>, BinanceFuturesError> {
        let mut query = format!("symbol={symbol}&limit={limit}");
        if let Some(t) = start_time {
            query.push_str(&format!("&startTime={t}"));
        }
        if let Some(t) = end_time {
            query.push_str(&format!("&endTime={t}"));
        }
        Ok(self.get_noauth("/fapi/v1/fundingRate", query).await?)
    }

    pub async fn get_funding_info(&self) -> Result<Vec<m::FundingInfo>, BinanceFuturesError> {
        Ok(self
            .get_noauth("/fapi/v1/fundingInfo", String::new())
            .await?)
    }

    pub async fn get_open_interest(
        &self,
        symbol: &str,
    ) -> Result<m::OpenInterest, BinanceFuturesError> {
        Ok(self
            .get_noauth("/fapi/v1/openInterest", format!("symbol={symbol}"))
            .await?)
    }

    pub async fn get_insurance_balance(
        &self,
    ) -> Result<Vec<serde_json::Value>, BinanceFuturesError> {
        Ok(self
            .get_noauth("/fapi/v1/insuranceBalance", String::new())
            .await?)
    }

    pub async fn get_index_info(&self) -> Result<Vec<serde_json::Value>, BinanceFuturesError> {
        Ok(self.get_noauth("/fapi/v1/indexInfo", String::new()).await?)
    }

    pub async fn get_asset_index(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, BinanceFuturesError> {
        let query = symbol.map(|s| format!("symbol={s}")).unwrap_or_default();
        Ok(self.get_noauth("/fapi/v1/assetIndex", query).await?)
    }

    pub async fn get_constituents(
        &self,
        symbol: &str,
    ) -> Result<serde_json::Value, BinanceFuturesError> {
        Ok(self
            .get_noauth("/fapi/v1/constituents", format!("symbol={symbol}"))
            .await?)
    }

    pub async fn get_trading_schedule(&self) -> Result<serde_json::Value, BinanceFuturesError> {
        Ok(self
            .get_noauth("/fapi/v1/tradingSchedule", String::new())
            .await?)
    }

    pub async fn get_data_rows(
        &self,
        path: &str,
        symbol: &str,
        period: &str,
        limit: u32,
    ) -> Result<Vec<m::DataRow>, BinanceFuturesError> {
        let query = format!("symbol={symbol}&period={period}&limit={limit}");
        Ok(self.get_noauth(path, query).await?)
    }

    pub async fn get_delivery_price(
        &self,
        pair: &str,
    ) -> Result<Vec<serde_json::Value>, BinanceFuturesError> {
        Ok(self
            .get_noauth("/futures/data/delivery-price", format!("pair={pair}"))
            .await?)
    }

    pub async fn get_basis(
        &self,
        pair: &str,
        period: &str,
        limit: u32,
    ) -> Result<Vec<serde_json::Value>, BinanceFuturesError> {
        let query = format!("pair={pair}&contractType=PERPETUAL&period={period}&limit={limit}");
        Ok(self.get_noauth("/futures/data/basis", query).await?)
    }

    // ---------------- 交易 ----------------

    pub async fn get_order(
        &self,
        symbol: &str,
        order_id: Option<i64>,
        client_order_id: Option<&str>,
    ) -> Result<m::Order, BinanceFuturesError> {
        let mut query = format!("symbol={symbol}");
        if let Some(id) = order_id {
            query.push_str(&format!("&orderId={id}"));
        }
        if let Some(id) = client_order_id {
            query.push_str(&format!("&origClientOrderId={id}"));
        }
        let response: m::ApiResponse<m::Order> = self.get("/fapi/v1/order", query).await?;
        match response {
            m::ApiResponse::Success(order) => Ok(order),
            m::ApiResponse::Error(error) => Err(BinanceFuturesError::OrderError {
                code: error.code,
                msg: error.msg,
            }),
        }
    }

    pub async fn get_open_order(
        &self,
        symbol: &str,
        order_id: Option<i64>,
        client_order_id: Option<&str>,
    ) -> Result<m::Order, BinanceFuturesError> {
        let mut query = format!("symbol={symbol}");
        if let Some(id) = order_id {
            query.push_str(&format!("&orderId={id}"));
        }
        if let Some(id) = client_order_id {
            query.push_str(&format!("&origClientOrderId={id}"));
        }
        Ok(self.get("/fapi/v1/openOrder", query).await?)
    }

    pub async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<m::Order>, BinanceFuturesError> {
        let query = symbol.map(|s| format!("symbol={s}")).unwrap_or_default();
        Ok(self.get("/fapi/v1/openOrders", query).await?)
    }

    pub async fn get_all_orders(
        &self,
        symbol: &str,
        limit: u32,
        order_id: Option<i64>,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Vec<m::Order>, BinanceFuturesError> {
        let mut query = format!("symbol={symbol}&limit={limit}");
        if let Some(id) = order_id {
            query.push_str(&format!("&orderId={id}"));
        }
        if let Some(t) = start_time {
            query.push_str(&format!("&startTime={t}"));
        }
        if let Some(t) = end_time {
            query.push_str(&format!("&endTime={t}"));
        }
        Ok(self.get("/fapi/v1/allOrders", query).await?)
    }

    pub async fn get_order_amendment(
        &self,
        symbol: &str,
        order_id: Option<i64>,
        client_order_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<serde_json::Value>, BinanceFuturesError> {
        let mut query = format!("symbol={symbol}&limit={limit}");
        if let Some(id) = order_id {
            query.push_str(&format!("&orderId={id}"));
        }
        if let Some(id) = client_order_id {
            query.push_str(&format!("&origClientOrderId={id}"));
        }
        Ok(self.get("/fapi/v1/orderAmendment", query).await?)
    }

    pub async fn get_user_trades(
        &self,
        symbol: &str,
        limit: u32,
        from_id: Option<i64>,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Vec<m::UserTrade>, BinanceFuturesError> {
        let mut query = format!("symbol={symbol}&limit={limit}");
        if let Some(id) = from_id {
            query.push_str(&format!("&fromId={id}"));
        }
        if let Some(t) = start_time {
            query.push_str(&format!("&startTime={t}"));
        }
        if let Some(t) = end_time {
            query.push_str(&format!("&endTime={t}"));
        }
        Ok(self.get("/fapi/v1/userTrades", query).await?)
    }

    pub async fn get_force_orders(
        &self,
        symbol: Option<&str>,
        auto_close_type: Option<&str>,
        limit: u32,
    ) -> Result<Vec<serde_json::Value>, BinanceFuturesError> {
        let mut query = format!("limit={limit}");
        if let Some(s) = symbol {
            query.push_str(&format!("&symbol={s}"));
        }
        if let Some(t) = auto_close_type {
            query.push_str(&format!("&autoCloseType={t}"));
        }
        Ok(self.get("/fapi/v1/forceOrders", query).await?)
    }

    pub async fn countdown_cancel_all(
        &self,
        symbol: &str,
        timeout_ms: u64,
    ) -> Result<m::CountdownCancelAll, BinanceFuturesError> {
        let body = format!("symbol={symbol}&countdownTime={timeout_ms}");
        Ok(self.post("/fapi/v1/countdownCancelAll", body).await?)
    }

    pub async fn submit_test_order(&self, body: String) -> Result<(), BinanceFuturesError> {
        let _: serde_json::Value = self.post("/fapi/v1/order/test", body).await?;
        Ok(())
    }

    // ---------------- 账户 ----------------

    pub async fn set_margin_type(
        &self,
        symbol: &str,
        margin_type: &str,
    ) -> Result<(), BinanceFuturesError> {
        let body = format!("symbol={symbol}&marginType={margin_type}");
        checked_exchange_value(self.post("/fapi/v1/marginType", body).await?)?;
        Ok(())
    }

    pub async fn set_position_mode(&self, dual: bool) -> Result<(), BinanceFuturesError> {
        let body = format!("dualSidePosition={dual}");
        checked_exchange_value(self.post("/fapi/v1/positionSide/dual", body).await?)?;
        Ok(())
    }

    pub async fn get_position_mode(&self) -> Result<bool, BinanceFuturesError> {
        let resp: m::BoolSetting = self
            .get("/fapi/v1/positionSide/dual", String::new())
            .await?;
        Ok(resp.dual_side_position.unwrap_or(false))
    }

    pub async fn set_leverage(
        &self,
        symbol: &str,
        leverage: u32,
    ) -> Result<serde_json::Value, BinanceFuturesError> {
        let body = format!("symbol={symbol}&leverage={leverage}");
        checked_exchange_value(self.post("/fapi/v1/leverage", body).await?)
    }

    pub async fn set_multi_assets_mode(&self, enabled: bool) -> Result<(), BinanceFuturesError> {
        let body = format!("multiAssetsMargin={enabled}");
        checked_exchange_value(self.post("/fapi/v1/multiAssetsMargin", body).await?)?;
        Ok(())
    }

    pub async fn get_multi_assets_mode(&self) -> Result<bool, BinanceFuturesError> {
        let resp: m::BoolSetting = self
            .get("/fapi/v1/multiAssetsMargin", String::new())
            .await?;
        Ok(resp.multi_assets_margin.unwrap_or(false))
    }

    pub async fn set_position_margin(
        &self,
        symbol: &str,
        amount: f64,
        position_side: &str,
        margin_type: u8,
    ) -> Result<serde_json::Value, BinanceFuturesError> {
        let body = format!(
            "symbol={symbol}&amount={amount}&positionSide={position_side}&type={margin_type}"
        );
        checked_exchange_value(self.post("/fapi/v1/positionMargin", body).await?)
    }

    pub async fn get_position_margin_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<serde_json::Value>, BinanceFuturesError> {
        let query = format!("symbol={symbol}&limit={limit}");
        Ok(self.get("/fapi/v1/positionMargin/history", query).await?)
    }

    pub async fn get_balance(&self) -> Result<Vec<m::Balance>, BinanceFuturesError> {
        Ok(self.get("/fapi/v2/balance", String::new()).await?)
    }

    pub async fn get_account(&self) -> Result<m::Account, BinanceFuturesError> {
        Ok(self.get("/fapi/v2/account", String::new()).await?)
    }

    pub async fn get_position_information_v3(
        &self,
    ) -> Result<Vec<serde_json::Value>, BinanceFuturesError> {
        Ok(self.get("/fapi/v3/positionRisk", String::new()).await?)
    }

    pub async fn get_commission_rate(
        &self,
        symbol: &str,
    ) -> Result<m::CommissionRate, BinanceFuturesError> {
        Ok(self
            .get("/fapi/v1/commissionRate", format!("symbol={symbol}"))
            .await?)
    }

    pub async fn get_account_config(&self) -> Result<serde_json::Value, BinanceFuturesError> {
        Ok(self.get("/fapi/v1/accountConfig", String::new()).await?)
    }

    pub async fn get_symbol_config(
        &self,
        symbol: Option<&str>,
    ) -> Result<serde_json::Value, BinanceFuturesError> {
        let query = symbol.map(|s| format!("symbol={s}")).unwrap_or_default();
        Ok(self.get("/fapi/v1/symbolConfig", query).await?)
    }

    pub async fn get_rate_limit_order(&self) -> Result<serde_json::Value, BinanceFuturesError> {
        Ok(self.get("/fapi/v1/rateLimit/order", String::new()).await?)
    }

    pub async fn get_leverage_bracket(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<m::LeverageBracket>, BinanceFuturesError> {
        let query = symbol.map(|s| format!("symbol={s}")).unwrap_or_default();
        Ok(self.get("/fapi/v1/leverageBracket", query).await?)
    }

    pub async fn get_income(
        &self,
        symbol: Option<&str>,
        limit: u32,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Vec<m::Income>, BinanceFuturesError> {
        let mut query = format!("limit={limit}");
        if let Some(s) = symbol {
            query.push_str(&format!("&symbol={s}"));
        }
        if let Some(t) = start_time {
            query.push_str(&format!("&startTime={t}"));
        }
        if let Some(t) = end_time {
            query.push_str(&format!("&endTime={t}"));
        }
        Ok(self.get("/fapi/v1/income", query).await?)
    }

    pub async fn get_api_trading_status(&self) -> Result<serde_json::Value, BinanceFuturesError> {
        Ok(self.get("/fapi/v1/apiTradingStatus", String::new()).await?)
    }

    pub async fn get_adl_quantile(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<m::AdlQuantile>, BinanceFuturesError> {
        let query = symbol.map(|s| format!("symbol={s}")).unwrap_or_default();
        let response: EmptyObjectOrMany<m::AdlQuantile> =
            self.get("/fapi/v1/adlQuantile", query).await?;
        match response {
            EmptyObjectOrMany::Many(values) => Ok(values),
            EmptyObjectOrMany::Empty(value) if value.is_empty() => Ok(Vec::new()),
            EmptyObjectOrMany::Empty(value) => Err(BinanceFuturesError::OrderError {
                code: value.get("code").and_then(|v| v.as_i64()).unwrap_or(-1),
                msg: value
                    .get("msg")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unexpected ADL quantile response")
                    .to_owned(),
            }),
        }
    }

    pub async fn get_symbol_adl_risk(
        &self,
        symbol: Option<&str>,
    ) -> Result<serde_json::Value, BinanceFuturesError> {
        let query = symbol.map(|s| format!("symbol={s}")).unwrap_or_default();
        Ok(self.get("/fapi/v1/symbolAdlRisk", query).await?)
    }

    // ---------------- 用户数据流 ----------------

    pub async fn close_user_data_stream(&self) -> Result<(), BinanceFuturesError> {
        let _: serde_json::Value = self.delete("/fapi/v1/listenKey", String::new()).await?;
        Ok(())
    }

    // ---------------- 异步历史下载 ----------------

    pub async fn get_income_asyn(
        &self,
        start_time: i64,
        end_time: i64,
    ) -> Result<m::AsyncDownloadId, BinanceFuturesError> {
        let query = format!("startTime={start_time}&endTime={end_time}");
        Ok(self.get("/fapi/v1/income/asyn", query).await?)
    }

    pub async fn get_income_asyn_link(
        &self,
        download_id: &str,
    ) -> Result<m::AsyncDownloadLink, BinanceFuturesError> {
        let query = format!("downloadId={download_id}");
        Ok(self.get("/fapi/v1/income/asyn/id", query).await?)
    }

    // ---------------- 算法单 / 条件单 ----------------

    pub async fn submit_algo_order(
        &self,
        body: String,
    ) -> Result<serde_json::Value, BinanceFuturesError> {
        checked_exchange_value(self.post("/fapi/v1/algoOrder", body).await?)
    }

    pub async fn cancel_algo_order(
        &self,
        body: String,
    ) -> Result<serde_json::Value, BinanceFuturesError> {
        checked_exchange_value(self.delete("/fapi/v1/algoOrder", body).await?)
    }

    pub async fn cancel_all_algo_orders(&self) -> Result<serde_json::Value, BinanceFuturesError> {
        checked_exchange_value(
            self.delete("/fapi/v1/algoOpenOrders", String::new())
                .await?,
        )
    }

    pub async fn get_algo_order(
        &self,
        query: String,
    ) -> Result<serde_json::Value, BinanceFuturesError> {
        Ok(self.get("/fapi/v1/algoOrder", query).await?)
    }

    pub async fn get_open_algo_orders(
        &self,
        query: String,
    ) -> Result<serde_json::Value, BinanceFuturesError> {
        Ok(self.get("/fapi/v1/openAlgoOrders", query).await?)
    }

    pub async fn get_all_algo_orders(
        &self,
        query: String,
    ) -> Result<serde_json::Value, BinanceFuturesError> {
        Ok(self.get("/fapi/v1/allAlgoOrders", query).await?)
    }
}

// ------------------------------------------------------------------
// 统一 BrokerApi 实现
// ------------------------------------------------------------------

#[async_trait::async_trait]
impl BrokerApi for BinanceFuturesClient {
    async fn ping(&self) -> Result<(), ApiError> {
        Ok(BinanceFuturesClient::ping(self).await?)
    }

    async fn get_server_time(&self) -> Result<i64, ApiError> {
        Ok(BinanceFuturesClient::get_server_time(self).await?)
    }

    async fn get_instruments(&self) -> Result<Vec<InstrumentInfo>, ApiError> {
        let symbols = self.get_exchange_info().await?;
        Ok(symbols.iter().map(instrument_from_symbol).collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<Ticker, ApiError> {
        let t24 = self
            .get_ticker_24h(Some(symbol))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| ApiError::new("binance", "EMPTY", "ticker not found"))?;
        let p = self
            .get_premium_index(Some(symbol))
            .await?
            .into_iter()
            .next();
        Ok(ticker_from(&t24, p.as_ref()))
    }

    async fn get_tickers(&self) -> Result<Vec<Ticker>, ApiError> {
        let t24s = self.get_ticker_24h(None).await?;
        let premiums = self.get_premium_index(None).await?;
        Ok(t24s
            .iter()
            .map(|t24| {
                let p = premiums.iter().find(|p| p.symbol == t24.symbol);
                ticker_from(t24, p)
            })
            .collect())
    }

    async fn get_order_book(&self, symbol: &str, _limit: u32) -> Result<OrderBook, ApiError> {
        let depth = self.get_depth(symbol).await?;
        Ok(OrderBook {
            symbol: symbol.to_lowercase(),
            bids: depth
                .bids
                .iter()
                .map(|(p, q)| PriceLevel {
                    price: p.parse().unwrap_or(0.0),
                    qty: q.parse().unwrap_or(0.0),
                })
                .collect(),
            asks: depth
                .asks
                .iter()
                .map(|(p, q)| PriceLevel {
                    price: p.parse().unwrap_or(0.0),
                    qty: q.parse().unwrap_or(0.0),
                })
                .collect(),
            timestamp: depth.event_time,
        })
    }

    async fn get_trades(&self, symbol: &str, limit: u32) -> Result<Vec<Trade>, ApiError> {
        let trades = self.get_public_trades(symbol, limit.min(1000)).await?;
        Ok(trades
            .iter()
            .map(|t| Trade {
                symbol: symbol.to_lowercase(),
                id: t.id.to_string(),
                price: t.price,
                qty: t.qty,
                side: if t.is_buyer_maker {
                    ApiSide::Sell
                } else {
                    ApiSide::Buy
                },
                timestamp: t.time,
            })
            .collect())
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
    ) -> Result<Vec<Kline>, ApiError> {
        let rows = self
            .get_klines(symbol, interval, limit.min(1500), None, None)
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                if r.len() < 9 {
                    return None;
                }
                let f = |i: usize| r[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                Some(Kline {
                    symbol: symbol.to_lowercase(),
                    interval: interval.to_string(),
                    open_time: r[0].as_i64().unwrap_or(0),
                    close_time: r[6].as_i64().unwrap_or(0),
                    open: f(1),
                    high: f(2),
                    low: f(3),
                    close: f(4),
                    volume: f(5),
                    quote_volume: f(7),
                })
            })
            .collect())
    }

    async fn get_funding_rate(&self, symbol: &str) -> Result<FundingRate, ApiError> {
        let p = self
            .get_premium_index(Some(symbol))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| ApiError::new("binance", "EMPTY", "premium index not found"))?;
        Ok(FundingRate {
            symbol: symbol.to_lowercase(),
            funding_rate: f64_or_zero(p.last_funding_rate.as_deref().unwrap_or("0")),
            next_funding_time: p.next_funding_time,
            timestamp: p.time,
        })
    }

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRate>, ApiError> {
        let records = self
            .get_funding_rate_records(symbol, limit.min(1000), None, None)
            .await?;
        Ok(records
            .iter()
            .map(|r| FundingRate {
                symbol: symbol.to_lowercase(),
                funding_rate: r.funding_rate,
                next_funding_time: 0,
                timestamp: r.funding_time,
            })
            .collect())
    }

    async fn get_open_interest(&self, symbol: &str) -> Result<OpenInterest, ApiError> {
        let oi = self.get_open_interest(symbol).await?;
        Ok(OpenInterest {
            symbol: symbol.to_lowercase(),
            open_interest: oi.open_interest,
            timestamp: oi.time,
        })
    }

    async fn submit_order(&self, req: &UnifiedOrderRequest) -> Result<OrderInfo, ApiError> {
        let body = build_submit_body(req);
        let resp: m::OrderResponseResult = self.post("/fapi/v1/order", body).await?;
        match resp {
            m::OrderResponseResult::Ok(o) => Ok(order_info_from_response(&o)),
            m::OrderResponseResult::Err(e) => {
                Err(ApiError::new("binance", e.code.to_string(), e.msg))
            }
        }
    }

    async fn submit_orders(
        &self,
        reqs: &[UnifiedOrderRequest],
    ) -> Result<Vec<OrderInfo>, ApiError> {
        if reqs.len() > 5 {
            return Err(ApiError::new(
                "binance",
                "INVALID",
                "batch orders limited to 5",
            ));
        }
        let body = build_batch_submit_body(reqs);
        let resp: Vec<m::OrderResponseResult> = self.post("/fapi/v1/batchOrders", body).await?;
        resp.into_iter()
            .map(|r| match r {
                m::OrderResponseResult::Ok(o) => Ok(order_info_from_response(&o)),
                m::OrderResponseResult::Err(e) => {
                    Err(ApiError::new("binance", e.code.to_string(), e.msg))
                }
            })
            .collect()
    }

    async fn cancel_order(&self, req: &CancelOrderRequest) -> Result<OrderInfo, ApiError> {
        let mut body = format!("symbol={}", req.symbol);
        if let Some(id) = &req.order_id {
            body.push_str(&format!("&orderId={id}"));
        }
        if let Some(id) = &req.client_order_id {
            body.push_str(&format!("&origClientOrderId={id}"));
        }
        let resp: m::OrderResponseResult = self.delete("/fapi/v1/order", body).await?;
        match resp {
            m::OrderResponseResult::Ok(o) => Ok(order_info_from_response(&o)),
            m::OrderResponseResult::Err(e) => {
                Err(ApiError::new("binance", e.code.to_string(), e.msg))
            }
        }
    }

    async fn cancel_orders(&self, reqs: &[CancelOrderRequest]) -> Result<Vec<OrderInfo>, ApiError> {
        if reqs.is_empty() || reqs.len() > 10 {
            return Err(ApiError::new(
                "binance",
                "INVALID",
                "batch cancel limited to 1-10 orders",
            ));
        }
        let symbol = &reqs[0].symbol;
        let order_ids: Vec<String> = reqs.iter().filter_map(|r| r.order_id.clone()).collect();
        let client_ids: Vec<String> = reqs
            .iter()
            .filter_map(|r| r.client_order_id.clone())
            .collect();
        if order_ids.is_empty() && client_ids.is_empty() {
            return Err(ApiError::new("binance", "INVALID", "no order ids"));
        }
        let body = if !order_ids.is_empty() {
            format!(
                "symbol={symbol}&orderIdList=[{}]",
                order_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        } else {
            format!(
                "symbol={symbol}&origClientOrderIdList=[{}]",
                client_ids
                    .iter()
                    .map(|id| format!("\"{id}\""))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        };
        let resp: Vec<m::OrderResponseResult> = self.delete("/fapi/v1/batchOrders", body).await?;
        resp.into_iter()
            .map(|r| match r {
                m::OrderResponseResult::Ok(o) => Ok(order_info_from_response(&o)),
                m::OrderResponseResult::Err(e) => {
                    Err(ApiError::new("binance", e.code.to_string(), e.msg))
                }
            })
            .collect()
    }

    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), ApiError> {
        Ok(BinanceFuturesClient::cancel_all_orders(self, symbol).await?)
    }

    async fn cancel_all_after(&self, timeout_ms: u64) -> Result<(), ApiError> {
        let symbols = self.registered_symbols();
        if symbols.is_empty() {
            return Err(ApiError::new(
                "binance",
                "INVALID",
                "countdown cancel requires at least one registered symbol",
            ));
        }
        for symbol in symbols {
            let _ = self
                .countdown_cancel_all(&symbol.to_ascii_uppercase(), timeout_ms)
                .await?;
        }
        Ok(())
    }

    async fn amend_order(&self, req: &AmendOrderRequest) -> Result<OrderInfo, ApiError> {
        let mut attempt = 0_u32;
        let current = loop {
            match BinanceFuturesClient::get_order(
                self,
                &req.symbol,
                req.order_id.as_deref().and_then(|id| id.parse().ok()),
                req.client_order_id.as_deref(),
            )
            .await
            {
                Ok(order) => break order,
                Err(BinanceFuturesError::OrderError { code: -2013, .. }) if attempt < 5 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(50 * attempt as u64)).await;
                }
                Err(error) => return Err(error.into()),
            }
        };
        let mut body = format!("symbol={}", req.symbol);
        if let Some(id) = &req.order_id {
            body.push_str(&format!("&orderId={id}"));
        }
        if let Some(id) = &req.client_order_id {
            body.push_str(&format!("&origClientOrderId={id}"));
        }
        body.push_str(&format!("&side={}", current.side));
        body.push_str(&format!(
            "&price={}",
            decimal_param(req.new_price.unwrap_or(current.price))
        ));
        body.push_str(&format!(
            "&quantity={}",
            decimal_param(req.new_qty.unwrap_or(current.orig_qty))
        ));
        let resp: m::OrderResponseResult = self.put("/fapi/v1/order", body).await?;
        match resp {
            m::OrderResponseResult::Ok(o) => Ok(order_info_from_response(&o)),
            m::OrderResponseResult::Err(e) => {
                Err(ApiError::new("binance", e.code.to_string(), e.msg))
            }
        }
    }

    async fn get_order(
        &self,
        symbol: &str,
        order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> Result<OrderInfo, ApiError> {
        let order = self
            .get_order(
                symbol,
                order_id.and_then(|id| id.parse().ok()),
                client_order_id,
            )
            .await?;
        Ok(order_info_from(&order))
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, ApiError> {
        let orders = self.get_open_orders(Some(symbol)).await?;
        Ok(orders.iter().map(order_info_from).collect())
    }

    async fn get_order_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<OrderInfo>, ApiError> {
        let orders = self
            .get_all_orders(symbol, limit.min(1000), None, None, None)
            .await?;
        Ok(orders.iter().map(order_info_from).collect())
    }

    async fn get_fills(&self, symbol: &str, limit: u32) -> Result<Vec<Fill>, ApiError> {
        let trades = self
            .get_user_trades(symbol, limit.min(1000), None, None, None)
            .await?;
        Ok(trades
            .iter()
            .map(|t| Fill {
                symbol: t.symbol.clone(),
                trade_id: t.id.to_string(),
                order_id: t.order_id.to_string(),
                client_order_id: String::new(),
                price: t.price,
                qty: t.qty,
                side: ApiSide::from_str(t.side.as_ref()),
                fee: t.commission,
                fee_asset: t.commission_asset.clone(),
                realized_pnl: t.realized_pnl,
                maker: t.maker,
                timestamp: t.time,
            })
            .collect())
    }

    async fn get_account(&self) -> Result<AccountInfo, ApiError> {
        let account = BinanceFuturesClient::get_account(self).await?;
        Ok(account_from(&account))
    }

    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<PositionInfo>, ApiError> {
        let positions = self.get_position_information().await?;
        Ok(positions
            .iter()
            .filter(|position| symbol_matches(symbol, &position.symbol))
            .map(position_from)
            .collect())
    }

    async fn set_leverage(
        &self,
        symbol: &str,
        leverage: f64,
        _position_side: Option<ApiPositionSide>,
    ) -> Result<LeverageInfo, ApiError> {
        let resp = self.set_leverage(symbol, leverage as u32).await?;
        let leverage = resp
            .get("leverage")
            .and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(0.0);
        let margin_type = resp
            .get("marginType")
            .and_then(|v| v.as_str())
            .map(ApiMarginType::from_str)
            .unwrap_or(ApiMarginType::Unknown);
        Ok(LeverageInfo {
            symbol: symbol.to_lowercase(),
            leverage,
            margin_type,
            position_side: ApiPositionSide::Unknown,
        })
    }

    async fn get_leverage(&self, symbol: &str) -> Result<LeverageInfo, ApiError> {
        // Binance 无独立查询当前杠杆的端点，从杠杆档位表返回初始档杠杆。
        let brackets = self.get_leverage_bracket(Some(symbol)).await?;
        let leverage = brackets
            .iter()
            .find(|b| b.symbol == symbol)
            .and_then(|b| b.brackets.first())
            .map(|b| b.initial_leverage as f64)
            .unwrap_or(0.0);
        Ok(LeverageInfo {
            symbol: symbol.to_lowercase(),
            leverage,
            margin_type: ApiMarginType::Unknown,
            position_side: ApiPositionSide::Unknown,
        })
    }

    async fn get_fee_rates(&self, symbol: &str) -> Result<FeeRate, ApiError> {
        let rate = self.get_commission_rate(symbol).await?;
        Ok(FeeRate {
            symbol: rate.symbol.clone(),
            maker_fee: rate.maker_commission_rate,
            taker_fee: rate.taker_commission_rate,
            timestamp: 0,
        })
    }

    async fn get_income_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<IncomeRecord>, ApiError> {
        let incomes = self
            .get_income(Some(symbol), limit.min(1000), None, None)
            .await?;
        Ok(incomes
            .iter()
            .map(|i| IncomeRecord {
                symbol: i.symbol.clone(),
                income_type: i.income_type.clone(),
                income: i.income,
                asset: i.asset.clone(),
                timestamp: i.time,
            })
            .collect())
    }
}

// ------------------------------------------------------------------
// 请求体构建（可单测）
// ------------------------------------------------------------------

fn decimal_param(value: f64) -> String {
    let formatted = format!("{value:.12}");
    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
    if trimmed == "-0" {
        "0".to_owned()
    } else {
        trimmed.to_owned()
    }
}

/// 构建 POST /fapi/v1/order 的 body。
pub(crate) fn build_submit_body(req: &UnifiedOrderRequest) -> String {
    let mut body = format!(
        "symbol={}&side={}&quantity={}&type={}",
        req.symbol,
        req.side.as_str(),
        decimal_param(req.qty),
        req.order_type.as_str()
    );
    if let Some(id) = &req.client_order_id {
        body.push_str(&format!("&newClientOrderId={id}"));
    }
    if let Some(p) = req.price {
        body.push_str(&format!("&price={}", decimal_param(p)));
    }
    if req.order_type == ApiOrderType::Limit {
        body.push_str(&format!("&timeInForce={}", req.time_in_force.as_str()));
    }
    if req.reduce_only {
        body.push_str("&reduceOnly=true");
    }
    if let Some(side) = req.position_side {
        body.push_str(&format!("&positionSide={}", side.as_str()));
    }
    if let Some(sp) = req.stop_price {
        body.push_str(&format!("&stopPrice={}", decimal_param(sp)));
    }
    body
}

/// 构建 POST /fapi/v1/batchOrders 的 form body。
pub(crate) fn build_batch_submit_body(reqs: &[UnifiedOrderRequest]) -> String {
    let items: Vec<String> = reqs
        .iter()
        .map(|req| {
            let mut inner = format!(
                "\"symbol\":\"{}\",\"side\":\"{}\",\"quantity\":\"{}\",\"type\":\"{}\"",
                req.symbol,
                req.side.as_str(),
                decimal_param(req.qty),
                req.order_type.as_str()
            );
            if let Some(id) = &req.client_order_id {
                inner.push_str(&format!(",\"newClientOrderId\":\"{id}\""));
            }
            if let Some(p) = req.price {
                inner.push_str(&format!(",\"price\":\"{}\"", decimal_param(p)));
            }
            if req.order_type == ApiOrderType::Limit {
                inner.push_str(&format!(
                    ",\"timeInForce\":\"{}\"",
                    req.time_in_force.as_str()
                ));
            }
            if req.reduce_only {
                inner.push_str(",\"reduceOnly\":\"true\"");
            }
            if let Some(side) = req.position_side {
                inner.push_str(&format!(",\"positionSide\":\"{}\"", side.as_str()));
            }
            if let Some(sp) = req.stop_price {
                inner.push_str(&format!(",\"stopPrice\":\"{}\"", decimal_param(sp)));
            }
            format!("{{{inner}}}")
        })
        .collect();
    format!("batchOrders=[{}]", items.join(","))
}

impl From<Side> for ApiSide {
    fn from(s: Side) -> Self {
        match s {
            Side::Buy => ApiSide::Buy,
            Side::Sell => ApiSide::Sell,
            _ => ApiSide::Unknown,
        }
    }
}

impl From<OrdType> for ApiOrderType {
    fn from(t: OrdType) -> Self {
        match t {
            OrdType::Limit => ApiOrderType::Limit,
            OrdType::Market => ApiOrderType::Market,
            _ => ApiOrderType::Unknown,
        }
    }
}

impl From<Status> for ApiOrderStatus {
    fn from(s: Status) -> Self {
        match s {
            Status::New => ApiOrderStatus::New,
            Status::PartiallyFilled => ApiOrderStatus::PartiallyFilled,
            Status::Filled => ApiOrderStatus::Filled,
            Status::Canceled => ApiOrderStatus::Canceled,
            Status::Rejected => ApiOrderStatus::Rejected,
            Status::Expired => ApiOrderStatus::Expired,
            _ => ApiOrderStatus::Unknown,
        }
    }
}

impl From<TimeInForce> for ApiTimeInForce {
    fn from(t: TimeInForce) -> Self {
        match t {
            TimeInForce::GTC => ApiTimeInForce::GTC,
            TimeInForce::IOC => ApiTimeInForce::IOC,
            TimeInForce::FOK => ApiTimeInForce::FOK,
            TimeInForce::GTX => ApiTimeInForce::GTX,
            _ => ApiTimeInForce::Unknown,
        }
    }
}

/// AlgoOrderRequest 转 Binance algoOrder body（STOP_MARKET/TAKE_PROFIT_MARKET 等）。
pub(crate) fn build_algo_body(req: &AlgoOrderRequest) -> String {
    let mut body = format!(
        "algoType=CONDITIONAL&symbol={}&side={}&quantity={}&type={}&triggerPrice={}",
        req.symbol,
        req.side.as_str(),
        decimal_param(req.qty),
        req.order_type.as_str(),
        decimal_param(req.trigger_price)
    );
    if let Some(p) = req.price {
        body.push_str(&format!("&price={}", decimal_param(p)));
    }
    if let Some(id) = &req.client_order_id {
        body.push_str(&format!("&clientAlgoId={id}"));
    }
    if let Some(ro) = req.reduce_only {
        body.push_str(&format!("&reduceOnly={ro}"));
    }
    body
}

use hftbacktest::types::Status;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        AlgoOrderRequest, AmendOrderRequest, ApiOrderStatus, ApiOrderType, ApiPositionSide,
        ApiSide, ApiTimeInForce, CancelOrderRequest, UnifiedOrderRequest,
    };

    fn limit_req() -> UnifiedOrderRequest {
        UnifiedOrderRequest {
            symbol: "BTCUSDT".to_string(),
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

    /// Opt-in mainnet inventory test for every read-only endpoint implemented by the
    /// Binance USD-M client. Credentials are read from the process environment and
    /// are never persisted. This test deliberately excludes endpoints that place an
    /// order or change account configuration; those are covered by the guarded live
    /// round-trip example.
    #[tokio::test]
    #[ignore = "requires explicit Binance mainnet credentials"]
    async fn live_readonly_endpoint_inventory() {
        let api_key =
            std::env::var("BINANCE_FUTURES_API_KEY").expect("BINANCE_FUTURES_API_KEY is required");
        let secret = std::env::var("BINANCE_FUTURES_API_SECRET")
            .expect("BINANCE_FUTURES_API_SECRET is required");
        let api_url = std::env::var("BINANCE_FUTURES_API_URL")
            .unwrap_or_else(|_| "https://fapi.binance.com".to_owned());
        let symbol = std::env::var("BINANCE_FUTURES_TEST_SYMBOL")
            .unwrap_or_else(|_| "XRPUSDT".to_owned())
            .to_uppercase();
        let client = BinanceFuturesClient::new(&api_url, &api_key, &secret);
        let mut failures = Vec::new();

        macro_rules! check {
            ($name:literal, $future:expr) => {
                match $future.await {
                    Ok(_) => println!("PASS {}", $name),
                    Err(error) => {
                        let failure = format!("{}: {error}", $name);
                        println!("FAIL {failure}");
                        failures.push(failure);
                    }
                }
            };
        }

        check!("ping", client.ping());
        check!("server_time", client.get_server_time());
        check!("exchange_info", client.get_exchange_info());
        check!("ticker_24h_one", client.get_ticker_24h(Some(&symbol)));
        check!("ticker_24h_all", client.get_ticker_24h(None));
        check!("premium_index_one", client.get_premium_index(Some(&symbol)));
        check!("premium_index_all", client.get_premium_index(None));
        check!("ticker_price_one", client.get_ticker_price(Some(&symbol)));
        check!("ticker_price_all", client.get_ticker_price(None));
        check!("book_ticker_one", client.get_book_ticker(Some(&symbol)));
        check!("book_ticker_all", client.get_book_ticker(None));
        check!("depth", client.get_depth(&symbol));
        check!("public_trades", client.get_public_trades(&symbol, 5));
        check!(
            "historical_trades",
            client.get_historical_trades(&symbol, 5, None)
        );
        check!(
            "aggregate_trades",
            client.get_agg_trades(&symbol, 5, None, None, None)
        );
        check!("klines", client.get_klines(&symbol, "1m", 5, None, None));
        check!(
            "funding_rate_history",
            client.get_funding_rate_records(&symbol, 5, None, None)
        );
        check!("funding_info", client.get_funding_info());
        check!("open_interest", client.get_open_interest(&symbol));
        check!("insurance_balance", client.get_insurance_balance());
        check!("index_info", client.get_index_info());
        check!("asset_index_all", client.get_asset_index(None));
        check!("constituents", client.get_constituents(&symbol));
        check!("trading_schedule", client.get_trading_schedule());
        for (name, path) in [
            ("open_interest_history", "/futures/data/openInterestHist"),
            (
                "top_long_short_account_ratio",
                "/futures/data/topLongShortAccountRatio",
            ),
            (
                "top_long_short_position_ratio",
                "/futures/data/topLongShortPositionRatio",
            ),
            (
                "global_long_short_account_ratio",
                "/futures/data/globalLongShortAccountRatio",
            ),
            (
                "taker_long_short_ratio",
                "/futures/data/takerlongshortRatio",
            ),
        ] {
            match client.get_data_rows(path, &symbol, "5m", 5).await {
                Ok(_) => println!("PASS {name}"),
                Err(error) => {
                    let failure = format!("{name}: {error}");
                    println!("FAIL {failure}");
                    failures.push(failure);
                }
            }
        }
        check!("delivery_price", client.get_delivery_price(&symbol));
        check!("basis", client.get_basis(&symbol, "5m", 5));

        check!("open_orders", client.get_open_orders(Some(&symbol)));
        match client.get_all_orders(&symbol, 5, None, None, None).await {
            Ok(orders) => {
                println!("PASS all_orders");
                if let Some(order) = orders.last() {
                    check!(
                        "order_amendment",
                        client.get_order_amendment(&symbol, Some(order.order_id), None, 5)
                    );
                } else {
                    let failure = "order_amendment: no historical order available".to_owned();
                    println!("FAIL {failure}");
                    failures.push(failure);
                }
            }
            Err(error) => {
                let failure = format!("all_orders: {error}");
                println!("FAIL {failure}");
                failures.push(failure);
            }
        }
        check!(
            "user_trades",
            client.get_user_trades(&symbol, 5, None, None, None)
        );
        check!(
            "force_orders",
            client.get_force_orders(Some(&symbol), None, 5)
        );
        check!(
            "test_order",
            client.submit_test_order(format!("symbol={symbol}&side=BUY&type=MARKET&quantity=3.7"))
        );

        check!("position_mode", client.get_position_mode());
        check!("multi_assets_mode", client.get_multi_assets_mode());
        check!("balance", client.get_balance());
        check!("account", client.get_account());
        check!("positions_v2", client.get_position_information());
        check!("positions_v3", client.get_position_information_v3());
        check!("commission_rate", client.get_commission_rate(&symbol));
        check!("account_config", client.get_account_config());
        check!("symbol_config_one", client.get_symbol_config(Some(&symbol)));
        check!("symbol_config_all", client.get_symbol_config(None));
        check!("rate_limit_order", client.get_rate_limit_order());
        check!(
            "leverage_bracket",
            client.get_leverage_bracket(Some(&symbol))
        );
        check!("income", client.get_income(Some(&symbol), 5, None, None));
        check!("api_trading_status", client.get_api_trading_status());
        check!("adl_quantile", client.get_adl_quantile(Some(&symbol)));
        check!("symbol_adl_risk", client.get_symbol_adl_risk(Some(&symbol)));
        check!(
            "position_margin_history",
            client.get_position_margin_history(&symbol, 5)
        );
        check!(
            "open_algo_orders",
            client.get_open_algo_orders(format!("symbol={symbol}"))
        );
        check!(
            "all_algo_orders",
            client.get_all_algo_orders(format!("symbol={symbol}&limit=5"))
        );

        match client.start_user_data_stream().await {
            Ok(listen_key) => {
                println!("PASS user_stream_start");
                check!("user_stream_keepalive", client.keepalive_user_data_stream());
                check!("user_stream_close", client.close_user_data_stream());
                assert!(!listen_key.is_empty(), "empty listen key");
            }
            Err(error) => failures.push(format!("user_stream_start: {error}")),
        }

        assert!(
            failures.is_empty(),
            "{} Binance endpoints failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    #[tokio::test]
    #[ignore = "changes and restores Binance mainnet account configuration"]
    async fn live_reversible_endpoint_inventory() {
        assert_eq!(
            std::env::var("BINANCE_FUTURES_LIVE_CONFIRM").as_deref(),
            Ok("I_UNDERSTAND_REAL_ORDERS")
        );
        let api_key =
            std::env::var("BINANCE_FUTURES_API_KEY").expect("BINANCE_FUTURES_API_KEY is required");
        let secret = std::env::var("BINANCE_FUTURES_API_SECRET")
            .expect("BINANCE_FUTURES_API_SECRET is required");
        let api_url = std::env::var("BINANCE_FUTURES_API_URL")
            .unwrap_or_else(|_| "https://fapi.binance.com".to_owned());
        let symbol = std::env::var("BINANCE_FUTURES_TEST_SYMBOL")
            .unwrap_or_else(|_| "XRPUSDT".to_owned())
            .to_uppercase();
        let client = BinanceFuturesClient::new(&api_url, &api_key, &secret);
        let mut failures = Vec::new();

        let original_position_mode = client.get_position_mode().await.unwrap();
        let original_multi_assets = client.get_multi_assets_mode().await.unwrap();
        let original_config = client.get_symbol_config(Some(&symbol)).await.unwrap();
        let config = original_config
            .as_array()
            .and_then(|values| values.first())
            .expect("missing symbol configuration");
        let original_leverage = config
            .get("leverage")
            .and_then(|value| value.as_u64())
            .expect("missing leverage") as u32;
        let original_margin = config
            .get("marginType")
            .and_then(|value| value.as_str())
            .expect("missing margin type")
            .to_owned();
        assert!(
            client
                .get_open_orders(Some(&symbol))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            client
                .get_position_information()
                .await
                .unwrap()
                .iter()
                .filter(|position| position.symbol.eq_ignore_ascii_case(&symbol))
                .all(|position| position.position_amount.abs() < 1e-12)
        );

        let alternate_leverage = if original_leverage == 6 { 5 } else { 6 };
        if let Err(error) = client.set_leverage(&symbol, alternate_leverage).await {
            failures.push(format!("set_leverage: {error}"));
        }
        if let Err(error) = client.set_leverage(&symbol, original_leverage).await {
            panic!("failed to restore leverage: {error}");
        }
        println!("PASS set_leverage_restore");

        let alternate_margin = if original_margin == "ISOLATED" {
            "CROSSED"
        } else {
            "ISOLATED"
        };
        if original_multi_assets {
            if let Err(error) = client.set_multi_assets_mode(false).await {
                failures.push(format!("disable_multi_assets_for_margin_test: {error}"));
            }
        }
        if let Err(error) = client.set_margin_type(&symbol, alternate_margin).await {
            failures.push(format!("set_margin_type: {error}"));
        }
        if let Err(error) = client.set_margin_type(&symbol, &original_margin).await {
            panic!("failed to restore margin type: {error}");
        }
        if original_multi_assets {
            if let Err(error) = client.set_multi_assets_mode(true).await {
                panic!("failed to restore multi-assets mode after margin test: {error}");
            }
        }
        println!("PASS set_margin_type_restore");

        if let Err(error) = client.set_position_mode(!original_position_mode).await {
            failures.push(format!("set_position_mode: {error}"));
        }
        if let Err(error) = client.set_position_mode(original_position_mode).await {
            panic!("failed to restore position mode: {error}");
        }
        println!("PASS set_position_mode_restore");

        if let Err(error) = client.set_multi_assets_mode(!original_multi_assets).await {
            failures.push(format!("set_multi_assets_mode: {error}"));
        }
        if let Err(error) = client.set_multi_assets_mode(original_multi_assets).await {
            panic!("failed to restore multi-assets mode: {error}");
        }
        println!("PASS set_multi_assets_mode_restore");

        match client.countdown_cancel_all(&symbol, 5_000).await {
            Ok(value) if value.countdown_time == 5_000 => println!("PASS countdown_cancel_all_set"),
            Ok(value) => failures.push(format!(
                "countdown_cancel_all_set: returned {}",
                value.countdown_time
            )),
            Err(error) => failures.push(format!("countdown_cancel_all_set: {error}")),
        }
        if let Err(error) = client.countdown_cancel_all(&symbol, 0).await {
            panic!("failed to disable countdown cancel: {error}");
        }
        println!("PASS countdown_cancel_all_disable");

        let algo_id = format!(
            "titanalgo{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        let algo = AlgoOrderRequest {
            symbol: symbol.clone(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::StopMarket,
            qty: 3.7,
            price: None,
            trigger_price: 2.0,
            stop_price: None,
            reduce_only: Some(false),
            client_order_id: Some(algo_id),
        };
        match client.submit_algo_order(build_algo_body(&algo)).await {
            Ok(value) => {
                if let Some(id) = value.get("algoId").and_then(|value| value.as_i64()) {
                    match client.get_algo_order(format!("algoId={id}")).await {
                        Ok(_) => println!("PASS algo_order_query"),
                        Err(error) => failures.push(format!("algo_order_query: {error}")),
                    }
                    match client.cancel_algo_order(format!("algoId={id}")).await {
                        Ok(_) => println!("PASS algo_order_submit_cancel"),
                        Err(error) => failures.push(format!("algo_order_cancel: {error}")),
                    }
                } else {
                    failures.push(format!("algo_order_submit: missing algoId in {value}"));
                }
            }
            Err(error) => failures.push(format!("algo_order_submit: {error}")),
        }
        match client.cancel_all_algo_orders().await {
            Ok(_) => println!("PASS cancel_all_algo_orders"),
            Err(error) => failures.push(format!("cancel_all_algo_orders: {error}")),
        }

        let ticker = client
            .get_ticker_24h(Some(&symbol))
            .await
            .unwrap()
            .remove(0);
        let market_qty = ((5.02 / ticker.last_price) * 10.0).ceil() / 10.0;
        assert!(ticker.last_price * market_qty <= 5.10);
        let position_run_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if original_multi_assets {
            client.set_multi_assets_mode(false).await.unwrap();
        }
        client.set_margin_type(&symbol, "ISOLATED").await.unwrap();
        let opened = BrokerApi::submit_order(
            &client,
            &UnifiedOrderRequest {
                symbol: symbol.clone(),
                side: ApiSide::Buy,
                order_type: ApiOrderType::Market,
                price: None,
                qty: market_qty,
                time_in_force: ApiTimeInForce::GTC,
                reduce_only: false,
                position_side: None,
                client_order_id: Some(format!("titanmargin{position_run_id}o")),
                stop_price: None,
            },
        )
        .await;
        match opened {
            Ok(_) => {
                match client.set_position_margin(&symbol, 0.1, "BOTH", 1).await {
                    Ok(_) => println!("PASS position_margin_add"),
                    Err(error) => failures.push(format!("position_margin_add: {error}")),
                }
                let mut history_visible = false;
                for _ in 0..10 {
                    match client.get_position_margin_history(&symbol, 5).await {
                        Ok(history) if !history.is_empty() => {
                            history_visible = true;
                            break;
                        }
                        Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                        Err(error) => {
                            failures.push(format!("position_margin_history_after_change: {error}"));
                            break;
                        }
                    }
                }
                if history_visible {
                    println!("PASS position_margin_history_after_change");
                } else if !failures
                    .iter()
                    .any(|failure| failure.starts_with("position_margin_history_after_change:"))
                {
                    failures.push("position_margin_history_after_change: empty".to_owned());
                }
                match client.set_position_margin(&symbol, 0.05, "BOTH", 2).await {
                    Ok(_) => println!("PASS position_margin_reduce"),
                    Err(error) => failures.push(format!("position_margin_reduce: {error}")),
                }
            }
            Err(error) => failures.push(format!("position_margin_open: {error}")),
        }
        let live_position = client
            .get_position_information()
            .await
            .unwrap()
            .into_iter()
            .find(|position| {
                position.symbol.eq_ignore_ascii_case(&symbol)
                    && position.position_amount.abs() > 1e-12
            });
        if let Some(position) = live_position {
            BrokerApi::submit_order(
                &client,
                &UnifiedOrderRequest {
                    symbol: symbol.clone(),
                    side: if position.position_amount > 0.0 {
                        ApiSide::Sell
                    } else {
                        ApiSide::Buy
                    },
                    order_type: ApiOrderType::Market,
                    price: None,
                    qty: position.position_amount.abs(),
                    time_in_force: ApiTimeInForce::GTC,
                    reduce_only: true,
                    position_side: None,
                    client_order_id: Some(format!("titanmargin{position_run_id}c")),
                    stop_price: None,
                },
            )
            .await
            .expect("emergency position-margin test close failed");
        }
        client
            .set_margin_type(&symbol, &original_margin)
            .await
            .expect("failed to restore margin after position-margin test");
        if original_multi_assets {
            client
                .set_multi_assets_mode(true)
                .await
                .expect("failed to restore multi-assets after position-margin test");
        }
        client
            .set_leverage(&symbol, original_leverage)
            .await
            .expect("failed to restore leverage after position-margin test");

        let mut passive_price = (ticker.last_price * 0.98 * 10_000.0).floor() / 10_000.0;
        let mut passive_qty = ((5.02 / passive_price) * 10.0).ceil() / 10.0;
        // Lot-size rounding can push the minimal passive size above the 5.10 USDT
        // hard cap once the reference price rises; step the price down one tick at
        // a time until the resulting order fits inside the cap.
        let mut notional_steps = 0;
        while passive_price * passive_qty > 5.10 && notional_steps < 1_000 {
            passive_price = ((passive_price * 10_000.0 - 1.0).floor()) / 10_000.0;
            passive_qty = ((5.02 / passive_price) * 10.0).ceil() / 10.0;
            notional_steps += 1;
        }
        assert!(
            passive_price > 0.0 && passive_price * passive_qty <= 5.10,
            "could not fit a passive order under the 5.10 USDT notional cap"
        );
        let cancel_all_id = format!("titancancelall{position_run_id}");
        match BrokerApi::submit_order(
            &client,
            &UnifiedOrderRequest {
                symbol: symbol.clone(),
                side: ApiSide::Buy,
                order_type: ApiOrderType::Limit,
                price: Some(passive_price),
                qty: passive_qty,
                time_in_force: ApiTimeInForce::GTC,
                reduce_only: false,
                position_side: None,
                client_order_id: Some(cancel_all_id),
                stop_price: None,
            },
        )
        .await
        {
            Ok(order) => {
                match client
                    .get_open_order(&symbol, Some(order.order_id.parse().unwrap()), None)
                    .await
                {
                    Ok(_) => println!("PASS get_open_order"),
                    Err(error) => failures.push(format!("get_open_order: {error}")),
                }
                match client.cancel_all_orders(&symbol).await {
                    Ok(_) => println!("PASS cancel_all_orders"),
                    Err(error) => failures.push(format!("cancel_all_orders: {error}")),
                }
            }
            Err(error) => failures.push(format!("cancel_all_test_submit: {error}")),
        }

        let now = chrono::Utc::now().timestamp_millis();
        match client.get_income_asyn(now - 3_600_000, now).await {
            Ok(download) => {
                println!("PASS income_async_request");
                match client.get_income_asyn_link(&download.id).await {
                    Ok(_) => println!("PASS income_async_status"),
                    Err(error) => failures.push(format!("income_async_status: {error}")),
                }
            }
            Err(error) => failures.push(format!("income_async_request: {error}")),
        }

        let restored = client.get_symbol_config(Some(&symbol)).await.unwrap();
        let restored = restored
            .as_array()
            .and_then(|values| values.first())
            .unwrap();
        assert_eq!(
            restored.get("leverage").and_then(|value| value.as_u64()),
            Some(original_leverage as u64)
        );
        assert_eq!(
            restored.get("marginType").and_then(|value| value.as_str()),
            Some(original_margin.as_str())
        );
        assert_eq!(
            client.get_position_mode().await.unwrap(),
            original_position_mode
        );
        assert_eq!(
            client.get_multi_assets_mode().await.unwrap(),
            original_multi_assets
        );
        assert!(
            client
                .get_open_orders(Some(&symbol))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            client
                .get_position_information()
                .await
                .unwrap()
                .iter()
                .filter(|position| position.symbol.eq_ignore_ascii_case(&symbol))
                .all(|position| position.position_amount.abs() < 1e-12)
        );
        assert!(
            failures.is_empty(),
            "{} reversible Binance endpoints failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    #[test]
    fn one_or_many_accepts_binance_symbol_and_all_symbol_shapes() {
        let one: OneOrMany<m::TickerPrice> =
            serde_json::from_str(r#"{"symbol":"BTCUSDT","price":"50000","time":1}"#).unwrap();
        assert_eq!(one.into_vec().len(), 1);

        let many: OneOrMany<m::TickerPrice> =
            serde_json::from_str(r#"[{"symbol":"BTCUSDT","price":"50000","time":1}]"#).unwrap();
        assert_eq!(many.into_vec().len(), 1);
    }

    #[test]
    fn decimal_params_do_not_leak_binary_float_precision() {
        assert_eq!(decimal_param(37.0 * 0.1), "3.7");
        assert_eq!(decimal_param(13_653.0 * 0.0001), "1.3653");
        assert_eq!(decimal_param(-0.0), "0");
    }

    #[test]
    fn server_time_accepts_binance_camel_case_field() {
        let value: m::ServerTime = serde_json::from_str(r#"{"serverTime":1599518383171}"#).unwrap();
        assert_eq!(value.server_time, 1_599_518_383_171);
    }

    #[test]
    fn order_query_error_is_not_deserialized_as_an_empty_order() {
        let response: m::ApiResponse<m::Order> =
            serde_json::from_str(r#"{"code":-2013,"msg":"Order does not exist."}"#).unwrap();
        match response {
            m::ApiResponse::Error(error) => assert_eq!(error.code, -2013),
            m::ApiResponse::Success(_) => panic!("error response was accepted as an order"),
        }
    }

    #[test]
    fn test_build_submit_body_limit() {
        let body = build_submit_body(&limit_req());
        assert!(body.contains("symbol=BTCUSDT"));
        assert!(body.contains("side=BUY"));
        assert!(body.contains("quantity=1"));
        assert!(body.contains("type=LIMIT"));
        assert!(body.contains("price=50000"));
        assert!(body.contains("timeInForce=GTC"));
        assert!(body.contains("newClientOrderId=c1"));
        assert!(body.contains("positionSide=LONG"));
    }

    #[test]
    fn test_build_submit_body_market_omits_price_and_tif() {
        let mut req = limit_req();
        req.order_type = ApiOrderType::Market;
        req.price = None;
        req.time_in_force = ApiTimeInForce::IOC;
        let body = build_submit_body(&req);
        assert!(!body.contains("price="));
        assert!(!body.contains("timeInForce="));
        assert!(body.contains("type=MARKET"));
    }

    #[test]
    fn test_build_submit_body_stop_and_reduce_only() {
        let mut req = limit_req();
        req.order_type = ApiOrderType::StopMarket;
        req.stop_price = Some(49000.0);
        req.reduce_only = true;
        let body = build_submit_body(&req);
        assert!(body.contains("stopPrice=49000"));
        assert!(body.contains("reduceOnly=true"));
    }

    #[test]
    fn test_build_batch_submit_body() {
        let reqs = vec![limit_req(), limit_req()];
        let body = build_batch_submit_body(&reqs);
        assert!(body.starts_with("batchOrders=["));
        assert_eq!(body.matches("BTCUSDT").count(), 2);
        assert!(body.contains("\"timeInForce\":\"GTC\""));
    }

    #[test]
    fn test_build_algo_body() {
        let req = AlgoOrderRequest {
            symbol: "BTCUSDT".to_string(),
            side: ApiSide::Sell,
            order_type: ApiOrderType::StopMarket,
            qty: 1.0,
            price: None,
            trigger_price: 51000.0,
            stop_price: None,
            reduce_only: Some(true),
            client_order_id: Some("a1".to_string()),
        };
        let body = build_algo_body(&req);
        assert!(body.contains("symbol=BTCUSDT"));
        assert!(body.contains("algoType=CONDITIONAL"));
        assert!(body.contains("triggerPrice=51000"));
        assert!(body.contains("clientAlgoId=a1"));
        assert!(body.contains("reduceOnly=true"));
    }

    // ---------------- 响应解析 -> 统一结构 ----------------

    #[test]
    fn test_parse_exchange_info() {
        let json = r#"{
            "symbols": [{
                "symbol": "BTCUSDT",
                "pair": "BTCUSDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "pricePrecision": 2,
                "quantityPrecision": 3,
                "contractSize": "0.001",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"}
                ]
            }]
        }"#;
        let info: m::ExchangeInfo = serde_json::from_str(json).unwrap();
        let inst = instrument_from_symbol(&info.symbols[0]);
        assert_eq!(inst.symbol, "btcusdt");
        assert_eq!(inst.tick_size, 0.10);
        assert_eq!(inst.lot_size, 0.001);
        assert_eq!(inst.min_qty, 0.001);
        assert_eq!(inst.contract_size, 0.001);
        assert_eq!(inst.margin_asset, "USDT");
        assert!(inst.tradable);
    }

    #[test]
    fn test_parse_ticker() {
        let t24: m::Ticker24h = serde_json::from_str(
            r#"{
                "symbol": "BTCUSDT",
                "priceChange": "-100",
                "priceChangePercent": "-0.5",
                "lastPrice": "50000.0",
                "highPrice": "51000.0",
                "lowPrice": "49000.0",
                "volume": "1000.0",
                "quoteVolume": "50000000.0",
                "openPrice": "50100.0",
                "closeTime": 1700000000000,
                "count": 123
            }"#,
        )
        .unwrap();
        let p: m::PremiumIndex = serde_json::from_str(
            r#"{
                "symbol": "BTCUSDT",
                "markPrice": "50005.0",
                "indexPrice": "50000.0",
                "lastFundingRate": "0.0001",
                "nextFundingTime": 1700003600000,
                "time": 1700000000000
            }"#,
        )
        .unwrap();
        let ticker = ticker_from(&t24, Some(&p));
        assert_eq!(ticker.last_price, 50000.0);
        assert_eq!(ticker.mark_price, Some(50005.0));
        assert_eq!(ticker.funding_rate, Some(0.0001));
        assert_eq!(ticker.next_funding_time, Some(1_700_003_600_000));
        assert_eq!(ticker.quote_volume_24h, 50_000_000.0);
    }

    #[test]
    fn test_parse_order_info() {
        let json = r#"{
            "symbol": "BTCUSDT",
            "orderId": 123456,
            "clientOrderId": "c1",
            "price": "50000.00",
            "origQty": "2.000",
            "executedQty": "0.500",
            "avgPrice": "49990.00",
            "status": "PARTIALLY_FILLED",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "stopPrice": "0.00",
            "reduceOnly": false,
            "positionSide": "BOTH",
            "time": 1700000000000,
            "updateTime": 1700000001000,
            "origType": "LIMIT",
            "leavesQty": "1.500"
        }"#;
        let order: m::Order = serde_json::from_str(json).unwrap();
        let info = order_info_from(&order);
        assert_eq!(info.symbol, "btcusdt");
        assert_eq!(info.order_id, "123456");
        assert_eq!(info.client_order_id, "c1");
        assert_eq!(info.side, ApiSide::Buy);
        assert_eq!(info.order_type, ApiOrderType::Limit);
        assert_eq!(info.status, ApiOrderStatus::PartiallyFilled);
        assert_eq!(info.qty, 2.0);
        assert_eq!(info.executed_qty, 0.5);
        assert_eq!(info.leaves_qty, 1.5);
        assert_eq!(info.avg_price, 49990.0);
        assert_eq!(info.position_side, ApiPositionSide::Long);
    }

    #[test]
    fn test_parse_position() {
        let json = r#"{
            "entryPrice": "50000.0",
            "breakEvenPrice": "50010.0",
            "marginType": "cross",
            "isAutoAddMargin": "false",
            "leverage": "10",
            "liquidationPrice": "45000.0",
            "markPrice": "50100.0",
            "maxNotionalValue": "1000000",
            "positionAmt": "1.500",
            "notional": "75150.0",
            "isolatedWallet": "0",
            "symbol": "BTCUSDT",
            "unRealizedProfit": "150.0",
            "positionSide": "BOTH",
            "updateTime": 1700000000000
        }"#;
        let pos: m::PositionInformationV2 = serde_json::from_str(json).unwrap();
        assert!(symbol_matches(Some("BTCUSDT"), &pos.symbol));
        assert!(!symbol_matches(Some("ETHUSDT"), &pos.symbol));
        let info = position_from(&pos);
        assert_eq!(info.symbol, "btcusdt");
        assert_eq!(info.qty, 1.5);
        assert_eq!(info.position_side, ApiPositionSide::Long);
        assert_eq!(info.entry_price, 50000.0);
        assert_eq!(info.liquidation_price, 45000.0);
        assert_eq!(info.leverage, 10.0);
        assert_eq!(info.margin_type, ApiMarginType::Cross);
        assert_eq!(info.unrealized_pnl, 150.0);
    }

    #[test]
    fn test_parse_account() {
        let json = r#"{
            "totalWalletBalance": "10000.0",
            "totalUnrealizedProfit": "100.0",
            "totalMarginBalance": "10100.0",
            "availableBalance": "5000.0",
            "updateTime": 1700000000000,
            "assets": [{
                "asset": "USDT",
                "walletBalance": "10000.0",
                "unrealizedProfit": "100.0",
                "marginBalance": "10100.0",
                "availableBalance": "5000.0",
                "updateTime": 1700000000000
            }]
        }"#;
        let account: m::Account = serde_json::from_str(json).unwrap();
        let info = account_from(&account);
        assert_eq!(info.total_wallet_balance, 10000.0);
        assert_eq!(info.total_unrealized_pnl, 100.0);
        assert_eq!(info.available_balance, 5000.0);
        assert_eq!(info.balances.len(), 1);
        assert_eq!(info.balances[0].asset, "USDT");
    }

    #[test]
    fn test_parse_fills() {
        let json = r#"[
            {
                "symbol": "BTCUSDT",
                "id": 55555,
                "orderId": 123456,
                "price": "50000.0",
                "qty": "0.5",
                "quoteQty": "25000.0",
                "commission": "1.25",
                "commissionAsset": "USDT",
                "time": 1700000000000,
                "buyer": false,
                "maker": false,
                "positionSide": "BOTH",
                "side": "BUY",
                "realizedPnl": "0.0"
            }
        ]"#;
        let trades: Vec<m::UserTrade> = serde_json::from_str(json).unwrap();
        let fill = Fill {
            symbol: trades[0].symbol.clone(),
            trade_id: trades[0].id.to_string(),
            order_id: trades[0].order_id.to_string(),
            client_order_id: String::new(),
            price: trades[0].price,
            qty: trades[0].qty,
            side: ApiSide::from_str(trades[0].side.as_ref()),
            fee: trades[0].commission,
            fee_asset: trades[0].commission_asset.clone(),
            realized_pnl: trades[0].realized_pnl,
            maker: trades[0].maker,
            timestamp: trades[0].time,
        };
        assert_eq!(fill.symbol, "btcusdt");
        assert_eq!(fill.price, 50000.0);
        assert_eq!(fill.fee, 1.25);
        assert_eq!(fill.side, ApiSide::Buy);
    }

    #[test]
    fn test_parse_funding_rate_and_open_interest() {
        let fr: m::FundingRateRecord = serde_json::from_str(
            r#"{"symbol":"BTCUSDT","fundingRate":"0.0001","fundingTime":1700000000000,"markPrice":"50000"}"#,
        )
        .unwrap();
        assert_eq!(fr.funding_rate, 0.0001);
        assert_eq!(fr.funding_time, 1_700_000_000_000);

        let oi: m::OpenInterest = serde_json::from_str(
            r#"{"symbol":"BTCUSDT","openInterest":"123.456","time":1700000000000}"#,
        )
        .unwrap();
        assert_eq!(oi.open_interest, 123.456);
    }

    #[test]
    fn test_parse_klines() {
        let json = r#"[
            [1700000000000,"50000.0","50100.0","49900.0","50050.0","10.5",1700000030000,"525525.0",100]
        ]"#;
        let rows: m::KlineResponse = serde_json::from_str(json).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 9);
    }

    #[test]
    fn test_book_ticker_stream_parse() {
        let msg = r#"{"e":"bookTicker","u":400900217,"E":1568014460893,"T":1568014460894,"s":"BTCUSDT","b":"50000.00","B":"0.500","a":"50001.00","A":"1.200"}"#;
        let stream: crate::binancefutures::msg::stream::Stream = serde_json::from_str(msg).unwrap();
        match stream {
            crate::binancefutures::msg::stream::Stream::EventStream(
                crate::binancefutures::msg::stream::EventStream::BookTicker(bt),
            ) => {
                assert_eq!(bt.symbol, "btcusdt");
                assert_eq!(bt.bid_price, 50000.0);
                assert_eq!(bt.ask_price, 50001.0);
                assert_eq!(bt.bid_qty, 0.5);
                assert_eq!(bt.ask_qty, 1.2);
            }
            _ => panic!("expected bookTicker"),
        }
    }

    #[test]
    fn test_cancel_and_amend_requests_serde() {
        let cancel = CancelOrderRequest {
            symbol: "BTCUSDT".to_string(),
            order_id: Some("123".to_string()),
            client_order_id: None,
        };
        let amend = AmendOrderRequest {
            symbol: "BTCUSDT".to_string(),
            order_id: Some("123".to_string()),
            client_order_id: None,
            new_price: Some(50100.0),
            new_qty: Some(2.0),
            new_stop_price: None,
        };
        let json = serde_json::to_string(&cancel).unwrap();
        let back: CancelOrderRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(cancel, back);
        assert!(amend.new_price.is_some());
    }
}
