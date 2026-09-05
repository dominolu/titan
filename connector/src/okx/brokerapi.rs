//! OKX V5 全量 REST 接口 + 统一 [`BrokerApi`] 实现。
//!
//! 对照官方文档补齐：
//! - 账户：balance/positions/positions-history/bills/config/set-position-mode/set-leverage/
//!   leverage-info/max-size/max-avail-size/position-margin/trade-fee/risk-state/max-withdrawal
//! - 交易：order/batch-orders/cancel-order/cancel-batch-orders/amend-order/amend-batch-orders/
//!   close-position/order(GET)/orders-pending/orders-history/fills/mass-cancel/cancel-all-after/
//!   order-precheck/order-algo 系列
//! - 行情：tickers/ticker/books/books-full/candles/history-candles/trades/history-trades
//! - 公共：instruments/funding-rate/funding-rate-history/open-interest/price-limit/time/mark-price
//! - 系统：system/status

use crate::{
    api::{
        AccountInfo, AlgoOrderRequest, AmendOrderRequest, ApiError, ApiMarginType, ApiOrderStatus,
        ApiOrderType, ApiPositionSide, ApiSide, ApiTimeInForce, Balance, BrokerApi,
        CancelOrderRequest, FeeRate, Fill, FundingRate, IncomeRecord, InstrumentInfo, Kline,
        LeverageInfo, OpenInterest, OrderBook, OrderInfo, PositionInfo, PriceLevel, Ticker, Trade,
        UnifiedOrderRequest,
    },
    okx::{OkxError, msg::rest as m, rest::OkxClient},
};

impl From<OkxError> for ApiError {
    fn from(e: OkxError) -> Self {
        match e {
            OkxError::OrderError { code, msg } => ApiError::new("okx", code, msg),
            OkxError::AuthError { code, msg } => ApiError::new("okx", code, msg),
            OkxError::Reqwest(err) => ApiError::transport("okx", err),
            other => ApiError::new("okx", "ERR", other.to_string()),
        }
    }
}

// ------------------------------------------------------------------
// 原始响应 → 统一结构 转换
// ------------------------------------------------------------------

fn f(s: &str) -> f64 {
    s.parse::<f64>().unwrap_or(0.0)
}

fn i(s: &str) -> i64 {
    s.parse::<i64>().unwrap_or(0)
}

fn okx_side(s: &str) -> ApiSide {
    match s.to_uppercase().as_str() {
        "BUY" => ApiSide::Buy,
        "SELL" => ApiSide::Sell,
        _ => ApiSide::Unknown,
    }
}

fn okx_ord_type(t: &str) -> ApiOrderType {
    match t {
        "limit" => ApiOrderType::Limit,
        "market" => ApiOrderType::Market,
        "post_only" => ApiOrderType::Limit,
        "fok" => ApiOrderType::Limit,
        "ioc" => ApiOrderType::Limit,
        "optimal_limit_ioc" => ApiOrderType::Limit,
        "stop_limit" | "conditional_limit" => ApiOrderType::StopLimit,
        "stop_market" | "conditional_market" => ApiOrderType::StopMarket,
        "take_profit_limit" => ApiOrderType::TakeProfitLimit,
        "take_profit_market" => ApiOrderType::TakeProfitMarket,
        _ => ApiOrderType::Unknown,
    }
}

fn okx_tif(t: &str) -> ApiTimeInForce {
    match t {
        "post_only" => ApiTimeInForce::GTX,
        "fok" => ApiTimeInForce::FOK,
        "ioc" | "optimal_limit_ioc" => ApiTimeInForce::IOC,
        _ => ApiTimeInForce::GTC,
    }
}

fn okx_pos_side(s: &str) -> ApiPositionSide {
    match s {
        "long" => ApiPositionSide::Long,
        "short" => ApiPositionSide::Short,
        "net" => ApiPositionSide::Net,
        _ => ApiPositionSide::Unknown,
    }
}

fn order_info_from(o: &m::Order) -> OrderInfo {
    let executed = f(&o.fill_sz);
    let qty = f(&o.sz);
    let stop_price = [&o.sl_trigger_px, &o.tp_trigger_px]
        .iter()
        .map(|s| f(s))
        .find(|v| *v > 0.0);
    OrderInfo {
        symbol: o.inst_id.clone(),
        order_id: o.ord_id.clone(),
        client_order_id: o.cl_ord_id.clone(),
        side: okx_side(&o.side),
        order_type: okx_ord_type(&o.ord_type),
        status: ApiOrderStatus::from_str(&o.state),
        price: f(&o.px),
        qty,
        executed_qty: executed,
        avg_price: f(&o.avg_px),
        leaves_qty: (qty - executed).max(0.0),
        time_in_force: okx_tif(&o.ord_type),
        reduce_only: o.reduce_only,
        position_side: okx_pos_side(&o.pos_side),
        create_time: i(&o.c_time),
        update_time: i(&o.u_time),
        stop_price,
    }
}

fn position_from(p: &m::Position) -> PositionInfo {
    let pos = f(&p.pos);
    PositionInfo {
        symbol: p.inst_id.clone(),
        position_side: okx_pos_side(&p.pos_side),
        qty: pos,
        entry_price: f(&p.avg_px),
        mark_price: f(&p.mark_px),
        liquidation_price: f(&p.liq_px),
        leverage: f(&p.lever),
        margin_type: ApiMarginType::from_str(&p.mgn_mode),
        unrealized_pnl: f(&p.upl),
        realized_pnl: f(&p.realized_pnl),
        notional: f(&p.notional_usd),
        update_time: i(&p.u_time),
    }
}

fn account_from(a: &m::AccountBalance) -> AccountInfo {
    AccountInfo {
        total_wallet_balance: a.total_eq,
        total_margin_balance: a.adj_eq,
        total_unrealized_pnl: a.details.iter().map(|d| d.u_pnl).sum(),
        // OKX 账户级可用余额：adjEq 已扣除占用保证金。
        available_balance: a.adj_eq,
        balances: a
            .details
            .iter()
            .map(|d| Balance {
                asset: d.ccy.clone(),
                wallet_balance: d.total_eq,
                available_balance: d.avail_eq,
                unrealized_pnl: d.u_pnl,
                margin_balance: d.iso_eq,
            })
            .collect(),
        timestamp: i(&a.ts),
    }
}

fn ticker_from(t: &m::Ticker, funding: Option<&m::FundingRate>) -> Ticker {
    Ticker {
        symbol: t.inst_id.clone(),
        last_price: f(&t.last),
        mark_price: None,
        index_price: None,
        funding_rate: funding.map(|f| f.funding_rate),
        next_funding_time: funding.map(|f| i(&f.next_funding_time)),
        open_24h: f(&t.open24h),
        high_24h: f(&t.high24h),
        low_24h: f(&t.low24h),
        volume_24h: f(&t.vol24h),
        quote_volume_24h: f(&t.vol_ccy24h),
        timestamp: i(&t.ts),
    }
}

fn instrument_from(i: &m::Instrument) -> InstrumentInfo {
    // SWAP 合约：ctValCcy 为标的币，settleCcy 为结算币（保证金币）。
    let tick = f(&i.tick_sz);
    let lot = f(&i.lot_sz);
    let price_precision = decimals(tick);
    let qty_precision = decimals(lot);
    InstrumentInfo {
        symbol: i.inst_id.clone(),
        base_asset: i.ct_val_ccy.clone(),
        quote_asset: i.settle_ccy.clone(),
        tick_size: tick,
        lot_size: lot,
        min_qty: f(&i.min_sz),
        contract_size: f(&i.ct_val),
        margin_asset: i.settle_ccy.clone(),
        price_precision,
        qty_precision,
        tradable: i.state == "live",
    }
}

fn decimals(step: f64) -> u32 {
    if step <= 0.0 || !step.is_finite() {
        return 0;
    }
    let s = format!("{step:.10}");
    let s = s.trim_end_matches('0');
    s.split('.').nth(1).map(|d| d.len() as u32).unwrap_or(0)
}

// ------------------------------------------------------------------
// 原始端点（对照官方文档全量）
// ------------------------------------------------------------------

impl OkxClient {
    /// 通用 POST 业务响应检查：顶层 code == "0"。
    fn check_code(code: &str, msg: &str) -> Result<(), OkxError> {
        if code == "0" {
            Ok(())
        } else {
            Err(OkxError::OrderError {
                code: code.to_string(),
                msg: msg.to_string(),
            })
        }
    }

    // ---------------- 基础 ----------------

    pub async fn get_system_time(&self) -> Result<i64, OkxError> {
        let resp: m::OkxResponse<m::SystemTime> = self.get_noauth("/api/v5/public/time").await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp
            .data
            .first()
            .map(|t| i(&t.ts))
            .ok_or(OkxError::InvalidArg("empty time response"))?)
    }

    pub async fn get_system_status(&self) -> Result<Vec<m::SystemStatus>, OkxError> {
        let resp: m::OkxResponse<m::SystemStatus> =
            self.get_noauth("/api/v5/system/status").await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_all_instruments(&self) -> Result<Vec<m::Instrument>, OkxError> {
        let resp: m::InstrumentsResponse = self
            .get_noauth("/api/v5/public/instruments?instType=SWAP")
            .await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    // ---------------- 账户 ----------------

    pub async fn get_balance(&self) -> Result<m::AccountBalance, OkxError> {
        let resp: m::OkxResponse<m::AccountBalance> = self.get("/api/v5/account/balance").await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty balance response"))
    }

    pub async fn get_positions_history(
        &self,
        inst_id: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, OkxError> {
        let mut path = "/api/v5/account/positions-history".to_string();
        if let Some(id) = inst_id {
            path.push_str(&format!("?instId={id}"));
        }
        let resp: m::OkxResponse<serde_json::Value> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_bills(
        &self,
        inst_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<serde_json::Value>, OkxError> {
        let mut path = format!("/api/v5/account/bills?limit={limit}");
        if let Some(id) = inst_id {
            path.push_str(&format!("&instId={id}"));
        }
        let resp: m::OkxResponse<serde_json::Value> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_account_config(&self) -> Result<m::AccountConfig, OkxError> {
        let resp: m::OkxResponse<m::AccountConfig> = self.get("/api/v5/account/config").await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty config response"))
    }

    pub async fn set_position_mode(&self, pos_mode: &str) -> Result<(), OkxError> {
        let body = format!("{{\"posMode\":\"{pos_mode}\"}}");
        let resp: m::OkxResponse<serde_json::Value> =
            self.post("/api/v5/account/set-position-mode", body).await?;
        Self::check_code(&resp.code, &resp.msg)
    }

    pub async fn set_leverage_raw(
        &self,
        inst_id: &str,
        lever: &str,
        mgn_mode: &str,
        pos_side: Option<&str>,
    ) -> Result<Vec<m::Leverage>, OkxError> {
        let pos_side = pos_side
            .map(|s| format!(",\"posSide\":\"{s}\""))
            .unwrap_or_default();
        let body = format!(
            "{{\"instId\":\"{inst_id}\",\"lever\":\"{lever}\",\"mgnMode\":\"{mgn_mode}\"{pos_side}}}"
        );
        let resp: m::OkxResponse<m::Leverage> =
            self.post("/api/v5/account/set-leverage", body).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_leverage_info(
        &self,
        inst_id: &str,
        mgn_mode: &str,
    ) -> Result<Vec<m::Leverage>, OkxError> {
        let path = format!("/api/v5/account/leverage-info?instId={inst_id}&mgnMode={mgn_mode}");
        let resp: m::OkxResponse<m::Leverage> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_max_size(&self, inst_id: &str, td_mode: &str) -> Result<m::MaxSize, OkxError> {
        let path = format!("/api/v5/account/max-size?instId={inst_id}&tdMode={td_mode}");
        let resp: m::OkxResponse<m::MaxSize> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty max-size response"))
    }

    pub async fn get_max_avail_size(
        &self,
        inst_id: &str,
        td_mode: &str,
    ) -> Result<m::MaxAvailSize, OkxError> {
        let path = format!("/api/v5/account/max-avail-size?instId={inst_id}&tdMode={td_mode}");
        let resp: m::OkxResponse<m::MaxAvailSize> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty max-avail-size response"))
    }

    pub async fn change_position_margin(
        &self,
        inst_id: &str,
        pos_side: &str,
        amt: &str,
    ) -> Result<serde_json::Value, OkxError> {
        let body =
            format!("{{\"instId\":\"{inst_id}\",\"posSide\":\"{pos_side}\",\"amt\":\"{amt}\"}}");
        let resp: m::OkxResponse<serde_json::Value> = self
            .post("/api/v5/account/position/margin-balance", body)
            .await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp
            .data
            .into_iter()
            .next()
            .unwrap_or(serde_json::Value::Null))
    }

    pub async fn get_trade_fee(&self, inst_id: Option<&str>) -> Result<m::TradeFee, OkxError> {
        let path = match inst_id {
            Some(id) => format!("/api/v5/account/trade-fee?instType=SWAP&instId={id}"),
            None => "/api/v5/account/trade-fee?instType=SWAP".to_string(),
        };
        let resp: m::OkxResponse<m::TradeFee> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty trade-fee response"))
    }

    pub async fn get_risk_state(&self) -> Result<Vec<m::RiskState>, OkxError> {
        let resp: m::OkxResponse<m::RiskState> = self.get("/api/v5/account/risk-state").await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_max_withdrawal(&self) -> Result<Vec<m::MaxWithdrawal>, OkxError> {
        let resp: m::OkxResponse<m::MaxWithdrawal> =
            self.get("/api/v5/account/max-withdrawal").await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_account_position_risk(&self) -> Result<Vec<serde_json::Value>, OkxError> {
        let resp: m::OkxResponse<serde_json::Value> =
            self.get("/api/v5/account/account-position-risk").await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn set_isolated_mode(&self, iso_mode: &str, acct_lv: &str) -> Result<(), OkxError> {
        let body = format!("{{\"isoMode\":\"{iso_mode}\",\"acctLv\":\"{acct_lv}\"}}");
        let resp: m::OkxResponse<serde_json::Value> =
            self.post("/api/v5/account/set-isolated-mode", body).await?;
        Self::check_code(&resp.code, &resp.msg)
    }

    // ---------------- 交易 ----------------

    pub async fn submit_batch_orders(
        &self,
        orders: Vec<String>,
    ) -> Result<Vec<m::OrderResult>, OkxError> {
        let body = format!("[{}]", orders.join(","));
        let resp: m::OrderResponse = self.post("/api/v5/trade/batch-orders", body).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn cancel_batch_orders(
        &self,
        orders: Vec<String>,
    ) -> Result<Vec<m::CancelResult>, OkxError> {
        let body = format!("[{}]", orders.join(","));
        let resp: m::CancelResponse = self.post("/api/v5/trade/cancel-batch-orders", body).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn amend_order(
        &self,
        inst_id: &str,
        ord_id: Option<&str>,
        cl_ord_id: Option<&str>,
        new_px: Option<&str>,
        new_sz: Option<&str>,
    ) -> Result<m::AmendResult, OkxError> {
        let mut body = format!("{{\"instId\":\"{inst_id}\"");
        if let Some(id) = ord_id {
            body.push_str(&format!(",\"ordId\":\"{id}\""));
        }
        if let Some(id) = cl_ord_id {
            body.push_str(&format!(",\"clOrdId\":\"{id}\""));
        }
        if let Some(px) = new_px {
            body.push_str(&format!(",\"newPx\":\"{px}\""));
        }
        if let Some(sz) = new_sz {
            body.push_str(&format!(",\"newSz\":\"{sz}\""));
        }
        body.push('}');
        let resp: m::OkxResponse<m::AmendResult> =
            self.post("/api/v5/trade/amend-order", body).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty amend response"))
    }

    pub async fn amend_batch_orders(
        &self,
        orders: Vec<String>,
    ) -> Result<Vec<m::AmendResult>, OkxError> {
        let body = format!("[{}]", orders.join(","));
        let resp: m::OkxResponse<m::AmendResult> =
            self.post("/api/v5/trade/amend-batch-orders", body).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn close_position(
        &self,
        inst_id: &str,
        mgn_mode: &str,
        pos_side: Option<&str>,
    ) -> Result<Vec<m::ClosePositionResult>, OkxError> {
        let pos_side = pos_side
            .map(|s| format!(",\"posSide\":\"{s}\""))
            .unwrap_or_default();
        let body = format!("{{\"instId\":\"{inst_id}\",\"mgnMode\":\"{mgn_mode}\"{pos_side}}}");
        let resp: m::OkxResponse<m::ClosePositionResult> =
            self.post("/api/v5/trade/close-position", body).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_order_details(
        &self,
        inst_id: &str,
        ord_id: Option<&str>,
        cl_ord_id: Option<&str>,
    ) -> Result<m::Order, OkxError> {
        let mut path = format!("/api/v5/trade/order?instId={inst_id}");
        if let Some(id) = ord_id {
            path.push_str(&format!("&ordId={id}"));
        }
        if let Some(id) = cl_ord_id {
            path.push_str(&format!("&clOrdId={id}"));
        }
        let resp: m::OkxResponse<m::Order> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty order response"))
    }

    pub async fn get_orders_pending(&self, inst_id: &str) -> Result<Vec<m::Order>, OkxError> {
        let path = format!("/api/v5/trade/orders-pending?instId={inst_id}");
        let resp: m::OkxResponse<m::Order> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_orders_history(
        &self,
        inst_id: &str,
        limit: u32,
    ) -> Result<Vec<m::Order>, OkxError> {
        let path =
            format!("/api/v5/trade/orders-history?instType=SWAP&instId={inst_id}&limit={limit}");
        let resp: m::OkxResponse<m::Order> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_fills(&self, inst_id: &str, limit: u32) -> Result<Vec<m::Fill>, OkxError> {
        let path = format!("/api/v5/trade/fills?instType=SWAP&instId={inst_id}&limit={limit}");
        let resp: m::OkxResponse<m::Fill> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn mass_cancel(&self, inst_id: &str) -> Result<bool, OkxError> {
        let body = format!("{{\"instId\":\"{inst_id}\"}}");
        let resp: m::OkxResponse<m::MassCancelResult> =
            self.post("/api/v5/trade/mass-cancel", body).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp
            .data
            .into_iter()
            .next()
            .map(|r| r.result)
            .unwrap_or(false))
    }

    pub async fn cancel_all_after(&self, timeout_ms: u64) -> Result<m::CancelAllAfter, OkxError> {
        let body = format!("{{\"timeOut\":\"{timeout_ms}\"}}");
        let resp: m::OkxResponse<m::CancelAllAfter> =
            self.post("/api/v5/trade/cancel-all-after", body).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty cancel-all-after response"))
    }

    pub async fn order_precheck(&self, body: String) -> Result<serde_json::Value, OkxError> {
        let resp: m::OkxResponse<serde_json::Value> =
            self.post("/api/v5/trade/order-precheck", body).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp
            .data
            .into_iter()
            .next()
            .unwrap_or(serde_json::Value::Null))
    }

    // ---------------- 算法单 ----------------

    pub async fn place_algo_order(&self, body: String) -> Result<Vec<m::AlgoOrder>, OkxError> {
        let resp: m::OkxResponse<m::AlgoOrder> =
            self.post("/api/v5/trade/order-algo", body).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn cancel_algo_orders(&self, body: String) -> Result<Vec<m::AlgoOrder>, OkxError> {
        let resp: m::OkxResponse<m::AlgoOrder> =
            self.post("/api/v5/trade/cancel-algos", body).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_algo_orders_pending(
        &self,
        inst_id: &str,
    ) -> Result<Vec<m::AlgoOrder>, OkxError> {
        let path = format!("/api/v5/trade/orders-algo-pending?instType=SWAP&instId={inst_id}");
        let resp: m::OkxResponse<m::AlgoOrder> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_algo_orders_history(
        &self,
        inst_id: &str,
        limit: u32,
    ) -> Result<Vec<m::AlgoOrder>, OkxError> {
        let path = format!(
            "/api/v5/trade/orders-algo-history?instType=SWAP&instId={inst_id}&limit={limit}"
        );
        let resp: m::OkxResponse<m::AlgoOrder> = self.get(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    // ---------------- 行情 ----------------

    pub async fn get_tickers(&self) -> Result<Vec<m::Ticker>, OkxError> {
        let resp: m::OkxResponse<m::Ticker> = self
            .get_noauth("/api/v5/market/tickers?instType=SWAP")
            .await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_ticker(&self, inst_id: &str) -> Result<m::Ticker, OkxError> {
        let path = format!("/api/v5/market/ticker?instId={inst_id}");
        let resp: m::OkxResponse<m::Ticker> = self.get_noauth(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty ticker response"))
    }

    pub async fn get_books_full(&self, inst_id: &str) -> Result<m::Books, OkxError> {
        let path = format!("/api/v5/market/books-full?instId={inst_id}");
        let resp: m::BooksResponse = self.get_noauth(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty books response"))
    }

    pub async fn get_candles(
        &self,
        inst_id: &str,
        bar: &str,
        limit: u32,
    ) -> Result<m::CandleResponse, OkxError> {
        let path = format!("/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}");
        let resp: m::OkxResponse<m::CandleResponse> = self.get_noauth(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data.into_iter().next().unwrap_or_default())
    }

    pub async fn get_history_candles(
        &self,
        inst_id: &str,
        bar: &str,
        limit: u32,
    ) -> Result<m::CandleResponse, OkxError> {
        let path =
            format!("/api/v5/market/history-candles?instId={inst_id}&bar={bar}&limit={limit}");
        let resp: m::OkxResponse<m::CandleResponse> = self.get_noauth(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data.into_iter().next().unwrap_or_default())
    }

    pub async fn get_public_trades(
        &self,
        inst_id: &str,
        limit: u32,
    ) -> Result<Vec<m::PublicTrade>, OkxError> {
        let path = format!("/api/v5/market/trades?instId={inst_id}&limit={limit}");
        let resp: m::OkxResponse<m::PublicTrade> = self.get_noauth(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_history_trades(
        &self,
        inst_id: &str,
        limit: u32,
    ) -> Result<Vec<m::PublicTrade>, OkxError> {
        let path = format!("/api/v5/market/history-trades?instId={inst_id}&limit={limit}");
        let resp: m::OkxResponse<m::PublicTrade> = self.get_noauth(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_funding_rate(&self, inst_id: &str) -> Result<m::FundingRate, OkxError> {
        let path = format!("/api/v5/public/funding-rate?instId={inst_id}");
        let resp: m::OkxResponse<m::FundingRate> = self.get_noauth(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty funding-rate response"))
    }

    pub async fn get_funding_rate_history(
        &self,
        inst_id: &str,
        limit: u32,
    ) -> Result<Vec<m::FundingRateHistory>, OkxError> {
        let path = format!("/api/v5/public/funding-rate-history?instId={inst_id}&limit={limit}");
        let resp: m::OkxResponse<m::FundingRateHistory> = self.get_noauth(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        Ok(resp.data)
    }

    pub async fn get_open_interest(&self, inst_id: &str) -> Result<m::OpenInterest, OkxError> {
        let path = format!("/api/v5/public/open-interest?instType=SWAP&instId={inst_id}");
        let resp: m::OkxResponse<m::OpenInterest> = self.get_noauth(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty open-interest response"))
    }

    pub async fn get_price_limit(&self, inst_id: &str) -> Result<m::PriceLimit, OkxError> {
        let path = format!("/api/v5/public/price-limit?instId={inst_id}");
        let resp: m::OkxResponse<m::PriceLimit> = self.get_noauth(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty price-limit response"))
    }

    pub async fn get_mark_price(&self, inst_id: &str) -> Result<m::MarkPrice, OkxError> {
        let path = format!("/api/v5/public/mark-price?instType=SWAP&instId={inst_id}");
        let resp: m::OkxResponse<m::MarkPrice> = self.get_noauth(&path).await?;
        Self::check_code(&resp.code, &resp.msg)?;
        resp.data
            .into_iter()
            .next()
            .ok_or(OkxError::InvalidArg("empty mark-price response"))
    }
}

// ------------------------------------------------------------------
// 统一 BrokerApi 实现
// ------------------------------------------------------------------

#[async_trait::async_trait]
impl BrokerApi for OkxClient {
    async fn ping(&self) -> Result<(), ApiError> {
        let _ = self.get_system_time().await?;
        Ok(())
    }

    async fn get_server_time(&self) -> Result<i64, ApiError> {
        Ok(self.get_system_time().await?)
    }

    async fn get_instruments(&self) -> Result<Vec<InstrumentInfo>, ApiError> {
        let instruments = self.get_all_instruments().await?;
        Ok(instruments.iter().map(instrument_from).collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<Ticker, ApiError> {
        let ticker = self.get_ticker(symbol).await?;
        let funding = self.get_funding_rate(symbol).await.ok();
        Ok(ticker_from(&ticker, funding.as_ref()))
    }

    async fn get_tickers(&self) -> Result<Vec<Ticker>, ApiError> {
        let tickers = self.get_tickers().await?;
        Ok(tickers.iter().map(|t| ticker_from(t, None)).collect())
    }

    async fn get_order_book(&self, symbol: &str, limit: u32) -> Result<OrderBook, ApiError> {
        let books = self.get_books(symbol, limit.min(400)).await?;
        Ok(OrderBook {
            symbol: symbol.to_string(),
            bids: books
                .bids
                .iter()
                .filter_map(|l| {
                    if l.len() >= 2 {
                        Some(PriceLevel {
                            price: l[0].parse().unwrap_or(0.0),
                            qty: l[1].parse().unwrap_or(0.0),
                        })
                    } else {
                        None
                    }
                })
                .collect(),
            asks: books
                .asks
                .iter()
                .filter_map(|l| {
                    if l.len() >= 2 {
                        Some(PriceLevel {
                            price: l[0].parse().unwrap_or(0.0),
                            qty: l[1].parse().unwrap_or(0.0),
                        })
                    } else {
                        None
                    }
                })
                .collect(),
            timestamp: i(&books.ts),
        })
    }

    async fn get_trades(&self, symbol: &str, limit: u32) -> Result<Vec<Trade>, ApiError> {
        let trades = self.get_public_trades(symbol, limit.min(500)).await?;
        Ok(trades
            .iter()
            .map(|t| Trade {
                symbol: t.inst_id.clone(),
                id: t.trade_id.clone(),
                price: f(&t.px),
                qty: f(&t.sz),
                side: okx_side(&t.side),
                timestamp: i(&t.ts),
            })
            .collect())
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
    ) -> Result<Vec<Kline>, ApiError> {
        let rows = self.get_candles(symbol, interval, limit.min(300)).await?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                if r.len() < 9 {
                    return None;
                }
                Some(Kline {
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    open_time: r[0].parse().unwrap_or(0),
                    close_time: r[0].parse::<i64>().unwrap_or(0) + 60_000,
                    open: r[1].parse().unwrap_or(0.0),
                    high: r[2].parse().unwrap_or(0.0),
                    low: r[3].parse().unwrap_or(0.0),
                    close: r[4].parse().unwrap_or(0.0),
                    volume: r[5].parse().unwrap_or(0.0),
                    quote_volume: r[7].parse().unwrap_or(0.0),
                })
            })
            .collect())
    }

    async fn get_funding_rate(&self, symbol: &str) -> Result<FundingRate, ApiError> {
        let f = self.get_funding_rate(symbol).await?;
        Ok(FundingRate {
            symbol: f.inst_id.clone(),
            funding_rate: f.funding_rate,
            next_funding_time: i(&f.next_funding_time),
            timestamp: i(&f.funding_time),
        })
    }

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRate>, ApiError> {
        let records = self
            .get_funding_rate_history(symbol, limit.min(100))
            .await?;
        Ok(records
            .iter()
            .map(|r| FundingRate {
                symbol: r.inst_id.clone(),
                funding_rate: r.funding_rate,
                next_funding_time: 0,
                timestamp: i(&r.funding_time),
            })
            .collect())
    }

    async fn get_open_interest(&self, symbol: &str) -> Result<OpenInterest, ApiError> {
        let oi = self.get_open_interest(symbol).await?;
        Ok(OpenInterest {
            symbol: oi.inst_id.clone(),
            open_interest: f(&oi.oi),
            timestamp: i(&oi.ts),
        })
    }

    async fn submit_order(&self, req: &UnifiedOrderRequest) -> Result<OrderInfo, ApiError> {
        let body = build_okx_order_body(req, self.td_mode());
        let resp: m::OrderResponse = self.post("/api/v5/trade/order", body).await?;
        if resp.code != "0" {
            return Err(ApiError::new("okx", resp.code, resp.msg));
        }
        let result = resp
            .data
            .into_iter()
            .next()
            .ok_or_else(|| ApiError::new("okx", "EMPTY", "empty order response"))?;
        if result.s_code != "0" && !result.s_code.is_empty() {
            return Err(ApiError::new("okx", result.s_code, result.s_msg));
        }
        Ok(OrderInfo {
            symbol: req.symbol.clone(),
            order_id: result.ord_id,
            client_order_id: result.cl_ord_id,
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
            position_side: req.position_side.unwrap_or(ApiPositionSide::Unknown),
            create_time: 0,
            update_time: 0,
            stop_price: req.stop_price,
        })
    }

    async fn submit_orders(
        &self,
        reqs: &[UnifiedOrderRequest],
    ) -> Result<Vec<OrderInfo>, ApiError> {
        if reqs.is_empty() || reqs.len() > 20 {
            return Err(ApiError::new(
                "okx",
                "INVALID",
                "batch orders limited to 1-20",
            ));
        }
        let bodies: Vec<String> = reqs
            .iter()
            .map(|r| build_okx_order_body(r, self.td_mode()))
            .collect();
        let results = self.submit_batch_orders(bodies).await?;
        Ok(results
            .into_iter()
            .map(|r| OrderInfo {
                symbol: String::new(),
                order_id: r.ord_id,
                client_order_id: r.cl_ord_id,
                side: ApiSide::Unknown,
                order_type: ApiOrderType::Unknown,
                status: if r.s_code == "0" {
                    ApiOrderStatus::New
                } else {
                    ApiOrderStatus::Rejected
                },
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
            .collect())
    }

    async fn cancel_order(&self, req: &CancelOrderRequest) -> Result<OrderInfo, ApiError> {
        let mut body = format!("{{\"instId\":\"{}\"", req.symbol);
        if let Some(id) = &req.order_id {
            body.push_str(&format!(",\"ordId\":\"{id}\""));
        }
        if let Some(id) = &req.client_order_id {
            body.push_str(&format!(",\"clOrdId\":\"{id}\""));
        }
        body.push('}');
        let resp: m::CancelResponse = self.post("/api/v5/trade/cancel-order", body).await?;
        if resp.code != "0" {
            return Err(ApiError::new("okx", resp.code, resp.msg));
        }
        let result = resp
            .data
            .into_iter()
            .next()
            .ok_or_else(|| ApiError::new("okx", "EMPTY", "empty cancel response"))?;
        if result.s_code != "0" && !result.s_code.is_empty() {
            return Err(ApiError::new("okx", result.s_code, result.s_msg));
        }
        Ok(OrderInfo {
            symbol: req.symbol.clone(),
            order_id: result.ord_id,
            client_order_id: result.cl_ord_id,
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
            // OKX 撤单响应携带完成时间（毫秒），回填后 REST 撤单事实的
            // exchange_ts 才与私有流终态事件处于同一时钟域。
            create_time: 0,
            update_time: i(&result.ts),
            stop_price: None,
        })
    }

    async fn cancel_orders(&self, reqs: &[CancelOrderRequest]) -> Result<Vec<OrderInfo>, ApiError> {
        if reqs.is_empty() || reqs.len() > 20 {
            return Err(ApiError::new(
                "okx",
                "INVALID",
                "batch cancel limited to 1-20",
            ));
        }
        let bodies: Vec<String> = reqs
            .iter()
            .map(|r| {
                let mut body = format!("{{\"instId\":\"{}\"", r.symbol);
                if let Some(id) = &r.order_id {
                    body.push_str(&format!(",\"ordId\":\"{id}\""));
                }
                if let Some(id) = &r.client_order_id {
                    body.push_str(&format!(",\"clOrdId\":\"{id}\""));
                }
                body.push('}');
                body
            })
            .collect();
        let results = self.cancel_batch_orders(bodies).await?;
        Ok(results
            .into_iter()
            .map(|r| OrderInfo {
                symbol: String::new(),
                order_id: r.ord_id,
                client_order_id: r.cl_ord_id,
                side: ApiSide::Unknown,
                order_type: ApiOrderType::Unknown,
                status: if r.s_code == "0" {
                    ApiOrderStatus::Canceled
                } else {
                    ApiOrderStatus::Unknown
                },
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
            .collect())
    }

    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), ApiError> {
        let _ = OkxClient::cancel_all_orders(self, symbol, self.td_mode(), None).await?;
        Ok(())
    }

    async fn cancel_all_after(&self, timeout_ms: u64) -> Result<(), ApiError> {
        let _ = self.cancel_all_after(timeout_ms).await?;
        Ok(())
    }

    async fn amend_order(&self, req: &AmendOrderRequest) -> Result<OrderInfo, ApiError> {
        let result = self
            .amend_order(
                &req.symbol,
                req.order_id.as_deref(),
                req.client_order_id.as_deref(),
                req.new_price.map(|p| p.to_string()).as_deref(),
                req.new_qty.map(|q| q.to_string()).as_deref(),
            )
            .await?;
        if result.s_code != "0" && !result.s_code.is_empty() {
            return Err(ApiError::new("okx", result.s_code, result.s_msg));
        }
        Ok(OrderInfo {
            symbol: result.inst_id.clone(),
            order_id: result.ord_id,
            client_order_id: result.cl_ord_id,
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
        })
    }

    async fn get_order(
        &self,
        symbol: &str,
        order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> Result<OrderInfo, ApiError> {
        let order = self
            .get_order_details(symbol, order_id, client_order_id)
            .await?;
        Ok(order_info_from(&order))
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, ApiError> {
        let orders = self.get_orders_pending(symbol).await?;
        Ok(orders.iter().map(order_info_from).collect())
    }

    async fn get_order_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<OrderInfo>, ApiError> {
        let orders = self.get_orders_history(symbol, limit.min(100)).await?;
        Ok(orders.iter().map(order_info_from).collect())
    }

    async fn get_fills(&self, symbol: &str, limit: u32) -> Result<Vec<Fill>, ApiError> {
        let fills = self.get_fills(symbol, limit.min(100)).await?;
        Ok(fills
            .iter()
            .map(|fill| Fill {
                symbol: fill.inst_id.clone(),
                trade_id: fill.trade_id.clone(),
                order_id: fill.ord_id.clone(),
                client_order_id: fill.cl_ord_id.clone(),
                price: f(&fill.fill_px),
                qty: f(&fill.fill_sz),
                side: okx_side(&fill.side),
                fee: fill.fee,
                fee_asset: fill.fee_ccy.clone(),
                realized_pnl: fill.pnl,
                maker: fill.fee < 0.0,
                timestamp: i(&fill.fill_time),
            })
            .collect())
    }

    async fn get_account(&self) -> Result<AccountInfo, ApiError> {
        let account = self.get_balance().await?;
        Ok(account_from(&account))
    }

    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<PositionInfo>, ApiError> {
        let positions = match symbol {
            Some(id) => self.get_positions(id).await?,
            None => {
                let resp: m::PositionsResponse =
                    self.get("/api/v5/account/positions?instType=SWAP").await?;
                if resp.code != "0" {
                    return Err(ApiError::new("okx", resp.code, resp.msg));
                }
                resp.data
            }
        };
        Ok(positions.iter().map(position_from).collect())
    }

    async fn set_leverage(
        &self,
        symbol: &str,
        leverage: f64,
        position_side: Option<ApiPositionSide>,
    ) -> Result<LeverageInfo, ApiError> {
        let mgn_mode = self.td_mode().to_string();
        let pos_side = position_side.map(|s| s.as_str().to_lowercase());
        let results = self
            .set_leverage_raw(
                symbol,
                &leverage.to_string(),
                &mgn_mode,
                pos_side.as_deref(),
            )
            .await?;
        let first = results
            .into_iter()
            .next()
            .ok_or_else(|| ApiError::new("okx", "EMPTY", "empty leverage response"))?;
        Ok(LeverageInfo {
            symbol: first.inst_id.clone(),
            leverage: f(&first.lever),
            margin_type: ApiMarginType::from_str(&first.mgn_mode),
            position_side: okx_pos_side(&first.pos_side),
        })
    }

    async fn get_leverage(&self, symbol: &str) -> Result<LeverageInfo, ApiError> {
        let results = self.get_leverage_info(symbol, self.td_mode()).await?;
        let first = results
            .into_iter()
            .next()
            .ok_or_else(|| ApiError::new("okx", "EMPTY", "empty leverage response"))?;
        Ok(LeverageInfo {
            symbol: first.inst_id.clone(),
            leverage: f(&first.lever),
            margin_type: ApiMarginType::from_str(&first.mgn_mode),
            position_side: okx_pos_side(&first.pos_side),
        })
    }

    async fn get_fee_rates(&self, symbol: &str) -> Result<FeeRate, ApiError> {
        let fee = self.get_trade_fee(Some(symbol)).await?;
        Ok(FeeRate {
            symbol: symbol.to_string(),
            maker_fee: fee.maker,
            taker_fee: fee.taker,
            timestamp: i(&fee.ts),
        })
    }

    async fn get_income_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<IncomeRecord>, ApiError> {
        let bills = self.get_bills(Some(symbol), limit.min(100)).await?;
        Ok(bills
            .iter()
            .filter_map(|b| {
                let ts = b.get("ts").and_then(|v| v.as_str()).unwrap_or("0");
                let bal_chg = b
                    .get("balChg")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                Some(IncomeRecord {
                    symbol: b
                        .get("instId")
                        .and_then(|v| v.as_str())
                        .unwrap_or(symbol)
                        .to_string(),
                    income_type: b
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("0")
                        .to_string(),
                    income: bal_chg,
                    asset: b
                        .get("ccy")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    timestamp: ts.parse().unwrap_or(0),
                })
            })
            .collect())
    }
}

// ------------------------------------------------------------------
// 请求体构建（可单测）
// ------------------------------------------------------------------

/// 构建 OKX 下单 body（统一请求 → OKX 字段）。
pub(crate) fn build_okx_order_body(req: &UnifiedOrderRequest, td_mode: &str) -> String {
    let ord_type = match req.order_type {
        ApiOrderType::Limit => match req.time_in_force {
            ApiTimeInForce::FOK => "fok",
            ApiTimeInForce::IOC => "ioc",
            ApiTimeInForce::GTX => "post_only",
            _ => "limit",
        },
        ApiOrderType::Market => "market",
        ApiOrderType::StopLimit => "stop_limit",
        ApiOrderType::StopMarket => "stop_market",
        ApiOrderType::TakeProfitLimit => "take_profit_limit",
        ApiOrderType::TakeProfitMarket => "take_profit_market",
        _ => "limit",
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        serde_json::Value::String(req.symbol.clone()),
    );
    body.insert(
        "tdMode".to_string(),
        serde_json::Value::String(td_mode.to_string()),
    );
    if let Some(side) = req.position_side {
        body.insert(
            "posSide".to_string(),
            serde_json::Value::String(side.as_str().to_lowercase()),
        );
    }
    if let Some(id) = &req.client_order_id {
        body.insert("clOrdId".to_string(), serde_json::Value::String(id.clone()));
    }
    body.insert(
        "side".to_string(),
        serde_json::Value::String(req.side.as_str().to_lowercase()),
    );
    body.insert(
        "ordType".to_string(),
        serde_json::Value::String(ord_type.to_string()),
    );
    body.insert(
        "sz".to_string(),
        serde_json::Value::String(req.qty.to_string()),
    );
    if let Some(px) = req.price {
        body.insert("px".to_string(), serde_json::Value::String(px.to_string()));
    }
    if let Some(sp) = req.stop_price {
        body.insert(
            "slTriggerPx".to_string(),
            serde_json::Value::String(sp.to_string()),
        );
    }
    if req.reduce_only {
        body.insert("reduceOnly".to_string(), serde_json::Value::Bool(true));
    }
    serde_json::Value::Object(body).to_string()
}

/// 构建 OKX 算法单 body（统一 AlgoOrderRequest → OKX 字段）。
pub(crate) fn build_okx_algo_body(req: &AlgoOrderRequest) -> String {
    let ord_type = match req.order_type {
        ApiOrderType::StopMarket => "conditional-market",
        ApiOrderType::StopLimit => "conditional-limit",
        ApiOrderType::TakeProfitMarket => "conditional-market",
        _ => "conditional-market",
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        serde_json::Value::String(req.symbol.clone()),
    );
    body.insert(
        "tdMode".to_string(),
        serde_json::Value::String("cross".to_string()),
    );
    body.insert(
        "side".to_string(),
        serde_json::Value::String(req.side.as_str().to_lowercase()),
    );
    body.insert(
        "ordType".to_string(),
        serde_json::Value::String(ord_type.to_string()),
    );
    body.insert(
        "sz".to_string(),
        serde_json::Value::String(req.qty.to_string()),
    );
    body.insert(
        "triggerPx".to_string(),
        serde_json::Value::String(req.trigger_price.to_string()),
    );
    if let Some(px) = req.price {
        body.insert("px".to_string(), serde_json::Value::String(px.to_string()));
    }
    if let Some(id) = &req.client_order_id {
        body.insert("clOrdId".to_string(), serde_json::Value::String(id.clone()));
    }
    serde_json::Value::Object(body).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        AlgoOrderRequest, ApiOrderStatus, ApiOrderType, ApiPositionSide, ApiSide, ApiTimeInForce,
        UnifiedOrderRequest,
    };
    use crate::okx::msg::stream as s;

    fn limit_req() -> UnifiedOrderRequest {
        UnifiedOrderRequest {
            symbol: "BTC-USDT-SWAP".to_string(),
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
    fn test_build_order_body_limit() {
        let body = build_okx_order_body(&limit_req(), "cross");
        assert!(body.contains("\"instId\":\"BTC-USDT-SWAP\""));
        assert!(body.contains("\"tdMode\":\"cross\""));
        assert!(body.contains("\"side\":\"buy\""));
        assert!(body.contains("\"ordType\":\"limit\""));
        assert!(body.contains("\"sz\":\"1\""));
        assert!(body.contains("\"px\":\"50000\""));
        assert!(body.contains("\"posSide\":\"long\""));
        assert!(body.contains("\"clOrdId\":\"c1\""));
    }

    #[test]
    fn test_build_order_body_tif_variants() {
        let mut req = limit_req();
        req.time_in_force = ApiTimeInForce::FOK;
        assert!(build_okx_order_body(&req, "cross").contains("\"ordType\":\"fok\""));
        req.time_in_force = ApiTimeInForce::IOC;
        assert!(build_okx_order_body(&req, "cross").contains("\"ordType\":\"ioc\""));
        req.time_in_force = ApiTimeInForce::GTX;
        assert!(build_okx_order_body(&req, "cross").contains("\"ordType\":\"post_only\""));
    }

    #[test]
    fn test_build_order_body_market_and_stop() {
        let mut req = limit_req();
        req.order_type = ApiOrderType::Market;
        req.price = None;
        let body = build_okx_order_body(&req, "isolated");
        assert!(body.contains("\"ordType\":\"market\""));
        assert!(!body.contains("\"px\""));
        assert!(body.contains("\"tdMode\":\"isolated\""));

        let mut req = limit_req();
        req.order_type = ApiOrderType::StopMarket;
        req.stop_price = Some(49000.0);
        let body = build_okx_order_body(&req, "cross");
        assert!(body.contains("\"ordType\":\"stop_market\""));
        assert!(body.contains("\"slTriggerPx\":\"49000\""));
    }

    #[test]
    fn test_build_algo_body() {
        let req = AlgoOrderRequest {
            symbol: "BTC-USDT-SWAP".to_string(),
            side: ApiSide::Sell,
            order_type: ApiOrderType::StopLimit,
            qty: 1.0,
            price: Some(50000.0),
            trigger_price: 51000.0,
            stop_price: None,
            reduce_only: Some(true),
            client_order_id: Some("a1".to_string()),
        };
        let body = build_okx_algo_body(&req);
        assert!(body.contains("\"ordType\":\"conditional-limit\""));
        assert!(body.contains("\"triggerPx\":\"51000\""));
        assert!(body.contains("\"px\":\"50000\""));
    }

    // ---------------- 响应解析 -> 统一结构 ----------------

    #[test]
    fn test_parse_balance() {
        let json = r#"{
            "code": "0",
            "msg": "",
            "data": [{
                "totalEq": "10000.0",
                "adjEq": "9900.0",
                "isoEq": "100.0",
                "imr": "100.0",
                "mmr": "50.0",
                "notionalUsd": "5000.0",
                "mgnRatio": "99.0",
                "ts": "1700000000000",
                "details": [{
                    "ccy": "USDT",
                    "totalEq": "10000.0",
                    "availEq": "9900.0",
                    "cashBal": "10000.0",
                    "uPnl": "100.0",
                    "isoEq": "100.0",
                    "ordFrozen": "0.0",
                    "ts": "1700000000000"
                }]
            }]
        }"#;
        let resp: m::OkxResponse<m::AccountBalance> = serde_json::from_str(json).unwrap();
        let info = account_from(&resp.data[0]);
        assert_eq!(info.total_wallet_balance, 10000.0);
        assert_eq!(info.total_unrealized_pnl, 100.0);
        assert_eq!(info.balances.len(), 1);
        assert_eq!(info.balances[0].asset, "USDT");
    }

    #[test]
    fn test_parse_position() {
        let json = r#"{
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "posSide": "long",
                "pos": "1.5",
                "availPos": "1.5",
                "avgPx": "50000.0",
                "markPx": "50100.0",
                "liqPx": "45000.0",
                "lever": "10",
                "mgnMode": "cross",
                "upl": "150.0",
                "realizedPnl": "10.0",
                "notionalUsd": "75150.0",
                "cTime": "1700000000000",
                "uTime": "1700000001000"
            }]
        }"#;
        let resp: m::PositionsResponse = serde_json::from_str(json).unwrap();
        let info = position_from(&resp.data[0]);
        assert_eq!(info.symbol, "BTC-USDT-SWAP");
        assert_eq!(info.qty, 1.5);
        assert_eq!(info.position_side, ApiPositionSide::Long);
        assert_eq!(info.entry_price, 50000.0);
        assert_eq!(info.liquidation_price, 45000.0);
        assert_eq!(info.leverage, 10.0);
        assert_eq!(info.margin_type, ApiMarginType::Cross);
        assert_eq!(info.unrealized_pnl, 150.0);
    }

    #[test]
    fn test_parse_order() {
        let json = r#"{
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "ordId": "123456",
                "clOrdId": "c1",
                "px": "50000.0",
                "sz": "2.0",
                "ordType": "limit",
                "side": "buy",
                "posSide": "long",
                "tdMode": "cross",
                "fillPx": "49990.0",
                "fillSz": "0.5",
                "avgPx": "49990.0",
                "state": "partially_filled",
                "lever": "10",
                "fee": "-0.5",
                "feeCcy": "USDT",
                "reduceOnly": false,
                "cTime": "1700000000000",
                "uTime": "1700000001000"
            }]
        }"#;
        let resp: m::OkxResponse<m::Order> = serde_json::from_str(json).unwrap();
        let info = order_info_from(&resp.data[0]);
        assert_eq!(info.symbol, "BTC-USDT-SWAP");
        assert_eq!(info.order_id, "123456");
        assert_eq!(info.side, ApiSide::Buy);
        assert_eq!(info.order_type, ApiOrderType::Limit);
        assert_eq!(info.status, ApiOrderStatus::PartiallyFilled);
        assert_eq!(info.qty, 2.0);
        assert_eq!(info.executed_qty, 0.5);
        assert_eq!(info.leaves_qty, 1.5);
        assert_eq!(info.position_side, ApiPositionSide::Long);
    }

    #[test]
    fn test_parse_fills() {
        let json = r#"{
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "tradeId": "55555",
                "ordId": "123456",
                "clOrdId": "c1",
                "fillPx": "50000.0",
                "fillSz": "0.5",
                "side": "buy",
                "posSide": "long",
                "fee": "-1.25",
                "feeCcy": "USDT",
                "rebate": "0.0",
                "rebateCcy": "USDT",
                "pnl": "0.0",
                "fillTime": "1700000000000"
            }]
        }"#;
        let resp: m::OkxResponse<m::Fill> = serde_json::from_str(json).unwrap();
        let fill = &resp.data[0];
        assert_eq!(fill.symbol(), "BTC-USDT-SWAP");
        assert_eq!(fill.fee, -1.25);
        assert_eq!(fill.price(), 50000.0);
    }

    #[test]
    fn test_parse_ticker_and_funding() {
        let json = r#"{
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "last": "50000.0",
                "open24h": "49900.0",
                "high24h": "50100.0",
                "low24h": "49800.0",
                "vol24h": "1000.0",
                "volCcy24h": "50000000.0",
                "ts": "1700000000000"
            }]
        }"#;
        let resp: m::OkxResponse<m::Ticker> = serde_json::from_str(json).unwrap();
        let ticker = ticker_from(&resp.data[0], None);
        assert_eq!(ticker.symbol, "BTC-USDT-SWAP");
        assert_eq!(ticker.last_price, 50000.0);
        assert_eq!(ticker.quote_volume_24h, 50_000_000.0);

        let json = r#"{
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "fundingRate": "0.0001",
                "nextFundingRate": "0.0002",
                "fundingTime": "1700000000000",
                "nextFundingTime": "1700003600000"
            }]
        }"#;
        let resp: m::OkxResponse<m::FundingRate> = serde_json::from_str(json).unwrap();
        assert_eq!(resp.data[0].funding_rate, 0.0001);
    }

    #[test]
    fn test_parse_instruments() {
        let json = r#"{
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "ctVal": "0.001",
                "ctValCcy": "BTC",
                "settleCcy": "USDT",
                "lotSz": "0.001",
                "tickSz": "0.1",
                "minSz": "0.001",
                "state": "live",
                "ctType": "linear",
                "lever": "50"
            }]
        }"#;
        let resp: m::InstrumentsResponse = serde_json::from_str(json).unwrap();
        let inst = instrument_from(&resp.data[0]);
        assert_eq!(inst.symbol, "BTC-USDT-SWAP");
        assert_eq!(inst.base_asset, "BTC");
        assert_eq!(inst.quote_asset, "USDT");
        assert_eq!(inst.tick_size, 0.1);
        assert_eq!(inst.lot_size, 0.001);
        assert!(inst.tradable);
        assert_eq!(inst.price_precision, 1);
        assert_eq!(inst.qty_precision, 3);
    }

    // ---------------- WS 补充频道解析 ----------------

    #[test]
    fn test_ws_extra_public_channels() {
        let books5: s::Books5 = serde_json::from_str(
            r#"{"asks":[["50001.0","1.0"],["50002.0","2.0"]],"bids":[["50000.0","1.5"]],"ts":"1700000000000"}"#,
        )
        .unwrap();
        assert_eq!(books5.asks.len(), 2);
        assert_eq!(books5.bids[0][0], "50000.0");

        let bbo: s::BboTbt = serde_json::from_str(
            r#"{"instId":"BTC-USDT-SWAP","asks":[["50001.0","1.0","0","2"]],"bids":[["50000.0","1.5","0","3"]],"ts":"1700000000000"}"#,
        )
        .unwrap();
        assert_eq!(bbo.best_bid(), Some(("50000.0", "1.5")));
        assert_eq!(bbo.best_ask(), Some(("50001.0", "1.0")));

        let ticker: s::TickerWs = serde_json::from_str(
            r#"{"instId":"BTC-USDT-SWAP","last":"50000.0","ts":"1700000000000"}"#,
        )
        .unwrap();
        assert_eq!(ticker.last, "50000.0");

        let oi: s::OpenInterestWs =
            serde_json::from_str(r#"{"instId":"BTC-USDT-SWAP","oi":"123.4","ts":"1"}"#).unwrap();
        assert_eq!(oi.oi, "123.4");

        let mark: s::MarkPriceWs =
            serde_json::from_str(r#"{"instId":"BTC-USDT-SWAP","markPx":"50001.0","ts":"1"}"#)
                .unwrap();
        assert_eq!(mark.mark_px, "50001.0");

        let liq: s::LiquidationOrderWs = serde_json::from_str(
            r#"{"instId":"BTC-USDT-SWAP","px":"49000.0","sz":"1.0","side":"buy","ts":"1"}"#,
        )
        .unwrap();
        assert_eq!(liq.side, "buy");

        let status: s::StatusWs = serde_json::from_str(
            r#"{"state":"ongoing","serviceType":"0","title":"maintenance","ts":"1"}"#,
        )
        .unwrap();
        assert_eq!(status.state, "ongoing");
    }

    #[test]
    fn test_ws_extra_private_channels() {
        let account: s::AccountWs = serde_json::from_str(
            r#"{"uTime":"1","totalEq":"10000.0","adjEq":"9900.0","availEq":"9900.0","details":[]}"#,
        )
        .unwrap();
        assert_eq!(account.total_eq, "10000.0");

        let bap: s::BalanceAndPositionWs = serde_json::from_str(
            r#"{"uTime":"1","pTime":"1","eventType":"snapshot","balData":[],"posData":[]}"#,
        )
        .unwrap();
        assert_eq!(bap.event_type, "snapshot");

        let algo: s::OrdersAlgoWs = serde_json::from_str(
            r#"{"algoId":"a1","instId":"BTC-USDT-SWAP","ordType":"conditional-market","side":"sell","sz":"1.0","state":"live","ts":"1"}"#,
        )
        .unwrap();
        assert_eq!(algo.algo_id, "a1");
        assert_eq!(algo.state, "live");
    }

    /// 实盘冒烟：OKX 公共行情接口（无需签名）。
    /// 当前环境需走代理，读取 `HTTPS_PROXY` 环境变量（如 127.0.0.1:7897）。
    /// 运行：`HTTPS_PROXY=127.0.0.1:7897 cargo test --all-features okx::brokerapi::tests::live -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn live_public_api_smoke() {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(10));
        if let Ok(proxy) = std::env::var("HTTPS_PROXY") {
            builder = builder.proxy(reqwest::Proxy::all(&proxy).unwrap());
        }
        let client = OkxClient::with_options(
            "https://www.okx.com",
            "",
            "",
            "",
            false,
            builder.build().unwrap(),
            "cross".to_string(),
        );
        let api: &dyn BrokerApi = &client;
        let time = api.get_server_time().await.unwrap();
        println!("server_time={time}");
        assert!(time > 1_700_000_000_000);

        let ticker = api.get_ticker("BTC-USDT-SWAP").await.unwrap();
        println!("ticker={ticker:?}");
        assert!(ticker.last_price > 0.0);

        let book = api.get_order_book("BTC-USDT-SWAP", 5).await.unwrap();
        println!("book bids={} asks={}", book.bids.len(), book.asks.len());
        assert!(!book.bids.is_empty());

        let funding = api.get_funding_rate("BTC-USDT-SWAP").await.unwrap();
        println!("funding={funding:?}");
    }
}

impl m::Fill {
    fn symbol(&self) -> &str {
        &self.inst_id
    }

    fn price(&self) -> f64 {
        f(&self.fill_px)
    }
}
