use reqwest::Method;
use serde_json::{Value, json};

use super::rest::*;
use crate::api::*;

pub(super) fn position_side(value: &Value) -> Result<ApiPositionSide, ApiError> {
    match value.as_str() {
        Some("NONE" | "") => Ok(ApiPositionSide::Net),
        Some("LONG") => Ok(ApiPositionSide::Long),
        Some("SHORT") => Ok(ApiPositionSide::Short),
        _ => Err(protocol("invalid position_side")),
    }
}
pub(super) fn order_info(value: &Value) -> Result<OrderInfo, ApiError> {
    let symbol = text(value, "symbol")?;
    symbol_parts(&symbol)?;
    let qty = number(value, "qty")?;
    let filled = number(value, "executed_qty")?;
    if qty < 0.0 || filled < 0.0 || filled > qty {
        return Err(protocol("invalid cumulative order quantities"));
    }
    let status = match value["state"].as_str() {
        Some("NEW" | "OPEN" | "SUBMITTED") => ApiOrderStatus::New,
        Some("PARTIALLY_FILLED") => ApiOrderStatus::PartiallyFilled,
        Some("FILLED") => ApiOrderStatus::Filled,
        Some("CANCELED" | "CANCELLED") => ApiOrderStatus::Canceled,
        Some("FAIL" | "REJECT" | "REJECTED") => ApiOrderStatus::Rejected,
        Some("EXPIRED") => ApiOrderStatus::Expired,
        _ => return Err(protocol("unknown CrossEx order state")),
    };
    let side = ApiSide::from_venue_str(&text(value, "side")?);
    let order_type = ApiOrderType::from_venue_str(&text(value, "type")?);
    let tif = match value["time_in_force"].as_str() {
        Some("POC") => ApiTimeInForce::GTX,
        _ => ApiTimeInForce::from_venue_str(&text(value, "time_in_force")?),
    };
    if side == ApiSide::Unknown
        || !matches!(order_type, ApiOrderType::Limit | ApiOrderType::Market)
        || tif == ApiTimeInForce::Unknown
    {
        return Err(protocol("unsupported order side, type or time in force"));
    }
    let reduce_only = match &value["reduce_only"] {
        Value::Bool(b) => *b,
        Value::String(s) if s == "true" || s == "false" => s == "true",
        _ => return Err(protocol("invalid reduce_only")),
    };
    Ok(OrderInfo {
        symbol,
        order_id: text(value, "order_id")?,
        client_order_id: if value.get("text").is_some() {
            text(value, "text")?
        } else {
            text(value, "client_order_id")?
        },
        side,
        order_type,
        status,
        price: number(value, "price")?,
        qty,
        executed_qty: filled,
        avg_price: number(value, "executed_avg_price")?,
        leaves_qty: qty - filled,
        time_in_force: tif,
        reduce_only,
        position_side: position_side(&value["position_side"])?,
        create_time: integer(value, "create_time")?,
        update_time: integer(value, "update_time")?,
        stop_price: None,
    })
}
pub(super) fn position_info(value: &Value) -> Result<PositionInfo, ApiError> {
    let symbol = text(value, "symbol")?;
    symbol_parts(&symbol)?;
    let side = position_side(&value["position_side"])?;
    let qty = number(value, "position_qty")?;
    // Net positions retain their sign; hedge legs remain separate records, including zero legs.
    let qty = match side {
        ApiPositionSide::Short => -qty.abs(),
        ApiPositionSide::Long => qty.abs(),
        _ => qty,
    };
    Ok(PositionInfo {
        symbol,
        position_side: side,
        qty,
        entry_price: number(value, "entry_price")?,
        mark_price: number(value, "mark_price")?,
        liquidation_price: optional_number(value, "liq_price")?,
        leverage: number(value, "leverage")?,
        margin_type: match value["margin_mode"].as_str() {
            Some("ISOLATED") => ApiMarginType::Isolated,
            Some("CROSS") | None => ApiMarginType::Cross,
            _ => return Err(protocol("unknown margin_mode")),
        },
        unrealized_pnl: number(value, "upnl")?,
        realized_pnl: optional_number(value, "closed_pnl")?,
        notional: number(value, "position_value")?,
        update_time: integer(value, "update_time")?,
    })
}
fn positive(value: f64, name: &str) -> Result<(), ApiError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(invalid(format!("{name} must be finite and positive")));
    }
    Ok(())
}
fn aligned(value: f64, step: f64, name: &str) -> Result<(), ApiError> {
    positive(value, name)?;
    positive(step, "instrument step")?;
    crate::units::integer_units(value, step, true)
        .map_err(|_| invalid(format!("{name} is not aligned to instrument step")))?;
    Ok(())
}
fn grid_decimal(value: f64, step: f64, name: &str) -> Result<String, ApiError> {
    aligned(value, step, name)?;
    let text = format!("{:.*}", precision(step) as usize, value);
    Ok(if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.').to_owned()
    } else {
        text
    })
}
fn precision(step: f64) -> u32 {
    (0..=18)
        .find(|p| {
            let scaled = step * 10_f64.powi(*p as i32);
            scaled >= 1.0 && (scaled - scaled.round()).abs() < 1e-8
        })
        .unwrap_or(18)
}

impl GateCrossExClient {
    async fn prepare_order(&self, req: &UnifiedOrderRequest) -> Result<Value, ApiError> {
        symbol_parts(&req.symbol)?;
        positive(req.qty, "qty")?;
        if req.side == ApiSide::Unknown
            || !matches!(req.order_type, ApiOrderType::Limit | ApiOrderType::Market)
            || req.stop_price.is_some()
        {
            return Err(unsupported("conditional orders or unknown order side/type"));
        }
        let tif = match req.time_in_force {
            ApiTimeInForce::GTC => "GTC",
            ApiTimeInForce::IOC => "IOC",
            ApiTimeInForce::FOK => "FOK",
            ApiTimeInForce::GTX => "POC",
            _ => return Err(unsupported("unknown time in force")),
        };
        if req.order_type == ApiOrderType::Market && req.time_in_force == ApiTimeInForce::GTX {
            return Err(invalid("market orders cannot be post-only"));
        }
        if let Some(id) = &req.client_order_id {
            identifier(id)?;
            if id.bytes().any(|b| b.is_ascii_uppercase()) {
                return Err(invalid(
                    "CrossEx text allows lowercase letters, digits, '-' and '_'",
                ));
            }
        }
        if self
            .exchange_type
            .as_ref()
            .is_some_and(|exchange| !req.symbol.starts_with(&format!("{exchange}_")))
        {
            return Err(invalid("symbol is outside isolated exchange scope"));
        }
        if !req.symbol.contains("_FUTURE_")
            && req.order_type == ApiOrderType::Market
            && req.side == ApiSide::Buy
        {
            return Err(unsupported(
                "base-quantity spot/margin market BUY; use create_order with explicit quote_qty",
            ));
        }
        let mode = self.get_instrument_position_mode(&req.symbol).await?;
        let pos_side = req.position_side.unwrap_or(ApiPositionSide::Net);
        let position = match (mode, pos_side) {
            (ApiPositionMode::Net, ApiPositionSide::Net) => "NONE",
            (ApiPositionMode::Hedge, ApiPositionSide::Long) => "LONG",
            (ApiPositionMode::Hedge, ApiPositionSide::Short) => "SHORT",
            _ => {
                return Err(invalid(
                    "position_side does not match the exchange position mode",
                ));
            }
        };
        let rules = self.get("/crossex/rule/symbols", &[], false).await?;
        let rule = rows(&rules)?
            .iter()
            .find(|v| v["symbol"] == req.symbol)
            .ok_or_else(|| invalid("unknown CrossEx symbol"))?;
        if rule["state"] != "live" {
            return Err(invalid("instrument is not tradable"));
        }
        let qty_wire = grid_decimal(req.qty, number(rule, "lot_size")?, "qty")?;
        if req.qty < number(rule, "min_size")? {
            return Err(invalid("qty is below min_size"));
        }
        let max = optional_number(
            rule,
            if req.order_type == ApiOrderType::Limit {
                "max_limit_size"
            } else {
                "max_market_size"
            },
        )?;
        if max > 0.0 && req.qty > max {
            return Err(invalid("qty exceeds maximum order size"));
        }
        let mut body = json!({"symbol":req.symbol,"side":req.side.as_str(),"type":req.order_type.as_str(),
            "qty":qty_wire,"time_in_force":tif,"position_side":position,"reduce_only":req.reduce_only.to_string()});
        if req.order_type == ApiOrderType::Limit {
            let price = req
                .price
                .ok_or_else(|| invalid("limit order requires price"))?;
            let price_wire = grid_decimal(price, number(rule, "tick_size")?, "price")?;
            if price * req.qty < optional_number(rule, "min_notional")? {
                return Err(invalid("order is below min_notional"));
            }
            body["price"] = Value::String(price_wire);
        } else if req.price.is_some() {
            return Err(invalid("market order must not specify price"));
        }
        if let Some(id) = &req.client_order_id {
            body["text"] = Value::String(id.clone());
        }
        Ok(body)
    }
    async fn after_action(&self, symbol: &str, response: &Value) -> Result<OrderInfo, ApiError> {
        let id = text(response, "order_id").map_err(|_| {
            ApiError::transport(
                EXCHANGE,
                "mutation accepted without order id; reconciliation required",
            )
        })?;
        self.get_order(symbol, Some(&id), None)
            .await
            .map_err(|error| {
                ApiError::transport(
                    EXCHANGE,
                    format!(
                        "mutation accepted; order {id} requires reconciliation ({})",
                        error.code
                    ),
                )
            })
    }
}

#[async_trait::async_trait]
impl BrokerApi for GateCrossExClient {
    fn supports_order_execution_evidence(&self) -> bool {
        true
    }
    async fn account_ledger_page(
        &self,
        from_ms: i64,
        to_ms: i64,
        page: u32,
        limit: u32,
    ) -> Result<Vec<AccountLedgerRecord>, ApiError> {
        if from_ms < 0 || to_ms < from_ms || page == 0 || !(1..=1000).contains(&limit) {
            return Err(protocol("invalid ledger range or page"));
        }
        let value = self
            .get(
                "/crossex/account_book",
                &[
                    ("from", from_ms.to_string()),
                    ("to", to_ms.to_string()),
                    ("page", page.to_string()),
                    ("limit", limit.to_string()),
                ],
                true,
            )
            .await?;
        let entries = rows(&value)?;
        if entries.len() > limit as usize {
            return Err(protocol("ledger page exceeds requested bound"));
        }
        let mut records = Vec::with_capacity(entries.len());
        for row in entries {
            let record = AccountLedgerRecord {
                id: text(row, "id")?,
                kind: text(row, "statement_type")?,
                currency: text(row, "coin")?,
                amount: text(row, "change")?,
                occurred_ms: integer(row, "create_time")?,
                instrument: row["symbol"].as_str().unwrap_or("").into(),
                exchange: text(row, "exchange_type")?,
            };
            if record.id.is_empty()
                || [
                    &record.id,
                    &record.kind,
                    &record.currency,
                    &record.amount,
                    &record.instrument,
                    &record.exchange,
                ]
                .iter()
                .any(|s| s.len() > 256)
                || record.occurred_ms < from_ms
                || record.occurred_ms > to_ms
            {
                return Err(protocol("ledger identity or time range mismatch"));
            }
            records.push(record);
        }
        Ok(records)
    }
    async fn ping(&self) -> Result<(), ApiError> {
        self.get("/crossex/rule/symbols", &[], false)
            .await
            .map(|_| ())
    }
    async fn get_server_time(&self) -> Result<i64, ApiError> {
        Err(unsupported("REST server time"))
    }
    async fn get_instruments(&self) -> Result<Vec<InstrumentInfo>, ApiError> {
        let value = self.get("/crossex/rule/symbols", &[], false).await?;
        rows(&value)?
            .iter()
            .filter(|v| {
                matches!(
                    v["business_type"].as_str(),
                    Some("FUTURE" | "SPOT" | "MARGIN")
                )
            })
            .map(|v| {
                let symbol = text(v, "symbol")?;
                let (_, base, quote) = symbol_parts(&symbol)?;
                let tick = number(v, "tick_size")?;
                let lot = number(v, "lot_size")?;
                positive(tick, "tick_size")?;
                positive(lot, "lot_size")?;
                Ok(InstrumentInfo {
                    symbol: symbol.clone(),
                    base_asset: base.to_owned(),
                    quote_asset: quote.to_owned(),
                    tick_size: tick,
                    lot_size: lot,
                    min_qty: number(v, "min_size")?,
                    contract_size: 1.0,
                    margin_asset: quote.to_owned(),
                    price_precision: precision(tick),
                    qty_precision: precision(lot),
                    tradable: v["state"] == "live",
                })
            })
            .collect()
    }
    async fn get_ticker(&self, symbol: &str) -> Result<Ticker, ApiError> {
        self.ticker_snapshot(symbol).await
    }
    async fn get_tickers(&self) -> Result<Vec<Ticker>, ApiError> {
        let symbols = self
            .get_instruments()
            .await?
            .into_iter()
            .filter(|v| v.tradable)
            .map(|v| v.symbol)
            .collect::<Vec<_>>();
        let mut tickers = Vec::new();
        for chunk in symbols.chunks(100) {
            tickers.extend(
                self.public_snapshots("ticker", chunk)
                    .await?
                    .iter()
                    .map(super::market::ticker)
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(tickers)
    }
    async fn get_order_book(&self, symbol: &str, limit: u32) -> Result<OrderBook, ApiError> {
        super::stream::validate_depth(symbol, limit)?;
        let multiplier = self.market_quantity_multiplier(symbol).await?;
        let mut book = super::market::book(
            &self
                .public_snapshot(&format!("order_book_{limit}"), symbol)
                .await?,
            limit,
        )?;
        for level in book.bids.iter_mut().chain(book.asks.iter_mut()) {
            level.qty = super::market_units::scale_quantity(level.qty, multiplier)?;
        }
        Ok(book)
    }
    async fn get_trades(&self, _: &str, _: u32) -> Result<Vec<Trade>, ApiError> {
        Err(unsupported("REST public trades; use the market stream"))
    }
    async fn get_klines(&self, _: &str, _: &str, _: u32) -> Result<Vec<Kline>, ApiError> {
        Err(unsupported("REST klines"))
    }
    async fn get_funding_rate(&self, symbol: &str) -> Result<FundingRate, ApiError> {
        let frame = self.public_snapshot("funding_rate", symbol).await?;
        Ok(FundingRate {
            symbol: text(&frame.result, "s")?,
            funding_rate: number(&frame.result, "r")?,
            next_funding_time: integer(&frame.result, "T")?,
            timestamp: frame.time_ms,
        })
    }
    async fn get_funding_rate_history(
        &self,
        _: &str,
        _: u32,
    ) -> Result<Vec<FundingRate>, ApiError> {
        Err(unsupported("funding rate history"))
    }
    async fn get_open_interest(&self, symbol: &str) -> Result<OpenInterest, ApiError> {
        let (venue, base, quote) = symbol_parts(symbol)?;
        if !symbol.contains("_FUTURE_") {
            return Err(unsupported("spot/margin open interest"));
        }
        if venue == "GATE" && matches!(quote, "USDT" | "USDC") {
            // This CrossEx channel need not emit an initial snapshot. A query must return a
            // current observation, so use Gate's public ticker total_size in contract units.
            let value = self
                .get(
                    &format!("/futures/{}/tickers", quote.to_ascii_lowercase()),
                    &[("contract", format!("{base}_{quote}"))],
                    false,
                )
                .await?;
            let row = rows(&value)?
                .iter()
                .find(|v| v["contract"] == format!("{base}_{quote}"))
                .ok_or_else(|| protocol("Gate open interest ticker missing"))?;
            let quantity = super::market_units::scale_quantity(
                number(row, "total_size")?,
                self.market_quantity_multiplier(symbol).await?,
            )?;
            return Ok(OpenInterest {
                symbol: symbol.into(),
                open_interest: quantity,
                timestamp: chrono::Utc::now().timestamp_millis(),
            });
        }
        if symbol_parts(symbol)?.0 == "BINANCE" {
            return Err(unsupported("Binance open interest is absent from CrossEx"));
        }
        let frame = self.public_snapshot("open_interest", symbol).await?;
        Ok(OpenInterest {
            symbol: text(&frame.result, "s")?,
            open_interest: super::market_units::scale_quantity(
                number(&frame.result, "oi")?,
                self.market_quantity_multiplier(symbol).await?,
            )?,
            timestamp: frame.time_ms,
        })
    }
    async fn submit_order(&self, req: &UnifiedOrderRequest) -> Result<OrderInfo, ApiError> {
        self.submit_order_fenced(req, None).await
    }
    async fn submit_order_fenced(
        &self,
        req: &UnifiedOrderRequest,
        guard: Option<SubmitGuard>,
    ) -> Result<OrderInfo, ApiError> {
        check_submit_guard(&guard)?;
        let body = self.prepare_order(req).await?;
        let value = self
            .request(
                Method::POST,
                "/crossex/orders",
                &[],
                Some(body),
                true,
                guard,
            )
            .await?;
        self.after_action(&req.symbol, &value).await
    }
    async fn get_account_position_mode(&self) -> Result<ApiPositionMode, ApiError> {
        let value = self.account_payload().await?;
        match value["position_mode"].as_str() {
            Some("SINGLE") => Ok(ApiPositionMode::Net),
            Some("DUAL") => Ok(ApiPositionMode::Hedge),
            _ => Err(protocol("unknown account position_mode")),
        }
    }
    async fn get_instrument_position_mode(
        &self,
        symbol: &str,
    ) -> Result<ApiPositionMode, ApiError> {
        symbol_parts(symbol)?;
        if symbol.contains("_MARGIN_") {
            Ok(ApiPositionMode::Hedge)
        } else if symbol.contains("_SPOT_") {
            Ok(ApiPositionMode::Net)
        } else {
            self.get_account_position_mode().await
        }
    }
    fn position_currency(&self, symbol: &str) -> Option<String> {
        symbol_parts(symbol)
            .ok()
            .map(|(_, _, quote)| quote.to_owned())
    }
    async fn validate_account_configuration(&self) -> Result<(), ApiError> {
        for symbol in self
            .bound_symbols
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
        {
            symbol_parts(symbol)?;
            if self
                .exchange_type
                .as_ref()
                .is_some_and(|e| !symbol.starts_with(&format!("{e}_")))
            {
                return Err(invalid("bound symbol is outside isolated exchange scope"));
            }
        }
        self.get_account_position_mode().await.map(|_| ())
    }
    async fn submit_orders(
        &self,
        reqs: &[UnifiedOrderRequest],
    ) -> Result<Vec<OrderInfo>, ApiError> {
        // CrossEx has no atomic batch submit. Fail explicitly rather than hide partial success.
        if reqs.len() > 1 {
            return Err(unsupported(
                "batch submit; submit individually and track each result",
            ));
        }
        let mut out = Vec::new();
        for req in reqs {
            out.push(self.submit_order(req).await?);
        }
        Ok(out)
    }
    async fn cancel_order(&self, req: &CancelOrderRequest) -> Result<OrderInfo, ApiError> {
        let before = self
            .get_order(
                &req.symbol,
                req.order_id.as_deref(),
                req.client_order_id.as_deref(),
            )
            .await?;
        let id = identifier(&before.order_id)?;
        let value = self
            .request(
                Method::DELETE,
                &format!("/crossex/orders/{id}"),
                &[],
                None,
                true,
                None,
            )
            .await?;
        self.after_action(&req.symbol, &value).await
    }
    async fn cancel_orders(&self, reqs: &[CancelOrderRequest]) -> Result<Vec<OrderInfo>, ApiError> {
        if reqs.len() > 1 {
            return Err(unsupported(
                "batch cancellation through BrokerApi; cancel individually",
            ));
        }
        let mut out = Vec::new();
        for req in reqs {
            out.push(self.cancel_order(req).await?);
        }
        Ok(out)
    }
    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), ApiError> {
        let mut errors = Vec::new();
        for order in self.get_open_orders(symbol).await? {
            if let Err(error) = self
                .cancel_order(&CancelOrderRequest {
                    symbol: symbol.to_owned(),
                    order_id: Some(order.order_id),
                    client_order_id: None,
                })
                .await
            {
                errors.push(error);
            }
        }
        if !self.get_open_orders(symbol).await?.is_empty() {
            return Err(ApiError::transport(
                EXCHANGE,
                "cancel-all pending; reconciliation required",
            ));
        }
        // A final authoritative empty image resolves races and unknown cancellation outcomes.
        let _ = errors;
        Ok(())
    }
    async fn cancel_all_after(&self, _: u64) -> Result<(), ApiError> {
        Err(unsupported("exchange-side cancel-all-after"))
    }
    async fn amend_order(&self, req: &AmendOrderRequest) -> Result<OrderInfo, ApiError> {
        if req.new_stop_price.is_some() {
            return Err(unsupported("stop price amendment"));
        }
        let before = self
            .get_order(
                &req.symbol,
                req.order_id.as_deref(),
                req.client_order_id.as_deref(),
            )
            .await?;
        let unified = UnifiedOrderRequest {
            symbol: req.symbol.clone(),
            side: before.side,
            order_type: before.order_type,
            price: req.new_price.or(Some(before.price)),
            qty: req.new_qty.unwrap_or(before.qty),
            time_in_force: before.time_in_force,
            reduce_only: before.reduce_only,
            position_side: Some(before.position_side),
            client_order_id: None,
            stop_price: None,
        };
        let prepared = self.prepare_order(&unified).await?;
        let mut body = json!({});
        if req.new_qty.is_some() {
            body["qty"] = prepared["qty"].clone();
        }
        if req.new_price.is_some() {
            body["price"] = prepared["price"].clone();
        }
        if body.as_object().is_none_or(|b| b.is_empty()) {
            return Err(invalid("empty amendment"));
        }
        let expected_price = req
            .new_price
            .map(|_| number(&prepared, "price"))
            .transpose()?;
        let expected_qty = req.new_qty.map(|_| number(&prepared, "qty")).transpose()?;
        let value = self
            .request(
                Method::PUT,
                &format!("/crossex/orders/{}", identifier(&before.order_id)?),
                &[],
                Some(body),
                true,
                None,
            )
            .await?;
        // CrossEx acknowledges an asynchronous amendment. Its first order query can still
        // contain the old price/quantity, even with a newer update_time. Poll facts only;
        // never resend the PUT, and leave an unconfirmed outcome to platform reconciliation.
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let order = self.after_action(&req.symbol, &value).await?;
                if expected_price.is_none_or(|p| order.price == p)
                    && expected_qty.is_none_or(|q| order.qty == q)
                {
                    return Ok(order);
                }
                if matches!(order.status, ApiOrderStatus::Canceled | ApiOrderStatus::Filled
                    | ApiOrderStatus::Rejected | ApiOrderStatus::Expired)
                {
                    return Err(ApiError::transport(EXCHANGE,
                        "amendment accepted but terminal order does not confirm intent; reconciliation required"));
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }).await.unwrap_or_else(|_| Err(ApiError::transport(EXCHANGE,
            "amendment accepted but price/quantity confirmation timed out; reconciliation required")))
    }
    async fn get_order(
        &self,
        symbol: &str,
        order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> Result<OrderInfo, ApiError> {
        symbol_parts(symbol)?;
        let id = identifier(
            order_id
                .or(client_order_id)
                .ok_or_else(|| invalid("order_id or client_order_id required"))?,
        )?;
        let value = self
            .get(&format!("/crossex/orders/{id}"), &[], true)
            .await?;
        let order = order_info(&value)?;
        if order.symbol != symbol {
            return Err(protocol("order symbol does not match request"));
        }
        self.record_order(&order);
        Ok(order)
    }
    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, ApiError> {
        symbol_parts(symbol)?;
        // This endpoint returns all open orders and has no pagination parameters.
        let value = self
            .get(
                "/crossex/open_orders",
                &[("symbol", symbol.to_owned())],
                true,
            )
            .await?;
        let orders: Vec<OrderInfo> = rows(&value)?
            .iter()
            .filter(|v| v["symbol"] == symbol)
            .map(order_info)
            .collect::<Result<_, _>>()?;
        for order in &orders {
            self.record_order(order);
        }
        Ok(orders)
    }
    async fn get_order_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<OrderInfo>, ApiError> {
        self.paged("/crossex/history_orders", symbol, Some(limit))
            .await?
            .iter()
            .map(order_info)
            .collect()
    }
    async fn get_fills(&self, symbol: &str, limit: u32) -> Result<Vec<Fill>, ApiError> {
        self.paged("/crossex/history_trades", symbol, Some(limit))
            .await?
            .iter()
            .map(|v| {
                Ok(Fill {
                    symbol: text(v, "symbol")?,
                    trade_id: text(v, "transaction_id")?,
                    order_id: text(v, "order_id")?,
                    client_order_id: text(v, "text")?,
                    price: number(v, "price")?,
                    qty: number(v, "qty")?,
                    side: ApiSide::from_venue_str(&text(v, "side")?),
                    fee: number(v, "fee")?,
                    fee_asset: text(v, "fee_coin")?,
                    realized_pnl: number(v, "rpnl")?,
                    maker: v["match_role"] == "MAKER",
                    timestamp: integer(v, "create_time")?,
                })
            })
            .collect()
    }
    async fn get_account(&self) -> Result<AccountInfo, ApiError> {
        self.account_valued(&self.account_payload().await?).await
    }
    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<PositionInfo>, ApiError> {
        let bound = self
            .bound_symbols
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let symbols: Vec<_> = symbol
            .map(|s| vec![s.to_owned()])
            .unwrap_or_else(|| bound.into_iter().collect());
        let businesses: std::collections::BTreeSet<_> = if symbols.is_empty() {
            ["FUTURE", "MARGIN"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        } else {
            symbols
                .iter()
                .map(|s| {
                    symbol_parts(s)?;
                    Ok(s.split('_').nth(1).unwrap().to_owned())
                })
                .collect::<Result<_, ApiError>>()?
        };
        let mut result = Vec::new();
        for business in businesses {
            if business == "SPOT" {
                let account = self.account_payload().await?;
                for symbol in symbols.iter().filter(|s| s.contains("_SPOT_")) {
                    result.push(spot_inventory(&account, symbol)?);
                }
                continue;
            }
            let mut query = Vec::new();
            if let Some(symbol) = symbol {
                query.push(("symbol", symbol.to_owned()));
            }
            if let Some(exchange) = &self.exchange_type {
                query.push(("exchange_type", exchange.clone()));
            }
            let value = self
                .get(
                    if business == "MARGIN" {
                        "/crossex/margin_positions"
                    } else {
                        "/crossex/positions"
                    },
                    &query,
                    true,
                )
                .await?;
            for v in rows(&value)?
                .iter()
                .filter(|v| symbol.is_none_or(|s| v["symbol"] == s))
            {
                result.push(if business == "MARGIN" {
                    margin_position_info(v)?
                } else {
                    position_info(v)?
                });
            }
        }
        Ok(result)
    }
    async fn set_leverage(
        &self,
        symbol: &str,
        leverage: f64,
        side: Option<ApiPositionSide>,
    ) -> Result<LeverageInfo, ApiError> {
        symbol_parts(symbol)?;
        positive(leverage, "leverage")?;
        if side.is_some_and(|s| s != ApiPositionSide::Net) {
            return Err(unsupported(
                "per-side leverage; CrossEx leverage applies to the symbol",
            ));
        }
        self.request(
            Method::POST,
            if symbol.contains("_MARGIN_") {
                "/crossex/margin_positions/leverage"
            } else {
                "/crossex/positions/leverage"
            },
            &[],
            Some(json!({"symbol":symbol,"leverage":leverage.to_string()})),
            true,
            None,
        )
        .await?;
        self.get_leverage(symbol).await.map_err(|e| {
            ApiError::transport(
                EXCHANGE,
                format!("leverage accepted; confirmation failed ({})", e.code),
            )
        })
    }
    async fn get_leverage(&self, symbol: &str) -> Result<LeverageInfo, ApiError> {
        symbol_parts(symbol)?;
        let value = self
            .get(
                if symbol.contains("_MARGIN_") {
                    "/crossex/margin_positions/leverage"
                } else {
                    "/crossex/positions/leverage"
                },
                &[("symbols", symbol.to_owned())],
                true,
            )
            .await?;
        Ok(LeverageInfo {
            symbol: symbol.to_owned(),
            leverage: number(&value, symbol)?,
            margin_type: ApiMarginType::Unknown,
            position_side: ApiPositionSide::Net,
        })
    }
    async fn get_fee_rates(&self, symbol: &str) -> Result<FeeRate, ApiError> {
        let (exchange, _, _) = symbol_parts(symbol)?;
        let value = self.get("/crossex/fee", &[], true).await?;
        let fee = rows(&value)?
            .iter()
            .find(|v| v["exchange_type"] == exchange)
            .ok_or_else(|| protocol("missing exchange fee rates"))?;
        let special = fee["special_fee_list"]
            .as_array()
            .and_then(|v| v.iter().find(|v| v["symbol"] == symbol));
        let (maker, taker) = if let Some(v) = special {
            (number(v, "maker_fee_rate")?, number(v, "taker_fee_rate")?)
        } else {
            (
                number(
                    fee,
                    if symbol.contains("_FUTURE_") {
                        "future_maker_fee"
                    } else {
                        "spot_maker_fee"
                    },
                )?,
                number(
                    fee,
                    if symbol.contains("_FUTURE_") {
                        "future_taker_fee"
                    } else {
                        "spot_taker_fee"
                    },
                )?,
            )
        };
        Ok(FeeRate {
            symbol: symbol.to_owned(),
            maker_fee: maker,
            taker_fee: taker,
            timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }
    async fn get_income_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<IncomeRecord>, ApiError> {
        symbol_parts(symbol)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut result = Vec::new();
        for page in 1..=100 {
            let value = self
                .get(
                    "/crossex/account_book",
                    &[("page", page.to_string()), ("limit", "1000".into())],
                    true,
                )
                .await?;
            let entries = rows(&value)?;
            for row in entries.iter().filter(|v| v["symbol"] == symbol) {
                result.push(IncomeRecord {
                    symbol: symbol.into(),
                    income_type: text(row, "statement_type")?,
                    income: number(row, "change")?,
                    asset: text(row, "coin")?,
                    timestamp: integer(row, "create_time")?,
                });
                if result.len() >= limit as usize {
                    return Ok(result);
                }
            }
            if entries.len() < 1000 {
                return Ok(result);
            }
        }
        Err(protocol(
            "account book scan limit exceeded; use list_account_book with a time range",
        ))
    }
}

pub(super) fn margin_position_info(value: &Value) -> Result<PositionInfo, ApiError> {
    let mut normalized = value.clone();
    let side = position_side(&value["position_side"])?;
    if !matches!(side, ApiPositionSide::Long | ApiPositionSide::Short) {
        return Err(protocol("margin position requires LONG/SHORT"));
    }
    // Short debt is denominated in the base currency; a long position holds the base asset.
    normalized["position_qty"] = if side == ApiPositionSide::Short {
        value["liability"].clone()
    } else {
        value["asset_qty"].clone()
    };
    normalized["mark_price"] = value["index_price"].clone();
    position_info(&normalized)
}

pub(super) fn spot_inventory(account: &Value, symbol: &str) -> Result<PositionInfo, ApiError> {
    let (exchange, base, _) = symbol_parts(symbol)?;
    let qty = rows(&account["assets"])?
        .iter()
        .filter(|a| a["exchange_type"] == exchange && a["coin"] == base)
        .try_fold(0.0, |qty, a| number(a, "balance").map(|v| qty + v))?;
    if !qty.is_finite() {
        return Err(protocol("spot inventory overflow"));
    }
    Ok(PositionInfo {
        symbol: symbol.into(),
        position_side: ApiPositionSide::Net,
        qty,
        entry_price: 0.0,
        mark_price: 0.0,
        liquidation_price: 0.0,
        leverage: 1.0,
        margin_type: ApiMarginType::Cross,
        unrealized_pnl: 0.0,
        realized_pnl: 0.0,
        notional: 0.0,
        update_time: integer(account, "update_time")?,
    })
}
