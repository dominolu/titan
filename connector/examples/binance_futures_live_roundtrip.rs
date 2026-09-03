use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use connector::{
    api::{
        AmendOrderRequest, ApiOrderStatus, ApiOrderType, ApiSide, ApiTimeInForce, BrokerApi,
        CancelOrderRequest, OrderInfo, UnifiedOrderRequest,
    },
    binancefutures::BinanceFutures,
    connector::{Connector, ConnectorBuilder},
};
use serde_json::json;

const LIVE_CONFIRMATION: &str = "I_UNDERSTAND_REAL_ORDERS";
const MIN_TARGET_NOTIONAL: f64 = 5.02;

#[tokio::main]
async fn main() -> Result<()> {
    ensure!(
        env::var("BINANCE_FUTURES_LIVE_CONFIRM").as_deref() == Ok(LIVE_CONFIRMATION),
        "BINANCE_FUTURES_LIVE_CONFIRM must explicitly enable real orders"
    );
    let api_key = required_secret("BINANCE_FUTURES_API_KEY")?;
    let secret = required_secret("BINANCE_FUTURES_API_SECRET")?;
    let max_notional = env::var("BINANCE_FUTURES_MAX_NOTIONAL_USDT")
        .context("BINANCE_FUTURES_MAX_NOTIONAL_USDT is required")?
        .parse::<f64>()
        .context("invalid maximum notional")?;
    ensure!(
        max_notional.is_finite() && max_notional >= MIN_TARGET_NOTIONAL,
        "maximum notional must be at least {MIN_TARGET_NOTIONAL} USDT"
    );
    let symbol = env::var("BINANCE_FUTURES_TEST_SYMBOL")
        .unwrap_or_else(|_| "XRPUSDT".to_owned())
        .to_uppercase();
    let api_url = env::var("BINANCE_FUTURES_API_URL")
        .unwrap_or_else(|_| "https://fapi.binance.com".to_owned());
    let config = format!(
        "stream_url = \"wss://fstream.binance.com/ws\"\napi_url = {api_url:?}\norder_prefix = \"titanrt\"\napi_key = {api_key:?}\nsecret = {secret:?}\nsafety_timeout_ms = 0\n"
    );
    let connector = BinanceFutures::build_from(&config).context("build Binance connector")?;
    let api = connector
        .broker_api()
        .context("Binance connector did not expose BrokerApi")?;
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let prefix = format!("titanrt{run_id}");

    let result = execute(api.as_ref(), &symbol, max_notional, &prefix).await;
    let cleanup = cleanup(api.as_ref(), &symbol, &prefix).await;
    match (result, cleanup) {
        (Ok(report), Ok(cleanup_report)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "schema_version": 1,
                    "exchange": "binance-usdm",
                    "symbol": symbol,
                    "max_notional_usdt": max_notional,
                    "orders": report,
                    "cleanup": cleanup_report,
                }))?
            );
            Ok(())
        }
        (Err(error), Ok(cleanup_report)) => Err(error.context(format!(
            "roundtrip failed; cleanup completed: {cleanup_report}"
        ))),
        (Ok(_), Err(cleanup_error)) => Err(cleanup_error.context("mandatory cleanup failed")),
        (Err(error), Err(cleanup_error)) => Err(error.context(format!(
            "roundtrip failed and mandatory cleanup also failed: {cleanup_error:#}"
        ))),
    }
}

async fn execute(
    api: &dyn BrokerApi,
    symbol: &str,
    max_notional: f64,
    prefix: &str,
) -> Result<serde_json::Value> {
    api.ping().await.context("public ping")?;
    let account_before = api.get_account().await.context("account preflight")?;
    ensure!(
        account_before.available_balance > 0.0,
        "no available futures balance"
    );
    let positions_before = api
        .get_positions(Some(symbol))
        .await
        .context("positions preflight")?;
    ensure!(
        positions_before
            .iter()
            .all(|position| position.qty.abs() < 1e-12),
        "pre-existing position prevents isolated test"
    );
    ensure!(
        api.get_open_orders(symbol)
            .await
            .context("open-orders preflight")?
            .is_empty(),
        "pre-existing open order prevents isolated test"
    );

    let instrument = api
        .get_instruments()
        .await
        .context("instrument metadata")?
        .into_iter()
        .find(|value| value.symbol.eq_ignore_ascii_case(symbol))
        .with_context(|| format!("instrument {symbol} was not found"))?;
    ensure!(instrument.tradable, "instrument is not tradable");
    ensure!(
        instrument.lot_size > 0.0 && instrument.tick_size > 0.0,
        "invalid lot/tick size"
    );
    let book = api.get_order_book(symbol, 5).await.context("order book")?;
    let best_bid = book.bids.first().context("empty bid book")?.price;
    let passive_price = round_down(best_bid - 10.0 * instrument.tick_size, instrument.tick_size);
    let passive_qty = round_up(
        (MIN_TARGET_NOTIONAL / passive_price).max(instrument.min_qty),
        instrument.lot_size,
    );
    enforce_notional(passive_price, passive_qty, max_notional)?;

    let batch_id = format!("{prefix}b");
    let batch = api
        .submit_orders(&[UnifiedOrderRequest {
            symbol: symbol.to_owned(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Limit,
            price: Some(passive_price),
            qty: passive_qty,
            time_in_force: ApiTimeInForce::GTX,
            reduce_only: false,
            position_side: None,
            client_order_id: Some(batch_id.clone()),
            stop_price: None,
        }])
        .await
        .context("submit one-order batch")?
        .into_iter()
        .next()
        .context("batch submit returned no order")?;
    eprintln!(
        "batch order accepted: order_id={}, client_order_id={}, status={:?}",
        batch.order_id, batch.client_order_id, batch.status
    );
    let amended_price = round_down(passive_price - instrument.tick_size, instrument.tick_size);
    let amended = api
        .amend_order(&AmendOrderRequest {
            symbol: symbol.to_owned(),
            order_id: None,
            client_order_id: Some(batch_id.clone()),
            new_price: Some(amended_price),
            new_qty: Some(passive_qty),
            new_stop_price: None,
        })
        .await
        .context("amend batch-created order")?;
    ensure!(
        amended.status == ApiOrderStatus::New,
        "amended order did not remain open"
    );
    let batch_canceled = api
        .cancel_orders(&[CancelOrderRequest {
            symbol: symbol.to_owned(),
            order_id: Some(batch.order_id.clone()),
            client_order_id: None,
        }])
        .await
        .context("batch cancel one order")?
        .into_iter()
        .next()
        .context("batch cancel returned no order")?;
    ensure!(
        batch_canceled.status == ApiOrderStatus::Canceled,
        "batch-created order was not canceled"
    );

    let passive_id = format!("{prefix}p");
    let passive = api
        .submit_order(&UnifiedOrderRequest {
            symbol: symbol.to_owned(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Limit,
            price: Some(passive_price),
            qty: passive_qty,
            time_in_force: ApiTimeInForce::GTX,
            reduce_only: false,
            position_side: None,
            client_order_id: Some(passive_id.clone()),
            stop_price: None,
        })
        .await
        .context("submit passive test order")?;
    let passive_queried = query_order(api, symbol, &passive.order_id, &passive_id)
        .await
        .context("query passive test order")?;
    ensure!(
        matches!(
            passive_queried.status,
            ApiOrderStatus::New | ApiOrderStatus::PartiallyFilled
        ),
        "passive order entered unexpected state {:?}",
        passive_queried.status
    );
    let passive_canceled = api
        .cancel_order(&CancelOrderRequest {
            symbol: symbol.to_owned(),
            order_id: Some(passive.order_id.clone()),
            client_order_id: Some(passive_id.clone()),
        })
        .await
        .context("cancel passive test order")?;
    ensure!(
        passive_canceled.status == ApiOrderStatus::Canceled,
        "passive order was not canceled"
    );
    ensure!(
        passive_canceled.executed_qty.abs() < 1e-12,
        "passive order unexpectedly filled"
    );

    let ticker = api
        .get_ticker(symbol)
        .await
        .context("ticker before market order")?;
    let open_qty = round_up(
        (MIN_TARGET_NOTIONAL / ticker.last_price).max(instrument.min_qty),
        instrument.lot_size,
    );
    enforce_notional(ticker.last_price, open_qty, max_notional)?;
    let open_id = format!("{prefix}o");
    let open = api
        .submit_order(&UnifiedOrderRequest {
            symbol: symbol.to_owned(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Market,
            price: None,
            qty: open_qty,
            time_in_force: ApiTimeInForce::GTC,
            reduce_only: false,
            position_side: None,
            client_order_id: Some(open_id.clone()),
            stop_price: None,
        })
        .await
        .context("submit market open order")?;
    let open_final = wait_terminal(api, symbol, &open.order_id, &open_id).await?;
    ensure!(
        open_final.status == ApiOrderStatus::Filled,
        "market open did not fill"
    );

    let live_position = api
        .get_positions(Some(symbol))
        .await
        .context("position after open")?
        .into_iter()
        .find(|position| position.qty.abs() > 1e-12)
        .context("filled order did not create a visible position")?;
    ensure!(
        live_position.qty > 0.0,
        "test unexpectedly created a short position"
    );
    let close_id = format!("{prefix}c");
    let close = api
        .submit_order(&UnifiedOrderRequest {
            symbol: symbol.to_owned(),
            side: ApiSide::Sell,
            order_type: ApiOrderType::Market,
            price: None,
            qty: live_position.qty.abs(),
            time_in_force: ApiTimeInForce::GTC,
            reduce_only: true,
            position_side: None,
            client_order_id: Some(close_id.clone()),
            stop_price: None,
        })
        .await
        .context("submit reduce-only close order")?;
    let close_final = wait_terminal(api, symbol, &close.order_id, &close_id).await?;
    ensure!(
        close_final.status == ApiOrderStatus::Filled,
        "market close did not fill"
    );

    let mut test_fills = Vec::new();
    for _ in 0..20 {
        let fills = api
            .get_fills(symbol, 20)
            .await
            .context("fills after close")?;
        test_fills = fills
            .iter()
            .filter(|fill| {
                fill.order_id == open_final.order_id || fill.order_id == close_final.order_id
            })
            .map(|fill| {
                json!({
                    "order_id": fill.order_id,
                    "client_order_id": fill.client_order_id,
                    "side": fill.side.as_str(),
                    "price": fill.price,
                    "qty": fill.qty,
                    "fee": fill.fee,
                    "fee_asset": fill.fee_asset,
                    "realized_pnl": fill.realized_pnl,
                    "maker": fill.maker,
                })
            })
            .collect();
        if test_fills.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ensure!(test_fills.len() >= 2, "expected open and close fills");

    Ok(json!({
        "batch_submit": order_summary(&batch),
        "amend": order_summary(&amended),
        "batch_cancel": order_summary(&batch_canceled),
        "passive_submit": order_summary(&passive),
        "passive_query": order_summary(&passive_queried),
        "passive_cancel": order_summary(&passive_canceled),
        "market_open": order_summary(&open_final),
        "market_close": order_summary(&close_final),
        "fills": test_fills,
    }))
}

async fn wait_terminal(
    api: &dyn BrokerApi,
    symbol: &str,
    order_id: &str,
    client_order_id: &str,
) -> Result<OrderInfo> {
    for _ in 0..20 {
        let order = query_order(api, symbol, order_id, client_order_id)
            .await
            .context("query order terminal state")?;
        if matches!(
            order.status,
            ApiOrderStatus::Filled
                | ApiOrderStatus::Canceled
                | ApiOrderStatus::Rejected
                | ApiOrderStatus::Expired
        ) {
            return Ok(order);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("order did not reach a terminal state before deadline")
}

async fn query_order(
    api: &dyn BrokerApi,
    symbol: &str,
    order_id: &str,
    client_order_id: &str,
) -> Result<OrderInfo> {
    let mut last_error = None;
    for _ in 0..10 {
        match api
            .get_order(symbol, Some(order_id), Some(client_order_id))
            .await
        {
            Ok(order) => return Ok(order),
            Err(error) if error.code == "-2013" => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(last_error
        .context("order was not visible before query deadline")?
        .into())
}

async fn cleanup(api: &dyn BrokerApi, symbol: &str, prefix: &str) -> Result<serde_json::Value> {
    for order in api
        .get_open_orders(symbol)
        .await
        .context("cleanup open orders")?
    {
        if order.client_order_id.starts_with(prefix) {
            api.cancel_order(&CancelOrderRequest {
                symbol: symbol.to_owned(),
                order_id: Some(order.order_id),
                client_order_id: Some(order.client_order_id),
            })
            .await
            .context("cleanup cancel")?;
        }
    }
    for position in api
        .get_positions(Some(symbol))
        .await
        .context("cleanup positions")?
    {
        if position.qty.abs() < 1e-12 {
            continue;
        }
        let side = if position.qty > 0.0 {
            ApiSide::Sell
        } else {
            ApiSide::Buy
        };
        api.submit_order(&UnifiedOrderRequest {
            symbol: symbol.to_owned(),
            side,
            order_type: ApiOrderType::Market,
            price: None,
            qty: position.qty.abs(),
            time_in_force: ApiTimeInForce::GTC,
            reduce_only: true,
            position_side: None,
            client_order_id: Some(format!("{prefix}x")),
            stop_price: None,
        })
        .await
        .context("emergency reduce-only cleanup")?;
    }
    let final_positions = api
        .get_positions(Some(symbol))
        .await
        .context("final positions")?;
    let remaining_test_orders = api
        .get_open_orders(symbol)
        .await
        .context("final open orders")?
        .into_iter()
        .filter(|order| order.client_order_id.starts_with(prefix))
        .count();
    ensure!(
        final_positions
            .iter()
            .all(|position| position.qty.abs() < 1e-12),
        "non-zero final position"
    );
    ensure!(remaining_test_orders == 0, "test order remained open");
    let account = api.get_account().await.context("final account")?;
    Ok(json!({
        "position_zero": true,
        "remaining_test_orders": remaining_test_orders,
        "wallet_balance": account.total_wallet_balance,
        "available_balance": account.available_balance,
    }))
}

fn enforce_notional(price: f64, qty: f64, max_notional: f64) -> Result<()> {
    let notional = price * qty;
    ensure!(
        notional <= max_notional + 1e-9,
        "order notional {notional} exceeds {max_notional}"
    );
    Ok(())
}

fn round_up(value: f64, step: f64) -> f64 {
    (value / step).ceil() * step
}

fn round_down(value: f64, step: f64) -> f64 {
    (value / step).floor() * step
}

fn order_summary(order: &OrderInfo) -> serde_json::Value {
    json!({
        "order_id": order.order_id,
        "client_order_id": order.client_order_id,
        "status": order.status.as_str(),
        "side": order.side.as_str(),
        "type": order.order_type.as_str(),
        "price": order.price,
        "qty": order.qty,
        "executed_qty": order.executed_qty,
        "avg_price": order.avg_price,
        "reduce_only": order.reduce_only,
    })
}

fn required_secret(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}
