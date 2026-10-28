use super::*;
use crate::{
    api::*,
    connector::{DirectPublication, PublishEvent, direct_publish_sender},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const SYMBOL: &str = "OKX_FUTURE_BTC_USDT";
fn account() -> Value {
    json!({"account_mode":"CROSS_EXCHANGE","position_mode":"SINGLE","margin_balance":"123","available_margin":"100","update_time":"1700000000000","assets":[
    {"coin":"USDT","exchange_type":"BINANCE","balance":"100","available_balance":"90","upnl":"2","equity":"102"},
    {"coin":"USDT","exchange_type":"OKX","balance":"20","available_balance":"10","upnl":"1","equity":"21"}]})
}
fn rules() -> Value {
    json!([{"symbol":SYMBOL,"business_type":"FUTURE","state":"live","tick_size":"0.1","lot_size":"0.001","min_size":"0.001","min_notional":"5","max_limit_size":"100","max_market_size":"50","contract_size":"100"}])
}
fn order() -> Value {
    json!({"symbol":SYMBOL,"order_id":"99","text":"owner_1","state":"OPEN","side":"BUY","type":"LIMIT","qty":"0.01","price":"50000","executed_qty":"0","executed_avg_price":"0","time_in_force":"POC","reduce_only":"false","position_side":"NONE","create_time":"1700000000000","update_time":"1700000000010"})
}
fn request() -> UnifiedOrderRequest {
    UnifiedOrderRequest {
        symbol: SYMBOL.into(),
        side: ApiSide::Buy,
        order_type: ApiOrderType::Limit,
        qty: 0.01,
        price: Some(50000.0),
        time_in_force: ApiTimeInForce::GTX,
        reduce_only: false,
        position_side: Some(ApiPositionSide::Net),
        client_order_id: Some("owner_1".into()),
        stop_price: None,
    }
}
async fn mock(
    responses: Vec<(u16, Value)>,
) -> (
    String,
    mpsc::UnboundedReceiver<String>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/api/v4", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        for (status, value) in responses {
            let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0; 2048];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|s| s.parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if bytes.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            let _ = tx.send(String::from_utf8(bytes).unwrap());
            let body = value.to_string();
            let response = format!(
                "HTTP/1.1 {status} Mock\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    (url, rx, handle)
}
fn client(url: &str) -> GateCrossExClient {
    GateCrossExClient::new(url, "test_key", "test_secret").unwrap()
}

#[tokio::test]
async fn account_ledger_preserves_decimal_and_native_identity_with_bounded_time_query() {
    let response = json!([{"id":"121","statement_type":"FUNDING_FEE","coin":"USDT","symbol":SYMBOL,"exchange_type":"OKX","change":"-9007199254740993.000001","create_time":"1700000000000"}]);
    let (url, mut requests, server) = mock(vec![(200, response)]).await;
    let api = client(&url);
    let records = api
        .account_ledger_page(1699999999000, 1700000001000, 1, 1000)
        .await
        .unwrap();
    assert_eq!(records[0].amount, "-9007199254740993.000001");
    assert_eq!(records[0].id, "121");
    assert_eq!(records[0].instrument, SYMBOL);
    let wire = requests.recv().await.unwrap();
    assert!(wire.starts_with(
        "GET /api/v4/crossex/account_book?from=1699999999000&to=1700000001000&page=1&limit=1000 "
    ));
    server.await.unwrap();
    assert!(api.account_ledger_page(10, 9, 1, 1000).await.is_err());
}

#[test]
fn signing_matches_independent_python_sha512_vector() {
    assert_eq!(
        rest::signature(
            "test_secret",
            "POST",
            "/api/v4/crossex/orders",
            "",
            r#"{"qty":"2","symbol":"OKX_FUTURE_BTC_USDT"}"#,
            1700000000
        ),
        "08fa4a37fa7270b08c808fb725609286b5cf3586fb17b02be84939ada47b00d83fb178a4bbf0734806fde91b05f64798d33514478bb12c271992444080248c05"
    );
}
#[test]
fn account_balances_aggregate_same_coin_across_venues() {
    let a = rest::aggregate_account(&account()).unwrap();
    assert_eq!(a.balances.len(), 1);
    assert_eq!(a.balances[0].wallet_balance, 120.0);
    assert_eq!(a.total_wallet_balance, 120.0);
    assert_eq!(a.total_margin_balance, 123.0);
    assert_eq!(a.balances[0].available_balance, 100.0);
}
#[test]
fn account_facts_fail_closed_on_unknown_mode_or_invalid_numbers() {
    let mut a = account();
    a["account_mode"] = json!("UNKNOWN");
    assert!(rest::aggregate_account(&a).is_err());
    let mut a = account();
    a["assets"][0]["balance"] = json!("NaN");
    assert!(rest::aggregate_account(&a).is_err());
    let mut a = account();
    a["assets"] = json!({});
    assert!(rest::aggregate_account(&a).is_err());
}
#[test]
fn dual_positions_preserve_both_sides_and_zero_leg() {
    let p = json!({"symbol":SYMBOL,"position_side":"SHORT","position_qty":"2","entry_price":"20","mark_price":"19","liq_price":"30","margin_mode":"ISOLATED","leverage":"3","upnl":"2","position_value":"38","update_time":"1700000000000"});
    let s = brokerapi::position_info(&p).unwrap();
    assert_eq!(s.qty, -2.0);
    assert_eq!(s.position_side, ApiPositionSide::Short);
    let mut p = p;
    p["position_side"] = json!("LONG");
    p["position_qty"] = json!("0");
    let long = brokerapi::position_info(&p).unwrap();
    assert_eq!(long.qty, 0.0);
    assert_eq!(long.position_side, ApiPositionSide::Long);
    p["position_side"] = json!("NONE");
    p["position_qty"] = json!("-3");
    assert_eq!(brokerapi::position_info(&p).unwrap().qty, -3.0);
}
#[test]
fn order_states_post_only_and_client_alias_normalize() {
    let mut o = order();
    o["state"] = json!("FAIL");
    assert_eq!(
        brokerapi::order_info(&o).unwrap().status,
        ApiOrderStatus::Rejected
    );
    o["state"] = json!("REJECT");
    assert_eq!(
        brokerapi::order_info(&o).unwrap().status,
        ApiOrderStatus::Rejected
    );
    assert_eq!(
        brokerapi::order_info(&o).unwrap().time_in_force,
        ApiTimeInForce::GTX
    );
    o.as_object_mut().unwrap().remove("text");
    o["client_order_id"] = json!("alias");
    assert_eq!(brokerapi::order_info(&o).unwrap().client_order_id, "alias");
    o["state"] = json!("something_new");
    assert!(brokerapi::order_info(&o).is_err());
}
#[tokio::test]
async fn metadata_uses_base_quantity_without_contract_multiplier() {
    let (url, mut requests, server) = mock(vec![(200, rules())]).await;
    let instruments = client(&url).get_instruments().await.unwrap();
    server.await.unwrap();
    assert_eq!(instruments[0].contract_size, 1.0);
    assert_eq!(instruments[0].lot_size, 0.001);
    assert!(
        !requests
            .recv()
            .await
            .unwrap()
            .to_ascii_lowercase()
            .contains("\r\nkey:")
    );
}
#[tokio::test]
async fn submission_signs_exact_wire_body_and_queries_actual_order() {
    let (url, mut requests, server) = mock(vec![
        (200, account()),
        (200, rules()),
        (200, json!({"order_id":"99","text":"owner_1"})),
        (200, order()),
    ])
    .await;
    let result = client(&url).submit_order(&request()).await.unwrap();
    server.await.unwrap();
    assert_eq!(result.status, ApiOrderStatus::New);
    let _ = requests.recv().await;
    let _ = requests.recv().await;
    let wire = requests.recv().await.unwrap();
    assert!(wire.starts_with("POST /api/v4/crossex/orders "));
    let (headers, body) = wire.split_once("\r\n\r\n").unwrap();
    let get = |name: &str| {
        headers
            .lines()
            .find_map(|line| {
                let (k, v) = line.split_once(':')?;
                k.eq_ignore_ascii_case(name).then(|| v.trim().to_owned())
            })
            .unwrap()
    };
    let ts = get("timestamp").parse().unwrap();
    assert_eq!(
        get("sign"),
        rest::signature(
            "test_secret",
            "POST",
            "/api/v4/crossex/orders",
            "",
            body,
            ts
        )
    );
    let body: Value = serde_json::from_str(body).unwrap();
    assert_eq!(body["qty"], "0.01");
    assert_eq!(body["time_in_force"], "POC");
    assert_eq!(body["reduce_only"], "false");
    assert!(
        requests
            .recv()
            .await
            .unwrap()
            .starts_with("GET /api/v4/crossex/orders/99 ")
    );
}
#[tokio::test]
async fn submit_fence_rechecks_after_async_preparation() {
    let (url, _, server) = mock(vec![(200, account()), (200, rules())]).await;
    let count = Arc::new(AtomicUsize::new(0));
    let counter = count.clone();
    let guard: SubmitGuard = Arc::new(move || {
        if counter.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(())
        } else {
            Err(ApiError::new("test", "FENCED", "session changed"))
        }
    });
    let error = client(&url)
        .submit_order_fenced(&request(), Some(guard))
        .await
        .unwrap_err();
    server.await.unwrap();
    assert_eq!(error.code, "FENCED");
    assert_eq!(count.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn uncertain_submission_is_not_retried() {
    let (url, _, server) = mock(vec![
        (200, account()),
        (200, rules()),
        (503, json!({"label":"BUSY","message":"retry later"})),
    ])
    .await;
    let error = client(&url).submit_order(&request()).await.unwrap_err();
    server.await.unwrap();
    assert_eq!(error.code, "BUSY");
    assert_eq!(error.status, Some(503));
    assert!(error.outcome_unknown());
}
#[tokio::test]
async fn accepted_submission_with_failed_followup_requires_reconciliation() {
    let (url, _, server) = mock(vec![
        (200, account()),
        (200, rules()),
        (200, json!({"order_id":"99"})),
        (
            404,
            json!({"label":"NOT_FOUND","message":"not visible yet"}),
        ),
    ])
    .await;
    let error = client(&url).submit_order(&request()).await.unwrap_err();
    server.await.unwrap();
    assert!(error.outcome_unknown());
}
#[tokio::test]
async fn cancellation_ack_does_not_invent_terminal_status() {
    let (url, _, server) = mock(vec![
        (200, order()),
        (200, json!({"order_id":"99"})),
        (200, order()),
    ])
    .await;
    let info = client(&url)
        .cancel_order(&CancelOrderRequest {
            symbol: SYMBOL.into(),
            order_id: Some("99".into()),
            client_order_id: None,
        })
        .await
        .unwrap();
    server.await.unwrap();
    assert_eq!(info.status, ApiOrderStatus::New);
}
#[tokio::test]
async fn invalid_precision_and_mismatched_position_mode_never_submit() {
    let (url, _, server) = mock(vec![(200, account()), (200, rules())]).await;
    let mut req = request();
    req.qty = 0.0015;
    assert_eq!(
        client(&url).submit_order(&req).await.unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    server.await.unwrap();
    let (url, _, server) = mock(vec![(200, account())]).await;
    req.position_side = Some(ApiPositionSide::Short);
    assert_eq!(
        client(&url).submit_order(&req).await.unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    server.await.unwrap();
}
#[tokio::test]
async fn open_orders_use_unpaginated_endpoint_and_full_native_symbol() {
    let (url, mut requests, server) = mock(vec![(200, json!([order()]))]).await;
    assert_eq!(client(&url).get_open_orders(SYMBOL).await.unwrap().len(), 1);
    server.await.unwrap();
    let wire = requests.recv().await.unwrap();
    assert!(wire.starts_with("GET /api/v4/crossex/open_orders?symbol=OKX_FUTURE_BTC_USDT "));
    assert!(!wire.contains("page="));
}
#[test]
fn cumulative_order_updates_coalesce_and_ignore_stale_facts() {
    let manager = Mutex::new(OrderManager::default());
    let mut v = order();
    v["state"] = json!("PARTIALLY_FILLED");
    v["executed_qty"] = json!("0.005");
    v["executed_avg_price"] = json!("50000");
    let first = stream::update_order(&manager, &brokerapi::order_info(&v).unwrap(), 0.1)
        .unwrap()
        .unwrap();
    assert_eq!(first.exec_qty, 0.005);
    v["state"] = json!("FILLED");
    v["executed_qty"] = json!("0.01");
    v["update_time"] = json!("1700000000020");
    let second = stream::update_order(&manager, &brokerapi::order_info(&v).unwrap(), 0.1)
        .unwrap()
        .unwrap();
    assert_eq!(second.exec_qty, 0.005);
    assert!(
        stream::update_order(&manager, &brokerapi::order_info(&v).unwrap(), 0.1)
            .unwrap()
            .is_none()
    );
    v["state"] = json!("OPEN");
    v["executed_qty"] = json!("0");
    v["update_time"] = json!("1700000000000");
    assert!(
        stream::update_order(&manager, &brokerapi::order_info(&v).unwrap(), 0.1)
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn public_snapshots_funding_and_native_identity_are_preserved() {
    let desired = Mutex::new(Desired::default());
    desired.lock().unwrap().market.insert(
        SYMBOL.into(),
        HashSet::from([MarketDataKind::Depth, MarketDataKind::FundingRate]),
    );
    let (tx, mut rx) = crate::connector::test_publish_channel();
    stream::public_frame(&desired,&tx,&json!({"event":"update","channel":"order_book_5","result":{"s":SYMBOL,"ts":1700000000000_i64,"b":[["50000","2"]],"a":[["50001","1"]]}}),7).unwrap();
    assert!(
        matches!(rx.recv().await,Some(PublishEvent::FeedBatch{symbol,stream:Some(m),events}) if symbol==SYMBOL && m.snapshot && m.epoch==7 && events.len()==2)
    );
    stream::public_frame(&desired,&tx,&json!({"event":"update","channel":"funding_rate","time_ms":1700000000000_i64,"result":{"s":SYMBOL,"r":"0.0001","T":1700001000000_i64}}),7).unwrap();
    assert!(matches!(
        rx.recv().await,
        Some(PublishEvent::Funding {
            next_funding_time: 1700001000000000000,
            ..
        })
    ));
    assert!(
        stream::public_frame(
            &desired,
            &tx,
            &json!({"event":"update","channel":"trade","result":{"s":"BTCUSDT"}}),
            7
        )
        .is_err()
    );
}
#[tokio::test]
async fn public_reconnect_replays_desired_subscriptions() {
    let (url, mut subscriptions, server) = crate::connector::reconnecting_websocket_server(1).await;
    let mut connector =
        GateCrossEx::build_from(&format!("public_ws_url = {url:?}\nreconnect_ms = 10\n[market_quantity_multipliers]\nOKX_FUTURE_BTC_USDT = 1.0\n")).unwrap();
    connector.subscribe_market_data(SYMBOL.into(), vec![MarketDataKind::Depth]);
    let (tx, _) = crate::connector::test_publish_channel();
    connector.run_market_data(tx);
    for _ in 0..2 {
        let s = tokio::time::timeout(Duration::from_secs(3), subscriptions.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(s.contains("order_book_5"));
        assert!(s.contains(SYMBOL));
    }
    server.await.unwrap();
    drop(connector);
}
#[tokio::test]
async fn private_ready_waits_for_all_confirmed_subscriptions_and_reconnects() {
    let (rest, _, http) = mock(vec![(200, rules()), (200, rules())]).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (observed, mut observations) = mpsc::unbounded_channel::<bool>();
    let (advance, mut next) = mpsc::unbounded_channel::<()>();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            let login = ws.next().await.unwrap().unwrap();
            assert!(login.into_text().unwrap().contains("login"));
            ws.send(Message::Text(
                json!({"event":"login","result":{"code":"100000"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            for _ in 0..4 {
                let request = ws.next().await.unwrap().unwrap();
                assert!(request.into_text().unwrap().contains("!all"));
            }
            for channel in ["order", "position", "usertrades"] {
                ws.send(Message::Text(
                    json!({"event":"subscribe","channel":channel,"result":{"code":"100000"}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            }
            observed.send(false).unwrap();
            next.recv().await.unwrap();
            ws.send(Message::Text(
                json!({"event":"subscribe","channel":"asset","result":{"code":"100000"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            observed.send(true).unwrap();
            next.recv().await.unwrap();
            ws.close(None).await.unwrap();
        }
    });
    let mut connector=GateCrossEx::build_from(&format!("rest_url={rest:?}\nprivate_ws_url={url:?}\napi_key='test'\nsecret='test'\nreconnect_ms=10\n")).unwrap();
    connector.register_account(SYMBOL.into());
    let (ready, mut ready_rx) = mpsc::unbounded_channel();
    connector.run_account(direct_publish_sender(move |p| {
        if let DirectPublication::Event(PublishEvent::PrivateStreamReady) = p {
            let _ = ready.send(());
        }
    }));
    for _ in 0..2 {
        assert!(
            !tokio::time::timeout(Duration::from_secs(3), observations.recv())
                .await
                .unwrap()
                .unwrap()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(30), ready_rx.recv())
                .await
                .is_err()
        );
        advance.send(()).unwrap();
        assert!(observations.recv().await.unwrap());
        tokio::time::timeout(Duration::from_secs(3), ready_rx.recv())
            .await
            .unwrap()
            .unwrap();
        advance.send(()).unwrap();
    }
    server.await.unwrap();
    http.await.unwrap();
    drop(connector);
}
#[tokio::test]
async fn unsupported_capabilities_are_explicit_and_factories_registered() {
    let api = client("http://127.0.0.1:9/api/v4");
    assert_eq!(
        api.cancel_all_after(1000).await.unwrap_err().code,
        "UNSUPPORTED"
    );
    assert_eq!(
        api.submit_orders(&[request(), request()])
            .await
            .unwrap_err()
            .code,
        "UNSUPPORTED"
    );
    assert!(rest::symbol_parts("OKX_SPOT_BTC_USDT").is_ok());
    assert!(rest::identifier("../orders").is_err());
    assert!(
        crate::account_runtime::venue_account_factories()
            .iter()
            .any(|f| f.connector_type() == "gate-crossex-account")
    );
    assert!(
        crate::market_runtime::venue_market_factories()
            .iter()
            .any(|f| f.connector_type() == "gate-crossex")
    );
}

#[test]
fn late_terminal_fill_correction_is_published_without_reopening_order() {
    let manager = Mutex::new(OrderManager::default());
    let mut value = order();
    value["state"] = json!("CANCELLED");
    value["executed_qty"] = json!("0.005");
    value["executed_avg_price"] = json!("50000");
    stream::update_order(&manager, &brokerapi::order_info(&value).unwrap(), 0.1)
        .unwrap()
        .unwrap();
    value["state"] = json!("FILLED");
    value["executed_qty"] = json!("0.01");
    value["update_time"] = json!("1700000000020");
    let correction = stream::update_order(&manager, &brokerapi::order_info(&value).unwrap(), 0.1)
        .unwrap()
        .unwrap();
    assert_eq!(correction.exec_qty, 0.005);
    assert_eq!(correction.status, hftbacktest::types::Status::Filled);
}
#[test]
fn empty_client_ids_do_not_merge_different_venue_orders() {
    let manager = Mutex::new(OrderManager::default());
    let mut value = order();
    value["text"] = json!("");
    stream::update_order(&manager, &brokerapi::order_info(&value).unwrap(), 0.1)
        .unwrap()
        .unwrap();
    value["order_id"] = json!("100");
    stream::update_order(&manager, &brokerapi::order_info(&value).unwrap(), 0.1)
        .unwrap()
        .unwrap();
    assert_eq!(manager.lock().unwrap().orders.len(), 2);
}

#[tokio::test]
async fn private_facts_use_payload_and_balances_refresh_all_venues() {
    use crate::connector::AccountPublication;
    let (url, _, server) = mock(vec![(200, account())]).await;
    let desired = Mutex::new(Desired::default());
    desired.lock().unwrap().accounts.insert(SYMBOL.into());
    let orders = Mutex::new(OrderManager::default());
    let ticks = HashMap::from([(SYMBOL.to_owned(), 0.1)]);
    let captured = Arc::new(Mutex::new(Vec::new()));
    let copy = captured.clone();
    let tx = direct_publish_sender(move |p| {
        if let DirectPublication::Account(fact) = p {
            copy.lock().unwrap().push(fact.clone());
        }
    });
    stream::private_frame(&client(&url),&desired,&orders,&ticks,&tx,&json!({"event":"subscribe","channel":"order","payload":order(),"result":{"code":"100000"}})).unwrap();
    let (account, _) = stream::start_balance_refresh(client(&url))
        .await
        .unwrap()
        .unwrap();
    stream::publish_balances(&tx, &account).unwrap();
    server.await.unwrap();
    let facts = captured.lock().unwrap();
    assert!(
        matches!(&facts[0],AccountPublication::OrderWithNativeFills{venue_order_id:Some(id),client_order_id:Some(client),order,..} if id=="99" && client=="owner_1" && order.exch_timestamp==1700000000010000000)
    );
    assert!(
        matches!(&facts[1],AccountPublication::Balance{wallet,exch_ts,..} if *wallet==120.0 && *exch_ts==1700000000000000000)
    );
    drop(facts);
    stream::private_frame(&client(&url), &desired, &orders, &ticks, &tx, &json!({"channel":"usertrades","payload":{"symbol":SYMBOL,"transaction_id":"2072784922594048","order_id":"2072784922592768","text":"owner_1","side":"BUY","qty":"0.0001","price":"80924.4","fee":"0.00404622","fee_coin":"USDT","rpnl":"0","match_role":"TAKER","position_side":"NONE","create_time":"1700000000020"}})).unwrap();
    assert!(
        matches!(&captured.lock().unwrap()[2], AccountPublication::NativeFill { fill, position_side: crate::api::ApiPositionSide::Net } if fill.trade_id=="2072784922594048" && fill.qty==0.0001 && fill.fee==0.00404622 && fill.timestamp==1700000000020)
    );
}
#[tokio::test]
async fn amendment_updates_quantity_without_synthetic_order_status() {
    let mut after = order();
    after["qty"] = json!("0.02");
    let (url, mut requests, server) = mock(vec![
        (200, order()),
        (200, account()),
        (200, rules()),
        (200, json!({"order_id":"99"})),
        (200, after),
    ])
    .await;
    let result = client(&url)
        .amend_order(&AmendOrderRequest {
            symbol: SYMBOL.into(),
            order_id: Some("99".into()),
            client_order_id: None,
            new_price: None,
            new_qty: Some(0.02),
            new_stop_price: None,
        })
        .await
        .unwrap();
    server.await.unwrap();
    assert_eq!(result.qty, 0.02);
    assert_eq!(result.status, ApiOrderStatus::New);
    let mut put = None;
    while let Some(wire) = requests.recv().await {
        if wire.starts_with("PUT ") {
            put = Some(wire);
        }
    }
    let put = put.unwrap();
    let body: Value = serde_json::from_str(put.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(body, json!({"qty":"0.02"}));
}

#[test]
fn collateral_margin_valuation_is_not_used_as_wallet_balance() {
    let mut value = account();
    value["margin_balance"] = json!("77");
    let result = rest::aggregate_account(&value).unwrap();
    assert_eq!(result.total_margin_balance, 77.0);
    assert_eq!(result.total_wallet_balance, 120.0);
    value["assets"][0]["coin"] = json!("BTC");
    assert_eq!(
        rest::aggregate_account(&value).unwrap_err().code,
        "UNSUPPORTED"
    );
}

#[test]
fn review_reconciliation_reseeds_the_order_cumulative_baseline() {
    use hftbacktest::types::{OrdType, Order, Side, Status, TimeInForce};
    let connector = GateCrossEx::build_from("").unwrap();
    let mut baseline = Order::new(
        1,
        500000,
        0.1,
        0.01,
        Side::Buy,
        OrdType::Limit,
        TimeInForce::GTC,
    );
    baseline.status = Status::PartiallyFilled;
    baseline.leaves_qty = 0.006;
    baseline.exch_timestamp = 1700000000010000000;
    connector.track_managed_order(SYMBOL, "owner_1", &baseline);
    // REST bootstrap after reconnect observes another 0.002 filled while disconnected.
    baseline.leaves_qty = 0.004;
    baseline.exch_timestamp = 1700000000020000000;
    connector.track_managed_order(SYMBOL, "owner_1", &baseline);
    let mut value = order();
    value["state"] = json!("PARTIALLY_FILLED");
    value["executed_qty"] = json!("0.008");
    value["executed_avg_price"] = json!("50000");
    value["update_time"] = json!("1700000000030");
    let observed = stream::update_order(
        &connector.orders,
        &brokerapi::order_info(&value).unwrap(),
        0.1,
    )
    .unwrap()
    .unwrap();
    assert!(
        (observed.exec_qty - 0.002).abs() < 1e-12,
        "new WS fill must exclude the 0.006 already seeded by REST, actual={}",
        observed.exec_qty
    );
}

#[test]
fn review_incremental_fill_price_is_not_the_cumulative_average() {
    let orders = Mutex::new(OrderManager::default());
    let mut value = order();
    value["state"] = json!("PARTIALLY_FILLED");
    value["executed_qty"] = json!("0.005");
    value["executed_avg_price"] = json!("100");
    value["last_executed_qty"] = json!("0.005");
    value["last_executed_price"] = json!("100");
    stream::update_order(&orders, &brokerapi::order_info(&value).unwrap(), 0.1)
        .unwrap()
        .unwrap();
    value["state"] = json!("FILLED");
    value["executed_qty"] = json!("0.01");
    value["executed_avg_price"] = json!("150");
    value["last_executed_qty"] = json!("0.005");
    value["last_executed_price"] = json!("200");
    value["update_time"] = json!("1700000000020");
    let observed = stream::update_order(&orders, &brokerapi::order_info(&value).unwrap(), 0.1)
        .unwrap()
        .unwrap();
    assert_eq!(
        observed.exec_price_tick, 2000,
        "the second fill is 200, not its 150 cumulative average"
    );
}

#[tokio::test]
async fn review_cancel_all_does_not_abandon_other_live_orders() {
    let mut second = order();
    second["order_id"] = json!("100");
    second["text"] = json!("owner_2");
    let mut first = order();
    first["state"] = json!("FILLED");
    first["executed_qty"] = json!("0.01");
    first["executed_avg_price"] = json!("50000");
    let mut canceled = second.clone();
    canceled["state"] = json!("CANCELLED");
    let (url, mut requests, server) = mock(vec![
        (200, json!([order(), second.clone()])),
        (200, first),
        (
            400,
            json!({"label":"TRADE_ORDER_CANNOT_CANCEL_ERROR","message":"already filled"}),
        ),
        (200, second),
        (200, json!({"order_id":"100"})),
        (200, canceled),
        (200, json!([])),
    ])
    .await;
    client(&url).cancel_all_orders(SYMBOL).await.unwrap();
    server.await.unwrap();
    let mut attempted_other = false;
    while let Some(wire) = requests.recv().await {
        if wire.starts_with("GET /api/v4/crossex/orders/100 ")
            || wire.starts_with("DELETE /api/v4/crossex/orders/100 ")
        {
            attempted_other = true;
        }
    }
    assert!(
        attempted_other,
        "a fill/cancel race on order 99 must not leave live order 100 untouched"
    );
}

#[tokio::test]
async fn review_slow_balance_refresh_does_not_block_order_delivery() {
    use tokio::sync::oneshot;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest = format!("http://{}/api/v4", listener.local_addr().unwrap());
    let (refresh_started, started) = oneshot::channel();
    let (release, wait_release) = oneshot::channel();
    let http = tokio::spawn(async move {
        let mut start = Some(refresh_started);
        let mut release = Some(wait_release);
        for value in [rules(), account()] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0; 2048];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&buf[..n]);
                if request.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            if String::from_utf8_lossy(&request).starts_with("GET /api/v4/crossex/accounts ") {
                start.take().unwrap().send(()).unwrap();
                release.take().unwrap().await.unwrap();
            }
            let body = value.to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("ws://{}", listener.local_addr().unwrap());
    let (go, wait_go) = oneshot::channel();
    let (finish, wait_finish) = oneshot::channel();
    let ws_server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(
            json!({"event":"login","result":{"code":"100000"}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        for _ in 0..4 {
            ws.next().await.unwrap().unwrap();
        }
        for channel in ["order", "position", "asset", "usertrades"] {
            ws.send(Message::Text(
                json!({"event":"subscribe","channel":channel,"result":{"code":"100000"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        }
        wait_go.await.unwrap();
        ws.send(Message::Text(json!({"event":"subscribe","channel":"asset","time_ms":1700000000020_i64,"payload":{"coin":"USDT"},"result":{"code":"100000"}}).to_string().into())).await.unwrap();
        ws.send(Message::Text(json!({"event":"subscribe","channel":"order","payload":order(),"result":{"code":"100000"}}).to_string().into())).await.unwrap();
        wait_finish.await.unwrap();
        let _ = ws.close(None).await;
    });
    let mut connector = GateCrossEx::build_from(&format!(
        "rest_url={rest:?}\nprivate_ws_url={ws_url:?}\napi_key='test'\nsecret='test'\n"
    ))
    .unwrap();
    connector.register_account(SYMBOL.into());
    let (ready, mut ready_rx) = mpsc::unbounded_channel();
    let (delivered, mut delivered_rx) = mpsc::unbounded_channel();
    connector.run_account(direct_publish_sender(move |p| match p {
        DirectPublication::Event(PublishEvent::PrivateStreamReady) => {
            let _ = ready.send(());
        }
        DirectPublication::Account(
            crate::connector::AccountPublication::OrderWithNativeFills { .. },
        ) => {
            let _ = delivered.send(());
        }
        _ => {}
    }));
    tokio::time::timeout(Duration::from_secs(3), ready_rx.recv())
        .await
        .unwrap()
        .unwrap();
    go.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), started)
        .await
        .unwrap()
        .unwrap();
    let timely = tokio::time::timeout(Duration::from_millis(100), delivered_rx.recv())
        .await
        .is_ok();
    release.send(()).unwrap();
    if !timely {
        tokio::time::timeout(Duration::from_secs(3), delivered_rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
    finish.send(()).unwrap();
    drop(connector);
    http.await.unwrap();
    ws_server.await.unwrap();
    assert!(
        timely,
        "order delivery waits for unrelated balance HTTP response while the private stream remains READY"
    );
}

#[tokio::test]
async fn native_rest_preserves_original_adapter_routes_auth_and_payloads() {
    let expected = [
        ("GET", "rule/symbols", false),
        ("GET", "rule/risk_limits", false),
        ("GET", "accounts", true),
        ("PUT", "accounts", true),
        ("GET", "positions", true),
        ("GET", "margin_positions", true),
        ("GET", "security_positions", true),
        ("GET", "positions/leverage", true),
        ("POST", "positions/leverage", true),
        ("GET", "margin_positions/leverage", true),
        ("POST", "margin_positions/leverage", true),
        ("GET", "fee", true),
        ("GET", "transfers/coin", true),
        ("GET", "transfers", true),
        ("POST", "transfers", true),
        ("POST", "orders", true),
        ("GET", "orders/client_1", true),
        ("DELETE", "orders/client_1", true),
        ("PUT", "orders/client_1", true),
        ("POST", "batch_cancel_orders", true),
        ("GET", "open_orders", true),
        ("GET", "history_orders", true),
        ("GET", "history_trades", true),
        ("GET", "account_book", true),
        ("GET", "history_positions", true),
        ("GET", "history_margin_positions", true),
        ("GET", "history_margin_interests", true),
        ("GET", "interest_rate", true),
        ("GET", "coin_discount_rate", true),
        ("GET", "adl_rank", true),
        ("POST", "position", true),
        ("GET", "positions/margin_mode", true),
        ("POST", "positions/margin_mode", true),
        ("POST", "positions/margin", true),
        ("POST", "convert/quote", true),
        ("POST", "convert/orders", true),
    ];
    let response = json!({"opaque_decimal":"0.000000000000000001","future_field":["x"]});
    let (url, mut requests, server) = mock(vec![(200, response.clone()); expected.len()]).await;
    let api = client(&url);
    let q = vec![
        ("page".into(), "2".into()),
        ("symbol".into(), SYMBOL.into()),
    ];
    let body = json!({"symbol":"OKX_SPOT_BTC_USDT","quote_qty":"12.340000000000000001","custom":"preserved"});
    macro_rules! call {
        ($expr:expr) => {
            assert_eq!($expr.await.unwrap(), response);
        };
    }
    call!(api.list_rule_symbols());
    call!(api.list_rule_risk_limits(&[SYMBOL.into()]));
    call!(api.get_account_raw(&q));
    call!(api.update_account(json!({"position_mode":"DUAL"})));
    call!(api.list_positions(&q));
    call!(api.list_margin_positions(&q));
    call!(api.list_security_positions(&q));
    call!(api.get_position_leverages(&q));
    call!(api.set_position_leverage(body.clone()));
    call!(api.get_margin_leverages(&q));
    call!(api.set_margin_leverage(body.clone()));
    call!(api.get_fee());
    call!(api.list_transfer_coins(&q));
    call!(api.list_transfers(&q));
    call!(api.create_transfer(json!({"asset":"USDC","amount":"1.000000000000000001","from_exchange":"SPOT","to_exchange":"HYPERLIQUID","text":"transfer_1"})));
    call!(api.create_order(body.clone()));
    call!(api.query_order("client_1"));
    call!(api.cancel_order_raw("client_1"));
    call!(api.update_order("client_1", body.clone()));
    call!(api.batch_cancel_orders(json!([{"order_id":"a"},{"text":"b"}])));
    call!(api.list_open_orders(&q));
    call!(api.list_history_orders(&q));
    call!(api.list_history_trades(&q));
    call!(api.list_account_book(&q));
    call!(api.list_history_positions(&q));
    call!(api.list_history_margin_positions(&q));
    call!(api.list_history_margin_interests(&q));
    call!(api.get_interest_rates(&q));
    call!(api.get_coin_discount_rates(&q));
    call!(api.get_adl_ranks(&q));
    call!(api.close_position(body.clone()));
    call!(api.get_margin_mode(&q));
    call!(api.set_margin_mode(body.clone()));
    call!(api.adjust_isolated_margin(body.clone()));
    call!(api.create_convert_quote(body.clone()));
    call!(api.create_convert_order(body.clone()));
    server.await.unwrap();
    for (method, path, auth) in expected {
        let wire = requests.recv().await.unwrap();
        let (headers, payload) = wire.split_once("\r\n\r\n").unwrap();
        assert!(
            headers.starts_with(&format!("{method} /api/v4/crossex/{path}")),
            "{headers}"
        );
        assert_eq!(headers.to_ascii_lowercase().contains("\r\nkey:"), auth);
        if path == "transfers" && method == "POST" {
            assert_eq!(
                serde_json::from_str::<Value>(payload).unwrap(),
                json!({"coin":"USDC","amount":"1.000000000000000001","from":"SPOT","to":"CROSSEX_HYPERLIQUID","text":"transfer_1"})
            );
        }
        if path == "orders" && method == "POST" {
            assert_eq!(serde_json::from_str::<Value>(payload).unwrap(), body);
        }
        if path == "history_orders" {
            assert!(headers.contains("page=2&symbol=OKX_FUTURE_BTC_USDT"));
        }
    }
}

#[tokio::test]
async fn income_filters_client_side_without_unsupported_server_filter() {
    let (url,mut requests,server)=mock(vec![(200,json!([
        {"symbol":"OTHER","statement_type":"FUNDING","coin":"USDT","change":"99","create_time":"1"},
        {"symbol":SYMBOL,"statement_type":"FUNDING","coin":"USDC","change":"-0.25","create_time":"2"}
    ]))]).await;
    let income = client(&url).get_income_history(SYMBOL, 10).await.unwrap();
    assert_eq!(income.len(), 1);
    assert_eq!(income[0].income, -0.25);
    assert_eq!(income[0].asset, "USDC");
    server.await.unwrap();
    let wire = requests.recv().await.unwrap();
    assert!(wire.starts_with("GET /api/v4/crossex/account_book?page=1&limit=1000 "));
    assert!(!wire.contains("symbol="));
}

#[test]
fn multiple_currency_balances_keep_units_and_convert_totals_with_explicit_market_rates() {
    let mut value = account();
    value["assets"][0]["coin"] = json!("BTC");
    let rates = std::collections::BTreeMap::from([("BTC".into(), 50_000.0)]);
    let result = rest::aggregate_account_valued(&value, &rates).unwrap();
    assert_eq!(result.balances[0].asset, "BTC");
    assert_eq!(result.balances[0].wallet_balance, 100.0);
    assert_eq!(result.total_wallet_balance, 5_000_020.0);
    assert_eq!(result.total_unrealized_pnl, 100_001.0);
    assert_eq!(result.total_margin_balance, 123.0);
}

#[tokio::test]
async fn isolated_accounts_are_scoped_and_never_modify_account_modes() {
    let mut value = account();
    value["account_mode"] = json!("ISOLATED_EXCHANGE");
    let (url, mut requests, server) = mock(vec![(200, value.clone()), (200, value)]).await;
    let api = client(&url).with_exchange_type("OKX").unwrap();
    assert_eq!(api.get_account().await.unwrap().total_wallet_balance, 120.0);
    assert!(api.get_account_position_mode().await.is_ok());
    server.await.unwrap();
    for _ in 0..2 {
        assert!(
            requests
                .recv()
                .await
                .unwrap()
                .starts_with("GET /api/v4/crossex/accounts?exchange_type=OKX ")
        );
    }
}

#[test]
fn margin_inventory_preserves_long_assets_short_liabilities_and_interest() {
    let mut value = json!({"symbol":"OKX_MARGIN_BTC_USDT","position_side":"LONG","asset_qty":"2","liability":"3","entry_price":"100","index_price":"90","upnl":"-20","position_value":"180","leverage":"3","update_time":"1"});
    let long = brokerapi::margin_position_info(&value).unwrap();
    assert_eq!(long.qty, 2.0);
    assert_eq!(long.mark_price, 90.0);
    value["position_side"] = json!("SHORT");
    let short = brokerapi::margin_position_info(&value).unwrap();
    assert_eq!(short.qty, -3.0);
    assert_eq!(short.position_side, ApiPositionSide::Short);
}

#[tokio::test]
async fn raw_websocket_keeps_private_payload_distinct_from_subscription_ack() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (done, wait) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        let login = ws.next().await.unwrap().unwrap().into_text().unwrap();
        let login: Value = serde_json::from_str(&login).unwrap();
        assert_eq!(
            login["payload"]["sign"],
            rest::sign(
                "test_secret",
                &format!("channel=&event=login&time={}", login["time"])
            )
        );
        ws.send(Message::Text(
            json!({"event":"login","result":{"code":"100000"}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let subscribe = loop {
            let m = ws.next().await.unwrap().unwrap();
            if let Message::Text(t) = m {
                break serde_json::from_str::<Value>(&t).unwrap();
            }
        };
        ws.send(Message::Text(json!({"channel":"margin_interest","event":"subscribe","payload":{"symbol":"OKX_MARGIN_BTC_USDT","interest":"0.01"},"result":{"code":"100000"}}).to_string().into())).await.unwrap();
        ws.send(Message::Text(json!({"request_id":subscribe["request_id"],"channel":"margin_interest","event":"subscribe","payload":["!all"],"result":{"code":"100000"}}).to_string().into())).await.unwrap();
        wait.await.unwrap();
    });
    let api = client("http://127.0.0.1:9")
        .with_websocket_urls(&endpoint, &endpoint)
        .unwrap();
    let mut socket = GateCrossExWebSocket::connect(&api, true).await.unwrap();
    let ack = socket
        .subscribe_confirmed("margin_interest", &["!all".into()])
        .await
        .unwrap();
    assert!(ack.payload.is_array());
    let fact = socket.next().await.unwrap();
    assert_eq!(fact.data()["interest"], "0.01");
    done.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn query_ticker_uses_real_public_protocol_and_preserves_native_symbol() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        let subscribe = loop {
            let m = ws.next().await.unwrap().unwrap();
            if let Message::Text(t) = m {
                break serde_json::from_str::<Value>(&t).unwrap();
            }
        };
        assert_eq!(subscribe["payload"][0], SYMBOL); // The public service need not send a subscription ack.
        ws.send(Message::Text(json!({"channel":"ticker","event":"update","result":{"s":SYMBOL,"lp":"100","o":"90","h":"110","l":"80","v":"12","q":"1200","ts":1700000000000_i64}}).to_string().into())).await.unwrap();
        while let Some(Ok(m)) = ws.next().await {
            if m.is_close() {
                break;
            }
        }
    });
    let api = client("http://127.0.0.1:9")
        .with_websocket_urls(&endpoint, &endpoint)
        .unwrap();
    let t = api.get_ticker(SYMBOL).await.unwrap();
    assert_eq!(t.last_price, 100.0);
    assert_eq!(t.volume_24h, 12.0);
    assert_eq!(t.symbol, SYMBOL);
    server.await.unwrap();
}

#[test]
fn reconciliation_reseeds_notional_and_never_overwrites_a_newer_stream_fact() {
    use hftbacktest::types::{OrdType, Order, Side, Status, TimeInForce};
    let connector = GateCrossEx::build_from("").unwrap();
    let mut v = order();
    v["state"] = json!("PARTIALLY_FILLED");
    v["executed_qty"] = json!("0.006");
    v["executed_avg_price"] = json!("100");
    v["update_time"] = json!("1700000000020");
    let info = brokerapi::order_info(&v).unwrap();
    connector.client.record_order(&info);
    let mut baseline = Order::new(
        1,
        500000,
        0.1,
        0.01,
        Side::Buy,
        OrdType::Limit,
        TimeInForce::GTC,
    );
    baseline.status = Status::PartiallyFilled;
    baseline.leaves_qty = 0.004;
    baseline.exch_timestamp = 1700000000020000000;
    connector.track_managed_order(SYMBOL, "owner_1", &baseline);
    v["executed_qty"] = json!("0.008");
    v["executed_avg_price"] = json!("125");
    v["update_time"] = json!("1700000000030");
    let fact = stream::update_order(&connector.orders, &brokerapi::order_info(&v).unwrap(), 0.1)
        .unwrap()
        .unwrap();
    assert!((fact.exec_qty - 0.002).abs() < 1e-12);
    assert_eq!(fact.exec_price_tick, 2000);
    connector.track_managed_order(SYMBOL, "owner_1", &baseline);
    assert!(
        (connector.orders.lock().unwrap().orders["owner_1"]
            .1
            .leaves_qty
            - 0.002)
            .abs()
            < 1e-12
    );
}

#[tokio::test]
async fn partial_batch_failures_keep_successes_and_continue_processing() {
    let (url, mut requests, server) = mock(vec![
        (200, account()),
        (200, rules()),
        (400, json!({"label":"REJECT","message":"first rejected"})),
        (200, account()),
        (200, rules()),
        (200, json!({"order_id":"99"})),
        (200, order()),
    ])
    .await;
    let results = client(&url)
        .submit_order_results(&[request(), request()])
        .await;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref().unwrap_err().code, "REJECT");
    assert_eq!(results[1].as_ref().unwrap().order_id, "99");
    server.await.unwrap();
    let mut posts = 0;
    while let Some(wire) = requests.recv().await {
        if wire.starts_with("POST ") {
            posts += 1;
        }
    }
    assert_eq!(posts, 2);
}

#[test]
fn spot_inventory_is_scoped_to_venue_and_coin_without_fabricated_cost_basis() {
    let mut value = account();
    value["assets"][0]["coin"] = json!("BTC");
    value["assets"][1]["coin"] = json!("BTC");
    let p = brokerapi::spot_inventory(&value, "OKX_SPOT_BTC_USDT").unwrap();
    assert_eq!(p.qty, 20.0);
    assert_eq!(p.entry_price, 0.0);
    assert_eq!(p.position_side, ApiPositionSide::Net);
}

#[tokio::test]
async fn margin_position_mode_and_leverage_do_not_reuse_futures_endpoint() {
    let (url, mut requests, server) = mock(vec![(200, json!({"OKX_MARGIN_BTC_USDT":"3"}))]).await;
    let api = client(&url);
    assert_eq!(
        api.get_instrument_position_mode("OKX_MARGIN_BTC_USDT")
            .await
            .unwrap(),
        ApiPositionMode::Hedge
    );
    assert_eq!(
        api.get_leverage("OKX_MARGIN_BTC_USDT")
            .await
            .unwrap()
            .leverage,
        3.0
    );
    server.await.unwrap();
    assert!(
        requests.recv().await.unwrap().starts_with(
            "GET /api/v4/crossex/margin_positions/leverage?symbols=OKX_MARGIN_BTC_USDT "
        )
    );
}

#[test]
fn native_symbols_preserve_case_and_unicode_asset_names() {
    assert_eq!(
        rest::symbol_parts("HYPERLIQUID_FUTURE_kPEPE_USDC").unwrap(),
        ("HYPERLIQUID", "kPEPE", "USDC")
    );
    assert_eq!(
        rest::symbol_parts("GATE_SPOT_哈基米_USDT").unwrap(),
        ("GATE", "哈基米", "USDT")
    );
    assert!(rest::symbol_parts("GATE_FUTURE_../BTC_USDT").is_err());
}

#[tokio::test]
async fn native_websocket_mutation_disconnect_is_an_unknown_outcome() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        loop {
            let m = ws.next().await.unwrap().unwrap();
            if let Message::Text(t) = m {
                let v: Value = serde_json::from_str(&t).unwrap();
                assert_eq!(v["event"], "api");
                break;
            }
        }
        ws.close(None).await.unwrap();
    });
    let api = client("http://127.0.0.1:9")
        .with_websocket_urls(&endpoint, &endpoint)
        .unwrap();
    let socket = GateCrossExWebSocket::connect(&api, false).await.unwrap();
    let error = socket
        .api("place_order", json!({"text":"client_1"}))
        .await
        .unwrap_err();
    assert!(error.outcome_unknown());
    server.await.unwrap();
}

#[tokio::test]
async fn risk_limits_require_symbol_list_and_keep_query_public() {
    let (url, mut requests, server) = mock(vec![(200, json!([]))]).await;
    let api = client(&url);
    assert_eq!(
        api.list_rule_risk_limits(&[]).await.unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    api.list_rule_risk_limits(&[SYMBOL.into(), "GATE_MARGIN_BTC_USDT".into()])
        .await
        .unwrap();
    server.await.unwrap();
    let wire = requests.recv().await.unwrap();
    assert!(wire.starts_with(
        "GET /api/v4/crossex/rule/risk_limits?symbols=OKX_FUTURE_BTC_USDT%2CGATE_MARGIN_BTC_USDT "
    ));
    assert!(!wire.to_ascii_lowercase().contains("\r\nkey:"));
}

#[tokio::test]
async fn gate_public_contract_multiplier_is_independent_from_order_base_quantity() {
    let (url, mut requests, server) = mock(vec![(
        200,
        json!({"name":"BTC_USDT","type":"direct","quanto_multiplier":"0.0001"}),
    )])
    .await;
    assert_eq!(
        client(&url)
            .market_quantity_multiplier("GATE_FUTURE_BTC_USDT")
            .await
            .unwrap(),
        0.0001
    );
    server.await.unwrap();
    let wire = requests.recv().await.unwrap();
    assert!(wire.starts_with("GET /api/v4/futures/usdt/contracts/BTC_USDT "));
    assert!(!wire.to_ascii_lowercase().contains("\r\nkey:"));
    let metadata = json!({"code":"0","data":[{"instId":"BTC-USDT-SWAP","ctType":"linear","ctVal":"0.01","ctMult":"1","ctValCcy":"BTC"}]});
    assert_eq!(
        market_units::okx_multiplier(&metadata, "BTC-USDT-SWAP", "BTC").unwrap(),
        0.01
    );
    assert!(market_units::okx_multiplier(&metadata, "BTC-USDT-SWAP", "USDT").is_err());
}

#[tokio::test]
async fn public_book_bbo_and_trades_convert_contract_counts_to_base() {
    let symbol = "GATE_FUTURE_BTC_USDT";
    let desired = Mutex::new(Desired::default());
    desired.lock().unwrap().market.insert(
        symbol.into(),
        HashSet::from([
            MarketDataKind::Depth,
            MarketDataKind::Bbo,
            MarketDataKind::Trades,
        ]),
    );
    let scales = HashMap::from([(symbol.into(), 0.0001)]);
    let (tx, mut rx) = crate::connector::test_publish_channel();
    stream::public_frame_scaled(&desired,&tx,&json!({"event":"update","channel":"order_book_5","result":{"s":symbol,"ts":1,"b":[["82665","60414"]],"a":[["82665.1","3110"]]}}),1,&scales).unwrap();
    match rx.recv().await.unwrap() {
        PublishEvent::FeedBatch { events, .. } => {
            assert!((events[0].qty - 6.0414).abs() < 1e-12);
            assert!((events[1].qty - 0.311).abs() < 1e-12);
        }
        _ => panic!("missing book"),
    }
    stream::public_frame_scaled(&desired,&tx,&json!({"event":"update","channel":"ticker","result":{"s":symbol,"ts":2,"bp":"82665","bs":"60414","ap":"82665.1","as":"3110"}}),1,&scales).unwrap();
    match rx.recv().await.unwrap() {
        PublishEvent::FeedBatch { events, .. } => assert!((events[0].qty - 6.0414).abs() < 1e-12),
        _ => panic!("missing bbo"),
    }
    stream::public_frame_scaled(&desired,&tx,&json!({"event":"update","channel":"trade","result":{"s":symbol,"ts":3,"p":"82665","q":"20","S":"SELL"}}),1,&scales).unwrap();
    match rx.recv().await.unwrap() {
        PublishEvent::FeedBatch { events, .. } => assert!((events[0].qty - 0.002).abs() < 1e-12),
        _ => panic!("missing trade"),
    }
    assert!(stream::public_frame_scaled(&desired,&tx,&json!({"event":"update","channel":"trade","result":{"s":symbol,"ts":4,"p":"1","q":"1","S":"SELL"}}),1,&HashMap::new()).is_err());
}

#[tokio::test]
async fn gate_open_interest_query_uses_a_current_public_snapshot_in_base_units() {
    let (url, mut requests, server) = mock(vec![
        (200, json!([{"contract":"BTC_USDT","total_size":"10000"}])),
        (
            200,
            json!({"name":"BTC_USDT","type":"direct","quanto_multiplier":"0.0001"}),
        ),
    ])
    .await;
    let value = client(&url)
        .get_open_interest("GATE_FUTURE_BTC_USDT")
        .await
        .unwrap();
    assert_eq!(value.open_interest, 1.0);
    server.await.unwrap();
    let first = requests.recv().await.unwrap();
    assert!(first.starts_with("GET /api/v4/futures/usdt/tickers?contract=BTC_USDT "));
    assert!(!first.to_ascii_lowercase().contains("\r\nkey:"));
}

#[test]
fn scaled_depth_quantities_stay_exact_for_native_decimal_lot_parser() {
    assert_eq!(
        market_units::scale_decimal("33560", 0.0001).unwrap(),
        "3.356"
    );
    assert_eq!(market_units::scale_decimal("0.02", 0.01).unwrap(), "0.0002");
    assert_eq!(
        market_units::scale_decimal("60414", 0.0001).unwrap(),
        "6.0414"
    );
    assert_eq!(market_units::scale_decimal("0.000", 0.0001).unwrap(), "0");
    assert!(market_units::scale_decimal("-1", 0.01).is_err());
    assert!(market_units::scale_decimal("1", f64::INFINITY).is_err());
    let v = json!({"code":"0","data":[{"instId":"BTC-USDT-SWAP","ctType":"linear","ctVal":"0.1","ctMult":"0.1","ctValCcy":"BTC"}]});
    assert_eq!(
        market_units::okx_multiplier(&v, "BTC-USDT-SWAP", "BTC").unwrap(),
        0.01
    );
}

#[tokio::test]
async fn aligned_float_orders_serialize_canonical_grid_decimals() {
    let (url, mut requests, server) = mock(vec![
        (200, account()),
        (200, rules()),
        (200, json!({"order_id":"99"})),
        (200, order()),
    ])
    .await;
    let mut req = request();
    req.price = Some(810132.0 * 0.1);
    req.qty = 0.1 * 0.1;
    client(&url).submit_order(&req).await.unwrap();
    server.await.unwrap();
    let _ = requests.recv().await;
    let _ = requests.recv().await;
    let wire = requests.recv().await.unwrap();
    let body: Value = serde_json::from_str(wire.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(body["price"], "81013.2");
    assert_eq!(body["qty"], "0.01");
}
#[tokio::test]
async fn sub_nanounit_rules_do_not_report_zero_decimal_precision() {
    let mut value = rules();
    value[0]["tick_size"] = json!("0.000000001");
    value[0]["lot_size"] = json!("0.000000001");
    let (url, _, server) = mock(vec![(200, value)]).await;
    let instruments = client(&url).get_instruments().await.unwrap();
    server.await.unwrap();
    assert_eq!(instruments[0].price_precision, 9);
    assert_eq!(instruments[0].qty_precision, 9);
}

#[tokio::test]
async fn amendment_waits_for_both_fields_without_resending_the_write() {
    let mut partial = order();
    partial["price"] = json!("49000");
    partial["update_time"] = json!("1700000000020");
    let mut complete = partial.clone();
    complete["qty"] = json!("0.02");
    let (url, mut requests, server) = mock(vec![
        (200, order()),
        (200, account()),
        (200, rules()),
        (200, json!({"order_id":"99"})),
        (200, order()),
        (200, partial),
        (200, complete),
    ])
    .await;
    let result = client(&url)
        .amend_order(&AmendOrderRequest {
            symbol: SYMBOL.into(),
            order_id: Some("99".into()),
            client_order_id: None,
            new_price: Some(49000.0),
            new_qty: Some(0.02),
            new_stop_price: None,
        })
        .await
        .unwrap();
    server.await.unwrap();
    assert_eq!((result.price, result.qty), (49000.0, 0.02));
    let mut puts = 0;
    let mut order_queries = 0;
    while let Some(wire) = requests.recv().await {
        puts += usize::from(wire.starts_with("PUT "));
        order_queries += usize::from(wire.starts_with("GET /api/v4/crossex/orders/99 "));
    }
    assert_eq!(puts, 1);
    assert_eq!(order_queries, 4);
}
#[tokio::test]
async fn amendment_terminal_without_requested_fields_is_unknown() {
    let mut terminal = order();
    terminal["state"] = json!("CANCELED");
    let (url, mut requests, server) = mock(vec![
        (200, order()),
        (200, account()),
        (200, rules()),
        (200, json!({"order_id":"99"})),
        (200, terminal),
    ])
    .await;
    let error = client(&url)
        .amend_order(&AmendOrderRequest {
            symbol: SYMBOL.into(),
            order_id: Some("99".into()),
            client_order_id: None,
            new_price: Some(49000.0),
            new_qty: None,
            new_stop_price: None,
        })
        .await
        .unwrap_err();
    server.await.unwrap();
    assert!(error.outcome_unknown());
    assert!(error.message.contains("does not confirm intent"));
    let mut puts = 0;
    while let Some(wire) = requests.recv().await {
        puts += usize::from(wire.starts_with("PUT "));
    }
    assert_eq!(puts, 1);
}

#[test]
fn borrowed_depth_matches_owned_market_abi_and_keeps_frame_storage() {
    use crate::connector::{NativeDepthLevels, NativeMarketBatch};
    use crate::decimal::DecimalMultiplier;
    use std::sync::Arc;
    use titan_market_service as market;
    struct Sink(Arc<Mutex<Vec<Vec<u8>>>>);
    impl market::MarketEventSink for Sink {
        fn publish_market(
            &self,
            _: &str,
            payload: &[u8],
            _: market::AssetId,
            _: i64,
            _: i64,
            _: titan_core_types::TraceContext,
        ) -> Result<(), titan_core_types::CoreError> {
            self.0.lock().unwrap().push(payload.to_vec());
            Ok(())
        }
        fn publish_control(
            &self,
            _: &str,
            _: &[u8],
            _: titan_core_types::TraceContext,
        ) -> Result<(), titan_core_types::CoreError> {
            Ok(())
        }
    }
    let desired = Mutex::new(Desired::default());
    desired.lock().unwrap().market.insert(
        SYMBOL.into(),
        HashSet::from([titan_market_service::MarketDataKind::Depth]),
    );
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let scope = titan_core_types::ResourceScope::new(titan_core_types::ComponentIdentity::new(
        "test",
        "scaled-depth",
    ));
    let context = market::MarketConnectorContext {
        source: market::MarketSourceHandle {
            source_id: market::MarketSourceId(1),
            generation: 1,
        },
        instruments: Arc::from([market::MarketInstrumentBinding {
            native_symbol: Arc::from(SYMBOL),
            asset_id: market::AssetId(7),
            price_tick: "0.1".parse().unwrap(),
            quantity_lot: "0.0001".parse().unwrap(),
        }]),
        market_source_stream: market::SourceStreamId(1),
        control_source_stream: market::SourceStreamId(2),
        event_publisher: market::MarketEventPublisher::from_sink(Arc::new(Sink(bytes.clone()))),
        resources: scope.handle(),
    };
    let bridge = crate::market_event::MarketEventBridge::new(
        context,
        Arc::new(HashMap::from([(SYMBOL.into(), market::AssetId(7))])),
        Arc::new(Mutex::new(HashMap::from([(
            market::AssetId(7),
            HashSet::from([market::MarketDataKind::Depth]),
        )]))),
    );
    let raw=json!({"event":"update","channel":"order_book_5","result":{"s":SYMBOL,"ts":1700000000000_i64,"b":[["50000.1","33560"]],"a":[["50000.2","2264"]]}}).to_string();
    let begin = raw.as_ptr() as usize;
    let end = begin + raw.len();
    let tx = direct_publish_sender(move |v| {
        if let DirectPublication::NativeMarket(batch) = v {
            if let NativeMarketBatch::Depth {
                bids: NativeDepthLevels::ScaledBorrowed { levels, .. },
                receive_ts,
                ..
            } = batch
            {
                assert_eq!(receive_ts, 7);
                assert!((begin..end).contains(&(levels[0].0.as_ptr() as usize)));
                assert!((begin..end).contains(&(levels[0].1.as_ptr() as usize)));
            }
            bridge.publish_native(batch).unwrap();
        }
    });
    let factors = HashMap::from([(SYMBOL.into(), DecimalMultiplier::parse("0.0001").unwrap())]);
    assert!(depth::publish(&raw, &desired, &tx, 1, &factors, 7).unwrap());
    stream::public_frame_scaled(
        &desired,
        &tx,
        &serde_json::from_str(&raw).unwrap(),
        1,
        &HashMap::from([(SYMBOL.into(), 0.0001)]),
    )
    .unwrap();
    let rows = bytes.lock().unwrap();
    assert_eq!(rows.len(), 2);
    let mut legacy = rows[1].clone();
    legacy[44..52].copy_from_slice(&7_i64.to_le_bytes());
    assert_eq!(rows[0], legacy);
    assert_eq!(
        i64::from_le_bytes(rows[0][60..68].try_into().unwrap()),
        33560
    );
}
#[test]
fn borrowed_depth_rejects_invalid_numbers_and_uses_fallback_for_escaped_strings() {
    use crate::decimal::DecimalMultiplier;
    let desired = Mutex::new(Desired::default());
    desired.lock().unwrap().market.insert(
        SYMBOL.into(),
        HashSet::from([titan_market_service::MarketDataKind::Depth]),
    );
    let tx = direct_publish_sender(|_| {});
    let factors = HashMap::from([(SYMBOL.into(), DecimalMultiplier::ONE)]);
    for (price, qty) in [("0", "1"), ("1", "-1"), ("NaN", "1"), ("1", "inf")] {
        let raw=json!({"event":"update","channel":"order_book_5","result":{"s":SYMBOL,"ts":1,"b":[[price,qty]],"a":[]}}).to_string();
        assert!(depth::publish(&raw, &desired, &tx, 1, &factors, 7).is_err());
    }
    let raw=json!({"event":"update","channel":"order_book_5","result":{"s":SYMBOL,"ts":1,"b":[["1","1"]],"a":[]}}).to_string().replace("OKX","\\u004fKX");
    assert!(!depth::publish(&raw, &desired, &tx, 1, &factors, 7).unwrap());
    stream::public_frame_scaled(
        &desired,
        &tx,
        &serde_json::from_str(&raw).unwrap(),
        1,
        &HashMap::from([(SYMBOL.into(), 1.0)]),
    )
    .unwrap();
}
