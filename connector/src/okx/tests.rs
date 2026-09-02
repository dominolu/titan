//! OKX connector tests — organized per connector/TESTING_TEMPLATE.md.
//!
//! Run: cargo test -p connector --features okx

use super::*;
use crate::okx::{
    msg::rest::{Books, CancelResult, Instrument, OrderResult, Position},
    msg::stream::{OrderUpdate, StreamMsg},
    ordermanager::OrderManager,
    rest::{OkxClient, build_submit_body, check_cancel_result},
};
use hftbacktest::types::{OrdType, Order, Side, Status, TimeInForce};

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

fn valid_config() -> String {
    r#"rest_url = "https://www.okx.com"
public_ws_url = "wss://ws.okx.com:8443/ws/v5/public"
private_ws_url = "wss://ws.okx.com:8443/ws/v5/private"
api_key = "test-key"
secret = "test-secret"
passphrase = "test-pass"
td_mode = "cross"
order_prefix = "titan"
"#
    .to_string()
}

fn test_order(order_id: u64) -> Order {
    Order::new(
        order_id,
        300_000,
        0.1,
        1.0,
        Side::Buy,
        OrdType::Limit,
        TimeInForce::GTC,
    )
}

fn order_result(s_code: &str) -> OrderResult {
    OrderResult {
        cl_ord_id: "c1".to_string(),
        ord_id: "1".to_string(),
        s_code: s_code.to_string(),
        s_msg: "err".to_string(),
    }
}

fn order_update(cl_ord_id: &str, state: &str) -> OrderUpdate {
    OrderUpdate {
        inst_id: "BTC-USDT-SWAP".to_string(),
        ord_id: "1".to_string(),
        cl_ord_id: cl_ord_id.to_string(),
        side: "buy".to_string(),
        ord_type: "limit".to_string(),
        px: "30000".to_string(),
        sz: "1".to_string(),
        state: state.to_string(),
        acc_fill_sz: "0".to_string(),
        fill_px: "".to_string(),
        avg_px: "0".to_string(),
        u_time: "1000".to_string(),
        c_time: "1000".to_string(),
        pos_side: "net".to_string(),
        td_mode: "cross".to_string(),
    }
}

// ------------------------------------------------------------------
// 1. Config parsing
// ------------------------------------------------------------------

#[test]
fn test_build_from_valid_config() {
    let connector = Okx::build_from(&valid_config()).unwrap();
    assert_eq!(connector.config.td_mode, "cross");
    assert_eq!(connector.config.order_prefix, "titan");
    assert!(connector.config.pos_side.is_none());
}

#[test]
fn test_build_from_rejects_order_prefix_too_long() {
    let config = valid_config().replace("titan", "12345678901234567");
    assert!(matches!(
        Okx::build_from(&config),
        Err(OkxError::InvalidArg(_))
    ));
}

#[test]
fn test_build_from_rejects_order_prefix_invalid_chars() {
    // OKX clOrdId only allows ASCII alphanumerics; a dash would corrupt every client order id.
    assert!(matches!(Okx::build_from(&valid_config()), Ok(_)));
    let config = valid_config().replace("titan", "titan-");
    assert!(matches!(
        Okx::build_from(&config),
        Err(OkxError::InvalidArg(_))
    ));
}

#[test]
fn test_build_from_td_mode_default() {
    let config = valid_config().replace("td_mode = \"cross\"\n", "");
    let connector = Okx::build_from(&config).unwrap();
    assert_eq!(connector.config.td_mode, "cross");
}

#[test]
fn test_build_from_pos_side_optional() {
    let config = valid_config().replace(
        "order_prefix = \"titan\"",
        "order_prefix = \"titan\"\npos_side = \"long\"",
    );
    let connector = Okx::build_from(&config).unwrap();
    assert_eq!(connector.config.pos_side.as_deref(), Some("long"));
}

#[test]
fn test_build_from_simulated_default_false() {
    let connector = Okx::build_from(&valid_config()).unwrap();
    assert!(!connector.config.simulated);
}

#[test]
fn test_build_from_simulated_true() {
    let config = valid_config().replace(
        "order_prefix = \"titan\"",
        "order_prefix = \"titan\"\nsimulated = true",
    );
    let connector = Okx::build_from(&config).unwrap();
    assert!(connector.config.simulated);
}

#[test]
fn test_build_from_proxy_default_empty() {
    let connector = Okx::build_from(&valid_config()).unwrap();
    assert!(connector.config.proxy.is_empty());
}

#[test]
fn test_build_from_proxy_configured() {
    let config = valid_config().replace(
        "order_prefix = \"titan\"",
        "order_prefix = \"titan\"\nproxy = \"socks5h://127.0.0.1:7897\"",
    );
    let connector = Okx::build_from(&config).unwrap();
    assert_eq!(connector.config.proxy, "socks5h://127.0.0.1:7897");
}

#[test]
fn test_build_from_rejects_invalid_proxy() {
    let config = valid_config().replace(
        "order_prefix = \"titan\"",
        "order_prefix = \"titan\"\nproxy = \"not a valid proxy url\"",
    );
    assert!(Okx::build_from(&config).is_err());
}

#[test]
fn test_build_from_invalid_toml() {
    assert!(Okx::build_from("not = [valid toml").is_err());
}

#[test]
fn test_simulated_header() {
    assert_eq!(
        crate::okx::rest::simulated_header(true),
        Some(("x-simulated-trading", "1"))
    );
    assert_eq!(crate::okx::rest::simulated_header(false), None);
}

// ------------------------------------------------------------------
// 2. Message deserialization (msg.rs)
// ------------------------------------------------------------------

#[test]
fn test_deserialize_order_response_with_s_code() {
    let resp: crate::okx::msg::rest::OrderResponse = serde_json::from_str(
            r#"{"code":"0","msg":"","data":[{"clOrdId":"c1","ordId":"1","sCode":"51000","sMsg":"insufficient margin"}]}"#,
        )
        .unwrap();
    assert_eq!(resp.code, "0");
    assert_eq!(resp.data[0].s_code, "51000");
    assert_eq!(resp.data[0].s_msg, "insufficient margin");
}

#[test]
fn test_deserialize_cancel_result_with_s_code() {
    let result: CancelResult = serde_json::from_str(
        r#"{"clOrdId":"c1","ordId":"1","sCode":"51401","sMsg":"Order does not exist"}"#,
    )
    .unwrap();
    assert_eq!(result.s_code, "51401");
}

#[test]
fn test_deserialize_books() {
    let books: Books = serde_json::from_str(
        r#"{"asks":[["64757.6","992.68"]],"bids":[["64757.5","213.28"]],"ts":"1787073009905","seqId":42,"prevSeqId":41,"checksum":123}"#,
    )
    .unwrap();
    assert_eq!(books.bids[0][0], "64757.5");
    assert_eq!(books.bids[0][1], "213.28");
    assert_eq!(books.asks[0][0], "64757.6");
    assert_eq!(books.seq_id, 42);
    assert_eq!(books.prev_seq_id, 41);
    assert_eq!(books.checksum, 123);
}

#[test]
fn test_deserialize_position() {
    let position: Position = serde_json::from_str(
        r#"{"instId":"BTC-USDT-SWAP","posSide":"short","pos":"-1.5","uTime":"1724742632153"}"#,
    )
    .unwrap();
    assert_eq!(position.pos, "-1.5");
    assert_eq!(position.pos_side, "short");
}

#[test]
fn test_deserialize_instrument_lot_sz() {
    let instrument: Instrument =
        serde_json::from_str(r#"{"instId":"BTC-USDT-SWAP","lotSz":"0.001","tickSz":"0.1"}"#)
            .unwrap();
    assert_eq!(instrument.lot_sz, "0.001");
}

#[test]
fn test_deserialize_stream_data_before_ack() {
    // Regression: the untagged enum must try DataMsg before AckMsg, otherwise the
    // all-Option AckMsg swallows every data message.
    let data: StreamMsg = serde_json::from_str(
            r#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"data":[{"asks":[],"bids":[],"ts":"1"}]}"#,
        )
        .unwrap();
    match data {
        StreamMsg::Data(data) => {
            assert_eq!(data.arg.channel, "books");
            assert_eq!(data.arg.inst_id.as_deref(), Some("BTC-USDT-SWAP"));
            assert_eq!(data.data.len(), 1);
        }
        StreamMsg::Ack(_) => panic!("data message parsed as an ack"),
    }

    let ack: StreamMsg = serde_json::from_str(
            r#"{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"connId":"abc"}"#,
        )
        .unwrap();
    assert!(matches!(ack, StreamMsg::Ack(_)));
}

#[test]
fn test_deserialize_order_update_camel_case() {
    let update: OrderUpdate = serde_json::from_str(
            r#"{"instId":"BTC-USDT-SWAP","ordId":"1","clOrdId":"c1","state":"partially_filled","accFillSz":"0.5","avgPx":"30000.5","uTime":"1724742632153"}"#,
        )
        .unwrap();
    assert_eq!(update.state, "partially_filled");
    assert_eq!(update.acc_fill_sz, "0.5");
    assert_eq!(update.avg_px, "30000.5");
}

#[test]
fn test_deserialize_trade() {
    let trade: crate::okx::msg::stream::Trade = serde_json::from_str(
            r#"{"instId":"BTC-USDT-SWAP","tradeId":"1","px":"64757.5","sz":"0.01","side":"buy","ts":"1787073009905"}"#,
        )
        .unwrap();
    assert_eq!(trade.px, "64757.5");
    assert_eq!(trade.side, "buy");
}

#[test]
fn test_deserialize_funding_rate() {
    let funding: crate::okx::msg::stream::FundingRate = serde_json::from_str(
            r#"{"instId":"BTC-USDT-SWAP","fundingRate":"0.0000990706811140","fundingTime":"1787126400000","nextFundingRate":"0.0001","nextFundingTime":"1787155200000"}"#,
        )
        .unwrap();
    assert_eq!(funding.inst_id, "BTC-USDT-SWAP");
    assert_eq!(funding.funding_rate, "0.0000990706811140");
    assert_eq!(funding.next_funding_time, "1787155200000");
}

// ------------------------------------------------------------------
// 3. Signing / auth (official HMAC-SHA256 vectors)
// ------------------------------------------------------------------

fn test_client() -> OkxClient {
    OkxClient::new(
        "https://www.okx.com",
        "test-key",
        "test-secret",
        "test-pass",
    )
}

#[test]
fn test_sign_matches_official_vector() {
    // Generated with the official OKX HMAC-SHA256 scheme:
    // Base64(HMAC-SHA256(timestamp + method + path + body, secret)).
    let client = test_client();
    let body = r#"{"instId":"BTC-USDT-SWAP","tdMode":"cross"}"#;
    let signature = client.sign(
        "2026-01-01T00:00:00.000Z",
        "POST",
        "/api/v5/trade/order",
        body,
    );
    assert_eq!(signature, "KqYG04QjnrzPlRL8uMV+Ce6g2DqjNOC7YXcpQAVW3x0=");
}

#[test]
fn test_sign_login_matches_official_vector() {
    let client = test_client();
    let signature = client.sign("2026-01-01T00:00:00.000Z", "GET", "/users/self/verify", "");
    assert_eq!(signature, "Mryw5teDykU3WLjAD+BF2XQkjPn91CGb2RNZmpWRh4A=");
}

#[test]
fn test_sign_get_with_query() {
    let client = test_client();
    let signature = client.sign(
        "2026-01-01T00:00:00.000Z",
        "GET",
        "/api/v5/account/positions?instId=BTC-USDT-SWAP",
        "",
    );
    assert_eq!(signature, "srHAEJgthakF+Jf+28uQskNFnEFTWUEUHbfpTis7WeE=");
}

#[test]
fn test_ws_login_sign_matches_official_vector() {
    // Official scheme: Base64(HMAC-SHA256(secret, "<unix-seconds>GET/users/self/verify")).
    // Vector generated with the OKX documented example timestamp 1704876947.
    let signature = crate::okx::private_stream::sign_login("test-secret", "1704876947");
    assert_eq!(signature, "fQfYjR1/qK14NIFHInX5tYolALbvDPMB8TZhKGlrC24=");
}

#[test]
fn test_timestamp_format() {
    let ts = OkxClient::timestamp();
    // yyyy-MM-ddTHH:mm:ss.SSSZ
    assert_eq!(ts.len(), 24);
    assert_eq!(&ts[10..11], "T");
    assert_eq!(&ts[23..24], "Z");
    assert!(ts.as_bytes()[0..4].iter().all(u8::is_ascii_digit));
}

// ------------------------------------------------------------------
// 4. Wire format (order body construction)
// ------------------------------------------------------------------

#[test]
fn test_build_submit_body_fields() {
    let body: serde_json::Value = serde_json::from_str(&build_submit_body(
        "BTC-USDT-SWAP",
        "cross",
        None,
        "c1",
        "buy",
        "limit",
        Some("30000.0"),
        "0.001",
    ))
    .unwrap();
    assert_eq!(body["instId"], "BTC-USDT-SWAP");
    assert_eq!(body["tdMode"], "cross");
    assert_eq!(body["clOrdId"], "c1");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["ordType"], "limit");
    assert_eq!(body["px"], "30000.0");
    assert_eq!(body["sz"], "0.001");
    assert!(body.get("posSide").is_none());
}

#[test]
fn test_build_submit_body_market_omits_px() {
    let body: serde_json::Value = serde_json::from_str(&build_submit_body(
        "BTC-USDT-SWAP",
        "cross",
        None,
        "c1",
        "sell",
        "market",
        None,
        "0.001",
    ))
    .unwrap();
    assert_eq!(body["ordType"], "market");
    assert!(body.get("px").is_none());
}

#[test]
fn test_build_submit_body_pos_side() {
    let body: serde_json::Value = serde_json::from_str(&build_submit_body(
        "BTC-USDT-SWAP",
        "cross",
        Some("long"),
        "c1",
        "buy",
        "limit",
        Some("30000.0"),
        "0.001",
    ))
    .unwrap();
    assert_eq!(body["posSide"], "long");
}

#[test]
fn test_lot_sz_decimals() {
    assert_eq!(lot_sz_decimals("0.001"), 3);
    assert_eq!(lot_sz_decimals("0.1"), 1);
    assert_eq!(lot_sz_decimals("1"), 0);
    assert_eq!(lot_sz_decimals("0.0100"), 2);
}

// ------------------------------------------------------------------
// 5. Order manager state machine
// ------------------------------------------------------------------

#[test]
fn test_rest_submit_success_clears_req() {
    let mut manager = OrderManager::new("titan-");
    let cid = manager
        .prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(1))
        .unwrap();
    let order = manager
        .update_from_rest_submit(&cid, &order_result("0"))
        .unwrap()
        .unwrap();
    assert_eq!(order.req, Status::None);
    assert_eq!(order.status, Status::New);
    assert!(manager.get_client_order_id("BTC-USDT-SWAP", 1).is_some());
}

#[test]
fn test_cancel_s_code_51401_clears_req() {
    let mut manager = OrderManager::new("titan-");
    let mut order = test_order(1);
    order.req = Status::Canceled;
    let cid = manager
        .prepare_client_order_id("BTC-USDT-SWAP".to_string(), order)
        .unwrap();
    let updated = manager
        .update_cancel_fail(
            &cid,
            &OkxError::OrderError {
                code: "51401".to_string(),
                msg: "Order does not exist".to_string(),
            },
        )
        .unwrap();
    assert_eq!(updated.req, Status::None);
    // The order status cannot be determined from this error; it must not be flipped.
    assert_eq!(updated.status, Status::None);
}

#[test]
fn test_ws_partial_fill() {
    let mut manager = OrderManager::new("titan-");
    let cid = manager
        .prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(1))
        .unwrap();
    let mut update = order_update(&cid, "partially_filled");
    update.acc_fill_sz = "0.5".to_string();
    let order = manager.update_from_ws(&update).unwrap().unwrap();
    assert_eq!(order.status, Status::PartiallyFilled);
    assert_eq!(order.exec_qty, 0.5);
    assert_eq!(order.leaves_qty, 0.5);
    // A partial fill must not remove the order.
    assert!(manager.get_client_order_id("BTC-USDT-SWAP", 1).is_some());
}

#[test]
fn test_ws_ignores_external_prefix() {
    let mut manager = OrderManager::new("titan-");
    let update = order_update("other-exchange-1", "filled");
    assert!(matches!(
        manager.update_from_ws(&update),
        Err(OkxError::PrefixUnmatched)
    ));
}

#[test]
fn test_gc_removes_stale_orders() {
    let mut manager = OrderManager::new("titan-");

    let cid = manager
        .prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(1))
        .unwrap();
    let mut stale = order_update(&cid, "canceled");
    stale.u_time = "0".to_string();
    manager.update_from_ws(&stale).unwrap();

    let mut fresh = test_order(2);
    fresh.status = Status::New;
    let _fresh_cid = manager
        .prepare_client_order_id("BTC-USDT-SWAP".to_string(), fresh)
        .unwrap();

    manager.gc();

    assert!(manager.get_client_order_id("BTC-USDT-SWAP", 1).is_none());
    assert!(manager.get_client_order_id("BTC-USDT-SWAP", 2).is_some());
    assert!(manager.get_client_order_id("BTC-USDT-SWAP", 1).is_none());
}

#[test]
fn test_orders_filters_active_and_symbol() {
    let mut manager = OrderManager::new("titan-");

    let mut btc_active = test_order(1);
    btc_active.status = Status::New;
    let mut btc_filled = test_order(2);
    btc_filled.status = Status::Filled;
    let mut eth_active = test_order(3);
    eth_active.status = Status::New;

    manager.prepare_client_order_id("BTC-USDT-SWAP".to_string(), btc_active);
    manager.prepare_client_order_id("BTC-USDT-SWAP".to_string(), btc_filled);
    manager.prepare_client_order_id("ETH-USDT-SWAP".to_string(), eth_active);

    assert_eq!(manager.orders(None).len(), 2);
    let btc = manager.orders(Some("BTC-USDT-SWAP".to_string()));
    assert_eq!(btc.len(), 1);
    assert_eq!(btc[0].order_id, 1);
}

#[test]
fn test_prepare_client_order_id_rejects_duplicate() {
    let mut manager = OrderManager::new("titan-");
    assert!(
        manager
            .prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(1))
            .is_some()
    );
    assert!(
        manager
            .prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(1))
            .is_none()
    );
    assert!(
        manager
            .prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(2))
            .is_some()
    );
}

#[test]
fn test_client_order_id_format() {
    let mut manager = OrderManager::new("titan-");
    let cid = manager
        .prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(1))
        .unwrap();
    assert!(cid.starts_with("titan-"));
    assert_eq!(cid.len(), "titan-".len() + 16);
    assert!(
        cid.chars()
            .skip("titan-".len())
            .all(|c| c.is_ascii_alphanumeric())
    );
}

#[test]
fn test_cancel_all_net_mode() {
    let mut manager = OrderManager::new("titan-");
    manager.prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(1));
    manager.prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(2));

    let canceled = manager.cancel_all("BTC-USDT-SWAP", None);
    assert_eq!(canceled.len(), 2);
    assert!(manager.get_client_order_id("BTC-USDT-SWAP", 1).is_none());
    assert!(manager.get_client_order_id("BTC-USDT-SWAP", 2).is_none());
}

// ------------------------------------------------------------------
// 6. Response parsing / error mapping
// ------------------------------------------------------------------

#[test]
fn test_check_cancel_result_variants() {
    assert!(
        check_cancel_result(vec![CancelResult {
            cl_ord_id: "c1".to_string(),
            ord_id: "1".to_string(),
            s_code: "0".to_string(),
            s_msg: String::new(),
        }])
        .is_ok()
    );

    assert!(matches!(
        check_cancel_result(vec![CancelResult {
            cl_ord_id: "c1".to_string(),
            ord_id: "1".to_string(),
            s_code: "51401".to_string(),
            s_msg: "Order does not exist".to_string(),
        }]),
        Err(OkxError::OrderError { code, .. }) if code == "51401"
    ));

    assert!(matches!(
        check_cancel_result(vec![]),
        Err(OkxError::InvalidArg(_))
    ));
}

#[test]
fn test_okx_error_to_value() {
    let order_error = OkxError::OrderError {
        code: "51000".to_string(),
        msg: "insufficient margin".to_string(),
    };
    match order_error.to_value() {
        Value::Map(map) => match (&map["code"], &map["msg"]) {
            (Value::String(code), Value::String(msg)) => {
                assert_eq!(code, "51000");
                assert_eq!(msg, "insufficient margin");
            }
            _ => panic!("expected string values in the error map"),
        },
        _ => panic!("expected a map"),
    }

    let plain = OkxError::OrderNotFound.to_value();
    assert!(matches!(plain, Value::String(_)));
}

// ------------------------------------------------------------------
// 8. E2E (simulated/mainnet, ignored by default)
// ------------------------------------------------------------------

fn e2e_config() -> String {
    let api_key = std::env::var("OKX_TEST_API_KEY").expect("OKX_TEST_API_KEY must be set");
    let secret = std::env::var("OKX_TEST_SECRET").expect("OKX_TEST_SECRET must be set");
    let passphrase = std::env::var("OKX_TEST_PASSPHRASE").expect("OKX_TEST_PASSPHRASE must be set");
    let proxy = std::env::var("OKX_TEST_PROXY").unwrap_or_default();
    // Defaults to demo trading; set OKX_TEST_SIMULATED=0 to run against the live account.
    let simulated = std::env::var("OKX_TEST_SIMULATED").as_deref() != Ok("0");
    let (public_ws_url, private_ws_url) = if simulated {
        (
            "wss://wspap.okx.com:8443/ws/v5/public",
            "wss://wspap.okx.com:8443/ws/v5/private",
        )
    } else {
        (
            "wss://ws.okx.com:8443/ws/v5/public",
            "wss://ws.okx.com:8443/ws/v5/private",
        )
    };
    format!(
        r#"rest_url = "https://www.okx.com"
public_ws_url = "{public_ws_url}"
private_ws_url = "{private_ws_url}"
api_key = "{api_key}"
secret = "{secret}"
passphrase = "{passphrase}"
simulated = {simulated}
proxy = "{proxy}"
td_mode = "cross"
order_prefix = "titan"
"#
    )
}

/// Places a resting limit order far from the market and cancels it. Requires network access
/// to OKX REST and valid API credentials (use the demo-trading header variant if available).
#[tokio::test]
#[ignore = "requires OKX credentials and network access"]
async fn e2e_order_roundtrip() {
    let connector = Okx::build_from(&e2e_config()).unwrap();
    let (tx, mut rx) = crate::connector::publish_channel(64);

    let order = Order::new(
        9_990_001,
        300_000,
        0.1,
        0.01,
        Side::Buy,
        OrdType::Limit,
        TimeInForce::GTC,
    );

    connector.submit("BTC-USDT-SWAP".to_string(), order.clone(), tx.clone());
    let submitted = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            match rx.recv().await {
                Some(PublishEvent::LiveEvent(LiveEvent::Order { order, .. })) => return order,
                Some(PublishEvent::LiveEvent(LiveEvent::Error(error))) => {
                    println!("submit error event: {error:?}");
                    continue;
                }
                Some(_) => continue,
                None => panic!("event channel closed"),
            }
        }
    })
    .await
    .expect("timed out waiting for the submit event");

    // Clean up regardless of the submit result so a failed assertion never leaves a live order.
    connector.cancel("BTC-USDT-SWAP".to_string(), order.clone(), tx.clone());
    let canceled = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            match rx.recv().await {
                Some(PublishEvent::LiveEvent(LiveEvent::Order { order, .. })) => return order,
                Some(PublishEvent::LiveEvent(LiveEvent::Error(error))) => {
                    println!("cancel error event: {error:?}");
                    continue;
                }
                Some(_) => continue,
                None => panic!("event channel closed"),
            }
        }
    })
    .await
    .expect("timed out waiting for the cancel event");

    println!(
        "submit -> status={:?} req={:?} | cancel -> status={:?} req={:?}",
        submitted.status, submitted.req, canceled.status, canceled.req
    );

    assert_eq!(submitted.status, Status::New);
    assert_eq!(canceled.status, Status::Canceled);
}
