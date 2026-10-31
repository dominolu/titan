#![cfg(all(feature = "backpack", feature = "lighter"))]
use crate::{
    api::*,
    backpack::{BackpackClient, BackpackOperation, Config as BackpackConfig},
    lighter::{LighterNativeClient, LighterOperation},
    native_api::*,
    venue::test_support::{http, path},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signature, SigningKey};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Arc};

fn schema<'a>(document: &'a Value, value: &'a Value) -> &'a Value {
    value["$ref"]
        .as_str()
        .map(|p| document.pointer(p.strip_prefix('#').unwrap()).unwrap())
        .unwrap_or(value)
}
fn sample(document: &Value, value: &Value) -> Value {
    let value = schema(document, value);
    if let Some(options) = value["enum"].as_array() {
        return if value["type"] == "integer" && options[0].is_string() {
            serde_json::from_str(options[0].as_str().unwrap()).unwrap()
        } else {
            options[0].clone()
        };
    }
    if let Some(parts) = value["allOf"].as_array() {
        let values: Vec<_> = parts
            .iter()
            .filter(|p| p.as_object().is_none_or(|p| !p.is_empty()))
            .map(|p| sample(document, p))
            .collect();
        if values.iter().all(Value::is_object) {
            return Value::Object(
                values
                    .into_iter()
                    .flat_map(|v| v.as_object().unwrap().clone())
                    .collect(),
            );
        }
        if let Some(value) = values.into_iter().next() {
            return value;
        }
    }
    for key in ["oneOf", "anyOf"] {
        if let Some(options) = value[key].as_array() {
            return sample(document, &options[0]);
        }
    }
    match value["type"].as_str() {
        Some("object") => Value::Object(
            value["required"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|key| {
                    let key = key.as_str().unwrap();
                    (key.into(), sample(document, &value["properties"][key]))
                })
                .collect(),
        ),
        Some("array") => Value::Array(
            (0..value["minItems"].as_u64().unwrap_or(1).max(1))
                .map(|_| sample(document, &value["items"]))
                .collect(),
        ),
        Some("integer") => value.get("minimum").cloned().unwrap_or(json!(1)),
        Some("number") => value.get("minimum").cloned().unwrap_or(json!(1.25)),
        Some("boolean") => json!(false),
        _ => json!("f".repeat(value["minLength"].as_u64().unwrap_or(1).max(1) as usize)),
    }
}
fn sample_request(operation: &Operation) -> NativeRequest {
    let mut request = NativeRequest::default();
    for p in operation.parameters() {
        let p = schema(operation.document, p);
        if p["required"] != true {
            continue;
        }
        let name = p["name"].as_str().unwrap();
        let v = sample(operation.document, &p["schema"]);
        match p["in"].as_str() {
            Some("query") => {
                request.query.insert(name.into(), v);
            }
            Some("path") => {
                request.path.insert(name.into(), v);
            }
            Some("header")
                if ![
                    "authorization",
                    "x-api-key",
                    "x-signature",
                    "x-timestamp",
                    "x-window",
                ]
                .contains(&name.to_ascii_lowercase().as_str()) =>
            {
                request.headers.insert(
                    name.into(),
                    v.as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| v.to_string()),
                );
            }
            _ => {}
        }
    }
    let body = schema(operation.document, &operation.definition["requestBody"]);
    if body["required"] == true {
        let content = body["content"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap();
        request.body = Some(sample(operation.document, &content["schema"]));
    }
    request
}
fn backpack(url: &str) -> (BackpackClient, String) {
    let seed = [7; 32];
    let key = SigningKey::from_bytes(&seed);
    let journal = path("native-http");
    let client = BackpackClient::new(BackpackConfig {
        rest_url: url.into(),
        api_key: STANDARD.encode(key.verifying_key().as_bytes()),
        secret: STANDARD.encode(seed),
        client_id_journal: Some(journal.clone()),
        ..Default::default()
    })
    .unwrap();
    (client, journal)
}
fn header<'a>(head: &'a str, name: &str) -> &'a str {
    head.lines()
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.eq_ignore_ascii_case(name).then(|| v.trim())
        })
        .unwrap()
}

#[test]
fn every_official_operation_has_one_named_binding() {
    let manifest: Value = serde_json::from_str(include_str!("../specs/manifest.json")).unwrap();
    for (venue, ops, expected) in [
        (
            "backpack",
            BackpackOperation::ALL
                .iter()
                .map(|o| o.metadata())
                .collect::<Vec<_>>(),
            75,
        ),
        (
            "lighter",
            LighterOperation::ALL
                .iter()
                .map(|o| o.metadata())
                .collect::<Vec<_>>(),
            104,
        ),
    ] {
        assert_eq!(ops.len(), expected);
        let actual: HashSet<_> = ops.iter().map(|o| (o.id, o.method, o.path)).collect();
        assert_eq!(actual.len(), expected);
        let documented: HashSet<_> = manifest["operations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|o| o["venue"] == venue)
            .map(|o| {
                (
                    o["operation"].as_str().unwrap(),
                    o["method"].as_str().unwrap(),
                    o["path"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(actual, documented);
    }
}
#[tokio::test]
async fn all_backpack_routes_dispatch_methods_native_bodies_and_signed_headers() {
    let (url, mut rx, task) = http(vec![
        (200, json!({"native":true}));
        BackpackOperation::ALL.len()
    ])
    .await;
    let (client, journal) = backpack(&url);
    for op in BackpackOperation::ALL {
        let meta = op.metadata();
        let request = sample_request(&meta);
        let expected_body = request.body.clone();
        let response = client
            .native_request(*op, request, None)
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", meta.id));
        assert_eq!(response.json().await.unwrap()["native"], true);
        let sent = rx.recv().await.unwrap();
        let mut fields = sent.head.lines().next().unwrap().split_whitespace();
        assert_eq!(fields.next(), Some(meta.method));
        let route = fields.next().unwrap();
        assert!(!route.contains('{'));
        assert_eq!(
            sent.head.to_ascii_lowercase().contains("x-signature:"),
            meta.instruction.is_some(),
            "{}",
            meta.id
        );
        if let Some(body) = expected_body {
            assert_eq!(
                serde_json::from_str::<Value>(&sent.body).unwrap(),
                body,
                "{}",
                meta.id
            );
        }
    }
    task.await.unwrap();
    drop(client);
    std::fs::remove_file(journal).unwrap();
}
#[tokio::test]
async fn all_lighter_and_explorer_routes_preserve_forms_and_do_not_leak_tokens_to_explorer() {
    let (url, mut rx, task) = http(vec![
        (200, json!({"native":true}));
        LighterOperation::ALL.len()
    ])
    .await;
    let client = LighterNativeClient::new(&url, &url)
        .unwrap()
        .with_authorization(Arc::new(|| Ok("fixture-auth".into())));
    for op in LighterOperation::ALL {
        let meta = op.metadata();
        let request = sample_request(&meta);
        let expected_body = request.body.clone();
        let response = client
            .native_request(*op, request, None)
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", meta.id));
        assert_eq!(response.json().await.unwrap()["native"], true);
        let sent = rx.recv().await.unwrap();
        assert_eq!(sent.head.split_whitespace().next(), Some(meta.method));
        assert_eq!(
            sent.head.to_ascii_lowercase().contains("authorization:"),
            meta.family != "explorer",
            "{}",
            meta.id
        );
        if let Some(body) = expected_body {
            if sent
                .head
                .to_ascii_lowercase()
                .contains("application/x-www-form-urlencoded")
            {
                let parsed =
                    reqwest::Url::parse(&format!("http://fixture/?{}", sent.body)).unwrap();
                for (k, v) in body.as_object().unwrap() {
                    let expected = v
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| v.to_string());
                    assert!(
                        parsed
                            .query_pairs()
                            .any(|(pk, pv)| pk == k.as_str() && pv == expected),
                        "{}: {}",
                        meta.id,
                        k
                    );
                }
            } else {
                assert_eq!(serde_json::from_str::<Value>(&sent.body).unwrap(), body);
            }
        }
    }
    task.await.unwrap();
}
#[tokio::test]
async fn backpack_batch_signature_matches_official_concatenated_order_vector() {
    let (url, mut rx, task) = http(vec![(200, json!([]))]).await;
    let (client, journal) = backpack(&url);
    let orders = json!([{"symbol":"SOL_USDC_PERP","side":"Bid","orderType":"Limit","price":"141","quantity":"12"},{"symbol":"SOL_USDC_PERP","side":"Bid","orderType":"Limit","price":"140","quantity":"11"}]);
    client
        .native_execute_order_batch(NativeRequest::body(orders.clone()), None)
        .await
        .unwrap();
    let sent = rx.recv().await.unwrap();
    assert_eq!(serde_json::from_str::<Value>(&sent.body).unwrap(), orders);
    let message = format!(
        "instruction=orderExecute&orderType=Limit&price=141&quantity=12&side=Bid&symbol=SOL_USDC_PERP&instruction=orderExecute&orderType=Limit&price=140&quantity=11&side=Bid&symbol=SOL_USDC_PERP&timestamp={}&window=5000",
        header(&sent.head, "x-timestamp")
    );
    let signature =
        Signature::from_slice(&STANDARD.decode(header(&sent.head, "x-signature")).unwrap())
            .unwrap();
    SigningKey::from_bytes(&[7; 32])
        .verifying_key()
        .verify_strict(message.as_bytes(), &signature)
        .unwrap();
    task.await.unwrap();
    drop(client);
    std::fs::remove_file(journal).unwrap();
}
#[tokio::test]
async fn backpack_repeated_market_type_values_are_all_signed() {
    let operations = [
        (BackpackOperation::GetFills, "fillHistoryQueryAll"),
        (BackpackOperation::GetOrderHistory, "orderHistoryQueryAll"),
        (
            BackpackOperation::GetPositionHistory,
            "positionHistoryQueryAll",
        ),
    ];
    let (url, mut rx, task) = http(vec![(200, json!([])); operations.len()]).await;
    let (client, journal) = backpack(&url);
    for (operation, instruction) in operations {
        client
            .native_request(
                operation,
                NativeRequest::query(json!({
                    "marketType":["SPOT","PERP"],"limit":2
                }))
                .unwrap(),
                None,
            )
            .await
            .unwrap();
        let sent = rx.recv().await.unwrap();
        let route = sent.head.split_whitespace().nth(1).unwrap();
        let url = reqwest::Url::parse(&format!("http://fixture{route}")).unwrap();
        let values: Vec<_> = url
            .query_pairs()
            .filter(|(k, _)| k == "marketType")
            .map(|(_, v)| v.into_owned())
            .collect();
        assert_eq!(values, ["SPOT", "PERP"]);
        let message = format!(
            "instruction={instruction}&limit=2&marketType=SPOT&marketType=PERP&timestamp={}&window=5000",
            header(&sent.head, "x-timestamp")
        );
        let signature =
            Signature::from_slice(&STANDARD.decode(header(&sent.head, "x-signature")).unwrap())
                .unwrap();
        SigningKey::from_bytes(&[7; 32])
            .verifying_key()
            .verify_strict(message.as_bytes(), &signature)
            .unwrap();
    }
    task.await.unwrap();
    drop(client);
    std::fs::remove_file(journal).unwrap();
}
#[tokio::test]
async fn backpack_withdrawal_sends_recipient_information_but_excludes_it_from_signature() {
    let (url, mut rx, task) = http(vec![(200, json!({"ok":true})); 2]).await;
    let (client, journal) = backpack(&url);
    for recipient in [
        json!({
            "withdrawal_address_id":1,"withdrawal_purpose":"fixture","sanctions_representation":true
        }),
        Value::Null,
    ] {
        let body = json!({
            "address":"fixture-address","blockchain":"Solana","quantity":"1","symbol":"USDC",
            "recipientInformation":recipient
        });
        client
            .native_request_withdrawal(NativeRequest::body(body.clone()), None)
            .await
            .unwrap();
        let sent = rx.recv().await.unwrap();
        assert_eq!(serde_json::from_str::<Value>(&sent.body).unwrap(), body);
        let message = format!(
            "instruction=withdraw&address=fixture-address&blockchain=Solana&quantity=1&symbol=USDC&timestamp={}&window=5000",
            header(&sent.head, "x-timestamp")
        );
        let signature =
            Signature::from_slice(&STANDARD.decode(header(&sent.head, "x-signature")).unwrap())
                .unwrap();
        SigningKey::from_bytes(&[7; 32])
            .verifying_key()
            .verify_strict(message.as_bytes(), &signature)
            .unwrap();
    }
    task.await.unwrap();
    drop(client);
    std::fs::remove_file(journal).unwrap();
}
#[tokio::test]
async fn native_writes_keep_final_guard_and_reject_unknown_or_credential_parameters() {
    let client = LighterNativeClient::new("http://127.0.0.1:1", "http://127.0.0.1:1").unwrap();
    let request = NativeRequest::body(json!({"tx_type":14,"tx_info":"{}"}));
    let result = client
        .native_send_tx(
            request,
            Some(Arc::new(|| Err(ApiError::new("test", "STALE", "closed")))),
        )
        .await;
    assert!(matches!(result,Err(e)if e.code=="STALE"));
    assert!(
        client
            .native_status(NativeRequest::query(json!({"unknown":1})).unwrap())
            .await
            .is_err()
    );
    assert!(
        client
            .native_status(NativeRequest::default().with_header("Authorization", "unbound-token"))
            .await
            .is_err()
    );
    let meta = LighterOperation::ExplorerGetAccountsParamAssets.metadata();
    let prepared = crate::native_api::prepare(
        "lighter",
        "http://127.0.0.1:1/api",
        &meta,
        NativeRequest::default().with_path("param", "../other?x=1"),
    )
    .unwrap();
    assert_eq!(
        prepared.url.path(),
        "/api/accounts/..%2Fother%3Fx%3D1/assets"
    );
    assert!(prepared.url.query().is_none());
    for value in [".", ".."] {
        assert!(
            crate::native_api::prepare(
                "lighter",
                "http://127.0.0.1:1/api",
                &meta,
                NativeRequest::default().with_path("param", value)
            )
            .is_err()
        );
    }
}
#[test]
fn official_integer_enum_typo_is_normalized_without_changing_wire_type() {
    let operation = LighterOperation::RfqCreate.metadata();
    let request = NativeRequest::body(
        json!({"market_index":0,"direction":0,"base_amount":"0.000000000000000001"}),
    );
    let prepared =
        crate::native_api::prepare("lighter", "http://127.0.0.1:1", &operation, request).unwrap();
    assert!(prepared.body.unwrap()["direction"].is_number());
    for direction in [json!("0"), json!(3)] {
        assert!(
            crate::native_api::prepare(
                "lighter",
                "http://127.0.0.1:1",
                &operation,
                NativeRequest::body(json!({"market_index":0,"direction":direction}))
            )
            .is_err()
        );
    }
}
#[tokio::test]
async fn variational_legacy_and_v2_share_limit_and_native_fields_remain_lossless() {
    use crate::variational::{StatsVersion, VariationalClient, VariationalError};
    let (url, mut rx, task) = http(vec![
        (
            200,
            json!({"listings":[{"ticker":"BTC","funding_rate":"0"}],"legacy_only":"kept"}),
        ),
        (
            200,
            json!({"native_extra":{"amount":"0.000000000000000001"}}),
        ),
    ])
    .await;
    let client = VariationalClient::new(&url).unwrap();
    let first = client.native_stats(StatsVersion::LegacyV1).await.unwrap();
    assert_eq!(first.body["legacy_only"], "kept");
    assert!(
        rx.recv()
            .await
            .unwrap()
            .head
            .contains("GET /metadata/stats ")
    );
    assert!(matches!(
        client.native_stats(StatsVersion::V2).await,
        Err(VariationalError::RateLimited { .. })
    ));
    tokio::time::sleep(std::time::Duration::from_millis(1050)).await;
    let second = client.native_stats(StatsVersion::V2).await.unwrap();
    assert_eq!(
        second.body["native_extra"]["amount"],
        "0.000000000000000001"
    );
    assert!(
        rx.recv()
            .await
            .unwrap()
            .head
            .contains("GET /metadata/v2/stats ")
    );
    task.await.unwrap();
}
#[tokio::test]
async fn native_websocket_preserves_subscription_fields_ping_binary_and_server_errors() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        let msg = ws.next().await.unwrap().unwrap();
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        assert_eq!(v["channel"], "account_all_assets/42");
        assert_eq!(v["auth"], "fixture-auth");
        assert_eq!(v["native_option"], 7);
        ws.send(Message::Text(json!({"type":"ping"}).to_string().into()))
            .await
            .unwrap();
        let pong: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(pong["type"], "pong");
        ws.send(Message::Binary(vec![0, 255, 1].into()))
            .await
            .unwrap();
        ws.send(Message::Text(
            json!({"type":"error","code":401}).to_string().into(),
        ))
        .await
        .unwrap();
    });
    let client =
        LighterNativeClient::default().with_authorization(Arc::new(|| Ok("fixture-auth".into())));
    let mut ws = client.websocket(&url, true).await.unwrap();
    ws.subscribe(
        "account_all_assets/42",
        json!({"native_option":7}).as_object().unwrap().clone(),
    )
    .await
    .unwrap();
    assert!(
        matches!(ws.socket.receive().await.unwrap(),crate::native_ws::NativeFrame::Binary(b)if b==vec![0,255,1])
    );
    assert!(
        matches!(ws.socket.receive().await.unwrap(),crate::native_ws::NativeFrame::Json(v)if v["code"]==401)
    );
    server.await.unwrap();
}
#[tokio::test]
async fn websocket_transaction_rpc_uses_object_for_single_and_json_strings_for_batch() {
    use crate::lighter::signer::NativeSignedTransaction;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        for batch in [false, true] {
            let frame = ws.next().await.unwrap().unwrap();
            let v: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
            assert_eq!(v["data"]["id"], "fixture-rpc");
            if batch {
                assert_eq!(v["type"], "jsonapi/sendtxbatch");
                assert_eq!(
                    serde_json::from_str::<Value>(v["data"]["tx_types"].as_str().unwrap()).unwrap(),
                    json!([14])
                );
                let infos: Vec<String> =
                    serde_json::from_str(v["data"]["tx_infos"].as_str().unwrap()).unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(&infos[0]).unwrap()["Nonce"],
                    9007199254740993u64
                );
            } else {
                assert_eq!(v["type"], "jsonapi/sendtx");
                assert!(v["data"]["tx_info"].is_object());
                assert_eq!(v["data"]["tx_info"]["Nonce"], 9007199254740993u64);
            }
            ws.send(Message::Text(
                json!({"id":"fixture-rpc","accepted":true})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        }
    });
    let mut ws = LighterNativeClient::default()
        .websocket(&url, false)
        .await
        .unwrap();
    let tx = NativeSignedTransaction {
        tx_type: 14,
        tx_info: json!({"Nonce":9007199254740993u64,"Sig":"fixture-only"}).to_string(),
        tx_hash: None,
        message_to_sign: None,
    };
    ws.send_transaction("fixture-rpc", &tx, None).await.unwrap();
    ws.socket.receive().await.unwrap();
    ws.send_transactions("fixture-rpc", &[tx], None)
        .await
        .unwrap();
    ws.socket.receive().await.unwrap();
    server.await.unwrap();
}
