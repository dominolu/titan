
#[test]
fn review_reconciliation_reseeds_the_order_cumulative_baseline() {
    use hftbacktest::types::{Order, OrdType, Side, Status, TimeInForce};
    let connector = GateCrossEx::build_from("").unwrap();
    let mut baseline = Order::new(1,500000,0.1,0.01,Side::Buy,OrdType::Limit,TimeInForce::GTC);
    baseline.status = Status::PartiallyFilled;
    baseline.leaves_qty = 0.006;
    baseline.exch_timestamp = 1700000000010000000;
    connector.track_managed_order(SYMBOL,"owner_1",&baseline);
    // REST bootstrap after reconnect observes another 0.002 filled while disconnected.
    baseline.leaves_qty = 0.004;
    baseline.exch_timestamp = 1700000000020000000;
    connector.track_managed_order(SYMBOL,"owner_1",&baseline);
    let mut value=order(); value["state"]=json!("PARTIALLY_FILLED");
    value["executed_qty"]=json!("0.008"); value["executed_avg_price"]=json!("50000");
    value["update_time"]=json!("1700000000030");
    let observed=stream::update_order(&connector.orders,&brokerapi::order_info(&value).unwrap(),0.1).unwrap().unwrap();
    assert!((observed.exec_qty-0.002).abs()<1e-12,"new WS fill must exclude the 0.006 already seeded by REST, actual={}",observed.exec_qty);
}

#[test]
fn review_incremental_fill_price_is_not_the_cumulative_average() {
    let orders=Mutex::new(OrderManager::default());
    let mut value=order(); value["state"]=json!("PARTIALLY_FILLED"); value["executed_qty"]=json!("0.005"); value["executed_avg_price"]=json!("100");
    value["last_executed_qty"]=json!("0.005"); value["last_executed_price"]=json!("100");
    stream::update_order(&orders,&brokerapi::order_info(&value).unwrap(),0.1).unwrap().unwrap();
    value["state"]=json!("FILLED"); value["executed_qty"]=json!("0.01"); value["executed_avg_price"]=json!("150");
    value["last_executed_qty"]=json!("0.005"); value["last_executed_price"]=json!("200"); value["update_time"]=json!("1700000000020");
    let observed=stream::update_order(&orders,&brokerapi::order_info(&value).unwrap(),0.1).unwrap().unwrap();
    assert_eq!(observed.exec_price_tick,2000,"the second fill is 200, not its 150 cumulative average");
}

#[tokio::test]
async fn review_cancel_all_does_not_abandon_other_live_orders() {
    let mut second=order(); second["order_id"]=json!("100"); second["text"]=json!("owner_2");
    let mut first=order(); first["state"]=json!("FILLED"); first["executed_qty"]=json!("0.01");
    first["executed_avg_price"]=json!("50000");
    let (url,mut requests,server)=mock(vec![
        (200,json!([order(),second])),(200,first),
        (400,json!({"label":"TRADE_ORDER_CANNOT_CANCEL_ERROR","message":"already filled"})),
    ]).await;
    let _=client(&url).cancel_all_orders(SYMBOL).await;
    server.await.unwrap();
    let mut attempted_other=false;
    while let Some(wire)=requests.recv().await {
        if wire.starts_with("GET /api/v4/crossex/orders/100 ") || wire.starts_with("DELETE /api/v4/crossex/orders/100 ") { attempted_other=true; }
    }
    assert!(attempted_other,"a fill/cancel race on order 99 must not leave live order 100 untouched");
}

#[tokio::test]
async fn review_slow_balance_refresh_does_not_block_order_delivery() {
    use tokio::sync::oneshot;
    let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest=format!("http://{}/api/v4",listener.local_addr().unwrap());
    let (refresh_started,started)=oneshot::channel();
    let (release,wait_release)=oneshot::channel();
    let http=tokio::spawn(async move {
        let mut start=Some(refresh_started); let mut release=Some(wait_release);
        for value in [rules(),account()] {
            let (mut socket,_)=listener.accept().await.unwrap();
            let mut request=Vec::new(); let mut buf=[0;2048];
            loop { let n=socket.read(&mut buf).await.unwrap(); assert!(n>0); request.extend_from_slice(&buf[..n]); if request.windows(4).any(|w|w==b"\r\n\r\n") { break; } }
            if String::from_utf8_lossy(&request).starts_with("GET /api/v4/crossex/accounts ") {
                start.take().unwrap().send(()).unwrap(); release.take().unwrap().await.unwrap();
            }
            let body=value.to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url=format!("ws://{}",listener.local_addr().unwrap());
    let (go,wait_go)=oneshot::channel(); let (finish,wait_finish)=oneshot::channel();
    let ws_server=tokio::spawn(async move {
        let (socket,_)=listener.accept().await.unwrap(); let mut ws=accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(json!({"event":"login","result":{"code":"100000"}}).to_string().into())).await.unwrap();
        for _ in 0..3 { ws.next().await.unwrap().unwrap(); }
        for channel in ["order","position","asset"] { ws.send(Message::Text(json!({"event":"subscribe","channel":channel,"result":{"code":"100000"}}).to_string().into())).await.unwrap(); }
        wait_go.await.unwrap();
        ws.send(Message::Text(json!({"event":"subscribe","channel":"asset","time_ms":1700000000020_i64,"payload":{"coin":"USDT"},"result":{"code":"100000"}}).to_string().into())).await.unwrap();
        ws.send(Message::Text(json!({"event":"subscribe","channel":"order","payload":order(),"result":{"code":"100000"}}).to_string().into())).await.unwrap();
        wait_finish.await.unwrap(); let _=ws.close(None).await;
    });
    let mut connector=GateCrossEx::build_from(&format!("rest_url={rest:?}\nprivate_ws_url={ws_url:?}\napi_key='test'\nsecret='test'\n")).unwrap();
    connector.register_account(SYMBOL.into());
    let (ready,mut ready_rx)=mpsc::unbounded_channel(); let (delivered,mut delivered_rx)=mpsc::unbounded_channel();
    connector.run_account(direct_publish_sender(move |p| match p {
        DirectPublication::Event(PublishEvent::PrivateStreamReady)=>{let _=ready.send(());},
        DirectPublication::Account(crate::connector::AccountPublication::Order{..})=>{let _=delivered.send(());},
        _=>{},
    }));
    tokio::time::timeout(Duration::from_secs(3),ready_rx.recv()).await.unwrap().unwrap(); go.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3),started).await.unwrap().unwrap();
    let timely=tokio::time::timeout(Duration::from_millis(100),delivered_rx.recv()).await.is_ok();
    release.send(()).unwrap();
    if !timely { tokio::time::timeout(Duration::from_secs(3),delivered_rx.recv()).await.unwrap().unwrap(); }
    finish.send(()).unwrap(); drop(connector); http.await.unwrap(); ws_server.await.unwrap();
    assert!(timely,"order delivery waits for unrelated balance HTTP response while the private stream remains READY");
}
