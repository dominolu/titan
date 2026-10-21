//! Public L2 latency measurement. No credentials, account streams or order APIs are used.
//! Raw frame arrival and the production connector's direct callback use separate connections.
use anyhow::{Context, Result, ensure};
use chrono::Utc;
use connector::{
    connector::{
        Connector, ConnectorBuilder, DirectPublication, NativeMarketBatch, PublishEvent,
        direct_publish_sender,
    },
    gatecrossex::GateCrossEx,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use titan_market_service::MarketDataKind;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const URL: &str = "wss://api.gateio.ws/ws/crossex/public";
const GATE: &str = "GATE_FUTURE_BTC_USDT";
const SYMBOLS: [&str; 3] = [GATE, "BINANCE_FUTURE_BTC_USDT", "OKX_FUTURE_BTC_USDT"];
const WARMUP: u64 = 10;

#[derive(Clone)]
struct Sample {
    path: &'static str,
    symbol: String,
    observed_ns: i64,
    elapsed_ns: u128,
    source_ms: i64,
    push_ms: Option<i64>,
    connector_receive_ns: Option<i64>,
    epoch: u64,
    bids: usize,
    asks: usize,
}
fn now_ns() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .expect("current UTC nanoseconds")
}
fn stats(mut values: Vec<f64>) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_by(f64::total_cmp);
    let n = values.len();
    let p = |q: f64| {
        values[((q * n as f64).ceil() as usize)
            .saturating_sub(1)
            .min(n - 1)]
    };
    json!({"n":n,"min":values[0],"mean":values.iter().sum::<f64>()/n as f64,"p50":p(0.50),"p90":p(0.90),"p95":p(0.95),"p99":p(0.99),"max":values[n-1],"negative":values.iter().filter(|v| **v<0.0).count()})
}
async fn raw(start: Instant, end: Instant) -> Result<(Vec<Sample>, Vec<f64>, Vec<Value>, usize)> {
    let (mut ws, _) = tokio::time::timeout(Duration::from_secs(10), connect_async(URL)).await??;
    ws.send(Message::Text(json!({"time":Utc::now().timestamp(),"channel":"order_book_5","event":"subscribe","payload":SYMBOLS}).to_string().into())).await?;
    let mut ping = tokio::time::interval(Duration::from_secs(10));
    let mut pending = BTreeMap::new();
    let mut rtts = Vec::new();
    let mut samples = Vec::with_capacity(15000);
    let mut first_frames = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut invalid = 0;
    loop {
        tokio::select! {
            _=tokio::time::sleep_until(end.into())=>break,
            _=ping.tick()=>{
                let token=(start.elapsed().as_nanos() as u64).to_be_bytes().to_vec();
                pending.insert(token.clone(),Instant::now());
                ws.send(Message::Ping(token.into())).await?;
            }
            frame=ws.next()=>{
                let observed_ns=now_ns();
                let elapsed_ns=start.elapsed().as_nanos();
                let frame=frame.context("raw socket closed")??;
                match frame {
                    Message::Text(text)=>{
                        let v:Value=serde_json::from_str(&text)?;
                        ensure!(v.get("error").is_none_or(Value::is_null),"server error: {}",v["error"]);
                        if v["event"]!="update" || v["channel"]!="order_book_5" {continue;}
                        let result=&v["result"];
                        let symbol=result["s"].as_str().context("L2 symbol missing")?;
                        ensure!(SYMBOLS.contains(&symbol),"unexpected symbol");
                        if seen.insert(symbol.to_owned()) {first_frames.push(v.clone());}
                        let source_ms=result["ts"].as_i64().context("L2 source timestamp missing")?;
                        let bids=result["b"].as_array().context("bids missing")?.len();
                        let asks=result["a"].as_array().context("asks missing")?.len();
                        if !(1..=5).contains(&bids) || !(1..=5).contains(&asks) || source_ms<1_000_000_000_000 {invalid+=1; continue;}
                        if elapsed_ns>=u128::from(WARMUP)*1_000_000_000 {
                            samples.push(Sample{path:"raw_socket",symbol:symbol.into(),observed_ns,elapsed_ns,source_ms,push_ms:v["time_ms"].as_i64(),connector_receive_ns:None,epoch:0,bids,asks});
                        }
                    }
                    Message::Ping(p)=>ws.send(Message::Pong(p)).await?,
                    Message::Pong(p)=>{if let Some(sent)=pending.remove(p.as_ref()) && start.elapsed().as_secs()>=WARMUP {rtts.push(sent.elapsed().as_secs_f64()*1000.0);}}
                    Message::Close(_)=>anyhow::bail!("raw server closed the connection"),
                    _=>{}
                }
            }
        }
    }
    let _ = ws.close(None).await;
    Ok((samples, rtts, first_frames, invalid))
}
fn summarize(samples: &[Sample]) -> Vec<Value> {
    let mut grouped: BTreeMap<(&str, &str), Vec<&Sample>> = BTreeMap::new();
    for sample in samples {
        grouped
            .entry((sample.path, &sample.symbol))
            .or_default()
            .push(sample);
    }
    grouped.into_iter().map(|((path,symbol),rows)| {
        let age=rows.iter().map(|v|v.observed_ns as f64/1e6-v.source_ms as f64).collect();
        let push_age=rows.iter().filter_map(|v|v.push_ms.map(|p|v.observed_ns as f64/1e6-p as f64)).collect();
        let relay=rows.iter().filter_map(|v|v.push_ms.map(|p|(p-v.source_ms) as f64)).collect();
        let post_receive=rows.iter().filter_map(|v|v.connector_receive_ns.map(|p|(v.observed_ns-p) as f64/1e3)).collect();
        let intervals=rows.windows(2).map(|v|(v[1].elapsed_ns-v[0].elapsed_ns) as f64/1e6).collect();
        let regressed=rows.windows(2).filter(|v|v[1].source_ms<v[0].source_ms).count();
        let repeated=rows.windows(2).filter(|v|v[1].source_ms==v[0].source_ms).count();
        let epochs=rows.iter().map(|v|v.epoch).collect::<std::collections::BTreeSet<_>>();
        json!({"path":path,"symbol":symbol,"samples":rows.len(),"source_age_ms":stats(age),"push_age_ms":stats(push_age),"push_minus_source_ms":stats(relay),"post_receive_to_callback_us":stats(post_receive),"interarrival_ms":stats(intervals),"source_timestamp_regressions":regressed,"source_timestamp_repeats":repeated,"epochs":epochs})
    }).collect()
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(std::io::stderr)
        .try_init();
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        args.len() == 3,
        "usage: gate_crossex_l2_latency_probe SECONDS OUTPUT_DIR"
    );
    let seconds: u64 = args[1].parse()?;
    ensure!(
        (30..=600).contains(&seconds),
        "duration must be 30..600 seconds"
    );
    let output = PathBuf::from(&args[2]);
    fs::create_dir_all(&output)?;
    let start = Instant::now();
    let wall_start = now_ns();
    let end = start + Duration::from_secs(WARMUP + seconds);
    let rows = Arc::new(Mutex::new(Vec::with_capacity(5000)));
    let callback_rows = rows.clone();
    let errors = Arc::new(AtomicUsize::new(0));
    let callback_errors = errors.clone();
    let invalid = Arc::new(AtomicUsize::new(0));
    let callback_invalid = invalid.clone();
    let mut connector = GateCrossEx::build_from("depth_levels = 5\n")?;
    let raw = tokio::spawn(raw(start, end));
    connector.subscribe_market_data(GATE.into(), vec![MarketDataKind::Depth]);
    connector.run_market_data(direct_publish_sender(move |publication| {
        let observed_ns = now_ns();
        let elapsed_ns = start.elapsed().as_nanos();
        match publication {
            DirectPublication::NativeMarket(NativeMarketBatch::Depth {
                symbol,
                exchange_ts,
                receive_ts,
                stream,
                bids,
                asks,
            }) => {
                if elapsed_ns < u128::from(WARMUP) * 1_000_000_000 {
                    return;
                }
                if exchange_ts <= 0 || bids.is_empty() || asks.is_empty() {
                    callback_invalid.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                callback_rows.lock().unwrap().push(Sample {
                    path: "connector_callback",
                    symbol: symbol.into(),
                    observed_ns,
                    elapsed_ns,
                    source_ms: exchange_ts / 1_000_000,
                    push_ms: None,
                    connector_receive_ns: Some(receive_ts),
                    epoch: stream.epoch,
                    bids: bids.len(),
                    asks: asks.len(),
                });
            }
            DirectPublication::Event(PublishEvent::ConnectorError(_)) => {
                callback_errors.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }));
    loop {
        tokio::time::sleep_until((Instant::now() + Duration::from_secs(30)).min(end).into()).await;
        eprintln!(
            "{}",
            json!({"progress_seconds":start.elapsed().as_secs(),"connector_samples":rows.lock().unwrap().len(),"connector_errors":errors.load(Ordering::Relaxed)})
        );
        if Instant::now() >= end {
            break;
        }
    }
    drop(connector);
    let wall_end = now_ns();
    let elapsed_ns = start.elapsed().as_nanos();
    let mut rows = rows.lock().unwrap().clone();
    let (mut raw_rows, rtts, first_frames, raw_invalid) = raw.await??;
    rows.append(&mut raw_rows);
    let mut csv = BufWriter::new(fs::File::create(output.join("samples.csv"))?);
    writeln!(
        csv,
        "path,symbol,observed_ns,monotonic_elapsed_ns,source_ms,push_ms,connector_receive_ns,epoch,bids,asks"
    )?;
    for v in &rows {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{}",
            v.path,
            v.symbol,
            v.observed_ns,
            v.elapsed_ns,
            v.source_ms,
            v.push_ms.map(|v| v.to_string()).unwrap_or_default(),
            v.connector_receive_ns
                .map(|v| v.to_string())
                .unwrap_or_default(),
            v.epoch,
            v.bids,
            v.asks
        )?;
    }
    csv.flush()?;
    let summary = json!({"endpoint":URL,"channel":"order_book_5","production_connector_symbol":GATE,"account_used":false,"warmup_seconds":WARMUP,"measurement_seconds":seconds,"wall_start_ns":wall_start,"wall_end_ns":wall_end,"wall_minus_monotonic_elapsed_ms":((wall_end-wall_start) as f64-elapsed_ns as f64)/1e6,"connector_errors":errors.load(Ordering::Relaxed),"connector_invalid":invalid.load(Ordering::Relaxed),"raw_invalid":raw_invalid,"ping_pong_rtt_ms":stats(rtts),"series":summarize(&rows)});
    fs::write(
        output.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    fs::write(
        output.join("first-frames.json"),
        serde_json::to_vec_pretty(&first_frames)?,
    )?;
    println!("{}", summary);
    ensure!(
        errors.load(Ordering::Relaxed) == 0,
        "connector interrupted during measurement"
    );
    ensure!(
        rows.iter().any(|v| v.path == "connector_callback"),
        "production connector received no depth"
    );
    for symbol in SYMBOLS {
        ensure!(
            rows.iter()
                .any(|v| v.path == "raw_socket" && v.symbol == symbol),
            "missing raw symbol: {symbol}"
        );
    }
    Ok(())
}
