//! Converts the collector's `<local_timestamp_ns> <exchange JSON>` gzip files into
//! the normalized `data.npy` member expected by hftbacktest.

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use flate2::read::GzDecoder;
use hftbacktest::{
    backtest::data::write_npy,
    types::{
        BUY_EVENT, DEPTH_BBO_EVENT, DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT, EXCH_EVENT, Event,
        LOCAL_EVENT, SELL_EVENT, TRADE_EVENT,
    },
};
use serde_json::Value;
use zip::{ZipWriter, write::SimpleFileOptions};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Exchange {
    Binance,
    Bybit,
    Hyperliquid,
}

#[derive(Parser, Debug)]
#[command(about = "Normalize collector gzip JSON records into an hftbacktest NPZ file")]
struct Args {
    #[arg(long, value_enum)]
    exchange: Exchange,
    #[arg(long)]
    input: String,
    #[arg(long)]
    output: String,
    /// Skip malformed or unsupported messages instead of stopping.
    #[arg(long)]
    skip_invalid: bool,
}

fn number(value: Option<&Value>, field: &str) -> Result<f64> {
    let value = value.ok_or_else(|| anyhow!("missing {field}"))?;
    match value {
        Value::Number(n) => n.as_f64().ok_or_else(|| anyhow!("invalid {field}")),
        Value::String(s) => s.parse().with_context(|| format!("invalid {field}: {s}")),
        _ => bail!("invalid {field}"),
    }
}

fn integer(value: Option<&Value>, field: &str) -> Result<i64> {
    let value = value.ok_or_else(|| anyhow!("missing {field}"))?;
    match value {
        Value::Number(n) => n.as_i64().ok_or_else(|| anyhow!("invalid {field}")),
        Value::String(s) => s.parse().with_context(|| format!("invalid {field}: {s}")),
        _ => bail!("invalid {field}"),
    }
}

fn event(ev: u64, exch_ts: i64, local_ts: i64, px: f64, qty: f64) -> Event {
    Event {
        ev: ev | EXCH_EVENT | LOCAL_EVENT,
        exch_ts,
        local_ts,
        px,
        qty,
        order_id: 0,
        ival: 0,
        fval: 0.0,
    }
}

fn levels(
    out: &mut Vec<Event>,
    rows: Option<&Value>,
    side: u64,
    kind: u64,
    exch_ts: i64,
    local_ts: i64,
) -> Result<()> {
    let Some(rows) = rows.and_then(Value::as_array) else {
        return Ok(());
    };
    for row in rows {
        let pair = row
            .as_array()
            .ok_or_else(|| anyhow!("depth level is not an array"))?;
        out.push(event(
            kind | side,
            exch_ts,
            local_ts,
            number(pair.first(), "price")?,
            number(pair.get(1), "quantity")?,
        ));
    }
    Ok(())
}

fn parse_binance(root: &Value, local_ts: i64, out: &mut Vec<Event>) -> Result<bool> {
    let data = root.get("data").unwrap_or(root);
    let kind = data.get("e").and_then(Value::as_str).unwrap_or_default();
    let exch_ts = integer(
        data.get("T").or_else(|| data.get("E")),
        "exchange timestamp",
    )? * 1_000_000;
    match kind {
        "trade" | "aggTrade" => {
            let buyer = data
                .get("m")
                .and_then(Value::as_bool)
                .map(|maker| !maker)
                .unwrap_or(true);
            out.push(event(
                TRADE_EVENT | if buyer { BUY_EVENT } else { SELL_EVENT },
                exch_ts,
                local_ts,
                number(data.get("p"), "price")?,
                number(data.get("q"), "quantity")?,
            ));
        }
        "depthUpdate" => {
            levels(
                out,
                data.get("b"),
                BUY_EVENT,
                DEPTH_EVENT,
                exch_ts,
                local_ts,
            )?;
            levels(
                out,
                data.get("a"),
                SELL_EVENT,
                DEPTH_EVENT,
                exch_ts,
                local_ts,
            )?;
        }
        "bookTicker" => {
            out.push(event(
                DEPTH_BBO_EVENT | BUY_EVENT,
                exch_ts,
                local_ts,
                number(data.get("b"), "bid")?,
                number(data.get("B"), "bid quantity")?,
            ));
            out.push(event(
                DEPTH_BBO_EVENT | SELL_EVENT,
                exch_ts,
                local_ts,
                number(data.get("a"), "ask")?,
                number(data.get("A"), "ask quantity")?,
            ));
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn parse_bybit(root: &Value, local_ts: i64, out: &mut Vec<Event>) -> Result<bool> {
    let topic = root
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = root.get("data").ok_or_else(|| anyhow!("missing data"))?;
    if topic.starts_with("orderbook.") {
        let exch_ts = integer(root.get("ts"), "timestamp")? * 1_000_000;
        let kind = if root.get("type").and_then(Value::as_str) == Some("snapshot") {
            DEPTH_SNAPSHOT_EVENT
        } else {
            DEPTH_EVENT
        };
        levels(out, data.get("b"), BUY_EVENT, kind, exch_ts, local_ts)?;
        levels(out, data.get("a"), SELL_EVENT, kind, exch_ts, local_ts)?;
        return Ok(true);
    }
    if topic.starts_with("publicTrade.") {
        for trade in data
            .as_array()
            .ok_or_else(|| anyhow!("trade data is not an array"))?
        {
            let exch_ts =
                integer(trade.get("T").or_else(|| root.get("ts")), "timestamp")? * 1_000_000;
            let buy = trade.get("S").and_then(Value::as_str) == Some("Buy");
            out.push(event(
                TRADE_EVENT | if buy { BUY_EVENT } else { SELL_EVENT },
                exch_ts,
                local_ts,
                number(trade.get("p"), "price")?,
                number(trade.get("v"), "quantity")?,
            ));
        }
        return Ok(true);
    }
    Ok(false)
}

fn parse_hyperliquid(root: &Value, local_ts: i64, out: &mut Vec<Event>) -> Result<bool> {
    let channel = root
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = root.get("data").ok_or_else(|| anyhow!("missing data"))?;
    match channel {
        "trades" => {
            for trade in data
                .as_array()
                .ok_or_else(|| anyhow!("trade data is not an array"))?
            {
                let exch_ts = integer(trade.get("time"), "timestamp")? * 1_000_000;
                let buy = trade.get("side").and_then(Value::as_str) == Some("B");
                out.push(event(
                    TRADE_EVENT | if buy { BUY_EVENT } else { SELL_EVENT },
                    exch_ts,
                    local_ts,
                    number(trade.get("px"), "price")?,
                    number(trade.get("sz"), "quantity")?,
                ));
            }
        }
        "l2Book" => {
            let exch_ts = integer(data.get("time"), "timestamp")? * 1_000_000;
            let books = data
                .get("levels")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("missing levels"))?;
            for (side_no, book) in books.iter().take(2).enumerate() {
                for level in book.as_array().ok_or_else(|| anyhow!("invalid levels"))? {
                    out.push(event(
                        DEPTH_SNAPSHOT_EVENT | if side_no == 0 { BUY_EVENT } else { SELL_EVENT },
                        exch_ts,
                        local_ts,
                        number(level.get("px"), "price")?,
                        number(level.get("sz"), "quantity")?,
                    ));
                }
            }
        }
        "bbo" => {
            let exch_ts = integer(data.get("time"), "timestamp")? * 1_000_000;
            let bbo = data
                .get("bbo")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("missing bbo"))?;
            for (side_no, level) in bbo.iter().take(2).enumerate() {
                if level.is_null() {
                    continue;
                }
                out.push(event(
                    DEPTH_BBO_EVENT | if side_no == 0 { BUY_EVENT } else { SELL_EVENT },
                    exch_ts,
                    local_ts,
                    number(level.get("px"), "price")?,
                    number(level.get("sz"), "quantity")?,
                ));
            }
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn reader(path: &str) -> Result<Box<dyn BufRead>> {
    let file = File::open(path).with_context(|| format!("failed to open {path}"))?;
    if Path::new(path).extension().and_then(|s| s.to_str()) == Some("gz") {
        Ok(Box::new(BufReader::new(GzDecoder::new(file))))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut events = Vec::new();
    let mut skipped = 0usize;
    for (line_no, line) in reader(&args.input)?.lines().enumerate() {
        let line = line?;
        let parsed = (|| -> Result<bool> {
            let (local_ts, json) = line
                .split_once(' ')
                .ok_or_else(|| anyhow!("missing timestamp separator"))?;
            let local_ts: i64 = local_ts.parse().context("invalid local timestamp")?;
            let root: Value = serde_json::from_str(json).context("invalid JSON")?;
            match args.exchange {
                Exchange::Binance => parse_binance(&root, local_ts, &mut events),
                Exchange::Bybit => parse_bybit(&root, local_ts, &mut events),
                Exchange::Hyperliquid => parse_hyperliquid(&root, local_ts, &mut events),
            }
        })();
        match parsed {
            Ok(true) => {}
            Ok(false) => skipped += 1,
            Err(error) if args.skip_invalid => {
                eprintln!("skipping line {}: {error:#}", line_no + 1);
                skipped += 1;
            }
            Err(error) => return Err(error).with_context(|| format!("line {}", line_no + 1)),
        }
    }
    if events.is_empty() {
        bail!("no supported market events found");
    }
    let file =
        File::create(&args.output).with_context(|| format!("failed to create {}", args.output))?;
    let mut zip = ZipWriter::new(file);
    zip.start_file("data.npy", SimpleFileOptions::default())?;
    write_npy(&mut zip, &events)?;
    zip.finish()?;
    eprintln!(
        "wrote {} events to {}; skipped {} messages",
        events.len(),
        args.output,
        skipped
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_binance_trade_and_depth() {
        let mut events = Vec::new();
        let trade: Value = serde_json::from_str(
            r#"{"stream":"btcusdt@trade","data":{"e":"trade","T":123,"p":"100.5","q":"2","m":false}}"#,
        )
        .unwrap();
        assert!(parse_binance(&trade, 124_000_000, &mut events).unwrap());
        assert!(events[0].is(TRADE_EVENT | BUY_EVENT));
        assert_eq!(events[0].exch_ts, 123_000_000);

        let depth: Value = serde_json::from_str(
            r#"{"data":{"e":"depthUpdate","T":125,"b":[["100","1"]],"a":[["101","3"]]}}"#,
        )
        .unwrap();
        assert!(parse_binance(&depth, 126_000_000, &mut events).unwrap());
        assert!(events[1].is(DEPTH_EVENT | BUY_EVENT));
        assert!(events[2].is(DEPTH_EVENT | SELL_EVENT));
    }

    #[test]
    fn parses_bybit_snapshot() {
        let root: Value = serde_json::from_str(
            r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":123,"data":{"b":[["100","1"]],"a":[["101","2"]]}}"#,
        )
        .unwrap();
        let mut events = Vec::new();
        assert!(parse_bybit(&root, 124_000_000, &mut events).unwrap());
        assert!(events[0].is(DEPTH_SNAPSHOT_EVENT | BUY_EVENT));
        assert!(events[1].is(DEPTH_SNAPSHOT_EVENT | SELL_EVENT));
    }

    #[test]
    fn parses_hyperliquid_book() {
        let root: Value = serde_json::from_str(
            r#"{"channel":"l2Book","data":{"time":123,"levels":[[{"px":"100","sz":"1"}],[{"px":"101","sz":"2"}]]}}"#,
        )
        .unwrap();
        let mut events = Vec::new();
        assert!(parse_hyperliquid(&root, 124_000_000, &mut events).unwrap());
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].px, 101.0);
    }
}
