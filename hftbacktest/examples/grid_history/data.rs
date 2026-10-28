//! Normalize official Binance USD-M archives without inventing an intrabar price path.
use anyhow::{Context, Result, bail, ensure};
use serde_json::json;
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

pub const QUOTE: u8 = 0;
pub const SELL_TRADE: u8 = 1;
pub const BUY_TRADE: u8 = 2;
pub const FUNDING: u8 = 3;
pub const NS_PER_MS: i64 = 1_000_000;
const DAY_MS: i64 = 86_400_000;

#[derive(Clone, Copy, Debug)]
pub struct MarketEvent {
    pub ts: i64,
    pub kind: u8,
    pub a: i64,
    pub b: i64,
    pub c: i64,
    pub d: i64,
}

/// Exact decimal-to-integer conversion. Extra zero decimals are allowed, rounding is not.
pub fn units(text: &str, decimals: usize) -> Result<i64> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    ensure!(
        !whole.is_empty() && whole.bytes().all(|b| b.is_ascii_digit()),
        "invalid nonnegative decimal: {text}"
    );
    ensure!(
        fraction.bytes().all(|b| b.is_ascii_digit()),
        "invalid decimal: {text}"
    );
    ensure!(
        fraction
            .get(decimals..)
            .is_none_or(|tail| tail.bytes().all(|b| b == b'0')),
        "precision loss: {text}"
    );
    let scale = 10_i64.pow(decimals as u32);
    let mut value = whole
        .parse::<i64>()?
        .checked_mul(scale)
        .context("decimal overflow")?;
    let digits = &fraction[..fraction.len().min(decimals)];
    if !digits.is_empty() {
        value = value
            .checked_add(digits.parse::<i64>()? * 10_i64.pow((decimals - digits.len()) as u32))
            .context("decimal overflow")?;
    }
    Ok(value)
}

fn each_row(path: &Path, mut visit: impl FnMut(&[&str]) -> Result<()>) -> Result<u64> {
    let mut reader = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut line = String::new();
    let mut count = 0;
    while reader.read_line(&mut line)? != 0 {
        let row: Vec<_> = line.trim_end().split(',').collect();
        if count > 0
            || row
                .first()
                .is_some_and(|first| first.as_bytes().first().is_some_and(u8::is_ascii_digit))
        {
            visit(&row).with_context(|| format!("{} row {}", path.display(), count + 1))?;
            count += 1;
        }
        line.clear();
    }
    ensure!(count > 0, "empty input {}", path.display());
    Ok(count)
}

pub fn prepare(trades: &Path, quotes: &Path, funding: &Path, output: &Path) -> Result<()> {
    let mut events = Vec::new();
    let (mut last_id, mut id_gaps, mut last_ts) = (None, 0_u64, 0_i64);
    let mut max_trade_gap = 0;
    let mut trade_low = i64::MAX;
    let mut trade_high = 0;
    let mut sub_tick_trades = 0_u64;
    let trade_rows = each_row(trades, |row| {
        ensure!(row.len() >= 7, "aggTrades needs 7 columns");
        let id = row[0].parse::<u64>()?;
        let ts = row[5].parse::<i64>()?;
        if let Some(previous) = last_id {
            ensure!(id > previous, "aggregate IDs must increase");
            id_gaps += id - previous - 1;
            ensure!(ts >= last_ts, "trade time regression");
            max_trade_gap = max_trade_gap.max(ts - last_ts);
        }
        last_id = Some(id);
        last_ts = ts;
        let (price, qty) = (units(row[1], 2)?, units(row[2], 3)?);
        sub_tick_trades += u64::from(price % 10 != 0);
        ensure!(price > 0 && qty > 0, "nonpositive trade");
        let kind = match row[6] {
            "true" => SELL_TRADE,
            "false" => BUY_TRADE,
            _ => bail!("invalid maker flag"),
        };
        events.push(MarketEvent {
            ts: ts * NS_PER_MS,
            kind,
            a: price,
            b: qty,
            c: 0,
            d: 0,
        });
        trade_low = trade_low.min(price);
        trade_high = trade_high.max(price);
        Ok(())
    })?;
    let first_trade = events[0];
    let last_trade = events[events.len() - 1];
    let day_start = first_trade.ts / NS_PER_MS / DAY_MS * DAY_MS;
    let day_end = day_start + DAY_MS;
    ensure!(
        last_trade.ts / NS_PER_MS < day_end,
        "input spans multiple UTC dates"
    );
    ensure!(
        first_trade.ts / NS_PER_MS - day_start < 30000
            && day_end - last_trade.ts / NS_PER_MS < 30000,
        "incomplete UTC day coverage"
    );
    ensure!(id_gaps == 0, "missing aggregate trade IDs: {id_gaps}");
    // Preserve every available BBO for matching and risk metrics. Only strategy tick callbacks
    // are rate-limited in the runner. Do not lose intra-bucket drawdown or executable liquidity.
    let mut raw_quotes = Vec::new();
    let mut invalid_quotes = 0_u64;
    let mut sub_tick_quotes = 0_u64;
    let quote_rows = each_row(quotes, |row| {
        ensure!(row.len() >= 7, "bookTicker needs 7 columns");
        let id = row[0].parse::<u64>()?;
        let ts = row[5].parse::<i64>()?.max(row[6].parse()?);
        if !(day_start..day_end).contains(&ts) {
            return Ok(());
        }
        let (bid, bid_qty, ask, ask_qty) = (
            units(row[1], 2)?,
            units(row[2], 3)?,
            units(row[3], 2)?,
            units(row[4], 3)?,
        );
        sub_tick_quotes += u64::from(bid % 10 != 0 || ask % 10 != 0);
        if bid <= 0 || ask <= bid || bid_qty == 0 || ask_qty == 0 {
            invalid_quotes += 1;
            return Ok(());
        }
        let event = MarketEvent {
            ts: ts * NS_PER_MS,
            kind: QUOTE,
            a: bid,
            b: bid_qty,
            c: ask,
            d: ask_qty,
        };
        raw_quotes.push((id, event));
        Ok(())
    })?;
    ensure!(!raw_quotes.is_empty(), "missing quotes");
    raw_quotes.sort_by_key(|(id, event)| (event.ts, *id));
    let mut quote_watermark = 0;
    let mut stale_quotes = 0_u64;
    raw_quotes.retain(|(id, _)| {
        if *id <= quote_watermark {
            stale_quotes += 1;
            false
        } else {
            quote_watermark = *id;
            true
        }
    });
    let quote_events: Vec<_> = raw_quotes.into_iter().map(|(_, event)| event).collect();
    let first_quote = quote_events[0];
    let max_quote_gap = quote_events
        .windows(2)
        .map(|pair| pair[1].ts - pair[0].ts)
        .max()
        .unwrap_or(0);
    events.extend(quote_events.iter());
    let mut funding_count = 0;
    each_row(funding, |row| {
        ensure!(row.len() >= 3, "fundingRate needs 3 columns");
        let ts = row[0].parse::<i64>()?;
        if (day_start..day_end).contains(&ts) {
            let rate = row[2].parse::<f64>()?;
            ensure!(rate.is_finite() && rate.abs() < 0.1, "invalid funding rate");
            events.push(MarketEvent {
                ts: ts * NS_PER_MS,
                kind: FUNDING,
                a: rate.to_bits() as i64,
                b: 0,
                c: 0,
                d: 0,
            });
            funding_count += 1;
        }
        Ok(())
    })?;
    ensure!(
        funding_count == 3,
        "expected three historical funding events, got {funding_count}"
    );
    // For equal timestamps, trades precede funding and then quotes. A new quote/order cannot
    // consume a historical print at the same timestamp as its first visibility.
    events.sort_by_key(|event| {
        (
            event.ts,
            match event.kind {
                QUOTE => 2,
                FUNDING => 1,
                _ => 0,
            },
        )
    });
    events.retain(|event| event.ts >= first_quote.ts);
    let mut writer = BufWriter::new(File::create(output)?);
    writer.write_all(b"GHB3")?;
    writer.write_all(&(events.len() as u64).to_le_bytes())?;
    for event in &events {
        for value in [
            event.ts,
            i64::from(event.kind),
            event.a,
            event.b,
            event.c,
            event.d,
        ] {
            writer.write_all(&value.to_le_bytes())?;
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let parquet = output.with_extension("parquet");
    titan_persistence::columnar::write_tape(
        &parquet,
        events
            .iter()
            .map(|e| [e.ts, i64::from(e.kind), e.a, e.b, e.c, e.d]),
    )
    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let summary = json!({
        "schema_version": 1, "trades": trades, "quotes": quotes, "funding": funding,
        "trade_rows": trade_rows, "quote_rows": quote_rows, "invalid_quotes_omitted": invalid_quotes,
        "strategy_tick_min_interval_ms": 100, "accepted_quotes": quote_events.len(), "stale_quotes_omitted": stale_quotes, "events": events.len(),
        "trade_id_gaps": id_gaps, "max_trade_gap_ms": max_trade_gap, "max_quote_gap_ns": max_quote_gap,
        "start_ns": first_quote.ts, "end_ns": last_trade.ts,
        "start_mid_ticks": (first_quote.a + first_quote.c) as f64 / 20.0,
        "first_trade_cents": first_trade.a, "last_trade_cents": last_trade.a,
        "low_cents": trade_low, "high_cents": trade_high,
        "sub_order_tick_trades": sub_tick_trades, "sub_order_tick_quotes": sub_tick_quotes,
        "input_price_unit_usdt": 0.01, "order_tick_usdt": 0.1,
        "market_return_pct": (last_trade.a as f64 / first_trade.a as f64 - 1.0) * 100.0,
        "funding_events_in_utc_day": funding_count,"parquet":parquet,"parquet_schema":"titan_integer_tape_v1",
    });
    std::fs::write(
        output.with_extension("json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{}", serde_json::to_string(&summary)?);
    Ok(())
}

pub fn scan(path: &Path, mut visit: impl FnMut(MarketEvent) -> Result<()>) -> Result<u64> {
    if path.extension().is_some_and(|e| e == "parquet") {
        let mut last = 0;
        return titan_persistence::columnar::scan_tape(path, |v| {
            if v[0] < last || !(0..=3).contains(&v[1]) {
                return Err(titan_persistence::failure(
                    "unordered/invalid history event",
                ));
            }
            last = v[0];
            visit(MarketEvent {
                ts: v[0],
                kind: v[1] as u8,
                a: v[2],
                b: v[3],
                c: v[4],
                d: v[5],
            })
            .map_err(|e| titan_persistence::failure(&e.to_string()))
        })
        .map_err(|e| anyhow::anyhow!(e.to_string()));
    }
    let mut reader = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut header = [0_u8; 12];
    reader.read_exact(&mut header)?;
    ensure!(&header[..4] == b"GHB3", "invalid history cache magic");
    let count = u64::from_le_bytes(header[4..].try_into()?);
    ensure!(count <= 100_000_000, "history cache too large");
    ensure!(
        reader.get_ref().metadata()?.len() == 12 + count * 48,
        "history cache length mismatch"
    );
    let mut bytes = [0_u8; 48];
    let mut last_ts = 0;
    for _ in 0..count {
        reader.read_exact(&mut bytes)?;
        let values: [i64; 6] = std::array::from_fn(|i| {
            i64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap())
        });
        ensure!(
            values[0] >= last_ts && (0..=3).contains(&values[1]),
            "unordered/invalid history event"
        );
        last_ts = values[0];
        visit(MarketEvent {
            ts: values[0],
            kind: values[1] as u8,
            a: values[2],
            b: values[3],
            c: values[4],
            d: values[5],
        })?;
    }
    Ok(count)
}
#[cfg(test)]
pub fn read(path: &Path) -> Result<Vec<MarketEvent>> {
    let mut events = Vec::new();
    scan(path, |e| {
        events.push(e);
        Ok(())
    })?;
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_units_are_exact_and_reject_silent_rounding() {
        assert_eq!(units("26000.10000000", 1).unwrap(), 260001);
        assert_eq!(units("0.01300000", 3).unwrap(), 13);
        for bad in ["0.0001", "NaN", "-1", "1e3"] {
            assert!(units(bad, 3).is_err());
        }
    }
    #[test]
    fn binary_and_parquet_tapes_preserve_exact_integer_units_and_source_order() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("tape.bin");
        let parquet = dir.path().join("tape.parquet");
        let rows = [
            [100_i64, 0, 2600001, 13, 2600009, 17],
            [100, 2, 2600003, 1, 0, 0],
            [101, 3, 0.0001_f64.to_bits() as i64, 0, 0, 0],
        ];
        let mut file = File::create(&binary).unwrap();
        file.write_all(b"GHB3").unwrap();
        file.write_all(&(rows.len() as u64).to_le_bytes()).unwrap();
        for row in rows {
            for value in row {
                file.write_all(&value.to_le_bytes()).unwrap();
            }
        }
        drop(file);
        titan_persistence::columnar::write_tape(&parquet, rows.into_iter()).unwrap();
        let a = read(&binary).unwrap();
        let b = read(&parquet).unwrap();
        assert_eq!(a.len(), b.len());
        for (a, b) in a.into_iter().zip(b) {
            assert_eq!(
                (a.ts, a.kind, a.a, a.b, a.c, a.d),
                (b.ts, b.kind, b.a, b.b, b.c, b.d)
            );
        }
    }
}
