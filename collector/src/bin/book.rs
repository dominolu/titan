use std::{
    collections::VecDeque,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use chrono::Utc;
use clap::{Parser, Subcommand};
use collector::book::{
    BlockKind, Change, DepthMessage, Feed, FeedMessage, GapMessage, GapReason, InstrumentType,
    Ladder, Level, MarketConfig, RecorderConfig, RecorderSettings, SegmentReader, Side,
    SnapshotMessage, archive_daily, decimal_steps, decode_data_payload, next_daily_archive,
    recover_segment, run_market,
};
use collector::file::{AuxRecord, AuxSource, OpenInterestUnit, TradeRecord, TradeSide};
use serde::Serialize;
use serde_json::Value;
use titan_market_service::MarketDataKind;
use tokio::{
    sync::watch,
    time::{Instant, interval_at},
};

use connector::transport::{self, Heartbeat, WsTarget};

/// One decoded text frame as the transport hands it over.
type FrameText = tokio_tungstenite::tungstenite::Utf8Bytes;

/// Frames a connection may queue for its recorder before the transport backs the socket up.
const CONNECTION_FRAMES: usize = 8192;

/// The market feed a recorder market reads from, dispatched by venue.
enum MarketFeed {
    Binance(Box<BinanceConnectorFeed>),
    Bybit(BybitFeed),
    Okx(OkxFeed),
    Hyperliquid(HyperliquidFeed),
}

impl Feed for MarketFeed {
    async fn next(&mut self) -> Option<FeedMessage> {
        match self {
            Self::Binance(feed) => feed.next().await,
            Self::Bybit(feed) => feed.next().await,
            Self::Okx(feed) => feed.next().await,
            Self::Hyperliquid(feed) => feed.next().await,
        }
    }
}

struct OkxFeed {
    /// Frames from the transport, which owns the connection and its reconnection.
    frames: tokio::sync::mpsc::Receiver<(chrono::DateTime<chrono::Utc>, FrameText)>,
    /// Kept alive so the transport stops when this feed does.
    _connection: tokio::task::JoinHandle<()>,
    market: MarketConfig,
    cancel: watch::Receiver<bool>,
    pending: VecDeque<FeedMessage>,
    previous_seq: Option<u64>,
    /// The transport reconnected: the next frame starts a fresh stream.
    reconnected: bool,
    /// Waiting for the snapshot that restarts a stream after a sequence break.
    awaiting_snapshot: bool,
}

impl OkxFeed {
    async fn connect(market: MarketConfig, cancel: watch::Receiver<bool>) -> anyhow::Result<Self> {
        if !market.symbol.contains('-')
            || !market
                .symbol
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            anyhow::bail!("OKX symbol must be an instrument id");
        }
        let mut args = vec![
            serde_json::json!({"channel":"books","instId":market.symbol}),
            serde_json::json!({"channel":"trades","instId":market.symbol}),
        ];
        if market.instrument != InstrumentType::Spot {
            let inst_type = if market.instrument == InstrumentType::Future {
                "FUTURES"
            } else {
                "SWAP"
            };
            args.extend([
                serde_json::json!({"channel":"open-interest","instId":market.symbol}),
                serde_json::json!({"channel":"funding-rate","instId":market.symbol}),
                serde_json::json!({"channel":"liquidation-orders","instType":inst_type}),
            ]);
        }
        let (tx, frames) = tokio::sync::mpsc::channel(CONNECTION_FRAMES);
        let target = WsTarget::new(
            "wss://ws.okx.com:8443/ws/v5/public".to_owned(),
            Heartbeat {
                check_interval: std::time::Duration::from_secs(5),
                timeout: std::time::Duration::from_secs(30),
            },
        )
        .subscribing(vec![
            serde_json::json!({"op":"subscribe","args":args}).to_string(),
        ]);
        let connection = tokio::spawn(transport::keep_connection(target, tx));
        Ok(Self {
            frames,
            _connection: connection,
            market,
            cancel,
            pending: VecDeque::new(),
            previous_seq: None,
            reconnected: false,
            awaiting_snapshot: false,
        })
    }

    fn parse(&mut self, text: &str) -> Option<FeedMessage> {
        let root: Value = serde_json::from_str(text).ok()?;
        let channel = root.get("arg")?.get("channel")?.as_str()?;
        let data = root.get("data")?.as_array()?;
        let local_ts_ns = now_ns();
        if channel == "books" {
            let action = root.get("action").and_then(Value::as_str)?;
            for update in data {
                let ts = update
                    .get("ts")
                    .and_then(Value::as_str)
                    .and_then(|v| v.parse::<i64>().ok())
                    .and_then(ms_to_ns)
                    .unwrap_or(local_ts_ns);
                let seq = update
                    .get("seqId")
                    .and_then(Value::as_i64)
                    .and_then(|v| u64::try_from(v).ok())?;
                if action == "snapshot" {
                    let snapshot = SnapshotMessage {
                        exch_ts_ns: ts,
                        local_ts_ns,
                        bids: parse_levels(update.get("bids"), &self.market, Side::Bid).ok()?,
                        asks: parse_levels(update.get("asks"), &self.market, Side::Ask).ok()?,
                        last_seq: Some(seq),
                    };
                    if let Some(previous) = self.previous_seq {
                        self.pending.push_back(FeedMessage::Gap(GapMessage {
                            start_local_ns: local_ts_ns,
                            start_exch_ns: ts,
                            end_local_ns: local_ts_ns,
                            end_exch_ns: ts,
                            reason: GapReason::Resync,
                            last_seq: previous,
                            next_seq: seq,
                            detail: "OKX orderbook snapshot reset".into(),
                        }));
                    }
                    self.previous_seq = Some(seq);
                    self.pending.push_back(FeedMessage::Snapshot(snapshot));
                    continue;
                }
                let previous = self.previous_seq?;
                let previous_in_message = update
                    .get("prevSeqId")
                    .and_then(Value::as_i64)
                    .and_then(|v| u64::try_from(v).ok())?;
                if seq == previous && previous_in_message == previous {
                    continue;
                }
                if previous_in_message != previous || seq < previous {
                    self.pending.push_back(FeedMessage::Gap(GapMessage {
                        start_local_ns: local_ts_ns,
                        start_exch_ns: ts,
                        end_local_ns: local_ts_ns,
                        end_exch_ns: ts,
                        reason: GapReason::SequenceBreak,
                        last_seq: previous,
                        next_seq: seq,
                        detail: "OKX sequence discontinuity".into(),
                    }));
                    // The stream restarts from the next snapshot the venue sends; reconnection is
                    // the transport's business, not a reason to drop this connection here.
                    self.previous_seq = None;
                    self.awaiting_snapshot = true;
                    return self.pending.pop_front();
                }
                let mut changes =
                    parse_changes(update.get("bids"), &self.market, Side::Bid).ok()?;
                changes.extend(parse_changes(update.get("asks"), &self.market, Side::Ask).ok()?);
                self.previous_seq = Some(seq);
                self.pending.push_back(FeedMessage::Depth(DepthMessage {
                    exch_ts_ns: ts,
                    local_ts_ns,
                    first_prev_seq: Some(previous),
                    first_seq: previous.checked_add(1),
                    last_seq: seq,
                    changes,
                }));
            }
            return self.pending.pop_front();
        }
        for event in data {
            match channel {
                "trades" => self.pending.push_back(FeedMessage::Trade(TradeRecord {
                    exch_ts_ns: event
                        .get("ts")
                        .and_then(Value::as_str)
                        .and_then(|v| v.parse::<i64>().ok())
                        .and_then(ms_to_ns)
                        .unwrap_or(local_ts_ns),
                    local_ts_ns,
                    side: match event.get("side").and_then(Value::as_str) {
                        Some("buy") => TradeSide::Buy,
                        Some("sell") => TradeSide::Sell,
                        _ => TradeSide::Unknown,
                    },
                    price: event.get("px")?.as_str()?.to_owned(),
                    qty: event.get("sz")?.as_str()?.to_owned(),
                    trade_id: event
                        .get("tradeId")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                })),
                "open-interest" => {
                    let value = event
                        .get("oiCcy")
                        .and_then(Value::as_str)
                        .or_else(|| event.get("oi").and_then(Value::as_str))?;
                    let unit = if event.get("oiCcy").and_then(Value::as_str).is_some() {
                        OpenInterestUnit::Base
                    } else {
                        OpenInterestUnit::Contracts
                    };
                    self.pending.push_back(FeedMessage::Aux(AuxRecord::Oi {
                        exch_ts_ns: event
                            .get("ts")
                            .and_then(Value::as_str)
                            .and_then(|v| v.parse::<i64>().ok())
                            .and_then(ms_to_ns)
                            .unwrap_or(local_ts_ns),
                        local_ts_ns,
                        value: value.to_owned(),
                        unit,
                        source: AuxSource::Stream,
                    }));
                }
                "funding-rate" => {
                    let next = event
                        .get("nextFundingTime")
                        .and_then(Value::as_str)
                        .and_then(|v| v.parse::<i64>().ok())
                        .and_then(ms_to_ns);
                    self.pending.push_back(FeedMessage::Aux(AuxRecord::Funding {
                        exch_ts_ns: event
                            .get("ts")
                            .and_then(Value::as_str)
                            .and_then(|v| v.parse::<i64>().ok())
                            .and_then(ms_to_ns)
                            .unwrap_or(local_ts_ns),
                        local_ts_ns,
                        rate: event.get("fundingRate")?.as_str()?.to_owned(),
                        next_funding_ts_ns: next,
                        mark_px: None,
                        index_px: None,
                        source: AuxSource::Stream,
                    }));
                }
                "liquidation-orders" => {
                    if event.get("instId").and_then(Value::as_str)
                        != Some(self.market.symbol.as_str())
                    {
                        continue;
                    }
                    for detail in event.get("details").and_then(Value::as_array)? {
                        self.pending
                            .push_back(FeedMessage::Aux(AuxRecord::Liquidation {
                                exch_ts_ns: detail
                                    .get("ts")
                                    .and_then(Value::as_str)
                                    .and_then(|v| v.parse::<i64>().ok())
                                    .and_then(ms_to_ns)
                                    .unwrap_or(local_ts_ns),
                                local_ts_ns,
                                side: match detail.get("side").and_then(Value::as_str) {
                                    Some("buy") => Some(TradeSide::Buy),
                                    Some("sell") => Some(TradeSide::Sell),
                                    _ => None,
                                },
                                price: detail
                                    .get("bkPx")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                                qty: detail.get("sz").and_then(Value::as_str).map(str::to_owned),
                                notional: None,
                                order_id: None,
                                status: Some("liquidated".into()),
                                source: AuxSource::Stream,
                            }));
                    }
                }
                _ => {}
            }
        }
        self.pending.pop_front()
    }
}

impl Feed for OkxFeed {
    async fn next(&mut self) -> Option<FeedMessage> {
        loop {
            if let Some(message) = self.pending.pop_front() {
                return Some(message);
            }
            if *self.cancel.borrow() {
                return None;
            }
            if self.reconnected {
                self.reconnected = false;
                self.previous_seq = None;
                let now = now_ns();
                self.pending.push_back(FeedMessage::Gap(GapMessage {
                    start_local_ns: now,
                    start_exch_ns: now,
                    end_local_ns: now,
                    end_exch_ns: now,
                    reason: GapReason::Disconnect,
                    last_seq: 0,
                    next_seq: 0,
                    detail: "OKX websocket reconnected".into(),
                }));
                continue;
            }
            tokio::select! {
                changed = self.cancel.changed() => if changed.is_err() || *self.cancel.borrow() { return None; },
                frame = self.frames.recv() => match frame {
                    // OKX answers the application-level "ping" frame itself.
                    Some((_, text)) if text.as_str() == "ping" => {}
                    Some((_, text)) => if let Some(value) = self.parse(text.as_str()) { return Some(value); },
                    None => {
                        self.reconnected = true;
                    }
                }
            }
        }
    }
}

struct BybitFeed {
    /// Frames from the transport, which owns the connection and its reconnection.
    frames: tokio::sync::mpsc::Receiver<(chrono::DateTime<chrono::Utc>, FrameText)>,
    _connection: tokio::task::JoinHandle<()>,
    market: MarketConfig,
    pending: VecDeque<FeedMessage>,
    cancel: watch::Receiver<bool>,
    previous_seq: Option<u64>,
    /// Waiting for the snapshot that restarts a stream after a sequence break.
    awaiting_snapshot: bool,
    reconnected: bool,
}

impl BybitFeed {
    async fn connect(market: MarketConfig, cancel: watch::Receiver<bool>) -> anyhow::Result<Self> {
        let category = bybit_category(&market)?;
        let depth = match market.depth_available {
            0..=1 => 1,
            2..=50 => 50,
            51..=200 => 200,
            _ => 500,
        };
        let endpoint = format!("wss://stream.bybit.com/v5/public/{category}");
        let mut args = vec![
            format!("orderbook.{depth}.{}", market.symbol),
            format!("publicTrade.{}", market.symbol),
        ];
        if market.instrument != InstrumentType::Spot {
            args.push(format!("tickers.{}", market.symbol));
            args.push(format!("allLiquidation.{}", market.symbol));
        }
        let (tx, frames) = tokio::sync::mpsc::channel(CONNECTION_FRAMES);
        let target = WsTarget::new(
            endpoint,
            Heartbeat {
                check_interval: std::time::Duration::from_secs(20),
                timeout: std::time::Duration::from_secs(60),
            },
        )
        .subscribing(vec![
            serde_json::json!({"op":"subscribe","args":args}).to_string(),
        ]);
        let connection = tokio::spawn(transport::keep_connection(target, tx));
        Ok(Self {
            frames,
            _connection: connection,
            market,
            pending: VecDeque::new(),
            cancel,
            previous_seq: None,
            awaiting_snapshot: false,
            reconnected: false,
        })
    }

    async fn parse(&mut self, text: &str) -> Option<FeedMessage> {
        let root: Value = serde_json::from_str(text).ok()?;
        let topic = root.get("topic")?.as_str()?;
        let data = root.get("data")?;
        let local_ts_ns = now_ns();
        let exch_ts_ns = root
            .get("ts")
            .and_then(Value::as_i64)
            .and_then(ms_to_ns)
            .unwrap_or(local_ts_ns);
        if topic.starts_with("orderbook.") {
            let bids = parse_changes(data.get("b"), &self.market, Side::Bid).ok()?;
            let asks = parse_changes(data.get("a"), &self.market, Side::Ask).ok()?;
            let update = data.get("u").and_then(Value::as_u64)?;
            if root.get("type").and_then(Value::as_str) == Some("snapshot") {
                let snapshot = SnapshotMessage {
                    exch_ts_ns,
                    local_ts_ns,
                    bids: parse_levels(data.get("b"), &self.market, Side::Bid).ok()?,
                    asks: parse_levels(data.get("a"), &self.market, Side::Ask).ok()?,
                    last_seq: Some(update),
                };
                if self.previous_seq.is_some() {
                    self.pending.push_back(FeedMessage::Gap(GapMessage {
                        start_local_ns: local_ts_ns,
                        start_exch_ns: exch_ts_ns,
                        end_local_ns: local_ts_ns,
                        end_exch_ns: exch_ts_ns,
                        reason: GapReason::Resync,
                        last_seq: self.previous_seq.unwrap_or_default(),
                        next_seq: update,
                        detail: "exchange orderbook snapshot reset".into(),
                    }));
                }
                self.previous_seq = Some(update);
                self.pending.push_back(FeedMessage::Snapshot(snapshot));
                return self.pending.pop_front();
            }
            let previous = self.previous_seq?;
            if update <= previous {
                return None;
            }
            if update != previous.saturating_add(1) || update == 1 {
                self.pending.push_back(FeedMessage::Gap(GapMessage {
                    start_local_ns: local_ts_ns,
                    start_exch_ns: exch_ts_ns,
                    end_local_ns: local_ts_ns,
                    end_exch_ns: exch_ts_ns,
                    reason: GapReason::SequenceBreak,
                    last_seq: previous,
                    next_seq: update,
                    detail: "Bybit update id discontinuity; awaiting snapshot".into(),
                }));
                self.previous_seq = None;
                self.awaiting_snapshot = true;
                return self.pending.pop_front();
            }
            self.previous_seq = Some(update);
            let mut changes = bids;
            changes.extend(asks);
            return Some(FeedMessage::Depth(DepthMessage {
                exch_ts_ns,
                local_ts_ns,
                first_prev_seq: Some(previous),
                first_seq: previous.checked_add(1),
                last_seq: update,
                changes,
            }));
        }
        if topic.starts_with("publicTrade.") {
            let trades = data.as_array()?;
            for trade in trades {
                self.pending.push_back(FeedMessage::Trade(TradeRecord {
                    exch_ts_ns: trade
                        .get("T")
                        .and_then(Value::as_i64)
                        .and_then(ms_to_ns)
                        .unwrap_or(exch_ts_ns),
                    local_ts_ns,
                    side: match trade.get("S").and_then(Value::as_str) {
                        Some("Buy") => TradeSide::Buy,
                        Some("Sell") => TradeSide::Sell,
                        _ => TradeSide::Unknown,
                    },
                    price: trade.get("p")?.as_str()?.to_owned(),
                    qty: trade.get("v")?.as_str()?.to_owned(),
                    trade_id: trade.get("i").and_then(Value::as_str).map(str::to_owned),
                }));
            }
            return self.pending.pop_front();
        }
        if topic.starts_with("tickers.") {
            let oi = data
                .get("openInterest")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let funding = data
                .get("fundingRate")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let next_funding = data
                .get("nextFundingTime")
                .and_then(Value::as_str)
                .and_then(|v| v.parse::<i64>().ok())
                .and_then(ms_to_ns);
            if let Some(value) = oi {
                self.pending.push_back(FeedMessage::Aux(AuxRecord::Oi {
                    exch_ts_ns,
                    local_ts_ns,
                    value,
                    unit: OpenInterestUnit::Contracts,
                    source: AuxSource::Stream,
                }));
            }
            if let (Some(rate), Some(next_funding_ts_ns)) = (funding, next_funding) {
                self.pending.push_back(FeedMessage::Aux(AuxRecord::Funding {
                    exch_ts_ns,
                    local_ts_ns,
                    rate,
                    next_funding_ts_ns: Some(next_funding_ts_ns),
                    mark_px: data
                        .get("markPrice")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    index_px: data
                        .get("indexPrice")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    source: AuxSource::Stream,
                }));
            }
            return self.pending.pop_front();
        }
        if topic.starts_with("allLiquidation.") {
            let entries = data.as_array()?;
            for entry in entries {
                self.pending
                    .push_back(FeedMessage::Aux(AuxRecord::Liquidation {
                        exch_ts_ns: entry
                            .get("T")
                            .and_then(Value::as_i64)
                            .and_then(ms_to_ns)
                            .unwrap_or(exch_ts_ns),
                        local_ts_ns,
                        side: match entry.get("S").and_then(Value::as_str) {
                            Some("Buy") => Some(TradeSide::Buy),
                            Some("Sell") => Some(TradeSide::Sell),
                            _ => None,
                        },
                        price: entry.get("p").and_then(Value::as_str).map(str::to_owned),
                        qty: entry.get("v").and_then(Value::as_str).map(str::to_owned),
                        notional: None,
                        order_id: None,
                        status: None,
                        source: AuxSource::Stream,
                    }));
            }
            return self.pending.pop_front();
        }
        None
    }
}

impl Feed for BybitFeed {
    async fn next(&mut self) -> Option<FeedMessage> {
        loop {
            if let Some(message) = self.pending.pop_front() {
                return Some(message);
            }
            if *self.cancel.borrow() {
                return None;
            }
            if self.reconnected {
                self.reconnected = false;
                self.previous_seq = None;
                let now = now_ns();
                self.pending.push_back(FeedMessage::Gap(GapMessage {
                    start_local_ns: now,
                    start_exch_ns: now,
                    end_local_ns: now,
                    end_exch_ns: now,
                    reason: GapReason::Disconnect,
                    last_seq: 0,
                    next_seq: 0,
                    detail: "Bybit websocket reconnected".into(),
                }));
                continue;
            }
            tokio::select! {
                changed = self.cancel.changed() => if changed.is_err() || *self.cancel.borrow() { return None; },
                frame = self.frames.recv() => match frame {
                    Some((_, text)) => if let Some(value) = self.parse(text.as_str()).await { return Some(value); },
                    // The transport is reconnecting; the next frame starts a fresh stream.
                    None => self.reconnected = true,
                }
            }
        }
    }
}

fn bybit_category(market: &MarketConfig) -> anyhow::Result<&'static str> {
    if market.instrument == InstrumentType::Spot {
        return Ok("spot");
    }
    let venue = market.venue.to_ascii_lowercase();
    if venue.contains("inverse") || venue.contains("coinm") {
        Ok("inverse")
    } else if venue.contains("linear") || venue.contains("perp") || venue.contains("future") {
        Ok("linear")
    } else {
        anyhow::bail!("Bybit derivative venue must identify linear or inverse category")
    }
}

struct HyperliquidFeed {
    /// Frames from the transport, which owns the connection and its reconnection.
    frames: tokio::sync::mpsc::Receiver<(chrono::DateTime<chrono::Utc>, FrameText)>,
    _connection: tokio::task::JoinHandle<()>,
    market: MarketConfig,
    cancel: watch::Receiver<bool>,
    pending: VecDeque<FeedMessage>,
    last_funding: Option<String>,
    reconnected: bool,
}

impl HyperliquidFeed {
    async fn connect(market: MarketConfig, cancel: watch::Receiver<bool>) -> anyhow::Result<Self> {
        // Hyperliquid takes one subscription message per channel; the transport sends them all
        // once the connection is open, and again after a reconnect.
        let mut messages = Vec::new();
        for subscription in [
            serde_json::json!({"type":"l2Book","coin":market.symbol}),
            serde_json::json!({"type":"trades","coin":market.symbol}),
            serde_json::json!({"type":"activeAssetCtx","coin":market.symbol}),
        ] {
            messages.push(
                serde_json::json!({"method":"subscribe","subscription":subscription}).to_string(),
            );
        }
        let (tx, frames) = tokio::sync::mpsc::channel(CONNECTION_FRAMES);
        let target = WsTarget::new(
            "wss://api.hyperliquid.xyz/ws".to_owned(),
            Heartbeat {
                check_interval: std::time::Duration::from_secs(10),
                timeout: std::time::Duration::from_secs(60),
            },
        )
        .subscribing(messages);
        let connection = tokio::spawn(transport::keep_connection(target, tx));
        Ok(Self {
            frames,
            _connection: connection,
            market,
            cancel,
            pending: VecDeque::new(),
            last_funding: None,
            reconnected: false,
        })
    }

    fn parse(&mut self, text: &str) -> Option<FeedMessage> {
        let root: Value = serde_json::from_str(text).ok()?;
        let channel = root.get("channel")?.as_str()?;
        let data = root.get("data")?;
        let local_ts_ns = now_ns();
        if channel == "l2Book" {
            if data.get("coin").and_then(Value::as_str)? != self.market.symbol {
                return None;
            }
            let levels = data.get("levels")?.as_array()?;
            let bids = parse_levels(levels.first(), &self.market, Side::Bid).ok()?;
            let asks = parse_levels(levels.get(1), &self.market, Side::Ask).ok()?;
            return Some(FeedMessage::Snapshot(SnapshotMessage {
                exch_ts_ns: data
                    .get("time")
                    .and_then(Value::as_i64)
                    .and_then(ms_to_ns)
                    .unwrap_or(local_ts_ns),
                local_ts_ns,
                bids,
                asks,
                last_seq: None,
            }));
        }
        if channel == "trades" {
            let rows = data.as_array()?;
            for row in rows {
                if row.get("coin").and_then(Value::as_str)? != self.market.symbol {
                    continue;
                }
                self.pending.push_back(FeedMessage::Trade(TradeRecord {
                    exch_ts_ns: row
                        .get("time")
                        .and_then(Value::as_i64)
                        .and_then(ms_to_ns)
                        .unwrap_or(local_ts_ns),
                    local_ts_ns,
                    side: match row.get("side").and_then(Value::as_str) {
                        Some("B") => TradeSide::Buy,
                        Some("A") => TradeSide::Sell,
                        _ => TradeSide::Unknown,
                    },
                    price: row.get("px")?.as_str()?.to_owned(),
                    qty: row.get("sz")?.as_str()?.to_owned(),
                    trade_id: row.get("tid").map(|v| v.to_string()),
                }));
            }
            return self.pending.pop_front();
        }
        if channel == "activeAssetCtx" {
            if data.get("coin").and_then(Value::as_str)? != self.market.symbol {
                return None;
            }
            let ctx = data.get("ctx")?;
            if let Some(oi) = ctx.get("openInterest").and_then(Value::as_str) {
                self.pending.push_back(FeedMessage::Aux(AuxRecord::Oi {
                    exch_ts_ns: local_ts_ns,
                    local_ts_ns,
                    value: oi.to_owned(),
                    unit: OpenInterestUnit::Base,
                    source: AuxSource::Stream,
                }));
            }
            if let Some(rate) = ctx.get("funding").and_then(Value::as_str)
                && self.last_funding.as_deref() != Some(rate)
            {
                self.last_funding = Some(rate.to_owned());
                self.pending.push_back(FeedMessage::Aux(AuxRecord::Funding {
                    exch_ts_ns: local_ts_ns,
                    local_ts_ns,
                    rate: rate.to_owned(),
                    next_funding_ts_ns: None,
                    mark_px: ctx.get("markPx").and_then(Value::as_str).map(str::to_owned),
                    index_px: ctx
                        .get("oraclePx")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    source: AuxSource::Stream,
                }));
            }
            return self.pending.pop_front();
        }
        None
    }
}

impl Feed for HyperliquidFeed {
    async fn next(&mut self) -> Option<FeedMessage> {
        loop {
            if let Some(message) = self.pending.pop_front() {
                return Some(message);
            }
            if *self.cancel.borrow() {
                return None;
            }
            if self.reconnected {
                self.reconnected = false;
                let now = now_ns();
                self.pending.push_back(FeedMessage::Gap(GapMessage {
                    start_local_ns: now,
                    start_exch_ns: now,
                    end_local_ns: now,
                    end_exch_ns: now,
                    reason: GapReason::Disconnect,
                    last_seq: 0,
                    next_seq: 0,
                    detail: "Hyperliquid websocket reconnected".into(),
                }));
                continue;
            }
            tokio::select! {
                changed = self.cancel.changed() => if changed.is_err() || *self.cancel.borrow() { return None; },
                frame = self.frames.recv() => match frame {
                    Some((_, text)) => if let Some(value) = self.parse(text.as_str()) { return Some(value); },
                    None => self.reconnected = true,
                }
            }
        }
    }
}

fn now_ns() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or_default()
}

fn ms_to_ns(milliseconds: i64) -> Option<i64> {
    milliseconds.checked_mul(1_000_000)
}

fn parse_changes(
    value: Option<&Value>,
    market: &MarketConfig,
    side: Side,
) -> anyhow::Result<Vec<Change>> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    levels
        .iter()
        .map(|row| {
            let row = row
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("invalid Binance depth row"))?;
            let price = row
                .first()
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("invalid Binance depth price"))?;
            let qty = row
                .get(1)
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("invalid Binance depth quantity"))?;
            Ok(Change {
                side,
                tick: i64::try_from(decimal_steps(
                    price,
                    market.price_decimals,
                    market.tick_units,
                )?)
                .map_err(|_| anyhow::anyhow!("price tick out of range"))?,
                qty: decimal_steps(qty, market.qty_decimals, market.qty_step_units)?,
            })
        })
        .collect()
}

fn parse_levels(
    value: Option<&Value>,
    market: &MarketConfig,
    side: Side,
) -> anyhow::Result<Vec<Level>> {
    let changes = parse_changes(value, market, side)?;
    let mut levels = changes
        .into_iter()
        .filter(|change| change.qty > 0)
        .map(|change| Level {
            tick: change.tick,
            qty: change.qty,
        })
        .collect::<Vec<_>>();
    match side {
        Side::Bid => levels.sort_by(|a, b| b.tick.cmp(&a.tick)),
        Side::Ask => levels.sort_by_key(|level| level.tick),
    }
    levels.truncate(usize::from(market.depth_available));
    Ok(levels)
}

/// A Binance market feed backed by the connector's market stream.
///
/// The connector owns the socket, the sequence check, the REST snapshot and its alignment, so
/// none of that is repeated here. What stays with the recorder is the two duties the connector
/// does not perform: polling open interest over REST.
struct BinanceConnectorFeed {
    inner: collector::feed::connector::ConnectorFeed,
    /// Kept alive so the connector stream stops when this feed does.
    stream: Option<connector::market_stream::MarketStreamHandle>,
    client: reqwest::Client,
    market: MarketConfig,
    cancel: watch::Receiver<bool>,
    pending: VecDeque<FeedMessage>,
    oi_timer: tokio::time::Interval,
    oi_url: &'static str,
}

impl BinanceConnectorFeed {
    async fn connect(
        market: MarketConfig,
        settings: RecorderSettings,
        cancel: watch::Receiver<bool>,
    ) -> anyhow::Result<Self> {
        let (inner, sender) = collector::feed::connector::ConnectorFeed::channel(market.clone());
        let mut config = connector::market_stream::BinanceMarketStreamConfig::new(
            &market.symbol,
            stream_kinds(&market),
        );
        if let Some(api) = binance_api_root(&market.venue) {
            config.api_url = api.to_owned();
        }
        if let Some(stream) = binance_stream_root(&market.venue) {
            config.stream_url = stream.to_owned();
        }
        config.depth_snapshot_limit = depth_snapshot_limit(settings.depth_levels);
        let stream = connector::market_stream::start_binance_market_stream(config, sender).await?;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;
        let oi_url = match market.venue.to_ascii_lowercase().as_str() {
            "binancefuturescm" | "binance_coinm" => "https://dapi.binance.com/dapi/v1/openInterest",
            _ => "https://fapi.binance.com/fapi/v1/openInterest",
        };
        let oi_timer = interval_at(
            Instant::now() + std::time::Duration::from_millis(u64::from(settings.oi_poll_ms)),
            std::time::Duration::from_millis(u64::from(settings.oi_poll_ms)),
        );
        Ok(Self {
            inner,
            stream: Some(stream),
            client,
            market,
            cancel,
            pending: VecDeque::new(),
            oi_timer,
            oi_url,
        })
    }

    async fn poll_open_interest(&self) -> Option<FeedMessage> {
        if self.market.instrument == InstrumentType::Spot {
            return None;
        }
        let response: Value = self
            .client
            .get(self.oi_url)
            .query(&[("symbol", self.market.symbol.as_str())])
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .ok()?
            .json()
            .await
            .ok()?;
        let value = response.get("openInterest")?.as_str()?.to_owned();
        let ts = response
            .get("time")
            .and_then(Value::as_i64)
            .and_then(ms_to_ns)
            .unwrap_or_else(now_ns);
        Some(FeedMessage::Aux(AuxRecord::Oi {
            exch_ts_ns: ts,
            local_ts_ns: now_ns(),
            value,
            unit: OpenInterestUnit::Contracts,
            source: AuxSource::Rest,
        }))
    }
}

impl Drop for BinanceConnectorFeed {
    fn drop(&mut self) {
        // Dropping the handle aborts the connector tasks, so a stopped market does not leave a
        // socket publishing into a channel nobody reads.
        if let Some(stream) = self.stream.take() {
            drop(stream);
        }
    }
}

impl Feed for BinanceConnectorFeed {
    async fn next(&mut self) -> Option<FeedMessage> {
        if let Some(message) = self.pending.pop_front() {
            return Some(message);
        }
        loop {
            if *self.cancel.borrow() {
                return None;
            }
            tokio::select! {
                changed = self.cancel.changed() => {
                    if changed.is_err() || *self.cancel.borrow() { return None; }
                }
                _ = self.oi_timer.tick(), if self.market.instrument != InstrumentType::Spot => {
                    if let Some(message) = self.poll_open_interest().await {
                        return Some(message);
                    }
                }
                message = self.inner.next() => return message,
            }
        }
    }
}

/// The market data kinds this recorder market needs from the connector.
fn stream_kinds(market: &MarketConfig) -> Vec<MarketDataKind> {
    let mut kinds = vec![MarketDataKind::Depth, MarketDataKind::Trades];
    if market.instrument != InstrumentType::Spot {
        kinds.push(MarketDataKind::FundingRate);
    }
    kinds
}

/// The REST base for a Binance venue, when the venue is one the connector serves.
fn binance_api_root(venue: &str) -> Option<&'static str> {
    match venue.to_ascii_lowercase().as_str() {
        "binance" | "binancespot" | "binance_spot" | "binancefuturesum" | "binance_futures" => {
            Some("https://fapi.binance.com")
        }
        "binancefuturescm" | "binance_coinm" => Some("https://dapi.binance.com"),
        _ => None,
    }
}

/// The websocket root for a Binance venue, when the venue is one the connector serves.
fn binance_stream_root(venue: &str) -> Option<&'static str> {
    match venue.to_ascii_lowercase().as_str() {
        "binance" | "binancespot" | "binance_spot" | "binancefuturesum" | "binance_futures" => {
            Some("wss://fstream.binance.com")
        }
        "binancefuturescm" | "binance_coinm" => Some("wss://dstream.binance.com"),
        _ => None,
    }
}

/// The depth snapshot limit the connector should request for a window of `depth_levels`.
fn depth_snapshot_limit(depth_levels: u16) -> u16 {
    match depth_levels {
        0..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        21..=50 => 50,
        51..=100 => 100,
        101..=500 => 500,
        _ => 1000,
    }
}

#[cfg(test)]
mod quality_gate_tests {
    use super::*;

    #[test]
    fn formal_replay_requires_quality_and_rejects_fail_market() {
        let root = std::env::temp_dir().join(format!("titan-quality-gate-{}", now_ns()));
        let date = "2026-11-08";
        let segment_dir = root
            .join("book/venue=binance/instrument=spot/symbol=BTCUSDT")
            .join(format!("date={date}"));
        fs::create_dir_all(&segment_dir).unwrap();
        let segment = segment_dir.join("segment.tnmd");
        fs::write(&segment, []).unwrap();
        let report_dir = root.join("quality").join(format!("date={date}"));
        fs::create_dir_all(&report_dir).unwrap();
        let report_path = report_dir.join("quality.json");
        fs::write(
            &report_path,
            br#"{"status":"FAIL","markets":[{"symbol":"BTCUSDT","status":"FAIL"}]}"#,
        )
        .unwrap();
        assert!(quality_gate(&root, std::slice::from_ref(&segment), "BTCUSDT").is_err());
        fs::write(&report_path, br#"{"status":"PASS_WITH_WARNINGS","markets":[{"symbol":"BTCUSDT","status":"PASS_WITH_WARNINGS"}]}"#).unwrap();
        assert!(quality_gate(&root, std::slice::from_ref(&segment), "BTCUSDT").is_ok());
        fs::write(
            &report_path,
            br#"{"status":"PASS","markets":[{"symbol":"BTCUSDT","status":"PASS"}]}"#,
        )
        .unwrap();
        assert!(quality_gate(&root, std::slice::from_ref(&segment), "BTCUSDT").is_ok());
        fs::remove_dir_all(root).unwrap();
    }
}

#[derive(Parser)]
#[command(name = "book", version, about = "Market data book tools")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Validate a recorder configuration. Feed adapters are selected by the
    /// collector runtime and cannot be inferred from a TOML file alone.
    Record {
        #[arg(long)]
        config: PathBuf,
    },
    /// Check every book segment under a recording root.
    Verify {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        date: Option<String>,
        #[arg(long)]
        market: Option<String>,
    },
    /// Rebuild a segment's sidecar index and truncate an incomplete tail.
    Repair {
        #[arg(long)]
        segment: PathBuf,
    },
    /// Emit a compact integrity summary for a date.
    Quality {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        date: String,
        /// Recorder config whose REST validation sample threshold applies to this report.
        #[arg(long)]
        config: PathBuf,
    },
    /// Read blocks from matching segments, optionally bounded by exchange time.
    Replay {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        market: String,
        #[arg(long)]
        from: Option<i64>,
        #[arg(long)]
        to: Option<i64>,
        #[arg(long, value_enum, default_value = "skip")]
        gap_policy: GapPolicy,
        /// Bypass the quality gate for diagnostics only.
        #[arg(long)]
        diagnostic: bool,
    },
    /// Package one UTC date of recorded data into a single zip with manifest.
    Archive {
        #[arg(long)]
        root: PathBuf,
        /// UTC date, YYYY-MM-DD.
        #[arg(long)]
        date: String,
        /// Archive directory (default: <root>/archives).
        #[arg(long)]
        archive_dir: Option<PathBuf>,
        /// Delete archived data files after verification (quality reports kept).
        #[arg(long)]
        prune: bool,
        /// Replace an existing archive for the same date.
        #[arg(long)]
        overwrite: bool,
    },
}

#[derive(Serialize)]
struct SegmentSummary {
    file: String,
    #[serde(skip)]
    path: PathBuf,
    market_key: String,
    market_id: u32,
    venue_id: u16,
    symbol: String,
    feed_mode: String,
    depth_requested: u16,
    depth_available: u16,
    grid_ms: u16,
    start_exch_ns: i64,
    end_exch_ns: i64,
    blocks: usize,
    readable: usize,
    corrupt: usize,
    gaps: usize,
    precision_rejects: u64,
    gap_ms: u64,
    truncated_tail: bool,
    #[serde(skip)]
    gap_durations_ms: Vec<u64>,
}

#[derive(Serialize)]
struct QualitySummary {
    status: &'static str,
    segments: usize,
    blocks: usize,
    readable: usize,
    corrupt: usize,
    events: collector::file::EventIntegrityReport,
    markets: Vec<MarketQualitySummary>,
}

#[derive(Serialize)]
struct MarketQualitySummary {
    market: String,
    market_id: u32,
    venue_id: u16,
    symbol: String,
    instrument_type: String,
    feed_mode: String,
    segment_count: usize,
    gap_count: usize,
    gap_ms: u64,
    gap_ms_ratio: f64,
    top_gaps: Vec<u64>,
    bad_blocks: usize,
    truncated_segments: usize,
    seq_check: &'static str,
    trade_count: u64,
    trade_truncated_tail: bool,
    aux_oi_samples: u64,
    aux_funding_samples: u64,
    aux_liquidation_count: u64,
    aux_unsupported: Vec<&'static str>,
    oi_max_age_ms: Option<i64>,
    funding_last_age_ms: Option<i64>,
    depth_requested: u16,
    depth_available: u16,
    depth_recorded: u16,
    block_diff_mismatches: usize,
    rest_samples_compared: Option<u64>,
    rest_samples_unjudged: Option<u64>,
    rest_mismatches: Option<u64>,
    precision_rejects: Option<u64>,
    precision_reject_ratio: Option<f64>,
    late_events: Option<u64>,
    late_ratio: Option<f64>,
    lag_p50_us: Option<i64>,
    lag_p95_us: Option<i64>,
    lag_p99_us: Option<i64>,
    changes_per_slot_p50: Option<u64>,
    changes_per_slot_p95: Option<u64>,
    aux_sources: Vec<&'static str>,
    status: &'static str,
    warnings: Vec<String>,
    failures: Vec<String>,
}

#[derive(Serialize)]
struct ReplaySummary {
    market: String,
    gap_policy: GapPolicy,
    blocks: usize,
    records: usize,
    changes: usize,
    gaps: usize,
    corrupt_blocks: usize,
    gap_events: Vec<GapSummary>,
    best_bid_tick: Option<i64>,
    best_ask_tick: Option<i64>,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum GapPolicy {
    Error,
    Skip,
    FastForward,
}

#[derive(Serialize)]
struct GapSummary {
    start_exch_ns: Option<i64>,
    end_exch_ns: Option<i64>,
    reason: u16,
    last_seq: u64,
    next_seq: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    // The transport reports what a connection does through `tracing`; without a subscriber those
    // reports go nowhere, which is how a stream that kept being refused stayed invisible.
    // `RUST_LOG` selects the level: the transport reports what a connection does through
    // `tracing`, and a recorder troubleshooting a feed needs the debug lines it used to drop.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .with_env_filter(filter)
        .try_init();
    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err((code, message)) => {
            eprintln!("{message}");
            ExitCode::from(code)
        }
    }
}

async fn run(args: Args) -> Result<(), (u8, String)> {
    match args.command {
        Command::Record { config } => {
            let text = fs::read_to_string(config).map_err(io_error)?;
            let config = RecorderConfig::parse(&text).map_err(|e| (2, e.to_string()))?;
            run_record(config).await
        }
        Command::Repair { segment } => {
            let report = recover_segment(&segment).map_err(io_error)?;
            println!("{}", serde_json::to_string(&report).map_err(json_error)?);
            Ok(())
        }
        Command::Verify { root, date, market } => {
            let reports = inspect(&root, date.as_deref(), market.as_deref())?;
            let events =
                collector::file::inspect_market_events(&root, date.as_deref()).map_err(io_error)?;
            let bad = reports.iter().any(|r| r.corrupt > 0)
                || events.truncated_files > 0
                || events.invalid_records > 0
                || events.mismatched_summaries > 0;
            let report = serde_json::json!({"book_segments": reports, "events": events});
            println!("{}", serde_json::to_string(&report).map_err(json_error)?);
            if bad {
                Err((3, "book verification found corrupt blocks".into()))
            } else {
                Ok(())
            }
        }
        Command::Quality { root, date, config } => {
            chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d")
                .map_err(|_| (2, "date must use YYYY-MM-DD".into()))?;
            // The config is still validated so a bad file is reported here rather than later.
            let text = fs::read_to_string(config).map_err(io_error)?;
            RecorderConfig::parse(&text).map_err(|error| (2, error.to_string()))?;
            let reports = inspect(&root, Some(&date), None)?;
            let events =
                collector::file::inspect_market_events(&root, Some(&date)).map_err(io_error)?;
            let blocks = reports.iter().map(|r| r.blocks).sum();
            let readable = reports.iter().map(|r| r.readable).sum();
            let corrupt = reports.iter().map(|r| r.corrupt).sum();
            let markets = build_quality_markets(&reports, &events, &date);
            let status = if markets.iter().any(|market| market.status == "FAIL") {
                "FAIL"
            } else if markets
                .iter()
                .any(|market| market.status == "PASS_WITH_WARNINGS")
                || events.truncated_files > 0
                || events.invalid_records > 0
                || events.mismatched_summaries > 0
            {
                "PASS_WITH_WARNINGS"
            } else {
                "PASS"
            };
            let report = QualitySummary {
                status,
                segments: reports.len(),
                blocks,
                readable,
                corrupt,
                events: events.clone(),
                markets,
            };
            let report_bytes = serde_json::to_vec_pretty(&report).map_err(json_error)?;
            let output_dir = root.join("quality").join(format!("date={date}"));
            fs::create_dir_all(&output_dir).map_err(io_error)?;
            let temporary = output_dir.join("quality.json.tmp");
            let output = output_dir.join("quality.json");
            let mut file = fs::File::create(&temporary).map_err(io_error)?;
            use std::io::Write;
            file.write_all(&report_bytes).map_err(io_error)?;
            file.write_all(b"\n").map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
            fs::rename(temporary, &output).map_err(io_error)?;
            fs::File::open(&output_dir)
                .and_then(|directory| directory.sync_all())
                .map_err(io_error)?;
            println!("{}", serde_json::to_string(&report).map_err(json_error)?);
            if status == "FAIL" {
                Err((3, format!("quality status is {status}")))
            } else {
                Ok(())
            }
        }
        Command::Archive {
            root,
            date,
            archive_dir,
            prune,
            overwrite,
        } => {
            let report = archive_daily(&root, &date, archive_dir.as_deref(), prune, overwrite)
                .map_err(|error| (4, error.to_string()))?;
            println!("{}", serde_json::to_string(&report).map_err(json_error)?);
            Ok(())
        }
        Command::Replay {
            root,
            market,
            from,
            to,
            gap_policy,
            diagnostic,
        } => {
            let reports = inspect(&root, None, Some(&market))?;
            let paths = matching_paths(&root, None, Some(&market)).map_err(io_error)?;
            if !diagnostic {
                quality_gate(&root, &paths, &market)?;
            }
            let mut count = 0usize;
            let mut records = 0usize;
            let mut changes = 0usize;
            let mut gaps = 0usize;
            let mut corrupt_blocks = 0usize;
            let mut gap_events = Vec::new();
            let mut ladder = Ladder::default();
            for path in paths {
                let reader = SegmentReader::open(&path).map_err(io_error)?;
                for i in 0..reader.block_count() {
                    let block = match reader.read_block(i) {
                        Ok(v) => v,
                        Err(_) => {
                            corrupt_blocks += 1;
                            gaps += 1;
                            gap_events.push(GapSummary {
                                start_exch_ns: None,
                                end_exch_ns: None,
                                reason: GapReason::CorruptBlock as u16,
                                last_seq: 0,
                                next_seq: 0,
                            });
                            if matches!(gap_policy, GapPolicy::Error) {
                                return Err((3, "replay encountered a corrupt block".into()));
                            }
                            continue;
                        }
                    };
                    if from.is_some_and(|t| block.header.block_end_exch_ns < t)
                        || to.is_some_and(|t| block.header.block_start_exch_ns > t)
                    {
                        continue;
                    }
                    count += 1;
                    if block.header.kind == BlockKind::Gap {
                        gaps += 1;
                        gap_events.push(GapSummary {
                            start_exch_ns: Some(block.header.block_start_exch_ns),
                            end_exch_ns: Some(block.header.block_end_exch_ns),
                            reason: block.header.control_reason,
                            last_seq: block.header.control_last_seq,
                            next_seq: block.header.control_next_seq,
                        });
                        if matches!(gap_policy, GapPolicy::Error) {
                            return Err((3, "replay encountered a recorded gap".into()));
                        }
                        continue;
                    }
                    let payload = decode_data_payload(&block).map_err(io_error)?;
                    ladder =
                        Ladder::from_snapshot(&payload.bids, &payload.asks).map_err(io_error)?;
                    records += payload.records.len();
                    for record in payload.records {
                        changes += record.changes.len();
                        ladder.apply(&record.changes).map_err(io_error)?;
                    }
                }
            }
            if reports.is_empty() {
                return Err((2, "no matching book segments".into()));
            }
            let summary = ReplaySummary {
                market,
                gap_policy,
                blocks: count,
                records,
                changes,
                gaps,
                corrupt_blocks,
                gap_events,
                best_bid_tick: ladder.best_bid().map(|level| level.tick),
                best_ask_tick: ladder.best_ask().map(|level| level.tick),
            };
            println!("{}", serde_json::to_string(&summary).map_err(json_error)?);
            Ok(())
        }
    }
}

fn build_quality_markets(
    segments: &[SegmentSummary],
    events: &collector::file::EventIntegrityReport,
    date: &str,
) -> Vec<MarketQualitySummary> {
    let mut grouped: std::collections::BTreeMap<&str, Vec<&SegmentSummary>> =
        std::collections::BTreeMap::new();
    for segment in segments {
        grouped
            .entry(&segment.market_key)
            .or_default()
            .push(segment);
    }
    let Ok(day) = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d") else {
        return Vec::new();
    };
    let day_end_ns = day
        .succ_opt()
        .and_then(|next| next.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp_nanos_opt().unwrap_or_default())
        .unwrap_or_default();
    grouped
        .into_iter()
        .filter_map(|(key, parts)| {
            let first = *parts.first()?;
            let mut warnings = Vec::new();
            let mut failures = Vec::new();
            let mut gaps = 0usize;
            let mut gap_ms = 0u64;
            let mut bad_blocks = 0usize;
            let mut truncated_segments = 0usize;
            let mut total_ms = 0u64;
            let mut total_slots = 0u64;
            let mut top_gaps = Vec::new();
            let mut block_diff_mismatches = 0usize;
            let mut sequence_failures = 0usize;
            let mut lags_us = Vec::new();
            let mut changes_per_slot = Vec::new();
            let mut precision_rejects = 0u64;
            for part in &parts {
                gaps += part.gaps;
                precision_rejects = precision_rejects.saturating_add(part.precision_rejects);
                gap_ms = gap_ms.saturating_add(part.gap_ms);
                bad_blocks += part.corrupt;
                truncated_segments += usize::from(part.truncated_tail);
                let part_ms =
                    part.end_exch_ns.saturating_sub(part.start_exch_ns).max(0) as u64 / 1_000_000;
                total_ms = total_ms.saturating_add(part_ms);
                total_slots = total_slots.saturating_add(
                    (part_ms / u64::from(part.grid_ms.max(1))).max(if part_ms > 0 { 1 } else { 0 }),
                );
                top_gaps.extend(part.gap_durations_ms.iter().copied());
                block_diff_mismatches += block_diff_mismatches_in_segment(part);
                if part.feed_mode == "diff" {
                    sequence_failures += sequence_failures_in_segment(part);
                }
                collect_slot_metrics(part, &mut lags_us, &mut changes_per_slot);
            }
            top_gaps.sort_unstable_by(|a, b| b.cmp(a));
            top_gaps.truncate(10);
            if gaps > 0 {
                warnings.push("gap_present".into());
            }
            if bad_blocks > 0 {
                warnings.push("bad_blocks".into());
            }
            if truncated_segments > 0 {
                warnings.push("truncated_segment_tail".into());
            }
            if first.feed_mode == "snapshot" {
                warnings.push("snapshot_feed_has_no_native_sequence".into());
            }
            if first.depth_available < first.depth_requested {
                warnings.push("depth_below_requested".into());
            }
            if precision_rejects > 0 {
                warnings.push("precision_reject_present".into());
            }
            if block_diff_mismatches > 0 {
                failures.push("block_snapshot_diff_mismatch".into());
            }
            if sequence_failures > 0 {
                failures.push("native_sequence_discontinuity".into());
            }
            if total_ms > 0 && gap_ms > total_ms {
                warnings.push("gap_duration_exceeds_observed_span".into());
            }

            let event = events.markets.get(key).cloned().unwrap_or_default();
            let late_events = event.late_events;
            let mut oi_timestamps = event.oi_timestamps_ns.clone();
            oi_timestamps.sort_unstable();
            let oi_max_interval_ms = oi_timestamps
                .windows(2)
                .map(|window| window[1].saturating_sub(window[0]).max(0) / 1_000_000)
                .max()
                .unwrap_or(0)
                .max(
                    event
                        .oi_latest_ts_ns
                        .map(|ts| day_end_ns.saturating_sub(ts).max(0) / 1_000_000)
                        .unwrap_or(0),
                );
            let instrument_type = key
                .split('/')
                .nth(1)
                .and_then(|component| component.strip_prefix("instrument="))
                .unwrap_or("unknown");
            let is_contract = instrument_type != "spot";
            if event.trade_count == 0 {
                failures.push("trade_missing_for_day".into());
            }
            if event.trade_truncated_tail {
                warnings.push("trade_truncated_tail".into());
            }
            let venue = key.split('/').next().unwrap_or_default();
            let hl_liquidation_unsupported = venue.contains("hyperliquid");
            let aux_unsupported = if is_contract && hl_liquidation_unsupported {
                warnings.push("liquidation_source_unsupported".into());
                vec!["liquidation"]
            } else {
                Vec::new()
            };
            if is_contract && event.aux_liquidation_count == 0 && !hl_liquidation_unsupported {
                warnings.push("no_liquidation_events_or_stream_evidence".into());
            }
            if events.missing_summaries > 0 {
                warnings.push("event_summary_missing".into());
            }
            if events.truncated_files > 0 || events.invalid_records > 0 {
                warnings.push("event_integrity_issue".into());
            }
            if events.mismatched_summaries > 0 {
                warnings.push("event_summary_mismatch".into());
            }
            if is_contract && event.aux_oi_samples == 0 {
                failures.push("open_interest_missing_for_day".into());
            }
            if is_contract && event.aux_funding_samples == 0 {
                failures.push("funding_missing_for_day".into());
            }
            if lags_us.is_empty() {
                warnings.push("latency_metrics_unavailable".into());
            }
            if late_events > 0 && late_events.saturating_mul(1_000) > total_slots.max(1) {
                warnings.push("late_ratio_above_0_1_percent".into());
            }

            let instrument_path = key.split('/').nth(1).unwrap_or_default();
            let instrument_type = instrument_path
                .strip_prefix("instrument=")
                .unwrap_or(instrument_type)
                .to_owned();
            let status = if !failures.is_empty() {
                "FAIL"
            } else if !warnings.is_empty() {
                "PASS_WITH_WARNINGS"
            } else {
                "PASS"
            };
            let latest_oi_age = (event.aux_oi_samples > 0).then_some(oi_max_interval_ms);
            let latest_funding_age = event
                .funding_latest_ts_ns
                .map(|ts| day_end_ns.saturating_sub(ts).max(0) / 1_000_000);
            let slot_count = total_slots.max(changes_per_slot.len() as u64);
            Some(MarketQualitySummary {
                market: key.to_owned(),
                market_id: first.market_id,
                venue_id: first.venue_id,
                symbol: first.symbol.clone(),
                instrument_type,
                feed_mode: first.feed_mode.clone(),
                segment_count: parts.len(),
                gap_count: gaps,
                gap_ms,
                gap_ms_ratio: if total_ms == 0 {
                    0.0
                } else {
                    gap_ms as f64 / total_ms as f64
                },
                top_gaps,
                bad_blocks,
                truncated_segments,
                seq_check: if first.feed_mode == "snapshot" {
                    "none"
                } else if sequence_failures > 0 {
                    "failed"
                } else {
                    "monotonic_only"
                },
                trade_count: event.trade_count,
                trade_truncated_tail: event.trade_truncated_tail,
                aux_oi_samples: event.aux_oi_samples,
                aux_funding_samples: event.aux_funding_samples,
                aux_liquidation_count: event.aux_liquidation_count,
                aux_unsupported,
                oi_max_age_ms: latest_oi_age,
                funding_last_age_ms: latest_funding_age,
                depth_requested: first.depth_requested,
                depth_available: first.depth_available,
                depth_recorded: first.depth_requested.min(first.depth_available),
                block_diff_mismatches,
                rest_samples_compared: Some(event.rest_samples_compared),
                rest_samples_unjudged: Some(event.rest_samples_unjudged),
                rest_mismatches: Some(event.rest_mismatches),
                precision_rejects: Some(precision_rejects),
                precision_reject_ratio: Some(if slot_count == 0 {
                    0.0
                } else {
                    precision_rejects as f64 / slot_count as f64
                }),
                late_events: Some(late_events),
                late_ratio: Some(if slot_count == 0 {
                    0.0
                } else {
                    late_events as f64 / slot_count as f64
                }),
                lag_p50_us: percentile(&mut lags_us.clone(), 50),
                lag_p95_us: percentile(&mut lags_us.clone(), 95),
                lag_p99_us: percentile(&mut lags_us, 99),
                changes_per_slot_p50: percentile_u64(&mut changes_per_slot.clone(), 50),
                changes_per_slot_p95: percentile_u64(&mut changes_per_slot, 95),
                aux_sources: [
                    (event.aux_stream_samples > 0, "stream"),
                    (event.aux_rest_samples > 0, "rest"),
                ]
                .into_iter()
                .filter_map(|(present, source)| present.then_some(source))
                .collect(),
                status,
                warnings,
                failures,
            })
        })
        .collect()
}

fn quality_gate(root: &Path, paths: &[PathBuf], market: &str) -> Result<(), (u8, String)> {
    let mut dates = std::collections::BTreeSet::new();
    for path in paths {
        for component in path.components() {
            if let Some(date) = component
                .as_os_str()
                .to_str()
                .and_then(|s| s.strip_prefix("date="))
            {
                dates.insert(date.to_owned());
            }
        }
    }
    if dates.is_empty() {
        return Err((3, "quality gate cannot determine recording dates".into()));
    }
    for date in dates {
        let report_path = root
            .join("quality")
            .join(format!("date={date}"))
            .join("quality.json");
        let bytes = fs::read(&report_path)
            .map_err(|_| (3, "quality report is required before replay".into()))?;
        let report: Value =
            serde_json::from_slice(&bytes).map_err(|_| (3, "quality report is invalid".into()))?;
        let global_status = report.get("status").and_then(Value::as_str);
        if !matches!(global_status, Some("PASS" | "PASS_WITH_WARNINGS" | "FAIL")) {
            return Err((3, format!("quality report status is invalid on {date}")));
        }
        let global_fail = global_status == Some("FAIL");
        let market_entries = report
            .get("markets")
            .and_then(Value::as_array)
            .ok_or_else(|| (3, "quality report has no market list".into()))?;
        let matching: Vec<_> = market_entries
            .iter()
            .filter(|entry| entry.get("symbol").and_then(Value::as_str) == Some(market))
            .collect();
        let market_fail = matching.is_empty()
            || matching.iter().any(|entry| {
                !matches!(
                    entry.get("status").and_then(Value::as_str),
                    Some("PASS" | "PASS_WITH_WARNINGS")
                )
            });
        if global_fail || market_fail {
            return Err((3, format!("market {market} has FAIL quality on {date}")));
        }
    }
    Ok(())
}

fn block_diff_mismatches_in_segment(segment: &SegmentSummary) -> usize {
    let Ok(reader) = SegmentReader::open(&segment.path) else {
        return 0;
    };
    let mut previous: Option<(Vec<Level>, Vec<Level>)> = None;
    let mut mismatches = 0;
    for index in 0..reader.block_count() {
        let Ok(block) = reader.read_block(index) else {
            continue;
        };
        if block.header.kind == BlockKind::Gap {
            previous = None;
            continue;
        }
        let Ok(payload) = decode_data_payload(&block) else {
            continue;
        };
        if let Some((bids, asks)) = &previous
            && (bids != &payload.bids || asks != &payload.asks)
        {
            mismatches += 1;
        }
        let Ok(mut ladder) = Ladder::from_snapshot(&payload.bids, &payload.asks) else {
            continue;
        };
        let mut valid = true;
        for record in &payload.records {
            if ladder.apply(&record.changes).is_err() {
                valid = false;
                break;
            }
        }
        if valid {
            previous = Some((
                ladder.top_n(Side::Bid, usize::from(reader.header.depth_available)),
                ladder.top_n(Side::Ask, usize::from(reader.header.depth_available)),
            ));
        } else {
            previous = None;
        }
    }
    mismatches
}

fn collect_slot_metrics(segment: &SegmentSummary, lags: &mut Vec<i64>, changes: &mut Vec<u64>) {
    let Ok(reader) = SegmentReader::open(&segment.path) else {
        return;
    };
    for index in 0..reader.block_count() {
        let Ok(block) = reader.read_block(index) else {
            continue;
        };
        if block.header.kind != BlockKind::Data {
            continue;
        }
        let Ok(payload) = decode_data_payload(&block) else {
            continue;
        };
        for record in payload.records {
            lags.push(i64::from(record.local_lag_us));
            changes.push(record.changes.len() as u64);
        }
    }
}

fn percentile(values: &mut [i64], percentile: usize) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let index = (values.len() - 1) * percentile / 100;
    values.get(index).copied()
}

fn percentile_u64(values: &mut [u64], percentile: usize) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let index = (values.len() - 1) * percentile / 100;
    values.get(index).copied()
}

fn sequence_failures_in_segment(segment: &SegmentSummary) -> usize {
    let Ok(reader) = SegmentReader::open(&segment.path) else {
        return 0;
    };
    let mut previous: Option<u64> = None;
    let mut failures = 0;
    for index in 0..reader.block_count() {
        let Ok(block) = reader.read_block(index) else {
            continue;
        };
        if block.header.kind == BlockKind::Gap {
            previous = None;
            continue;
        }
        let Ok(payload) = decode_data_payload(&block) else {
            continue;
        };
        let mut sequence = previous.unwrap_or(block.header.seq_base);
        for record in &payload.records {
            if record.seq_first_prev_delta == u32::MAX || record.seq_last_delta == u32::MAX {
                failures += 1;
                continue;
            }
            let Some(predecessor) = block
                .header
                .seq_base
                .checked_add(u64::from(record.seq_first_prev_delta))
            else {
                failures += 1;
                continue;
            };
            let Some(last) = block
                .header
                .seq_base
                .checked_add(u64::from(record.seq_last_delta))
            else {
                failures += 1;
                continue;
            };
            // Slot records omit empty/no-change windows, so the next stored
            // predecessor may advance past the prior stored sequence. Gaps
            // between native updates are checked by the feed adapter before
            // records reach this scanner; the persisted sparse chain can only
            // prove monotonicity.
            if predecessor < sequence || last < predecessor {
                failures += 1;
            }
            sequence = last;
        }
        if block.header.seq_last_processed_delta != u32::MAX {
            let Some(last) = block
                .header
                .seq_base
                .checked_add(u64::from(block.header.seq_last_processed_delta))
            else {
                failures += 1;
                continue;
            };
            if !payload.records.is_empty() && last < sequence {
                failures += 1;
            }
            previous = Some(last);
        }
    }
    failures
}

async fn run_record(config: RecorderConfig) -> Result<(), (u8, String)> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    let mut markets_remaining = config.markets.len();
    if config.recorder.daily_archive {
        let root = config.recorder.root.clone();
        let prune = config.recorder.daily_archive_prune;
        let grace = config.recorder.daily_archive_grace_secs;
        let cancel = shutdown_rx.clone();
        tasks.spawn(async move {
            daily_archive_loop(root, grace, prune, cancel).await;
            Ok::<(), String>(())
        });
    }
    for market in config.markets.clone() {
        let settings = config.recorder.clone();
        let meta = config.market_meta(&market);
        let root = settings.root.clone();
        let venue = market.venue.clone();
        let instrument = match market.instrument {
            InstrumentType::Spot => "spot",
            InstrumentType::Perpetual => "perpetual",
            InstrumentType::Future => "future",
        }
        .to_owned();
        let cancel = shutdown_rx.clone();
        tasks.spawn(async move {
            let venue_key = market.venue.to_ascii_lowercase();
            let feed = match venue_key.as_str() {
                "binance" | "binancespot" | "binance_spot" | "binancefuturesum"
                | "binance_futures" | "binancefuturescm" | "binance_coinm" => {
                    MarketFeed::Binance(Box::new(
                        BinanceConnectorFeed::connect(market.clone(), settings.clone(), cancel)
                            .await
                            .map_err(|error| {
                                format!("could not initialize {venue_key} feed: {error}")
                            })?,
                    ))
                }
                key if key.starts_with("bybit") => {
                    MarketFeed::Bybit(BybitFeed::connect(market.clone(), cancel).await.map_err(
                        |error| format!("could not initialize {venue_key} feed: {error}"),
                    )?)
                }
                key if key.starts_with("okx") => {
                    MarketFeed::Okx(OkxFeed::connect(market.clone(), cancel).await.map_err(
                        |error| format!("could not initialize {venue_key} feed: {error}"),
                    )?)
                }
                key if key.starts_with("hyperliquid") => {
                    if market.feed_mode != collector::book::FeedMode::Snapshot {
                        return Err("Hyperliquid l2Book requires snapshot feed mode".to_owned());
                    }
                    MarketFeed::Hyperliquid(
                        HyperliquidFeed::connect(market, cancel)
                            .await
                            .map_err(|error| {
                                format!("could not initialize {venue_key} feed: {error}")
                            })?,
                    )
                }
                _ => return Err("unsupported market venue".to_owned()),
            };
            run_market(meta, &root, &venue, &instrument, &settings, feed)
                .await
                .map_err(|error| {
                    tracing::error!(%venue, %instrument, %error, "market recorder stopped");
                    format!("market recorder stopped after a data or I/O error: {error}")
                })
        });
    }
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(io_error)?;
                let _ = shutdown_tx.send(true);
                while let Some(result) = tasks.join_next().await {
                    result.map_err(|_| (4, "market recorder task failed".into()))?
                        .map_err(|message| (4, message))?;
                }
                return Ok(());
            }
            result = tasks.join_next() => match result {
                Some(Ok(Ok(()))) => {
                    markets_remaining = markets_remaining.saturating_sub(1);
                    if markets_remaining == 0 {
                        let _ = shutdown_tx.send(true);
                        while tasks.join_next().await.is_some() {}
                        return Ok(());
                    }
                    continue;
                }
                Some(Ok(Err(message))) => {
                    let _ = shutdown_tx.send(true);
                    while tasks.join_next().await.is_some() {}
                    return Err((4, message));
                }
                Some(Err(_)) => {
                    let _ = shutdown_tx.send(true);
                    while tasks.join_next().await.is_some() {}
                    return Err((4, "market recorder task failed".into()));
                }
                None => return Ok(()),
            }
        }
    }
}

async fn daily_archive_loop(
    root: PathBuf,
    grace_secs: u64,
    prune: bool,
    mut cancel: watch::Receiver<bool>,
) {
    // Best-effort catch-up if the process starts after the previous midnight.
    if let Some(date) = Utc::now().date_naive().pred_opt() {
        archive_previous_date(&root, &date.to_string(), prune, true).await;
    }
    loop {
        let (wake, date) = next_daily_archive(Utc::now(), grace_secs);
        let wait = (wake - Utc::now())
            .to_std()
            .unwrap_or(std::time::Duration::from_secs(1));
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    return;
                }
            }
        }
        archive_previous_date(&root, &date.to_string(), prune, false).await;
    }
}

async fn archive_previous_date(root: &Path, date: &str, prune: bool, quiet: bool) {
    let root = root.to_path_buf();
    let archive_date = date.to_owned();
    let label = archive_date.clone();
    let result = tokio::task::spawn_blocking(move || {
        archive_daily(&root, &archive_date, None, prune, false)
    })
    .await;
    match result {
        Ok(Ok(report)) => eprintln!(
            "daily archive {}: files={} bytes={} sha256={} path={}",
            report.date,
            report.file_count,
            report.archive_bytes,
            report.archive_sha256,
            report.archive_path.display()
        ),
        Ok(Err(collector::book::ArchiveError::Empty(_)))
        | Ok(Err(collector::book::ArchiveError::Exists(_))) => {
            if !quiet {
                eprintln!("daily archive {label}: no data or archive already exists");
            }
        }
        Ok(Err(error)) => eprintln!("daily archive {label} failed: {error}"),
        Err(_) => eprintln!("daily archive {label} task failed"),
    }
}

fn inspect(
    root: &Path,
    date: Option<&str>,
    market: Option<&str>,
) -> Result<Vec<SegmentSummary>, (u8, String)> {
    matching_paths(root, date, market)
        .map_err(io_error)?
        .into_iter()
        .map(|path| {
            let reader = SegmentReader::open(&path).map_err(io_error)?;
            let blocks = reader.block_count();
            let mut readable = 0;
            let mut gaps = 0;
            let mut precision_rejects = 0u64;
            let mut gap_ms = 0u64;
            let mut gap_durations_ms = Vec::new();
            for i in 0..blocks {
                if let Ok(block) = reader.read_block(i) {
                    if block.header.kind != BlockKind::Data || decode_data_payload(&block).is_ok() {
                        readable += 1;
                    }
                    if block.header.kind == BlockKind::Gap {
                        gaps += 1;
                        if block.header.control_reason == GapReason::Precision as u16 {
                            precision_rejects = precision_rejects.saturating_add(1);
                        }
                        let duration_ms = block
                            .header
                            .block_end_exch_ns
                            .saturating_sub(block.header.block_start_exch_ns)
                            .max(0) as u64
                            / 1_000_000;
                        gap_ms = gap_ms.saturating_add(duration_ms);
                        gap_durations_ms.push(duration_ms);
                    }
                }
            }
            let directory = path
                .parent()
                .ok_or_else(|| (4, "segment path has no parent".to_owned()))?;
            let market_key = directory
                .parent()
                .and_then(|symbol_dir| {
                    let instrument_dir = symbol_dir.parent()?;
                    let venue_dir = instrument_dir.parent()?;
                    Some(format!(
                        "{}/{}/{}",
                        venue_dir.file_name()?.to_string_lossy(),
                        instrument_dir.file_name()?.to_string_lossy(),
                        symbol_dir.file_name()?.to_string_lossy(),
                    ))
                })
                .unwrap_or_default();
            Ok(SegmentSummary {
                file: path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned(),
                path,
                market_key,
                market_id: reader.header.market_id,
                venue_id: reader.header.venue_id,
                symbol: reader.header.symbol.clone(),
                feed_mode: match reader.header.feed_mode {
                    collector::book::FeedMode::Diff => "diff",
                    collector::book::FeedMode::Snapshot => "snapshot",
                }
                .to_owned(),
                depth_requested: reader.header.depth_requested,
                depth_available: reader.header.depth_available,
                grid_ms: reader.header.grid_ms,
                start_exch_ns: reader.header.segment_start_exch_ns,
                end_exch_ns: reader.header.segment_end_exch_ns,
                blocks,
                readable,
                corrupt: blocks - readable,
                gaps,
                precision_rejects,
                gap_ms,
                truncated_tail: reader.truncated_tail(),
                gap_durations_ms,
            })
        })
        .collect()
}

fn matching_paths(
    root: &Path,
    date: Option<&str>,
    market: Option<&str>,
) -> std::io::Result<Vec<PathBuf>> {
    let mut pending = vec![root.to_path_buf()];
    let mut found = Vec::new();
    while let Some(path) = pending.pop() {
        let entries = match fs::read_dir(&path) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let child = entry.path();
            if child.is_dir() {
                pending.push(child);
                continue;
            }
            if child.extension().is_some_and(|x| x == "tnmd")
                && date.is_none_or(|d| child.to_string_lossy().contains(&format!("date={d}")))
                && market.is_none_or(|m| child.to_string_lossy().contains(&format!("symbol={m}")))
            {
                found.push(child);
            }
        }
    }
    found.sort();
    Ok(found)
}

fn io_error(error: impl std::fmt::Display) -> (u8, String) {
    (4, error.to_string())
}
fn json_error(error: impl std::fmt::Display) -> (u8, String) {
    (4, error.to_string())
}
