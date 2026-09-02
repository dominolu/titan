//! Shared translation from the connector crate's already-normalized feed records to Market ABI.
//!
//! Exchange protocol state is deliberately absent here. Snapshot boundaries, epochs, update
//! sequences and recovery decisions are supplied by the concrete venue stream.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use hftbacktest::types::{
    BUY_EVENT, DEPTH_BBO_EVENT, DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT, Event, LiveEvent, SELL_EVENT,
    TRADE_EVENT,
};
use titan_market_plugin::{
    AssetId, BBO_EVENT, ConnectorError, DEPTH_BATCH_EVENT, DepthItemV1, FUNDING_RATE_EVENT,
    MarketBatchHeaderV1, MarketConnectorContext, MarketDataKind, STREAM_INVALIDATED_EVENT,
    TRADE_BATCH_EVENT,
};
use titan_plugin_engine::TraceContext;

use crate::connector::{MarketStreamMetadata, NativeDepthLevels, NativeMarketBatch, PublishEvent};

#[derive(Clone, Copy)]
pub(crate) struct InstrumentUnits {
    pub(crate) price_tick: f64,
    pub(crate) quantity_lot: f64,
    price_decimal: Option<DecimalUnit>,
    quantity_decimal: Option<DecimalUnit>,
}

#[derive(Clone, Copy)]
struct DecimalUnit {
    decimals: u32,
    scale: u64,
    quantum: u64,
}

impl DecimalUnit {
    const POW10: [u64; 13] = [
        1,
        10,
        100,
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        1_000_000_000,
        10_000_000_000,
        100_000_000_000,
        1_000_000_000_000,
    ];

    fn from_f64(unit: f64) -> Option<Self> {
        if !unit.is_finite() || unit <= 0.0 {
            return None;
        }
        let mut factor = 1.0;
        for decimals in 0..=12 {
            let scaled = unit * factor;
            let rounded = scaled.round();
            if rounded >= 1.0 && (scaled - rounded).abs() <= scaled.abs().max(1.0) * 1e-12 {
                return Some(Self {
                    decimals,
                    scale: Self::POW10[decimals as usize],
                    quantum: rounded as u64,
                });
            }
            factor *= 10.0;
        }
        None
    }

    fn parse(self, value: &str, field: &str) -> Result<i64, ConnectorError> {
        let bytes = value.as_bytes();
        if bytes.is_empty() {
            return Err(ConnectorError::new(format!("empty {field}")));
        }
        let (index, negative) = match bytes[0] {
            b'-' => (1, true),
            b'+' => (1, false),
            _ => (0, false),
        };
        let unsigned = &bytes[index..];
        let decimal_at = unsigned
            .iter()
            .position(|value| *value == b'.')
            .unwrap_or(unsigned.len());
        let whole_bytes = &unsigned[..decimal_at];
        let fraction_bytes = if decimal_at < unsigned.len() {
            &unsigned[decimal_at + 1..]
        } else {
            &[]
        };
        if whole_bytes.is_empty() && fraction_bytes.is_empty() {
            return Err(ConnectorError::new(format!("invalid {field}")));
        }

        let parse_digits = |digits: &[u8]| -> Result<u64, ConnectorError> {
            let mut value = 0_u64;
            for &digit in digits {
                if !digit.is_ascii_digit() {
                    return Err(ConnectorError::new(format!("invalid {field}")));
                }
                let digit = u64::from(digit - b'0');
                if value > (u64::MAX - digit) / 10 {
                    return Err(ConnectorError::new(format!("{field} is out of range")));
                }
                value = value * 10 + digit;
            }
            Ok(value)
        };
        let whole = parse_digits(whole_bytes)?;
        let retained_fraction_digits = fraction_bytes.len().min(self.decimals as usize);
        let mut fraction = parse_digits(&fraction_bytes[..retained_fraction_digits])?;
        let discarded_fraction = &fraction_bytes[retained_fraction_digits..];
        if discarded_fraction.iter().any(|digit| *digit != b'0') {
            return Err(ConnectorError::new(format!(
                "{field} is not aligned to its instrument unit"
            )));
        }
        fraction = fraction
            .checked_mul(Self::POW10[self.decimals as usize - retained_fraction_digits])
            .ok_or_else(|| ConnectorError::new(format!("{field} is out of range")))?;
        let mantissa = whole
            .checked_mul(self.scale)
            .and_then(|value| value.checked_add(fraction))
            .ok_or_else(|| ConnectorError::new(format!("{field} is out of range")))?;
        if mantissa % self.quantum != 0 {
            return Err(ConnectorError::new(format!(
                "{field} is not aligned to its instrument unit"
            )));
        }
        let scaled = mantissa / self.quantum;
        if negative {
            if scaled > i64::MAX as u64 + 1 {
                return Err(ConnectorError::new(format!("{field} is out of range")));
            }
            Ok(-(scaled as i128) as i64)
        } else {
            i64::try_from(scaled)
                .map_err(|_| ConnectorError::new(format!("{field} is out of range")))
        }
    }
}

#[derive(Clone)]
pub(crate) struct MarketEventBridge {
    context: MarketConnectorContext,
    symbols: Arc<HashMap<String, AssetId>>,
    units: Arc<HashMap<AssetId, InstrumentUnits>>,
    active_kinds: Arc<Mutex<HashMap<AssetId, HashSet<MarketDataKind>>>>,
}

impl MarketEventBridge {
    pub(crate) fn new(
        context: MarketConnectorContext,
        symbols: Arc<HashMap<String, AssetId>>,
        active_kinds: Arc<Mutex<HashMap<AssetId, HashSet<MarketDataKind>>>>,
    ) -> Self {
        let units = context
            .instruments
            .iter()
            .map(|binding| {
                (
                    binding.asset_id,
                    InstrumentUnits {
                        price_tick: binding.price_tick,
                        quantity_lot: binding.quantity_lot,
                        price_decimal: DecimalUnit::from_f64(binding.price_tick),
                        quantity_decimal: DecimalUnit::from_f64(binding.quantity_lot),
                    },
                )
            })
            .collect();
        Self {
            context,
            symbols,
            units: Arc::new(units),
            active_kinds,
        }
    }

    pub(crate) fn publish(&self, value: &PublishEvent) -> Result<(), ConnectorError> {
        publish_event(
            &self.context,
            &self.symbols,
            &self.units,
            &self.active_kinds,
            value,
        )
    }

    pub(crate) fn publish_native(
        &self,
        batch: NativeMarketBatch<'_>,
    ) -> Result<(), ConnectorError> {
        match batch {
            NativeMarketBatch::Depth {
                symbol,
                bids,
                asks,
                exchange_ts,
                receive_ts,
                stream,
            } => self.publish_native_depth(symbol, bids, asks, exchange_ts, receive_ts, stream),
            NativeMarketBatch::Trade {
                symbol,
                price,
                quantity,
                sell,
                exchange_ts,
                receive_ts,
            } => self.publish_native_values(
                symbol,
                TRADE_BATCH_EVENT,
                MarketDataKind::Trades,
                3,
                exchange_ts,
                receive_ts,
                &[(price, quantity, if sell { 2 } else { 1 })],
            ),
            NativeMarketBatch::Bbo {
                symbol,
                bid_price,
                bid_quantity,
                ask_price,
                ask_quantity,
                exchange_ts,
                receive_ts,
            } => {
                let mut values = [(0.0, 0.0, 0); 2];
                let mut count = 0;
                if bid_price > 0.0 {
                    values[count] = (bid_price, bid_quantity, 1);
                    count += 1;
                }
                if ask_price > 0.0 {
                    values[count] = (ask_price, ask_quantity, 2);
                    count += 1;
                }
                self.publish_native_values(
                    symbol,
                    BBO_EVENT,
                    MarketDataKind::Bbo,
                    4,
                    exchange_ts,
                    receive_ts,
                    &values[..count],
                )
            }
        }
    }

    fn binding(&self, symbol: &str) -> Result<(AssetId, InstrumentUnits), ConnectorError> {
        let asset_id = self
            .symbols
            .get(symbol)
            .copied()
            .ok_or_else(|| ConnectorError::new(format!("unbound symbol {symbol}")))?;
        let units = self.units.get(&asset_id).copied().ok_or_else(|| {
            ConnectorError::new(format!("missing units for asset {}", asset_id.0))
        })?;
        Ok((asset_id, units))
    }

    fn publish_native_depth(
        &self,
        symbol: &str,
        bids: NativeDepthLevels<'_>,
        asks: NativeDepthLevels<'_>,
        exchange_ts: i64,
        receive_ts: i64,
        stream: MarketStreamMetadata,
    ) -> Result<(), ConnectorError> {
        let (asset_id, units) = self.binding(symbol)?;
        if !is_active(&self.active_kinds, asset_id, MarketDataKind::Depth) {
            return Ok(());
        }
        let count = bids
            .len()
            .checked_add(asks.len())
            .ok_or_else(|| ConnectorError::new("depth item count overflow"))?;
        let item_count =
            u16::try_from(count).map_err(|_| ConnectorError::new("too many depth items"))?;
        let payload_length = MarketBatchHeaderV1::ENCODED_LEN
            .checked_add(
                count
                    .checked_mul(DepthItemV1::ENCODED_LEN)
                    .ok_or_else(|| ConnectorError::new("depth payload overflow"))?,
            )
            .ok_or_else(|| ConnectorError::new("depth payload overflow"))?;
        let header = MarketBatchHeaderV1 {
            asset_id: asset_id.0,
            kind: if stream.snapshot { 1 } else { 2 },
            flags: u16::from(stream.snapshot),
            item_count,
            reserved: 0,
            stream_epoch: stream.epoch,
            first_update_sequence: stream.first_update_sequence,
            last_update_sequence: stream.last_update_sequence,
            exchange_ts,
            receive_ts,
        };
        self.context.event_publisher.publish_market_batch(
            DEPTH_BATCH_EVENT,
            payload_length,
            asset_id,
            exchange_ts,
            receive_ts,
            TraceContext::default(),
            |payload| {
                header
                    .encode_into_slice(payload)
                    .map_err(ConnectorError::new)?;
                let mut offset = MarketBatchHeaderV1::ENCODED_LEN;
                for (side, levels) in [(1, bids), (2, asks)] {
                    let mut encode_level =
                        |price: &str, quantity: &str| -> Result<(), ConnectorError> {
                            let price_ticks = units.price_decimal.map_or_else(
                                || {
                                    scaled(
                                        price.parse::<f64>().map_err(|error| {
                                            ConnectorError::new(error.to_string())
                                        })?,
                                        units.price_tick,
                                        "price",
                                    )
                                },
                                |unit| unit.parse(price, "price"),
                            )?;
                            let quantity_lots = units.quantity_decimal.map_or_else(
                                || {
                                    scaled(
                                        quantity.parse::<f64>().map_err(|error| {
                                            ConnectorError::new(error.to_string())
                                        })?,
                                        units.quantity_lot,
                                        "quantity",
                                    )
                                },
                                |unit| unit.parse(quantity, "quantity"),
                            )?;
                            let end = offset + DepthItemV1::ENCODED_LEN;
                            let output = &mut payload[offset..end];
                            output[0..8].copy_from_slice(&price_ticks.to_le_bytes());
                            output[8..16].copy_from_slice(&quantity_lots.to_le_bytes());
                            output[16] = side;
                            output[17] = if quantity_lots == 0 { 2 } else { 1 };
                            output[18..24].fill(0);
                            offset = end;
                            Ok(())
                        };
                    match levels {
                        NativeDepthLevels::Owned(levels) => {
                            for (price, quantity) in levels {
                                encode_level(price, quantity)?;
                            }
                        }
                        NativeDepthLevels::Borrowed(levels) => {
                            for &(price, quantity) in levels {
                                encode_level(price, quantity)?;
                            }
                        }
                    }
                }
                Ok(())
            },
        )
    }

    fn publish_native_values(
        &self,
        symbol: &str,
        event_type: &str,
        requested_kind: MarketDataKind,
        kind: u16,
        exchange_ts: i64,
        receive_ts: i64,
        values: &[(f64, f64, u8)],
    ) -> Result<(), ConnectorError> {
        if values.is_empty() {
            return Ok(());
        }
        let (asset_id, units) = self.binding(symbol)?;
        if !is_active(&self.active_kinds, asset_id, requested_kind) {
            return Ok(());
        }
        let item_count = u16::try_from(values.len())
            .map_err(|_| ConnectorError::new("too many market items"))?;
        let payload_length =
            MarketBatchHeaderV1::ENCODED_LEN + values.len() * DepthItemV1::ENCODED_LEN;
        let header = MarketBatchHeaderV1 {
            asset_id: asset_id.0,
            kind,
            flags: 0,
            item_count,
            reserved: 0,
            stream_epoch: 0,
            first_update_sequence: 0,
            last_update_sequence: 0,
            exchange_ts,
            receive_ts,
        };
        self.context.event_publisher.publish_market_batch(
            event_type,
            payload_length,
            asset_id,
            exchange_ts,
            receive_ts,
            TraceContext::default(),
            |payload| {
                header
                    .encode_into_slice(payload)
                    .map_err(ConnectorError::new)?;
                let mut offset = MarketBatchHeaderV1::ENCODED_LEN;
                for &(price, quantity, side) in values {
                    let item = DepthItemV1 {
                        price_ticks: scaled(price, units.price_tick, "price")?,
                        quantity_lots: scaled(quantity, units.quantity_lot, "quantity")?,
                        side,
                        action: if quantity == 0.0 { 2 } else { 1 },
                        reserved: [0; 6],
                    };
                    let end = offset + DepthItemV1::ENCODED_LEN;
                    item.encode_into_slice(&mut payload[offset..end])
                        .map_err(ConnectorError::new)?;
                    offset = end;
                }
                Ok(())
            },
        )
    }
}

fn publish_event(
    context: &MarketConnectorContext,
    symbols: &HashMap<String, AssetId>,
    units: &HashMap<AssetId, InstrumentUnits>,
    active_kinds: &Mutex<HashMap<AssetId, HashSet<MarketDataKind>>>,
    value: &PublishEvent,
) -> Result<(), ConnectorError> {
    match value {
        PublishEvent::StreamInvalidated { symbol, epoch } => {
            let asset_id = symbols
                .get(symbol)
                .copied()
                .ok_or_else(|| ConnectorError::new(format!("unbound symbol {symbol}")))?;
            if !is_active(active_kinds, asset_id, MarketDataKind::Depth) {
                return Ok(());
            }
            let mut payload = Vec::with_capacity(12);
            payload.extend_from_slice(&asset_id.0.to_le_bytes());
            payload.extend_from_slice(&epoch.to_le_bytes());
            context
                .event_publisher
                .publish_control(STREAM_INVALIDATED_EVENT, &payload, TraceContext::default())
                .map_err(|error| ConnectorError::new(error.to_string()))
        }
        PublishEvent::FeedBatch {
            symbol,
            events,
            stream,
        } => publish_events(
            context,
            symbols,
            units,
            active_kinds,
            symbol,
            events,
            *stream,
        ),
        PublishEvent::LiveEvent(LiveEvent::Feed { symbol, event }) => publish_events(
            context,
            symbols,
            units,
            active_kinds,
            symbol,
            std::slice::from_ref(event),
            None,
        ),
        PublishEvent::LiveEvent(LiveEvent::Funding {
            symbol,
            funding_rate,
            exch_ts,
            ..
        }) => publish_funding(
            context,
            symbols,
            active_kinds,
            symbol,
            *funding_rate,
            *exch_ts,
        ),
        PublishEvent::LiveEvent(LiveEvent::Error(error)) => {
            Err(ConnectorError::new(format!("connector error: {error:?}")))
        }
        _ => Ok(()),
    }
}

fn publish_events(
    context: &MarketConnectorContext,
    symbols: &HashMap<String, AssetId>,
    units: &HashMap<AssetId, InstrumentUnits>,
    active_kinds: &Mutex<HashMap<AssetId, HashSet<MarketDataKind>>>,
    symbol: &str,
    events: &[Event],
    stream: Option<MarketStreamMetadata>,
) -> Result<(), ConnectorError> {
    let asset_id = symbols
        .get(symbol)
        .copied()
        .ok_or_else(|| ConnectorError::new(format!("unbound symbol {symbol}")))?;
    let units = units
        .get(&asset_id)
        .ok_or_else(|| ConnectorError::new(format!("missing units for asset {}", asset_id.0)))?;
    let mut counts = [0_usize; 3];
    let mut exchange_ts = 0;
    let mut receive_ts = 0;
    for event in events {
        let Some(index) = event_batch_index(event) else {
            continue;
        };
        exchange_ts = exchange_ts.max(event.exch_ts);
        receive_ts = receive_ts.max(event.local_ts);
        counts[index] += 1;
    }

    for (index, event_type, kind, item_count) in [
        (
            0,
            DEPTH_BATCH_EVENT,
            if stream.is_some_and(|value| value.snapshot) {
                1
            } else {
                2
            },
            counts[0],
        ),
        (1, TRADE_BATCH_EVENT, 3, counts[1]),
        (2, BBO_EVENT, 4, counts[2]),
    ] {
        if item_count == 0 {
            continue;
        }
        let requested_kind = match event_type {
            DEPTH_BATCH_EVENT => MarketDataKind::Depth,
            TRADE_BATCH_EVENT => MarketDataKind::Trades,
            BBO_EVENT => MarketDataKind::Bbo,
            _ => continue,
        };
        if !is_active(active_kinds, asset_id, requested_kind) {
            continue;
        }
        let coordinates = if event_type == DEPTH_BATCH_EVENT {
            Some(stream.ok_or_else(|| {
                ConnectorError::new("depth batch is missing connector-owned stream metadata")
            })?)
        } else {
            None
        };
        let item_count = u16::try_from(item_count)
            .map_err(|_| ConnectorError::new("too many market batch items"))?;
        let payload_length = MarketBatchHeaderV1::ENCODED_LEN
            .checked_add(usize::from(item_count) * DepthItemV1::ENCODED_LEN)
            .ok_or_else(|| ConnectorError::new("market batch payload overflow"))?;
        let header = MarketBatchHeaderV1 {
            asset_id: asset_id.0,
            kind,
            flags: u16::from(coordinates.is_some_and(|value| value.snapshot)),
            item_count,
            reserved: 0,
            stream_epoch: coordinates.map_or(0, |value| value.epoch),
            first_update_sequence: coordinates.map_or(0, |value| value.first_update_sequence),
            last_update_sequence: coordinates.map_or(0, |value| value.last_update_sequence),
            exchange_ts,
            receive_ts,
        };
        context.event_publisher.publish_market_batch(
            event_type,
            payload_length,
            asset_id,
            exchange_ts,
            receive_ts,
            TraceContext::default(),
            |payload| {
                header
                    .encode_into_slice(payload)
                    .map_err(ConnectorError::new)?;
                let mut offset = MarketBatchHeaderV1::ENCODED_LEN;
                for event in events
                    .iter()
                    .filter(|event| event_batch_index(event) == Some(index))
                {
                    let end = offset + DepthItemV1::ENCODED_LEN;
                    encode_item(event, units)?
                        .encode_into_slice(&mut payload[offset..end])
                        .map_err(ConnectorError::new)?;
                    offset = end;
                }
                debug_assert_eq!(offset, payload.len());
                Ok(())
            },
        )?;
    }
    Ok(())
}

fn event_batch_index(event: &Event) -> Option<usize> {
    match event.ev & 0xff {
        DEPTH_SNAPSHOT_EVENT | DEPTH_EVENT => Some(0),
        TRADE_EVENT => Some(1),
        DEPTH_BBO_EVENT => Some(2),
        _ => None,
    }
}

fn encode_item(event: &Event, units: &InstrumentUnits) -> Result<DepthItemV1, ConnectorError> {
    Ok(DepthItemV1 {
        price_ticks: scaled(event.px, units.price_tick, "price")?,
        quantity_lots: scaled(event.qty, units.quantity_lot, "quantity")?,
        side: if event.is(BUY_EVENT) {
            1
        } else if event.is(SELL_EVENT) {
            2
        } else {
            0
        },
        action: if event.qty == 0.0 { 2 } else { 1 },
        reserved: [0; 6],
    })
}

fn publish_funding(
    context: &MarketConnectorContext,
    symbols: &HashMap<String, AssetId>,
    active_kinds: &Mutex<HashMap<AssetId, HashSet<MarketDataKind>>>,
    symbol: &str,
    funding_rate: f64,
    exchange_ts: i64,
) -> Result<(), ConnectorError> {
    let asset_id = symbols
        .get(symbol)
        .copied()
        .ok_or_else(|| ConnectorError::new(format!("unbound symbol {symbol}")))?;
    if !is_active(active_kinds, asset_id, MarketDataKind::FundingRate) {
        return Ok(());
    }
    let mut payload = Vec::with_capacity(20);
    payload.extend_from_slice(&asset_id.0.to_le_bytes());
    payload.extend_from_slice(&funding_rate.to_le_bytes());
    payload.extend_from_slice(&exchange_ts.to_le_bytes());
    context
        .event_publisher
        .publish_market(
            FUNDING_RATE_EVENT,
            &payload,
            asset_id,
            exchange_ts,
            0,
            TraceContext::default(),
        )
        .map_err(|error| ConnectorError::new(error.to_string()))
}

fn is_active(
    active_kinds: &Mutex<HashMap<AssetId, HashSet<MarketDataKind>>>,
    asset_id: AssetId,
    kind: MarketDataKind,
) -> bool {
    active_kinds
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&asset_id)
        .is_some_and(|kinds| kinds.contains(&kind))
}

pub(crate) fn scaled(value: f64, unit: f64, field: &str) -> Result<i64, ConnectorError> {
    if !value.is_finite() || !unit.is_finite() || unit <= 0.0 {
        return Err(ConnectorError::new(
            "non-finite value or invalid instrument unit",
        ));
    }
    let normalized = value / unit;
    if !normalized.is_finite() || normalized.abs() > i64::MAX as f64 {
        return Err(ConnectorError::new(format!("{field} is out of range")));
    }
    let rounded = normalized.round();
    let tolerance = normalized.abs().max(1.0) * 1e-9;
    if (normalized - rounded).abs() > tolerance {
        return Err(ConnectorError::new(format!(
            "{field} is not aligned to its instrument unit"
        )));
    }
    Ok(rounded as i64)
}

#[cfg(test)]
mod tests {
    use super::{DecimalUnit, scaled};

    #[test]
    fn decimal_scaling_rejects_invalid_values() {
        assert_eq!(scaled(1.25, 0.01, "price").unwrap(), 125);
        assert_eq!(scaled(0.015, 0.001, "quantity").unwrap(), 15);
        assert!(scaled(1.25, 0.03, "price").is_err());
        assert!(scaled(f64::NAN, 0.01, "price").is_err());
        assert!(scaled(f64::INFINITY, 0.01, "price").is_err());
    }

    #[test]
    fn decimal_unit_parses_exchange_numbers_without_float_roundtrip() {
        let tick = DecimalUnit::from_f64(0.1).unwrap();
        let lot = DecimalUnit::from_f64(0.0001).unwrap();
        assert_eq!(tick.parse("12345.60000000", "price").unwrap(), 123_456);
        assert_eq!(lot.parse("0.01230000", "quantity").unwrap(), 123);
        assert_eq!(lot.parse("0.00000000", "quantity").unwrap(), 0);
        assert!(tick.parse("1.25", "price").is_err());
        assert!(lot.parse("1e-4", "quantity").is_err());
    }
}
