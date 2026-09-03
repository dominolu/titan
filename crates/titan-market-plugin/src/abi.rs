use std::mem::size_of;

use titan_runtime_abi::Bar;

pub const DEPTH_BATCH_EVENT: &str = "titan.market.DepthBatch";
pub const TRADE_BATCH_EVENT: &str = "titan.market.TradeBatch";
pub const BBO_EVENT: &str = "titan.market.Bbo";
pub const TICKER_EVENT: &str = "titan.market.Ticker";
pub const MARK_PRICE_EVENT: &str = "titan.market.MarkPrice";
pub const FUNDING_RATE_EVENT: &str = "titan.market.FundingRate";
pub const STREAM_STATE_CHANGED_EVENT: &str = "titan.market.StreamStateChanged";
pub const STREAM_INVALIDATED_EVENT: &str = "titan.market.StreamInvalidated";
pub const INSTRUMENT_CHANGED_EVENT: &str = "titan.market.InstrumentChanged";
pub const BAR_BATCH_EVENT: &str = "titan.market.BarBatch";
pub const MARKET_EVENT_SCHEMA_VERSION: u32 = 1;

pub const MARKET_EVENT_TYPES: [&str; 10] = [
    DEPTH_BATCH_EVENT,
    TRADE_BATCH_EVENT,
    BBO_EVENT,
    TICKER_EVENT,
    MARK_PRICE_EVENT,
    FUNDING_RATE_EVENT,
    STREAM_STATE_CHANGED_EVENT,
    STREAM_INVALIDATED_EVENT,
    INSTRUMENT_CHANGED_EVENT,
    BAR_BATCH_EVENT,
];

/// One closed-timeframe batch. All items share `timeframe_ns` and `close_ts`, matching the
/// strategy ABI rule that a same-timeframe close invokes `on_bar` exactly once.
#[derive(Clone, Debug, PartialEq)]
pub struct BarBatchV1 {
    pub timeframe_ns: i64,
    pub close_ts: i64,
    pub items: Vec<BarRecordV1>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BarRecordV1 {
    pub asset_id: u32,
    pub bar: Bar,
}

impl BarBatchV1 {
    pub const HEADER_LEN: usize = 24;
    pub const ITEM_LEN: usize = 96;

    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        if self.timeframe_ns <= 0 {
            return Err("bar timeframe must be positive");
        }
        let item_count = u32::try_from(self.items.len()).map_err(|_| "too many bar items")?;
        let payload_len = Self::HEADER_LEN
            .checked_add(
                self.items
                    .len()
                    .checked_mul(Self::ITEM_LEN)
                    .ok_or("payload overflow")?,
            )
            .ok_or("payload overflow")?;
        let mut output = Vec::with_capacity(payload_len);
        output.extend_from_slice(&item_count.to_le_bytes());
        output.extend_from_slice(&0_u32.to_le_bytes());
        output.extend_from_slice(&self.timeframe_ns.to_le_bytes());
        output.extend_from_slice(&self.close_ts.to_le_bytes());
        for item in &self.items {
            if !item.bar.is_complete()
                || item.bar.close_ts != self.close_ts
                || item.bar.close_ts - item.bar.open_ts != self.timeframe_ns
            {
                return Err("bar batch contains an incomplete or mismatched item");
            }
            output.extend_from_slice(&item.asset_id.to_le_bytes());
            output.extend_from_slice(&0_u32.to_le_bytes());
            output.extend_from_slice(&item.bar.open_ts.to_le_bytes());
            output.extend_from_slice(&item.bar.close_ts.to_le_bytes());
            for value in [
                item.bar.open,
                item.bar.high,
                item.bar.low,
                item.bar.close,
                item.bar.volume,
                item.bar.quote_volume,
                item.bar.buy_volume,
            ] {
                output.extend_from_slice(&value.to_le_bytes());
            }
            output.extend_from_slice(&item.bar.trade_count.to_le_bytes());
            output.extend_from_slice(&item.bar.flags.to_le_bytes());
        }
        Ok(output)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        if payload.len() < Self::HEADER_LEN {
            return Err("bar batch header is truncated");
        }
        let item_count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        if payload[4..8] != [0; 4] {
            return Err("bar batch reserved field is non-zero");
        }
        let timeframe_ns = i64::from_le_bytes(payload[8..16].try_into().unwrap());
        let close_ts = i64::from_le_bytes(payload[16..24].try_into().unwrap());
        let expected = Self::HEADER_LEN
            .checked_add(
                item_count
                    .checked_mul(Self::ITEM_LEN)
                    .ok_or("payload overflow")?,
            )
            .ok_or("payload overflow")?;
        if timeframe_ns <= 0 || payload.len() != expected {
            return Err("bar batch length or timeframe is invalid");
        }
        let mut items = Vec::with_capacity(item_count);
        for index in 0..item_count {
            let offset = Self::HEADER_LEN + index * Self::ITEM_LEN;
            let bytes = &payload[offset..offset + Self::ITEM_LEN];
            if bytes[4..8] != [0; 4] {
                return Err("bar item reserved field is non-zero");
            }
            let mut cursor = 8;
            let mut next_i64 = || {
                let value = i64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                value
            };
            let open_ts = next_i64();
            let item_close_ts = next_i64();
            let mut next_f64 = || {
                let value = f64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                value
            };
            let bar = Bar {
                open_ts,
                close_ts: item_close_ts,
                open: next_f64(),
                high: next_f64(),
                low: next_f64(),
                close: next_f64(),
                volume: next_f64(),
                quote_volume: next_f64(),
                buy_volume: next_f64(),
                trade_count: u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap()),
                flags: u64::from_le_bytes(bytes[cursor + 8..cursor + 16].try_into().unwrap()),
            };
            if !bar.is_complete()
                || bar.close_ts != close_ts
                || bar.close_ts - bar.open_ts != timeframe_ns
            {
                return Err("bar batch contains an incomplete or mismatched item");
            }
            items.push(BarRecordV1 {
                asset_id: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
                bar,
            });
        }
        Ok(Self {
            timeframe_ns,
            close_ts,
            items,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct MarketBatchHeaderV1 {
    pub asset_id: u32,
    pub kind: u16,
    pub flags: u16,
    pub item_count: u16,
    pub reserved: u16,
    pub stream_epoch: u64,
    pub first_update_sequence: u64,
    pub last_update_sequence: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct DepthItemV1 {
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub side: u8,
    pub action: u8,
    pub reserved: [u8; 6],
}

impl MarketBatchHeaderV1 {
    pub const ENCODED_LEN: usize = 52;

    pub fn encode_into(self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.asset_id.to_le_bytes());
        output.extend_from_slice(&self.kind.to_le_bytes());
        output.extend_from_slice(&self.flags.to_le_bytes());
        output.extend_from_slice(&self.item_count.to_le_bytes());
        output.extend_from_slice(&self.reserved.to_le_bytes());
        output.extend_from_slice(&self.stream_epoch.to_le_bytes());
        output.extend_from_slice(&self.first_update_sequence.to_le_bytes());
        output.extend_from_slice(&self.last_update_sequence.to_le_bytes());
        output.extend_from_slice(&self.exchange_ts.to_le_bytes());
        output.extend_from_slice(&self.receive_ts.to_le_bytes());
    }

    #[inline]
    pub fn encode_into_slice(self, output: &mut [u8]) -> Result<(), &'static str> {
        let output = output
            .get_mut(..Self::ENCODED_LEN)
            .ok_or("market batch header buffer is too small")?;
        output[0..4].copy_from_slice(&self.asset_id.to_le_bytes());
        output[4..6].copy_from_slice(&self.kind.to_le_bytes());
        output[6..8].copy_from_slice(&self.flags.to_le_bytes());
        output[8..10].copy_from_slice(&self.item_count.to_le_bytes());
        output[10..12].copy_from_slice(&self.reserved.to_le_bytes());
        output[12..20].copy_from_slice(&self.stream_epoch.to_le_bytes());
        output[20..28].copy_from_slice(&self.first_update_sequence.to_le_bytes());
        output[28..36].copy_from_slice(&self.last_update_sequence.to_le_bytes());
        output[36..44].copy_from_slice(&self.exchange_ts.to_le_bytes());
        output[44..52].copy_from_slice(&self.receive_ts.to_le_bytes());
        Ok(())
    }
}

impl DepthItemV1 {
    pub const ENCODED_LEN: usize = 24;

    pub fn encode_into(self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.price_ticks.to_le_bytes());
        output.extend_from_slice(&self.quantity_lots.to_le_bytes());
        output.push(self.side);
        output.push(self.action);
        output.extend_from_slice(&self.reserved);
    }

    #[inline]
    pub fn encode_into_slice(self, output: &mut [u8]) -> Result<(), &'static str> {
        let output = output
            .get_mut(..Self::ENCODED_LEN)
            .ok_or("depth item buffer is too small")?;
        output[0..8].copy_from_slice(&self.price_ticks.to_le_bytes());
        output[8..16].copy_from_slice(&self.quantity_lots.to_le_bytes());
        output[16] = self.side;
        output[17] = self.action;
        output[18..24].copy_from_slice(&self.reserved);
        Ok(())
    }
}

pub fn encode_depth_batch(
    mut header: MarketBatchHeaderV1,
    items: &[DepthItemV1],
) -> Result<Vec<u8>, &'static str> {
    header.item_count = u16::try_from(items.len()).map_err(|_| "too many depth items")?;
    let payload_len = MarketBatchHeaderV1::ENCODED_LEN
        .checked_add(
            items
                .len()
                .checked_mul(DepthItemV1::ENCODED_LEN)
                .ok_or("payload overflow")?,
        )
        .ok_or("payload overflow")?;
    let mut payload = Vec::with_capacity(payload_len);
    header.encode_into(&mut payload);
    for item in items {
        item.encode_into(&mut payload);
    }
    debug_assert_eq!(payload.len(), payload_len);
    Ok(payload)
}

const _: () = assert!(size_of::<MarketBatchHeaderV1>() >= MarketBatchHeaderV1::ENCODED_LEN);
const _: () = assert!(size_of::<DepthItemV1>() == DepthItemV1::ENCODED_LEN);
