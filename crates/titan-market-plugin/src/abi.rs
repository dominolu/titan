use std::mem::size_of;

pub const DEPTH_BATCH_EVENT: &str = "titan.market.DepthBatch";
pub const TRADE_BATCH_EVENT: &str = "titan.market.TradeBatch";
pub const BBO_EVENT: &str = "titan.market.Bbo";
pub const TICKER_EVENT: &str = "titan.market.Ticker";
pub const MARK_PRICE_EVENT: &str = "titan.market.MarkPrice";
pub const FUNDING_RATE_EVENT: &str = "titan.market.FundingRate";
pub const STREAM_STATE_CHANGED_EVENT: &str = "titan.market.StreamStateChanged";
pub const STREAM_INVALIDATED_EVENT: &str = "titan.market.StreamInvalidated";
pub const INSTRUMENT_CHANGED_EVENT: &str = "titan.market.InstrumentChanged";
pub const MARKET_EVENT_SCHEMA_VERSION: u32 = 1;

pub const MARKET_EVENT_TYPES: [&str; 9] = [
    DEPTH_BATCH_EVENT,
    TRADE_BATCH_EVENT,
    BBO_EVENT,
    TICKER_EVENT,
    MARK_PRICE_EVENT,
    FUNDING_RATE_EVENT,
    STREAM_STATE_CHANGED_EVENT,
    STREAM_INVALIDATED_EVENT,
    INSTRUMENT_CHANGED_EVENT,
];

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
