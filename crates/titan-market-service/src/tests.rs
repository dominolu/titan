use titan_runtime_abi::{BAR_COMPLETE, Bar};

use crate::*;

#[test]
fn abi_has_stable_little_endian_encoding() {
    let header = MarketBatchHeaderV1 {
        asset_id: 7,
        ..Default::default()
    };
    let item = DepthItemV1 {
        price_ticks: -2,
        quantity_lots: 3,
        side: 1,
        action: 2,
        ..Default::default()
    };
    let payload = encode_depth_batch(header, &[item]).unwrap();
    assert_eq!(
        payload.len(),
        MarketBatchHeaderV1::ENCODED_LEN + DepthItemV1::ENCODED_LEN
    );
    assert_eq!(&payload[..4], &7_u32.to_le_bytes());
    assert_eq!(&payload[8..10], &1_u16.to_le_bytes());
    assert_eq!(&payload[52..60], &(-2_i64).to_le_bytes());

    let mut direct = vec![0_u8; payload.len()];
    MarketBatchHeaderV1 {
        item_count: 1,
        ..header
    }
    .encode_into_slice(&mut direct)
    .unwrap();
    item.encode_into_slice(&mut direct[MarketBatchHeaderV1::ENCODED_LEN..])
        .unwrap();
    assert_eq!(direct, payload);
}

#[test]
fn closed_bar_batch_v1_round_trips_and_rejects_invalid_bars() {
    let batch = BarBatchV1 {
        timeframe_ns: 60,
        close_ts: 120,
        items: vec![BarRecordV1 {
            asset_id: 7,
            bar: Bar {
                open_ts: 60,
                close_ts: 120,
                open: 1.0,
                high: 3.0,
                low: 0.5,
                close: 2.0,
                volume: 4.0,
                quote_volume: 8.0,
                buy_volume: 2.5,
                trade_count: 9,
                flags: BAR_COMPLETE,
            },
        }],
    };
    let encoded = batch.encode().unwrap();
    assert_eq!(encoded.len(), BarBatchV1::HEADER_LEN + BarBatchV1::ITEM_LEN);
    assert_eq!(BarBatchV1::decode(&encoded).unwrap(), batch);

    let mut partial = batch.clone();
    partial.items[0].bar.flags = titan_runtime_abi::BAR_PARTIAL;
    assert!(partial.encode().is_err());
    let mut mismatched = batch;
    mismatched.items[0].bar.close_ts += 1;
    assert!(mismatched.encode().is_err());
}
