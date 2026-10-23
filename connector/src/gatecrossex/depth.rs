//! Common L2 frames borrow price/quantity strings from the websocket frame until publication
//! returns. Escaped strings or other channel schemas use the compatible generic decoder.
use super::{Desired, rest::*};
use crate::{
    api::ApiError,
    connector::{MarketStreamMetadata, NativeDepthLevels, NativeMarketBatch, PublishSender},
    decimal::DecimalMultiplier,
};
use serde::Deserialize;
use smallvec::SmallVec;
use std::{collections::HashMap, sync::Mutex};
use titan_market_service::MarketDataKind;

type Levels<'a> = SmallVec<[(&'a str, &'a str); 16]>;
#[derive(Deserialize)]
struct Depth<'a> {
    #[serde(borrow)]
    s: &'a str,
    ts: Option<i64>,
    #[serde(borrow)]
    b: Levels<'a>,
    #[serde(borrow)]
    a: Levels<'a>,
}
#[derive(Deserialize)]
struct Frame<'a> {
    #[serde(borrow)]
    event: &'a str,
    #[serde(borrow)]
    channel: &'a str,
    #[serde(borrow)]
    result: Depth<'a>,
    time_ms: Option<i64>,
    error: Option<serde::de::IgnoredAny>,
}
pub(super) fn publish(
    raw: &str,
    desired: &Mutex<Desired>,
    tx: &PublishSender,
    epoch: u64,
    multipliers: &HashMap<String, DecimalMultiplier>,
    receive_ns: i64,
) -> Result<bool, ApiError> {
    // Only a dispatch hint: the typed decoder still validates the actual channel and payload.
    // Avoid a failed full Depth deserialize for every BBO/trade/funding frame. Escaped channel
    // strings intentionally use the compatible generic path.
    if !raw.contains("\"order_book_") {
        return Ok(false);
    }
    let Ok(frame) = serde_json::from_str::<Frame<'_>>(raw) else {
        return Ok(false);
    };
    if frame.event != "update"
        || frame
            .channel
            .strip_prefix("order_book_")
            .and_then(|s| s.parse::<u32>().ok())
            .is_none()
    {
        return Ok(false);
    }
    if frame.error.is_some() {
        return Err(protocol("L2 server error"));
    }
    let depth = frame.result;
    let subscribed = desired
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .market
        .get(depth.s)
        .ok_or_else(|| protocol("unqualified or unbound public symbol"))?
        .contains(&MarketDataKind::Depth);
    if !subscribed {
        return Ok(true);
    }
    let multiplier = *multipliers
        .get(depth.s)
        .ok_or_else(|| protocol("missing public quantity multiplier"))?;
    // Preserve the generic decoder's finite/nonnegative validation even for custom publishers.
    for (price, quantity) in depth.b.iter().chain(depth.a.iter()) {
        let p = price
            .parse::<f64>()
            .map_err(|_| protocol("invalid depth price"))?;
        let q = quantity
            .parse::<f64>()
            .map_err(|_| protocol("invalid depth quantity"))?;
        if !p.is_finite() || p <= 0.0 || !q.is_finite() || q < 0.0 {
            return Err(protocol("invalid depth price or quantity"));
        }
    }
    let ms = depth
        .ts
        .or(frame.time_ms)
        .ok_or_else(|| protocol("missing depth timestamp"))?;
    let exchange_ts = ms
        .checked_mul(1_000_000)
        .ok_or_else(|| protocol("timestamp overflow"))?;
    Ok(tx.try_send_native_market(NativeMarketBatch::Depth {
        symbol: depth.s,
        bids: NativeDepthLevels::ScaledBorrowed {
            levels: depth.b.as_slice(),
            quantity_multiplier: multiplier,
        },
        asks: NativeDepthLevels::ScaledBorrowed {
            levels: depth.a.as_slice(),
            quantity_multiplier: multiplier,
        },
        exchange_ts,
        receive_ts: receive_ns,
        stream: MarketStreamMetadata {
            epoch,
            first_update_sequence: 0,
            last_update_sequence: 0,
            snapshot: true,
        },
    }))
}
