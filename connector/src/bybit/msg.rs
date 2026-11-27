//! Bybit V5 public market data wire types.
//!
//! Only what a market stream reads is modelled here: the envelope every frame shares, the
//! orderbook, trade and ticker payloads, and the REST snapshot. Number fields arrive as text on
//! Bybit and are parsed by the caller, so nothing in this module decides what a malformed value
//! means.

use serde::Deserialize;
use serde::de::DeserializeOwned;

/// The envelope of every V5 stream frame.
///
/// A subscription acknowledgement carries `success: true` and no `topic`; a rejection carries
/// `success: false` with `ret_msg`; a data frame carries `topic`, a `type` and `data`. The payload
/// stays a [`serde_json::Value`] until the topic decides its type, because one connection carries
/// orderbook, trade and ticker frames whose payloads share nothing.
#[derive(Deserialize, Debug, Clone)]
pub struct StreamEnvelope {
    pub topic: Option<String>,
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
    /// Exchange time of a data frame, in milliseconds.
    #[serde(default)]
    pub ts: Option<i64>,
    #[serde(default)]
    pub success: Option<bool>,
    #[serde(rename = "ret_msg", default)]
    pub ret_msg: Option<String>,
    #[serde(default)]
    pub op: Option<String>,
    #[serde(default)]
    pub id: Option<String>,
}

impl StreamEnvelope {
    /// Decodes the frame's payload as `T`.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.data.clone().unwrap_or(serde_json::Value::Null))
    }
}

/// An orderbook frame: `orderbook.{depth}.{symbol}`.
///
/// `type` in the envelope is `snapshot` for the whole book a (re)subscription answers with and
/// `delta` for an increment. `u` is the update id that increments by one per delta for the life of
/// the symbol's orderbook, including across the reconnect that starts it again — which is what
/// makes it usable as a continuity check.
#[derive(Deserialize, Debug, Clone)]
pub struct OrderbookData {
    /// Symbol as the venue echoes it.
    #[serde(default)]
    pub s: String,
    /// `[price, quantity]` rows, best bid first.
    #[serde(default)]
    pub b: Vec<Vec<String>>,
    /// `[price, quantity]` rows, best ask first.
    #[serde(default)]
    pub a: Vec<Vec<String>>,
    /// Update id.
    #[serde(default)]
    pub u: i64,
    /// Cross sequence; not published for every delta.
    #[serde(default)]
    pub seq: i64,
    /// Continuity id; a `1` marks the first frame after a reconnect, where `u` restarts below the
    /// value the stream had reached.
    #[serde(default)]
    pub cts: Option<i64>,
}

/// One trade on `publicTrade.{symbol}`.
#[derive(Deserialize, Debug, Clone)]
pub struct Trade {
    #[serde(rename = "T", default)]
    pub time_ms: i64,
    /// `Buy` is the taker buying, `Sell` the taker selling.
    #[serde(rename = "S", default)]
    pub side: String,
    #[serde(default)]
    pub s: String,
    #[serde(default)]
    pub p: String,
    #[serde(default)]
    pub v: String,
    /// Trade id; the venue reuses few values, so this is also the record's identity.
    #[serde(rename = "i", default)]
    pub id: String,
}

/// A `tickers.{symbol}` frame: last price, funding and, on derivatives, open interest.
#[derive(Deserialize, Debug, Clone)]
pub struct Ticker {
    #[serde(default)]
    pub symbol: String,
    #[serde(rename = "markPrice", default)]
    pub mark_price: Option<String>,
    #[serde(rename = "indexPrice", default)]
    pub index_price: Option<String>,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: Option<String>,
    #[serde(rename = "nextFundingTime", default)]
    pub next_funding_time: Option<String>,
    #[serde(rename = "openInterest", default)]
    pub open_interest: Option<String>,
}

/// The REST orderbook snapshot `GET /v5/market/orderbook`.
///
/// The endpoint answers with a fixed set of depths (1, 50, 200, 500) rather than a level count, so
/// the requested depth decides how deep the image is.
#[derive(Deserialize, Debug, Clone)]
pub struct RestOrderbook {
    #[serde(rename = "s", default)]
    pub symbol: String,
    #[serde(rename = "b", default)]
    pub bids: Vec<Vec<String>>,
    #[serde(rename = "a", default)]
    pub asks: Vec<Vec<String>>,
    #[serde(rename = "u", default)]
    pub update_id: i64,
    /// Exchange timestamp, in milliseconds. Optional so that a fixture without it still decodes.
    #[serde(rename = "ts", default)]
    pub ts: Option<i64>,
}

/// The REST body: `{"retCode":0,"retMsg":"OK","result":{…}}`.
///
/// A venue-level refusal arrives with a `200` status, so `retCode` is checked before the result is
/// trusted.
#[derive(Deserialize, Debug, Clone)]
pub struct RestResponse<T> {
    #[serde(rename = "retCode", default)]
    pub ret_code: i64,
    #[serde(rename = "retMsg", default)]
    pub ret_msg: String,
    pub result: Option<T>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_orderbook_delta_decodes_its_levels_and_update_id() {
        let envelope: StreamEnvelope = serde_json::from_str(
            r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1700000000000,
                "data":{"s":"BTCUSDT","b":[["75000.1","0.5"]],"a":[["75000.2","0"]],"u":123456,"seq":7}}"#,
        )
        .unwrap();
        assert_eq!(envelope.kind.as_deref(), Some("delta"));
        let book: OrderbookData = envelope.decode().unwrap();
        assert_eq!(book.s, "BTCUSDT");
        assert_eq!(book.u, 123_456);
        assert_eq!(book.seq, 7);
        assert_eq!(book.b, vec![vec!["75000.1".to_owned(), "0.5".to_owned()]]);
        assert_eq!(book.a, vec![vec!["75000.2".to_owned(), "0".to_owned()]]);
    }

    #[test]
    fn a_subscription_ack_and_a_rejection_are_distinguishable() {
        let ack: StreamEnvelope =
            serde_json::from_str(r#"{"success":true,"ret_msg":"subscribe","op":"subscribe"}"#)
                .unwrap();
        assert_eq!(ack.success, Some(true));
        assert!(ack.topic.is_none());
        let rejected: StreamEnvelope = serde_json::from_str(
            r#"{"success":false,"ret_msg":"Invalid symbol","op":"subscribe","id":"1"}"#,
        )
        .unwrap();
        assert_eq!(rejected.success, Some(false));
        assert_eq!(rejected.ret_msg.as_deref(), Some("Invalid symbol"));
    }

    #[test]
    fn a_trade_decodes_the_taker_side_and_venue_price_text() {
        let trade: Trade = serde_json::from_str(
            r#"{"T":1700000000123,"S":"Sell","s":"BTCUSDT","p":"75000.5","v":"0.001","i":"abc"}"#,
        )
        .unwrap();
        assert_eq!(trade.time_ms, 1_700_000_000_123);
        assert_eq!(trade.side, "Sell");
        assert_eq!(trade.p, "75000.5");
        assert_eq!(trade.id, "abc");
    }

    #[test]
    fn a_rest_orderbook_decodes_its_image_and_refusal() {
        let ok: RestResponse<RestOrderbook> = serde_json::from_str(
            r#"{"retCode":0,"retMsg":"OK","result":{"s":"BTCUSDT","b":[["1","2"]],
                "a":[["3","4"]],"u":99,"ts":1700000000000}}"#,
        )
        .unwrap();
        assert_eq!(ok.ret_code, 0);
        let result = ok.result.unwrap();
        assert_eq!(result.update_id, 99);
        assert_eq!(result.bids, vec![vec!["1".to_owned(), "2".to_owned()]]);
        let refused: RestResponse<RestOrderbook> =
            serde_json::from_str(r#"{"retCode":10001,"retMsg":"params error"}"#).unwrap();
        assert_eq!(refused.ret_code, 10_001);
        assert!(refused.result.is_none());
    }

    #[test]
    fn a_ticker_omits_fields_the_category_does_not_publish() {
        let spot: Ticker = serde_json::from_str(
            r#"{"symbol":"BTCUSDT","lastPrice":"1","markPrice":"1","bbo":[["1","1"]]}"#,
        )
        .unwrap();
        // Spot publishes no funding and no open interest; both stay absent rather than defaulted.
        assert!(spot.funding_rate.is_none());
        assert!(spot.open_interest.is_none());
        let linear: Ticker = serde_json::from_str(
            r#"{"symbol":"BTCUSDT","fundingRate":"0.0001","nextFundingTime":"1700000000000",
                "openInterest":"12345.6"}"#,
        )
        .unwrap();
        assert_eq!(linear.funding_rate.as_deref(), Some("0.0001"));
        assert_eq!(linear.open_interest.as_deref(), Some("12345.6"));
    }
}
