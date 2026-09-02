use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::Deserialize;
use smallvec::SmallVec;

use super::{from_str_to_side, from_str_to_status, from_str_to_tif, from_str_to_type};
use crate::utils::{from_str_to_f64, to_lowercase};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_mark_price_update() {
        let msg: Stream = serde_json::from_str(
            r#"{"e":"markPriceUpdate","E":1562305380000,"s":"BTCUSDT","p":"11794.15000000","i":"11793.84535841","P":"11780.83846368","r":"-0.00038100","T":1562306400000}"#,
        )
        .unwrap();
        match msg {
            Stream::EventStream(EventStream::MarkPriceUpdate(update)) => {
                assert_eq!(update.symbol, "btcusdt");
                assert_eq!(update.funding_rate, -0.000381);
                assert_eq!(update.next_funding_time, 1_562_306_400_000);
                assert_eq!(update.event_time, 1_562_305_380_000);
            }
            _ => panic!("expected a markPriceUpdate message"),
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Stream<'a> {
    EventStream(#[serde(borrow)] EventStream<'a>),
    Result(Result),
}

#[derive(Deserialize, Debug)]
pub struct Result {
    pub result: Option<String>,
    pub id: String,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "e")]
pub enum EventStream<'a> {
    #[serde(rename = "depthUpdate")]
    DepthUpdate(#[serde(borrow)] Depth<'a>),
    #[serde(rename = "markPriceUpdate")]
    MarkPriceUpdate(MarkPriceUpdate),
    #[serde(rename = "trade")]
    Trade(Trade),
    #[serde(rename = "bookTicker")]
    BookTicker(BookTicker),
    #[serde(rename = "ORDER_TRADE_UPDATE")]
    OrderTradeUpdate(OrderTradeUpdate),
    #[serde(rename = "TRADE_LITE")]
    TradeLite(TradeLite),
    #[serde(rename = "ACCOUNT_UPDATE")]
    AccountUpdate(AccountUpdate),
    #[serde(rename = "listenKeyExpired")]
    ListenKeyExpired(ListenKeyStream),
}

#[derive(Deserialize, Debug)]
pub struct Depth<'a> {
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    #[serde(borrow)]
    pub symbol: &'a str,
    // for Coin-M futures
    // #[serde(rename = "ps")]
    // pub pair: String,
    #[serde(rename = "U")]
    pub first_update_id: i64,
    #[serde(rename = "u")]
    pub last_update_id: i64,
    #[serde(rename = "pu")]
    pub prev_update_id: i64,
    #[serde(rename = "b")]
    #[serde(borrow)]
    pub bids: SmallVec<[(&'a str, &'a str); 64]>,
    #[serde(rename = "a")]
    #[serde(borrow)]
    pub asks: SmallVec<[(&'a str, &'a str); 64]>,
}

#[derive(Debug)]
pub struct OwnedDepth {
    pub transaction_time: i64,
    pub event_time: i64,
    pub symbol: String,
    pub first_update_id: i64,
    pub last_update_id: i64,
    pub prev_update_id: i64,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
}

impl Depth<'_> {
    pub fn into_owned(self, symbol: String) -> OwnedDepth {
        OwnedDepth {
            transaction_time: self.transaction_time,
            event_time: self.event_time,
            symbol,
            first_update_id: self.first_update_id,
            last_update_id: self.last_update_id,
            prev_update_id: self.prev_update_id,
            bids: self
                .bids
                .into_iter()
                .map(|(price, quantity)| (price.to_owned(), quantity.to_owned()))
                .collect(),
            asks: self
                .asks
                .into_iter()
                .map(|(price, quantity)| (price.to_owned(), quantity.to_owned()))
                .collect(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct MarkPriceUpdate {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    /// Funding rate of the upcoming settlement.
    #[serde(rename = "r")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub funding_rate: f64,
    /// Next funding settlement time (milliseconds).
    #[serde(rename = "T")]
    pub next_funding_time: i64,
}

#[derive(Deserialize, Debug)]
pub struct Trade {
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub id: i64,
    #[serde(rename = "p")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "q")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub qty: f64,
    #[serde(rename = "m")]
    pub is_the_buyer_the_market_maker: bool,
}

#[derive(Deserialize, Debug)]
pub struct TradeLite {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "q")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub qty: f64,
    #[serde(rename = "p")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "m")]
    pub is_this_trade_the_market_maker: bool,
    #[serde(rename = "c")]
    pub client_order_id: String,
    #[serde(rename = "S")]
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "L")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub last_filled_price: f64,
    #[serde(rename = "l")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub last_filled_qty: f64,
    #[serde(rename = "t")]
    pub trade_id: i64,
    #[serde(rename = "i")]
    pub order_id: i64,
}

#[derive(Deserialize, Debug)]
pub struct AccountUpdate {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "a")]
    pub account: Account,
}

#[derive(Deserialize, Debug)]
pub struct Account {
    #[serde(rename = "m")]
    pub ev_reason: String,
    #[serde(rename = "B")]
    pub balance: Vec<Balance>,
    #[serde(rename = "P")]
    pub position: Vec<Position>,
}

#[derive(Deserialize, Debug)]
pub struct Balance {
    #[serde(rename = "a")]
    pub asset: String,
    #[serde(rename = "wb")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub wallet_balance: f64,
    #[serde(rename = "cw")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub cross_wallet_balance: f64,
    #[serde(rename = "bc")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub balance_change: f64,
}

#[derive(Deserialize, Debug)]
pub struct Position {
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "pa")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub position_amount: f64,
    #[serde(rename = "ep")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub entry_price: f64,
    #[serde(rename = "bep")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub breakeven_price: f64,
    #[serde(rename = "cr")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub prefee_accumulated_realized: f64,
    #[serde(rename = "up")]
    pub unrealized_pnl: String,
    #[serde(rename = "mt")]
    pub margin_type: String,
    #[serde(rename = "iw")]
    pub isolated_wallet: Option<String>,
    #[serde(rename = "ps")]
    pub position_side: String,
}

#[derive(Deserialize, Debug)]
pub struct OrderTradeUpdate {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "o")]
    pub order: Order,
}

#[derive(Deserialize, Debug)]
pub struct Order {
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub client_order_id: String,
    #[serde(rename = "S")]
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "o")]
    #[serde(deserialize_with = "from_str_to_type")]
    pub order_type: OrdType,
    #[serde(rename = "f")]
    #[serde(deserialize_with = "from_str_to_tif")]
    pub time_in_force: TimeInForce,
    #[serde(rename = "q")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub original_qty: f64,
    #[serde(rename = "p")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub original_price: f64,
    #[serde(rename = "ap")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub average_price: f64,
    #[serde(rename = "sp")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub stop_price: f64,
    #[serde(rename = "x")]
    pub execution_type: String,
    #[serde(rename = "X")]
    #[serde(deserialize_with = "from_str_to_status")]
    pub order_status: Status,
    #[serde(rename = "i")]
    pub order_id: i64,
    #[serde(rename = "l")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub order_last_filled_qty: f64,
    #[serde(rename = "z")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub order_filled_accumulated_qty: f64,
    #[serde(rename = "L")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub last_filled_price: f64,
    // #[serde(rename = "N")]
    // pub commission_asset: Option<String>,
    // #[serde(rename = "n")]
    // pub commission: Option<String>,
    #[serde(rename = "T")]
    pub order_trade_time: i64,
    #[serde(rename = "t")]
    pub trade_id: i64,
    // #[serde(rename = "b")]
    // pub bid_notional: String,
    // #[serde(rename = "a")]
    // pub ask_notional: String,
    // #[serde(rename = "m")]
    // pub is_maker_side: bool,
    // #[serde(rename = "R")]
    // pub is_reduce_only: bool,
    // #[serde(rename = "wt")]
    // pub stop_price_working_type: String,
    // #[serde(rename = "ot")]
    // pub original_order_type: String,
    // #[serde(rename = "ps")]
    // pub position_side: String,
    // #[serde(rename = "cp")]
    // pub close_all: Option<String>,
    // #[serde(rename = "AP")]
    // pub activation_price: Option<String>,
    // #[serde(rename = "cr")]
    // pub callback_rate: Option<String>,
    // #[serde(rename = "pP")]
    // pub price_protection: bool,
    // #[serde(rename = "si")]
    // pub ignore: i64,
    // #[serde(rename = "ss")]
    // pub ignore: i64,
    // #[serde(rename = "rp")]
    // pub realized_profit: String,
    // #[serde(rename = "V")]
    // pub stp_mode: String,
    // #[serde(rename = "pm")]
    // pub price_match_mode: String,
    // #[serde(rename = "gtd")]
    // pub gtd_auto_cancel_time: i64,
}

#[derive(Deserialize, Debug)]
pub struct ListenKey {
    #[serde(rename = "listenKey")]
    pub listen_key: String,
}

#[derive(Deserialize, Debug)]
pub struct ListenKeyStream {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "listenKey")]
    pub listen_key: String,
}

/// `<symbol>@bookTicker` 流。
#[derive(Deserialize, Debug)]
pub struct BookTicker {
    #[serde(rename = "u")]
    pub update_id: i64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "b")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub bid_price: f64,
    #[serde(rename = "B")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub bid_qty: f64,
    #[serde(rename = "a")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub ask_price: f64,
    #[serde(rename = "A")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub ask_qty: f64,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "E")]
    pub event_time: i64,
}
