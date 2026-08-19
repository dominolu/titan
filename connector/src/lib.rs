//! Connector 库入口。
//!
//! 策略工程可通过 path 依赖本 crate，使用 [`api::BrokerApi`] 统一接口在
//! Binance USD-M / OKX V5 / Hyperliquid 之间自由切换：
//!
//! ```ignore
//! let api: Box<dyn BrokerApi> = match broker {
//!     "binance" => Box::new(BinanceFuturesClient::new(url, key, secret)),
//!     "okx" => Box::new(OkxClient::new(url, key, secret, passphrase)),
//!     "hyperliquid" => Box::new(HyperliquidClient::new(info_url, exchange_url)),
//!     _ => unreachable!(),
//! };
//! let ticker = api.get_ticker("BTCUSDT").await?;
//! ```

pub mod api;
pub mod connector;
mod utils;

#[cfg(feature = "binancefutures")]
pub mod binancefutures;
#[cfg(feature = "binancespot")]
pub mod binancespot;
#[cfg(feature = "bybit")]
pub mod bybit;
#[cfg(feature = "okx")]
pub mod okx;
#[cfg(feature = "hyperliquid")]
pub mod hyperliquid;
