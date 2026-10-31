//! Connector 库入口。
//!
//! 策略工程可通过 path 依赖本 crate，使用 [`api::BrokerApi`] 统一接口在
//! Binance USD-M / OKX V5 / Hyperliquid / Gate CrossEx 之间自由切换：
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

mod account_projection;
pub mod account_runtime;
pub mod api;
#[cfg(feature = "backpack")]
pub mod backpack;
pub mod connector;
pub mod decimal;
#[cfg(feature = "lighter")]
pub mod lighter;
mod market_event;
pub mod market_runtime;
#[cfg(any(feature = "backpack", feature = "lighter"))]
pub mod native_api;
#[cfg(test)]
mod native_api_tests;
#[cfg(any(feature = "backpack", feature = "lighter"))]
pub mod native_ws;
mod units;
mod utils;
#[cfg(feature = "variational")]
pub mod variational;
#[cfg(any(feature = "backpack", feature = "lighter", feature = "variational"))]
mod venue;

/// Installs the connector crate's process-wide TLS crypto provider before any
/// reqwest or websocket client is constructed. Static connector factories each
/// carry their own rustls instance, so this must run inside the connector
/// library rather than only in the Titan executable.
pub(crate) fn ensure_rustls_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }
}

#[cfg(feature = "binancefutures")]
pub mod binancefutures;
#[cfg(feature = "hyperliquid")]
pub mod hyperliquid;
#[cfg(feature = "okx")]
pub mod okx;

#[cfg(feature = "gatecrossex")]
pub mod gatecrossex;
