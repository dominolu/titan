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

pub mod account_plugin;
pub mod api;
pub mod connector;
pub mod dynamic_plugin;
mod market_event;
pub mod market_plugin;
mod utils;

/// Installs the connector crate's process-wide TLS crypto provider before any
/// reqwest or websocket client is constructed. Dynamic connector plugins each
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
