//! Venue metadata transports backed by public exchange endpoints.
//!
//! These are the production side of `titan_strategy_runtime::MetadataTransport`: they perform the
//! network call and translate the venue's own fields into [`VenueInstrumentFacts`], leaving every
//! admission decision (retry budget, 429 handling, missing-field refusal) to `VenueMetadataProvider`
//! so production and the unit tests exercise the same decision code.
//!
//! Only unauthenticated endpoints are used:
//!
//! | venue | endpoint | units published |
//! | --- | --- | --- |
//! | Gate.io USDT perpetual | `GET /api/v4/futures/usdt/contracts` | tick, size step, contract multiplier, minimum size |
//! | OKX linear swap | `GET /api/v5/public/instruments?instType=SWAP` | tick, contract value (`ctVal`), minimum size |
//! | Binance USDⓈ-M perpetual | `GET /fapi/v1/exchangeInfo` | tick, step size, minimum quantity and notional |
//! | Hyperliquid | `POST /info {"type":"meta"}` | size step and multiplier; **no price tick** |
//!
//! Hyperliquid does not publish a price tick anywhere in its public API — price rules depend on
//! significant figures per price level. `price_tick` therefore stays `None`, which makes
//! `VenueMetadataProvider` refuse the instrument (`metadata_unit_unrepresentable`) instead of
//! inventing a tick. Gate publishes the tick directly, so it is the venue used for real end-to-end
//! evidence.

use std::sync::Arc;

use titan_domain_types::DecimalUnit;
use titan_strategy_runtime::{MetadataTransport, MetadataTransportError, VenueInstrumentFacts};

/// Mainnet and testnet `/info` endpoints, as configured for the connector.
const HYPERLIQUID_MAINNET_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const HYPERLIQUID_TESTNET_INFO_URL: &str = "https://api.hyperliquid-testnet.xyz/info";
/// Gate.io public REST base. USDT perpetual contracts only; spot has no contract multiplier.
const GATE_FUTURES_USDT_URL: &str = "https://api.gateio.ws/api/v4/futures/usdt/contracts";
/// OKX public swap instruments. `ctVal` states the base-currency value of one contract.
const OKX_SWAP_INSTRUMENTS_URL: &str =
    "https://www.okx.com/api/v5/public/instruments?instType=SWAP";
/// Binance USDⓈ-M perpetual exchange info: tick/step/minimum live in symbol `filters`.
const BINANCE_FUTURES_EXCHANGE_INFO_URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";

pub(crate) struct HyperliquidMetadataTransport {
    testnet: bool,
}

impl HyperliquidMetadataTransport {
    pub(crate) fn mainnet() -> Self {
        Self { testnet: false }
    }

    pub(crate) fn testnet() -> Self {
        Self { testnet: true }
    }

    fn client(&self) -> connector::hyperliquid::client::HyperliquidClient {
        let info_url = if self.testnet {
            HYPERLIQUID_TESTNET_INFO_URL
        } else {
            HYPERLIQUID_MAINNET_INFO_URL
        };
        // The exchange endpoint is never called from here; the client requires one to construct.
        let exchange_url = info_url.replace("/info", "/exchange");
        connector::hyperliquid::client::HyperliquidClient::new(info_url, &exchange_url)
    }
}

impl MetadataTransport for HyperliquidMetadataTransport {
    fn instruments(
        &self,
        source_key: &str,
    ) -> Result<Vec<VenueInstrumentFacts>, MetadataTransportError> {
        if source_key.is_empty() {
            // The provider always passes the configured source key; an empty one is a caller bug,
            // not a transient failure, so it never retries.
            return Err(MetadataTransportError::Upstream);
        }
        let client = self.client();
        let meta = run_blocking(client.get_meta())?;
        Ok(meta
            .universe
            .iter()
            .map(|asset| {
                // Size steps are exact powers of ten; this is the venue's real statement, not an
                // approximation. The contract multiplier of a linear perp is 1 base unit per lot.
                let lot = u8::try_from(asset.sz_decimals)
                    .ok()
                    .and_then(|scale| DecimalUnit::new(1, scale).ok());
                VenueInstrumentFacts {
                    native_symbol: Arc::from(asset.name.as_str()),
                    price_tick: None,
                    quantity_lot: lot,
                    contract_multiplier: DecimalUnit::new(1, 0).ok(),
                    min_quantity: lot,
                    min_notional_quote: None,
                    linear_quote_currency: Some(Arc::from("USDC")),
                    synthetic: false,
                }
            })
            .collect())
    }
}

/// Gate.io USDT perpetual contracts, which publish every unit multi-leg admission needs.
pub(crate) struct GateFuturesMetadataTransport;

/// One row of `GET /api/v4/futures/usdt/contracts`. Unknown fields are ignored: the venue adds
/// fields over time and a new one must not break metadata acquisition.
#[derive(serde::Deserialize)]
struct GateContract {
    name: String,
    /// Base-currency value of one contract (Gate calls it the quanto multiplier).
    quanto_multiplier: String,
    /// Price step, as an exact decimal string.
    order_price_round: String,
    /// Minimum order size in contracts.
    #[serde(default)]
    order_size_min: i64,
    #[serde(default)]
    in_delisting: bool,
}

impl MetadataTransport for GateFuturesMetadataTransport {
    fn instruments(
        &self,
        source_key: &str,
    ) -> Result<Vec<VenueInstrumentFacts>, MetadataTransportError> {
        if source_key.is_empty() {
            return Err(MetadataTransportError::Upstream);
        }
        let body = fetch_json(GATE_FUTURES_USDT_URL)?;
        let contracts: Vec<GateContract> = serde_json::from_value(body).map_err(|_| {
            // A shape change is an upstream failure, never a reason to guess units.
            MetadataTransportError::Upstream
        })?;
        Ok(contracts
            .into_iter()
            .filter(|contract| !contract.in_delisting)
            .map(|contract| {
                let price_tick = contract.order_price_round.parse::<DecimalUnit>().ok();
                let contract_multiplier = contract.quanto_multiplier.parse::<DecimalUnit>().ok();
                // Gate quotes size in contracts with a step of one; `order_size_min` is that
                // minimum expressed in the same unit.
                let quantity_lot = Some(DecimalUnit::new(1, 0).expect("one lot is representable"));
                let min_quantity = u64::try_from(contract.order_size_min)
                    .ok()
                    .filter(|minimum| *minimum > 0)
                    .and_then(|minimum| DecimalUnit::new(minimum, 0).ok());

                VenueInstrumentFacts {
                    native_symbol: Arc::from(contract.name.as_str()),
                    price_tick,
                    quantity_lot,
                    contract_multiplier,
                    min_quantity,
                    min_notional_quote: None,
                    linear_quote_currency: Some(Arc::from("USDT")),
                    synthetic: false,
                }
            })
            .collect::<Vec<_>>())
    }
}

/// OKX linear swaps, which publish the tick and the contract value directly.
pub(crate) struct OkxSwapMetadataTransport;

/// One row of `data` in `GET /api/v5/public/instruments?instType=SWAP`.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OkxInstrument {
    inst_id: String,
    /// Price step.
    tick_sz: String,
    /// Minimum order size, in contracts.
    min_sz: String,
    /// Base-currency value of one contract.
    ct_val: String,
    /// `linear` or `inverse`. Only linear swaps have a quote-denominated contract value.
    #[serde(default)]
    ct_type: String,
    /// Margin/settlement currency, which is the quote currency of a linear swap.
    #[serde(default)]
    settle_ccy: String,
    /// `live` for tradable instruments; anything else is refused rather than admitted.
    #[serde(default)]
    state: String,
}

#[derive(serde::Deserialize)]
struct OkxInstrumentsResponse {
    code: String,
    #[serde(default)]
    data: Vec<OkxInstrument>,
}

impl MetadataTransport for OkxSwapMetadataTransport {
    fn instruments(
        &self,
        source_key: &str,
    ) -> Result<Vec<VenueInstrumentFacts>, MetadataTransportError> {
        if source_key.is_empty() {
            return Err(MetadataTransportError::Upstream);
        }
        let body = fetch_json(OKX_SWAP_INSTRUMENTS_URL)?;
        let response: OkxInstrumentsResponse =
            serde_json::from_value(body).map_err(|_| MetadataTransportError::Upstream)?;
        if response.code != "0" {
            // OKX reports application errors in a 200 response body.
            return Err(MetadataTransportError::Upstream);
        }
        Ok(response
            .data
            .into_iter()
            .filter(|instrument| instrument.state == "live")
            // An inverse swap states its contract value in the base currency, so `ctVal` is not a
            // quote-denominated multiplier. OKX marks the distinction with `ctType`; inverse
            // instruments are excluded rather than misfiled.
            .filter(|instrument| instrument.ct_type != "inverse")
            .map(|instrument| {
                // OKX sizes orders in contracts, so one lot is one contract and the base-currency
                // delta of that lot is `ctVal`.
                let contract_multiplier = instrument.ct_val.parse::<DecimalUnit>().ok();
                let min_quantity = instrument.min_sz.parse::<DecimalUnit>().ok();
                VenueInstrumentFacts {
                    native_symbol: Arc::from(instrument.inst_id.as_str()),
                    price_tick: instrument.tick_sz.parse::<DecimalUnit>().ok(),
                    quantity_lot: Some(DecimalUnit::new(1, 0).expect("one contract is one lot")),
                    contract_multiplier,
                    min_quantity,
                    min_notional_quote: None,
                    linear_quote_currency: if instrument.settle_ccy.is_empty() {
                        None
                    } else {
                        Some(Arc::from(instrument.settle_ccy.as_str()))
                    },
                    synthetic: false,
                }
            })
            .collect())
    }
}

/// Binance USDⓈ-M perpetuals. Units live in per-symbol filters, each an exact decimal string.
pub(crate) struct BinanceFuturesMetadataTransport;

#[derive(serde::Deserialize)]
struct BinanceSymbolFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(default, rename = "tickSize")]
    tick_size: Option<String>,
    #[serde(default, rename = "stepSize")]
    step_size: Option<String>,
    #[serde(default, rename = "minQty")]
    min_qty: Option<String>,
    // Binance USDⓈ-M publishes the notional floor as `notional`; the spot-style `minNotional`
    // name is accepted too so a venue-side naming change does not silently drop the field.
    #[serde(default, rename = "notional", alias = "minNotional")]
    min_notional: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceSymbolInfo {
    symbol: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    contract_type: String,
    #[serde(default)]
    quote_asset: String,
    #[serde(default)]
    filters: Vec<BinanceSymbolFilter>,
}

#[derive(serde::Deserialize)]
struct BinanceExchangeInfo {
    #[serde(default)]
    symbols: Vec<BinanceSymbolInfo>,
}

impl MetadataTransport for BinanceFuturesMetadataTransport {
    fn instruments(
        &self,
        source_key: &str,
    ) -> Result<Vec<VenueInstrumentFacts>, MetadataTransportError> {
        if source_key.is_empty() {
            return Err(MetadataTransportError::Upstream);
        }
        let body = fetch_json(BINANCE_FUTURES_EXCHANGE_INFO_URL)?;
        let info: BinanceExchangeInfo =
            serde_json::from_value(body).map_err(|_| MetadataTransportError::Upstream)?;
        Ok(info
            .symbols
            .into_iter()
            .filter(|symbol| symbol.status == "TRADING" && symbol.contract_type == "PERPETUAL")
            .map(|symbol| {
                let mut price_tick = None;
                let mut quantity_lot = None;
                let mut min_quantity = None;
                let mut min_notional_quote = None;
                for filter in &symbol.filters {
                    match filter.filter_type.as_str() {
                        "PRICE_FILTER" => {
                            price_tick = filter
                                .tick_size
                                .as_deref()
                                .and_then(|value| value.parse::<DecimalUnit>().ok());
                        }
                        "LOT_SIZE" => {
                            quantity_lot = filter
                                .step_size
                                .as_deref()
                                .and_then(|value| value.parse::<DecimalUnit>().ok());
                            min_quantity = filter
                                .min_qty
                                .as_deref()
                                .and_then(|value| value.parse::<DecimalUnit>().ok());
                        }
                        "MIN_NOTIONAL" => {
                            min_notional_quote = filter
                                .min_notional
                                .as_deref()
                                .and_then(|value| value.parse::<DecimalUnit>().ok());
                        }
                        _ => {}
                    }
                }
                VenueInstrumentFacts {
                    native_symbol: Arc::from(symbol.symbol.as_str()),
                    price_tick,
                    quantity_lot,
                    // A USDⓈ-M perpetual is linear with one base unit per size unit.
                    contract_multiplier: DecimalUnit::new(1, 0).ok(),
                    min_quantity,
                    min_notional_quote,
                    linear_quote_currency: if symbol.quote_asset.is_empty() {
                        None
                    } else {
                        Some(Arc::from(symbol.quote_asset.as_str()))
                    },
                    synthetic: false,
                }
            })
            .collect())
    }
}

/// Runs one async client call on a short-lived current-thread runtime.
///
/// Metadata acquisition happens during instance creation, outside the strategy callback threads,
/// where `tokio::runtime::Handle::try_current()` is asserted to fail. A private runtime therefore
/// bounds the call without contending with the live executor.
fn run_blocking<F, T>(future: F) -> Result<T, MetadataTransportError>
where
    F: std::future::Future<Output = Result<T, connector::hyperliquid::HyperliquidError>>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| MetadataTransportError::Unavailable)?;
    runtime
        .block_on(future)
        .map_err(|error| classify_hyperliquid(&error))
}

fn fetch_json(url: &str) -> Result<serde_json::Value, MetadataTransportError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| MetadataTransportError::Unavailable)?;
    runtime.block_on(async {
        let response = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|_| MetadataTransportError::Unavailable)?
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() || error.is_connect() {
                    MetadataTransportError::Unavailable
                } else {
                    MetadataTransportError::Upstream
                }
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(match status.as_u16() {
                429 => MetadataTransportError::RateLimited,
                _ => MetadataTransportError::Upstream,
            });
        }
        response
            .json()
            .await
            .map_err(|_| MetadataTransportError::Upstream)
    })
}

fn classify_hyperliquid(
    error: &connector::hyperliquid::HyperliquidError,
) -> MetadataTransportError {
    use connector::hyperliquid::HyperliquidError;
    match error {
        // The status is a fact, not a string to pattern-match: 429 is the only retryable class.
        HyperliquidError::HttpStatus { status, .. } => match status {
            429 => MetadataTransportError::RateLimited,
            408 | 425 | 500..=599 => MetadataTransportError::Upstream,
            _ => MetadataTransportError::Upstream,
        },
        HyperliquidError::Reqwest(error) => {
            if error.is_timeout() || error.is_connect() {
                MetadataTransportError::Unavailable
            } else {
                MetadataTransportError::Upstream
            }
        }
        _ => MetadataTransportError::Upstream,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use titan_strategy_runtime::{MetadataSnapshotProvider, VenueMetadataProvider};

    fn provider_for(
        transport: Arc<dyn MetadataTransport>,
        source: &str,
        symbol: &str,
    ) -> VenueMetadataProvider {
        VenueMetadataProvider::new(
            transport,
            vec![titan_strategy_runtime::MetadataRequest {
                local_asset_no: 0,
                source_key: Arc::from(source),
                native_symbol: Arc::from(symbol),
            }],
            titan_strategy_runtime::MetadataFetchPolicy::default(),
        )
    }

    /// Real public-endpoint fetch. Ignored by default because it needs outbound network; run it on
    /// the build server with `cargo test -p titan-cli --lib live_ -- --ignored`.
    ///
    /// Gate publishes every unit, so this is the end-to-end proof that a real venue snapshot can
    /// build an object table.
    #[test]
    #[ignore = "needs outbound access to the Gate.io public futures endpoint"]
    fn live_gate_metadata_yields_a_complete_admissible_snapshot() {
        let facts = GateFuturesMetadataTransport
            .instruments("gate")
            .expect("public contracts endpoint is reachable");
        assert!(
            facts.len() > 100,
            "Gate lists many perpetuals: {}",
            facts.len()
        );
        let btc = facts
            .iter()
            .find(|facts| facts.native_symbol.as_ref() == "BTC_USDT")
            .expect("BTC_USDT is listed");
        assert_eq!(btc.price_tick, Some("0.1".parse().unwrap()));
        assert_eq!(btc.contract_multiplier, Some("0.0001".parse().unwrap()));
        assert_eq!(btc.quantity_lot, Some(DecimalUnit::new(1, 0).unwrap()));
        assert_eq!(btc.min_quantity, Some(DecimalUnit::new(1, 0).unwrap()));
        assert_eq!(btc.linear_quote_currency.as_deref(), Some("USDT"));
        assert!(!btc.synthetic, "venue metadata is not a host constant");

        // End to end: the production provider accepts it and signs its own snapshot.
        let snapshot = provider_for(Arc::new(GateFuturesMetadataTransport), "gate", "BTC_USDT")
            .snapshot()
            .expect("a fully published instrument is admissible");
        assert!(snapshot.digest_matches());
        let instrument = &snapshot.instruments[0];
        assert_eq!(instrument.native_symbol.as_ref(), "BTC_USDT");
        assert_eq!(instrument.price_tick, "0.1".parse().unwrap());
    }

    /// OKX and Binance publish every unit, so once the host can reach them the same assertion as
    /// Gate applies. Ignored because the build server cannot currently resolve either host.
    #[test]
    #[ignore = "needs outbound access to OKX/Binance public endpoints"]
    fn live_okx_and_binance_metadata_are_complete() {
        let okx = OkxSwapMetadataTransport
            .instruments("okx")
            .expect("OKX public instruments are reachable");
        let btc = okx
            .iter()
            .find(|facts| facts.native_symbol.as_ref() == "BTC-USDT-SWAP")
            .expect("BTC-USDT-SWAP is listed");
        assert!(btc.price_tick.is_some(), "OKX publishes tickSz");
        assert!(btc.contract_multiplier.is_some(), "OKX publishes ctVal");
        assert_eq!(btc.linear_quote_currency.as_deref(), Some("USDT"));
        assert!(!btc.synthetic);

        let binance = BinanceFuturesMetadataTransport
            .instruments("binance")
            .expect("Binance exchangeInfo is reachable");
        let btcusdt = binance
            .iter()
            .find(|facts| facts.native_symbol.as_ref() == "BTCUSDT")
            .expect("BTCUSDT is listed");
        assert!(btcusdt.price_tick.is_some(), "PRICE_FILTER.tickSize");
        assert!(btcusdt.quantity_lot.is_some(), "LOT_SIZE.stepSize");
        assert!(
            btcusdt.min_notional_quote.is_some(),
            "MIN_NOTIONAL.minNotional"
        );
        assert_eq!(
            btcusdt.contract_multiplier,
            Some(DecimalUnit::new(1, 0).unwrap())
        );
        assert!(!binance.is_empty());
    }

    /// Offline check of the wire mapping, which the live tests cannot run here. It pins the field
    /// names the two venues actually use, so a rename on their side fails loudly.
    #[test]
    fn okx_and_binance_wire_fields_map_to_exact_units() {
        let okx: OkxInstrumentsResponse = serde_json::from_value(serde_json::json!({
            "code": "0",
            "data": [
                {"instId": "BTC-USDT-SWAP", "tickSz": "0.1", "minSz": "0.01",
                 "ctVal": "0.01", "ctValCcy": "BTC", "ctType": "linear", "settleCcy": "USDT", "state": "live"},
                {"instId": "DELISTED", "tickSz": "1", "minSz": "1",
                 "ctVal": "1", "ctValCcy": "BTC", "ctType": "linear", "settleCcy": "USDT", "state": "suspend"},
                {"instId": "BTC-USD-SWAP", "tickSz": "0.1", "minSz": "1",
                 "ctVal": "100", "ctValCcy": "USD", "ctType": "inverse", "settleCcy": "BTC", "state": "live"}
            ]
        }))
        .expect("OKX wire shape");
        assert_eq!(okx.code, "0");
        assert_eq!(okx.data.len(), 3);
        assert_eq!(okx.data[0].tick_sz, "0.1");
        assert_eq!(okx.data[0].ct_val, "0.01");
        // The non-live and the inverse instruments are filtered by the transport.
        let live_linear = okx
            .data
            .iter()
            .filter(|instrument| instrument.state == "live")
            .filter(|instrument| instrument.ct_type != "inverse")
            .count();
        assert_eq!(live_linear, 1, "only the live linear swap survives");

        let binance: BinanceExchangeInfo = serde_json::from_value(serde_json::json!({
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL",
                 "quoteAsset": "USDT",
                 "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                    {"filterType": "MIN_NOTIONAL", "notional": "5"}
                 ]},
                {"symbol": "OLDUSDT", "status": "SETTLING", "contractType": "PERPETUAL",
                 "quoteAsset": "USDT", "filters": []}
            ]
        }))
        .expect("Binance wire shape");
        let trading: Vec<_> = binance
            .symbols
            .iter()
            .filter(|symbol| symbol.status == "TRADING" && symbol.contract_type == "PERPETUAL")
            .collect();
        assert_eq!(trading.len(), 1);
        let filters = &trading[0].filters;
        assert_eq!(filters[0].tick_size.as_deref(), Some("0.10"));
        assert_eq!(filters[1].step_size.as_deref(), Some("0.001"));
        assert_eq!(filters[2].min_notional.as_deref(), Some("5"));
    }

    /// Hyperliquid is the second reachable venue, and the one that proves the refusal path with a
    /// real response: it publishes no tick, so admission stops at `metadata_unit_unrepresentable`.
    #[test]
    #[ignore = "needs outbound access to the Hyperliquid public /info endpoint"]
    fn live_hyperliquid_metadata_is_fetched_and_refused_for_a_missing_tick() {
        let facts = HyperliquidMetadataTransport::mainnet()
            .instruments("hyperliquid")
            .expect("public /info is reachable");
        assert!(
            facts.len() > 50,
            "Hyperliquid lists a large universe: {}",
            facts.len()
        );
        let btc = facts
            .iter()
            .find(|facts| facts.native_symbol.as_ref() == "BTC")
            .expect("BTC is listed");
        assert_eq!(btc.quantity_lot, Some(DecimalUnit::new(1, 5).unwrap()));
        assert_eq!(
            btc.contract_multiplier,
            Some(DecimalUnit::new(1, 0).unwrap())
        );
        assert_eq!(btc.linear_quote_currency.as_deref(), Some("USDC"));
        assert!(!btc.synthetic, "venue metadata is not a host constant");
        assert_eq!(btc.price_tick, None, "Hyperliquid publishes no tick");

        let error = provider_for(
            Arc::new(HyperliquidMetadataTransport::mainnet()),
            "hyperliquid",
            "BTC",
        )
        .snapshot()
        .expect_err("a symbol without a published tick cannot be admitted");
        assert!(
            error.starts_with("metadata_unit_unrepresentable"),
            "{error}"
        );
    }
}
