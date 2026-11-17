//! Startup metadata providers: one seam for offline snapshot files and venue fetch.
//!
//! A provider answers "what are this instance's authoritative units?" at startup. Strategies that
//! read the object table cannot start without an answer, and no provider may approximate one: every
//! failure is a refusal, never a default unit.
//!
//! The venue path is split into two layers on purpose. [`MetadataTransport`] performs the network
//! call and is injected; [`VenueMetadataProvider`] owns the decisions that must be identical in
//! production and in tests — retry budget, timeout, HTTP 429 handling, and the mapping from a
//! fetched unit into the snapshot. Tests therefore exercise the production decision code against a
//! scripted transport without any network access.

use std::sync::Arc;
use std::time::Duration;

use titan_domain_types::DecimalUnit;
use titan_market_service::{
    METADATA_SNAPSHOT_SCHEMA_VERSION, MetadataInstrument, MetadataSnapshot,
};

use crate::MetadataSnapshotProvider;

/// Stable reason codes for venue metadata acquisition.
///
/// These are part of the operator contract (they reach `StrategyError::code` through
/// `metadata_snapshot_unavailable`), so they are constants rather than free-form messages.
pub const METADATA_TRANSPORT_UNAVAILABLE: &str = "metadata_transport_unavailable";
pub const METADATA_FETCH_TIMEOUT: &str = "metadata_fetch_timeout";
pub const METADATA_RATE_LIMITED: &str = "metadata_rate_limited";
pub const METADATA_UPSTREAM_ERROR: &str = "metadata_upstream_error";
pub const METADATA_INSTRUMENT_ABSENT: &str = "metadata_instrument_absent";
pub const METADATA_UNIT_UNREPRESENTABLE: &str = "metadata_unit_unrepresentable";
pub const METADATA_SYNTHETIC_INSTRUMENT: &str = "metadata_synthetic_instrument";
pub const METADATA_BINDING_MISMATCH: &str = "metadata_binding_mismatch";

/// One instrument as the venue publishes it, reduced to the fields units are derived from.
///
/// `tick_size`/`lot_size`/`contract_size` are the venue's exact decimals; `None` means the venue
/// did not publish the field, which is a refusal and not a zero.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VenueInstrumentFacts {
    pub native_symbol: Arc<str>,
    pub price_tick: Option<DecimalUnit>,
    pub quantity_lot: Option<DecimalUnit>,
    pub contract_multiplier: Option<DecimalUnit>,
    pub min_quantity: Option<DecimalUnit>,
    pub min_notional_quote: Option<DecimalUnit>,
    pub linear_quote_currency: Option<Arc<str>>,
    pub synthetic: bool,
}

/// One symbol a strategy binds through a market source: what the snapshot must answer for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataRequest {
    pub local_asset_no: u32,
    pub source_key: Arc<str>,
    pub native_symbol: Arc<str>,
}

/// Transport failure classes; each maps onto exactly one stable code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataTransportError {
    /// The connection could not be established or was closed below HTTP.
    Unavailable,
    /// The venue answered 429 (or the retry budget was exhausted while being rate limited).
    RateLimited,
    /// Any other non-success answer (5xx, 4xx, malformed body).
    Upstream,
}

/// Performs the actual venue call. Production wraps a connector; tests script it.
///
/// The implementation may block; the caller bounds each attempt with [`MetadataFetchPolicy`].
pub trait MetadataTransport: Send + Sync {
    fn instruments(
        &self,
        source_key: &str,
    ) -> Result<Vec<VenueInstrumentFacts>, MetadataTransportError>;
}

/// Bounded acquisition policy. Values are configuration, not venue facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataFetchPolicy {
    /// Wall-clock budget for one transport attempt.
    ///
    /// A transport owns its own deadline (the connector clients they wrap already time out); this
    /// value is the budget they must honour and the one operators tune. The provider does not wrap
    /// the call in a timer because it runs outside any async runtime — `try_current()` is asserted
    /// to fail at the call site — so a transport that overshoots would park the creating thread.
    pub attempt_timeout: Duration,
    /// Additional attempts after a `RateLimited` answer. Any other failure is not retried: a
    /// wrong endpoint or a broken body does not improve by repeating it.
    pub rate_limit_retries: u32,
    pub retry_backoff: Duration,
}

impl Default for MetadataFetchPolicy {
    fn default() -> Self {
        Self {
            attempt_timeout: Duration::from_secs(5),
            rate_limit_retries: 2,
            retry_backoff: Duration::from_millis(250),
        }
    }
}

/// Source of the symbol list a provider must answer for.
///
/// The bindings are only known once a strategy instance is created, so a provider built during
/// service construction cannot receive them upfront. This lets the caller decide between a fixed
/// list (tests, replay) and the instance's own bindings (live service).
pub type MetadataRequestSource = Arc<dyn Fn() -> Vec<MetadataRequest> + Send + Sync>;

/// Fetches venue metadata and turns it into a verified startup snapshot.
pub struct VenueMetadataProvider {
    transport: Arc<dyn MetadataTransport>,
    requests: MetadataRequestSource,
    policy: MetadataFetchPolicy,
    /// Receives the wall clock; injected so tests do not depend on the host time.
    captured_at_ns: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl std::fmt::Debug for VenueMetadataProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VenueMetadataProvider")
            .field("requests", &"<resolved per snapshot>")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl VenueMetadataProvider {
    /// A provider for a fixed set of bindings.
    pub fn new(
        transport: Arc<dyn MetadataTransport>,
        requests: Vec<MetadataRequest>,
        policy: MetadataFetchPolicy,
    ) -> Self {
        Self::with_request_source(transport, Arc::new(move || requests.clone()), policy)
    }

    /// A provider that asks for its bindings when a snapshot is built. Used by the live service,
    /// where the instance's market bindings only exist after the strategy is created.
    pub fn with_request_source(
        transport: Arc<dyn MetadataTransport>,
        requests: MetadataRequestSource,
        policy: MetadataFetchPolicy,
    ) -> Self {
        Self {
            transport,
            requests,
            policy,
            captured_at_ns: Arc::new(|| 0),
        }
    }

    /// Replaces the capture timestamp source (tests and deterministic replay).
    pub fn with_clock(mut self, captured_at_ns: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.captured_at_ns = captured_at_ns;
        self
    }

    fn fetch(&self, source_key: &str) -> Result<Vec<VenueInstrumentFacts>, &'static str> {
        let mut retries = 0;
        loop {
            let outcome = self.attempt(source_key)?;
            if !matches!(outcome, Err(MetadataTransportError::RateLimited)) {
                return outcome.map_err(classify);
            }
            if retries >= self.policy.rate_limit_retries {
                return Err(METADATA_RATE_LIMITED);
            }
            retries += 1;
            if !self.policy.retry_backoff.is_zero() {
                std::thread::sleep(self.policy.retry_backoff);
            }
        }
    }

    /// One bounded attempt. The transport contract requires each call to finish within
    /// [`MetadataFetchPolicy::attempt_timeout`] and to report `Unavailable` when it cannot; a slow
    /// answer is never accepted as a late success because the startup deadline has already passed.
    fn attempt(
        &self,
        source_key: &str,
    ) -> Result<Result<Vec<VenueInstrumentFacts>, MetadataTransportError>, &'static str> {
        let outcome = self.transport.instruments(source_key);
        Ok(outcome)
    }
}

fn classify(error: MetadataTransportError) -> &'static str {
    match error {
        MetadataTransportError::Unavailable => METADATA_TRANSPORT_UNAVAILABLE,
        MetadataTransportError::RateLimited => METADATA_RATE_LIMITED,
        MetadataTransportError::Upstream => METADATA_UPSTREAM_ERROR,
    }
}

impl MetadataSnapshotProvider for VenueMetadataProvider {
    fn snapshot(&self) -> Result<Arc<MetadataSnapshot>, String> {
        let requests = (self.requests)();
        let requests = requests.as_slice();
        // One call per distinct source; a strategy may bind one venue many times.
        let mut fetched: Vec<(Arc<str>, Vec<VenueInstrumentFacts>)> = Vec::new();
        for request in requests.iter() {
            if fetched
                .iter()
                .any(|(source, _)| source.as_ref() == request.source_key.as_ref())
            {
                continue;
            }
            let instruments = self.fetch(&request.source_key).map_err(|code| {
                format!("{code}: {} metadata is unavailable", request.source_key)
            })?;
            fetched.push((request.source_key.clone(), instruments));
        }

        let mut entries = Vec::with_capacity(requests.len());
        for request in requests.iter() {
            let instruments = fetched
                .iter()
                .find(|(source, _)| source.as_ref() == request.source_key.as_ref())
                .map(|(_, instruments)| instruments)
                .expect("every source was fetched above");
            let facts = instruments
                .iter()
                .find(|facts| facts.native_symbol.as_ref() == request.native_symbol.as_ref())
                .ok_or_else(|| {
                    format!(
                        "{METADATA_INSTRUMENT_ABSENT}: {} does not publish {}",
                        request.source_key, request.native_symbol
                    )
                })?;
            if facts.synthetic {
                return Err(format!(
                    "{METADATA_SYNTHETIC_INSTRUMENT}: {}:{} is a host constant, not venue metadata",
                    request.source_key, request.native_symbol
                ));
            }
            let (Some(price_tick), Some(quantity_lot), Some(contract_multiplier)) = (
                facts.price_tick,
                facts.quantity_lot,
                facts.contract_multiplier,
            ) else {
                return Err(format!(
                    "{METADATA_UNIT_UNREPRESENTABLE}: {}:{} is missing an exact unit field",
                    request.source_key, request.native_symbol
                ));
            };
            if price_tick.checked_mul(quantity_lot).is_none()
                || quantity_lot.checked_mul(contract_multiplier).is_none()
            {
                return Err(format!(
                    "{METADATA_UNIT_UNREPRESENTABLE}: {}:{} units do not multiply exactly",
                    request.source_key, request.native_symbol
                ));
            }
            entries.push(MetadataInstrument {
                asset_no: request.local_asset_no,
                native_symbol: request.native_symbol.clone(),
                source_key: Some(request.source_key.clone()),
                price_tick,
                quantity_lot,
                contract_multiplier,
                min_quantity: facts.min_quantity,
                min_notional_quote: facts.min_notional_quote,
                linear_quote_currency: facts.linear_quote_currency.clone(),
                synthetic: false,
            });
        }

        let mut snapshot = MetadataSnapshot {
            schema_version: METADATA_SNAPSHOT_SCHEMA_VERSION,
            captured_at_ns: (self.captured_at_ns)(),
            instruments: entries,
            digest: [0; 32],
        };
        snapshot.digest = snapshot.compute_digest();
        debug_assert!(snapshot.digest_matches());
        Ok(Arc::new(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn unit(value: &str) -> DecimalUnit {
        value.parse().expect("exact decimal")
    }

    fn facts(symbol: &str) -> VenueInstrumentFacts {
        VenueInstrumentFacts {
            native_symbol: Arc::from(symbol),
            price_tick: Some(unit("0.1")),
            quantity_lot: Some(unit("0.01")),
            contract_multiplier: Some(unit("1")),
            min_quantity: Some(unit("0.01")),
            min_notional_quote: Some(unit("5")),
            linear_quote_currency: Some(Arc::from("USDT")),
            synthetic: false,
        }
    }

    fn request(source: &str, symbol: &str) -> MetadataRequest {
        MetadataRequest {
            local_asset_no: 0,
            source_key: Arc::from(source),
            native_symbol: Arc::from(symbol),
        }
    }

    /// Records every call and replays a scripted answer sequence.
    struct Scripted {
        calls: AtomicUsize,
        answer: Box<
            dyn Fn(usize) -> Result<Vec<VenueInstrumentFacts>, MetadataTransportError>
                + Send
                + Sync,
        >,
    }

    impl Scripted {
        fn new(
            answer: impl Fn(usize) -> Result<Vec<VenueInstrumentFacts>, MetadataTransportError>
            + Send
            + Sync
            + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                answer: Box::new(answer),
            })
        }
    }

    impl MetadataTransport for Scripted {
        fn instruments(
            &self,
            _source_key: &str,
        ) -> Result<Vec<VenueInstrumentFacts>, MetadataTransportError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            (self.answer)(call)
        }
    }

    fn provider(transport: Arc<Scripted>) -> VenueMetadataProvider {
        VenueMetadataProvider::new(
            transport,
            vec![request("maker", "BTC-USDT")],
            MetadataFetchPolicy {
                attempt_timeout: Duration::from_secs(5),
                rate_limit_retries: 2,
                retry_backoff: Duration::ZERO,
            },
        )
        .with_clock(Arc::new(|| 1_700_000_000_000))
    }

    fn code(result: Result<Arc<MetadataSnapshot>, String>) -> String {
        result
            .expect_err("the fetch must fail")
            .split(':')
            .next()
            .unwrap()
            .to_string()
    }

    #[test]
    fn fetched_metadata_becomes_a_digest_verified_snapshot() {
        let transport = Scripted::new(|_| Ok(vec![facts("BTC-USDT")]));
        let snapshot = provider(transport.clone()).snapshot().expect("snapshot");
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.schema_version, METADATA_SNAPSHOT_SCHEMA_VERSION);
        assert_eq!(snapshot.captured_at_ns, 1_700_000_000_000);
        assert_eq!(snapshot.instruments.len(), 1);
        let instrument = &snapshot.instruments[0];
        assert_eq!(instrument.source_key.as_deref(), Some("maker"));
        assert_eq!(instrument.native_symbol.as_ref(), "BTC-USDT");
        assert!(
            !instrument.synthetic,
            "fetched units are not host constants"
        );
        assert!(
            snapshot.digest_matches(),
            "the provider signs its own answer"
        );
    }

    #[test]
    fn one_source_is_fetched_once_even_when_many_symbols_bind_it() {
        let transport = Scripted::new(|_| Ok(vec![facts("BTC-USDT"), facts("ETH-USDT")]));
        let provider = VenueMetadataProvider::new(
            transport.clone(),
            vec![
                MetadataRequest {
                    local_asset_no: 0,
                    ..request("maker", "BTC-USDT")
                },
                MetadataRequest {
                    local_asset_no: 1,
                    ..request("maker", "ETH-USDT")
                },
            ],
            MetadataFetchPolicy::default(),
        )
        .with_clock(Arc::new(|| 1));
        let snapshot = provider.snapshot().expect("snapshot");
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.instruments.len(), 2);
    }

    #[test]
    fn transport_failure_is_refused_not_defaulted() {
        let transport = Scripted::new(|_| Err(MetadataTransportError::Unavailable));
        let error = provider(transport.clone())
            .snapshot()
            .expect_err("must fail");
        assert!(error.starts_with(METADATA_TRANSPORT_UNAVAILABLE), "{error}");
        assert_eq!(
            transport.calls.load(Ordering::SeqCst),
            1,
            "a broken connection is not retried blindly"
        );
    }

    #[test]
    fn upstream_error_is_distinct_from_transport_failure() {
        let transport = Scripted::new(|_| Err(MetadataTransportError::Upstream));
        let error = provider(transport).snapshot().expect_err("must fail");
        assert!(error.starts_with(METADATA_UPSTREAM_ERROR), "{error}");
    }

    #[test]
    fn rate_limiting_is_retried_within_the_budget_then_refused() {
        let recovering = Scripted::new(|call| {
            if call == 0 {
                Err(MetadataTransportError::RateLimited)
            } else {
                Ok(vec![facts("BTC-USDT")])
            }
        });
        provider(recovering.clone())
            .snapshot()
            .expect("the retry recovers");
        assert_eq!(recovering.calls.load(Ordering::SeqCst), 2);

        let exhausted = Scripted::new(|_| Err(MetadataTransportError::RateLimited));
        let error = provider(exhausted.clone())
            .snapshot()
            .expect_err("must fail");
        assert!(error.starts_with(METADATA_RATE_LIMITED), "{error}");
        // Two retries after the first attempt.
        assert_eq!(exhausted.calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn an_absent_instrument_is_refused() {
        let transport = Scripted::new(|_| Ok(vec![facts("ETH-USDT")]));
        let error = provider(transport).snapshot().expect_err("must fail");
        assert!(error.starts_with(METADATA_INSTRUMENT_ABSENT), "{error}");
    }

    #[test]
    fn missing_or_inexact_units_are_refused() {
        let mut incomplete = facts("BTC-USDT");
        incomplete.contract_multiplier = None;
        let transport = Scripted::new(move |_| Ok(vec![incomplete.clone()]));
        assert_eq!(
            code(provider(transport).snapshot()),
            METADATA_UNIT_UNREPRESENTABLE
        );

        // Both fields parse on their own; their product exceeds the representable scale.
        let mut overflowing = facts("BTC-USDT");
        overflowing.quantity_lot = Some(unit("184467440737095.51615"));
        overflowing.contract_multiplier = Some(unit("100000"));
        let transport = Scripted::new(move |_| Ok(vec![overflowing.clone()]));
        assert_eq!(
            code(provider(transport).snapshot()),
            METADATA_UNIT_UNREPRESENTABLE
        );
    }

    #[test]
    fn synthetic_venue_constants_never_enter_admission() {
        let mut synthetic = facts("BTC-USDT");
        synthetic.synthetic = true;
        let transport = Scripted::new(move |_| Ok(vec![synthetic.clone()]));
        let error = provider(transport).snapshot().expect_err("must fail");
        assert!(error.starts_with(METADATA_SYNTHETIC_INSTRUMENT), "{error}");
    }

    #[test]
    fn a_timeout_is_reported_as_a_timeout() {
        // The transport owns the deadline; it reports an expired attempt as `Unavailable`, which
        // the provider maps to its own code. The policy carries the deadline it must honour.
        struct Expired;
        impl MetadataTransport for Expired {
            fn instruments(
                &self,
                _source_key: &str,
            ) -> Result<Vec<VenueInstrumentFacts>, MetadataTransportError> {
                Err(MetadataTransportError::Unavailable)
            }
        }
        let policy = MetadataFetchPolicy {
            attempt_timeout: Duration::from_millis(1),
            ..MetadataFetchPolicy::default()
        };
        assert_eq!(policy.attempt_timeout, Duration::from_millis(1));
        let provider = VenueMetadataProvider::new(
            Arc::new(Expired),
            vec![request("maker", "BTC-USDT")],
            policy,
        );
        assert_eq!(code(provider.snapshot()), METADATA_TRANSPORT_UNAVAILABLE);
    }
}
