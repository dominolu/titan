//! Official Omni v2 statistics API. Trading is not publicly available.
//! Indicative quotes can be cached for 600s; this module intentionally exposes no BrokerApi or
//! MarketConnectorFactory, so statistics cannot accidentally enter executable market bindings.
use crate::{
    api::ApiError,
    venue::{endpoint, http_client},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
const NAME: &str = "variational";
pub const DEFAULT_URL: &str = "https://omni-client-api.prod.ap-northeast-1.variational.io";
#[derive(Debug, thiserror::Error)]
pub enum VariationalError {
    #[error("{0}")]
    Api(#[from] ApiError),
    #[error("statistics rate limited; retry after {retry_after_seconds} seconds")]
    RateLimited { retry_after_seconds: u64 },
}
/// Native decimal strings preserve the API's USDC amounts and rate precision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    pub total_volume_24h: String,
    pub cumulative_volume: String,
    pub tvl: String,
    pub open_interest: String,
    pub num_markets: u64,
    pub listings: Vec<Listing>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContractType {
    Perpetual,
    Swap,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Listing {
    pub ticker: String,
    pub contract_type: ContractType,
    pub name: Option<String>,
    pub mark_price: String,
    pub volume_24h: String,
    pub open_interest: OpenInterestUsdc,
    pub funding: Option<Funding>,
    pub base_spread_bps: Option<String>,
    pub quotes: Option<Quotes>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenInterestUsdc {
    pub long_open_interest: String,
    pub short_open_interest: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Funding {
    Perpetual(PerpetualFunding),
    Swap(SwapFunding),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerpetualFunding {
    pub last: PerpetualRate,
    pub next: Option<PerpetualRate>,
    pub interval_seconds: u64,
    pub next_timestamp: i64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerpetualRate {
    pub annualized_rate: String,
    pub interval_rate: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwapFunding {
    pub last: Option<SwapRate>,
    pub next: Option<SwapRate>,
    pub interval_seconds: u64,
    pub next_timestamp: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwapRate {
    pub annualized_long_rate: String,
    pub annualized_short_rate: String,
    pub interval_long_rate: String,
    pub interval_short_rate: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quotes {
    pub updated_at: DateTime<Utc>,
    pub base: Option<Quote>,
    pub size_1k: Option<Quote>,
    pub size_100k: Option<Quote>,
    pub size_1m: Option<Quote>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quote {
    pub bid: String,
    pub ask: String,
}
impl Quotes {
    /// Age uses the source timestamp, never the local HTTP receipt time.
    pub fn age(&self, now: DateTime<Utc>) -> Option<Duration> {
        now.signed_duration_since(self.updated_at).to_std().ok()
    }
    pub fn is_fresh(&self, now: DateTime<Utc>, maximum_age: Duration) -> bool {
        self.age(now).is_some_and(|age| age <= maximum_age)
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct StatsSnapshot {
    pub received_at_ms: i64,
    pub stats: Stats,
}
#[derive(Clone, Copy)]
pub enum StatsVersion {
    LegacyV1,
    V2,
}
pub struct NativeStatsSnapshot {
    pub received_at_ms: i64,
    pub body: serde_json::Value,
}
#[derive(Clone)]
pub struct VariationalClient {
    url: String,
    http: reqwest::Client,
    next: Arc<tokio::sync::Mutex<tokio::time::Instant>>,
}
impl VariationalClient {
    pub fn new(url: &str) -> Result<Self, ApiError> {
        endpoint(url, false, NAME)?;
        Ok(Self {
            url: url.trim_end_matches('/').into(),
            http: http_client(NAME)?,
            next: Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now())),
        })
    }
    pub async fn stats(&self) -> Result<StatsSnapshot, VariationalError> {
        let snapshot = self.native_stats(StatsVersion::V2).await?;
        let stats: Stats = serde_json::from_value(snapshot.body)
            .map_err(|_| ApiError::new(NAME, "PROTOCOL", "invalid v2 stats response"))?;
        validate(&stats)?;
        Ok(StatsSnapshot {
            received_at_ms: snapshot.received_at_ms,
            stats,
        })
    }
    /// Includes the documented legacy route and preserves all native response fields.
    /// v1 is deprecated and must never be silently interpreted as the v2 contract model.
    pub async fn native_stats(
        &self,
        version: StatsVersion,
    ) -> Result<NativeStatsSnapshot, VariationalError> {
        let mut next = self.next.lock().await;
        let now = tokio::time::Instant::now();
        if *next > now {
            return Err(VariationalError::RateLimited {
                retry_after_seconds: next.duration_since(now).as_secs().saturating_add(1),
            });
        }
        *next = now + Duration::from_secs(1);
        let r = self
            .http
            .get(format!(
                "{}{}",
                self.url,
                match version {
                    StatsVersion::LegacyV1 => "/metadata/stats",
                    StatsVersion::V2 => "/metadata/v2/stats",
                }
            ))
            .send()
            .await
            .map_err(|e| ApiError::transport(NAME, e.without_url()))?;
        if r.status().as_u16() == 429 {
            let v: serde_json::Value = r
                .json()
                .await
                .map_err(|_| ApiError::new(NAME, "PROTOCOL", "invalid 429 response"))?;
            let seconds = v["retry_after_seconds"]
                .as_u64()
                .filter(|n| *n > 0 && *n <= 86400)
                .ok_or_else(|| ApiError::new(NAME, "PROTOCOL", "invalid retry_after_seconds"))?;
            *next = tokio::time::Instant::now() + Duration::from_secs(seconds);
            return Err(VariationalError::RateLimited {
                retry_after_seconds: seconds,
            });
        }
        if !r.status().is_success() {
            return Err(ApiError::http(NAME, r.status().as_u16(), "stats request failed").into());
        }
        let stats: serde_json::Value = r
            .json()
            .await
            .map_err(|_| ApiError::new(NAME, "PROTOCOL", "invalid v2 stats response"))?;
        if !stats.is_object() {
            return Err(ApiError::new(NAME, "PROTOCOL", "invalid native stats response").into());
        }
        Ok(NativeStatsSnapshot {
            received_at_ms: Utc::now().timestamp_millis(),
            body: stats,
        })
    }
}
impl Default for VariationalClient {
    fn default() -> Self {
        Self::new(DEFAULT_URL).expect("valid built-in endpoint")
    }
}
fn decimal(s: &str, nonnegative: bool) -> Result<f64, ApiError> {
    let n = s
        .parse::<f64>()
        .ok()
        .filter(|n| n.is_finite() && (!nonnegative || *n >= 0.0));
    n.ok_or_else(|| ApiError::new(NAME, "PROTOCOL", "invalid decimal"))
}
fn validate(s: &Stats) -> Result<(), ApiError> {
    for v in [
        &s.total_volume_24h,
        &s.cumulative_volume,
        &s.tvl,
        &s.open_interest,
    ] {
        decimal(v, true)?;
    }
    let mut symbols = std::collections::HashSet::new();
    for l in &s.listings {
        let suffix = match l.contract_type {
            ContractType::Perpetual => "-PERP",
            ContractType::Swap => "-SWAP",
        };
        if !l.ticker.ends_with(suffix) || !symbols.insert(&l.ticker) {
            return Err(ApiError::new(
                NAME,
                "PROTOCOL",
                "duplicate or ambiguous listing",
            ));
        }
        for v in [
            &l.mark_price,
            &l.volume_24h,
            &l.open_interest.long_open_interest,
            &l.open_interest.short_open_interest,
        ] {
            decimal(v, true)?;
        }
        match (&l.contract_type, &l.funding) {
            (_, None) => {}
            (ContractType::Perpetual, Some(Funding::Perpetual(f))) => {
                if f.interval_seconds == 0 || f.next_timestamp < 0 {
                    return Err(ApiError::new(
                        NAME,
                        "PROTOCOL",
                        "invalid perpetual funding timing",
                    ));
                }
                for r in [Some(&f.last), f.next.as_ref()].into_iter().flatten() {
                    decimal(&r.annualized_rate, false)?;
                    decimal(&r.interval_rate, false)?;
                }
            }
            (ContractType::Swap, Some(Funding::Swap(f))) => {
                if f.interval_seconds != 86400
                    || f.last.is_none() && f.next.is_none()
                    || f.next.is_some() != f.next_timestamp.is_some()
                    || f.next_timestamp.is_some_and(|n| n < 0)
                {
                    return Err(ApiError::new(
                        NAME,
                        "PROTOCOL",
                        "invalid swap funding timing",
                    ));
                }
                for r in [f.last.as_ref(), f.next.as_ref()].into_iter().flatten() {
                    for v in [
                        &r.annualized_long_rate,
                        &r.annualized_short_rate,
                        &r.interval_long_rate,
                        &r.interval_short_rate,
                    ] {
                        decimal(v, false)?;
                    }
                }
            }
            _ => {
                return Err(ApiError::new(
                    NAME,
                    "PROTOCOL",
                    "funding contract type mismatch",
                ));
            }
        }
        if let Some(spread) = &l.base_spread_bps {
            decimal(spread, true)?;
        }
        if let Some(q) = &l.quotes {
            for p in [
                q.base.as_ref(),
                q.size_1k.as_ref(),
                q.size_100k.as_ref(),
                q.size_1m.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                let bid = decimal(&p.bid, true)?;
                let ask = decimal(&p.ask, true)?;
                if bid > ask || bid == 0.0 {
                    return Err(ApiError::new(NAME, "PROTOCOL", "invalid indicative quote"));
                }
            }
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::venue::test_support::*;
    use serde_json::json;
    fn stats() -> serde_json::Value {
        json!({"total_volume_24h":"1000","cumulative_volume":"2000","tvl":"500","open_interest":"100","num_markets":3,"listings":[{"ticker":"BTC-PERP","contract_type":"perpetual","mark_price":"100.123456789012","volume_24h":"10","open_interest":{"long_open_interest":"20","short_open_interest":"30"},"funding":{"last":{"annualized_rate":"0.09","interval_rate":"0.00008246"},"next":null,"interval_seconds":28800,"next_timestamp":1792137600},"quotes":{"updated_at":"2026-10-16T02:51:10.363561208Z","size_1k":{"bid":"100","ask":"101"}}},{"ticker":"XAU-SWAP","contract_type":"swap","mark_price":"4000","volume_24h":"20","open_interest":{"long_open_interest":"30","short_open_interest":"40"},"funding":{"last":{"annualized_long_rate":"0.05","annualized_short_rate":"0.02","interval_long_rate":"0.00013","interval_short_rate":"0.00005"},"next":null,"interval_seconds":86400,"next_timestamp":null}},{"ticker":"CLOSED-SWAP","contract_type":"swap","mark_price":"10","volume_24h":"0","open_interest":{"long_open_interest":"0","short_open_interest":"0"},"funding":null}]})
    }
    #[test]
    fn v2_preserves_contract_units_missing_data_and_source_quote_age() {
        let s: Stats = serde_json::from_value(stats()).unwrap();
        validate(&s).unwrap();
        assert_eq!(s.listings[0].mark_price, "100.123456789012");
        assert!(matches!(s.listings[1].funding, Some(Funding::Swap(_))));
        assert!(s.listings[2].funding.is_none());
        let q = s.listings[0].quotes.as_ref().unwrap();
        assert!(!q.is_fresh(
            "2026-10-16T03:01:11Z".parse().unwrap(),
            Duration::from_secs(600)
        ));
        assert!(q.age("2026-10-16T02:50:00Z".parse().unwrap()).is_none());
    }
    #[test]
    fn annualized_and_interval_rates_cannot_cross_contract_types() {
        let mut v = stats();
        v["listings"][0]["contract_type"] = json!("swap");
        let s: Stats = serde_json::from_value(v).unwrap();
        assert!(validate(&s).is_err());
        let mut v = stats();
        v["listings"][0]["mark_price"] = json!("NaN");
        let s: Stats = serde_json::from_value(v).unwrap();
        assert!(validate(&s).is_err());
    }
    #[tokio::test]
    async fn official_v2_path_and_shared_rate_limit_are_enforced() {
        let (url, mut rx, task) = http(vec![(200, stats())]).await;
        let c = VariationalClient::new(&url).unwrap();
        let s = c.stats().await.unwrap();
        assert_eq!(s.stats.num_markets, 3);
        assert!(matches!(
            c.clone().stats().await,
            Err(VariationalError::RateLimited { .. })
        ));
        assert!(
            rx.recv()
                .await
                .unwrap()
                .head
                .starts_with("GET /metadata/v2/stats ")
        );
        task.await.unwrap();
    }
    #[tokio::test]
    async fn rate_limit_response_defers_all_clones() {
        let (url, mut rx, task) = http(vec![(429, json!({"retry_after_seconds":12}))]).await;
        let c = VariationalClient::new(&url).unwrap();
        assert!(matches!(
            c.stats().await,
            Err(VariationalError::RateLimited {
                retry_after_seconds: 12
            })
        ));
        assert!(matches!(
            c.clone().stats().await,
            Err(VariationalError::RateLimited { .. })
        ));
        rx.recv().await.unwrap();
        task.await.unwrap();
    }
}
