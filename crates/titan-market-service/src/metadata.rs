//! Offline instrument-metadata snapshots.
//!
//! The online path fetches venue metadata at startup; backtests and traces must never fetch it.
//! Instead an explicit, digest-addressed snapshot travels with the offline input. A snapshot has
//! exactly one authoritative meaning: one entry per `(asset_no, native_symbol)` describing the
//! exchange facts and units that the run is allowed to assume.
//!
//! The digest covers a canonical byte encoding (sorted instruments, fixed key order). Any edit —
//! including reordering or re-serialization through a different language — changes it, so a loader
//! can refuse a snapshot whose digest does not match its own bytes. Synthesized constants are
//! allowed in offline fixtures but must say `synthetic = true`.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use titan_domain_types::DecimalUnit;

/// Schema version of the snapshot *body*. A body change that readers must understand bumps this.
pub const METADATA_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// One instrument's authoritative exchange facts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataInstrument {
    /// Strategy-local asset number used by the offline input.
    pub asset_no: u32,
    pub native_symbol: Arc<str>,
    /// Market source that produced the metadata; `None` for hand-authored fixtures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_key: Option<Arc<str>>,
    pub price_tick: DecimalUnit,
    pub quantity_lot: DecimalUnit,
    pub contract_multiplier: DecimalUnit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_quantity: Option<DecimalUnit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_notional_quote: Option<DecimalUnit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linear_quote_currency: Option<Arc<str>>,
    /// The host supplied these constants instead of venue metadata.
    #[serde(default)]
    pub synthetic: bool,
}

impl MetadataInstrument {
    /// Base-currency delta of one ABI quantity lot.
    pub fn base_delta_per_lot(&self) -> Option<DecimalUnit> {
        self.quantity_lot.checked_mul(self.contract_multiplier)
    }
}

/// A digest-addressed set of instrument facts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataSnapshot {
    pub schema_version: u32,
    pub captured_at_ns: i64,
    pub instruments: Vec<MetadataInstrument>,
    /// SHA-256 over [`MetadataSnapshot::canonical_bytes`].
    pub digest: [u8; 32],
}

fn canonical_unit(unit: DecimalUnit) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    map.insert(
        "coefficient".into(),
        serde_json::Value::from(unit.coefficient()),
    );
    map.insert("scale".into(), serde_json::Value::from(unit.scale()));
    serde_json::Value::Object(map)
}

fn canonical_optional_unit(unit: Option<DecimalUnit>) -> serde_json::Value {
    unit.map(canonical_unit).unwrap_or(serde_json::Value::Null)
}

fn canonical_optional_string(value: Option<&Arc<str>>) -> serde_json::Value {
    value
        .map(|value| serde_json::Value::from(value.as_ref()))
        .unwrap_or(serde_json::Value::Null)
}

/// Canonical JSON value with alphabetical keys and instruments sorted by identity.
///
/// The rule is intentionally simple and reproducible in another language: sort the instrument
/// array by `(source_key ?? "", asset_no, native_symbol)`, emit every key in alphabetical order and
/// every absent optional value as `null`.
pub fn canonical_snapshot_value(snapshot: &MetadataSnapshot) -> serde_json::Value {
    let mut instruments: Vec<&MetadataInstrument> = snapshot.instruments.iter().collect();
    instruments.sort_by(|left, right| {
        left.source_key
            .as_deref()
            .unwrap_or("")
            .cmp(right.source_key.as_deref().unwrap_or(""))
            .then(left.asset_no.cmp(&right.asset_no))
            .then(
                left.native_symbol
                    .as_ref()
                    .cmp(right.native_symbol.as_ref()),
            )
    });
    let instruments = instruments
        .into_iter()
        .map(|instrument| {
            let mut map = serde_json::Map::new();
            map.insert(
                "asset_no".into(),
                serde_json::Value::from(instrument.asset_no),
            );
            map.insert(
                "contract_multiplier".into(),
                canonical_unit(instrument.contract_multiplier),
            );
            map.insert(
                "linear_quote_currency".into(),
                canonical_optional_string(instrument.linear_quote_currency.as_ref()),
            );
            map.insert(
                "min_notional_quote".into(),
                canonical_optional_unit(instrument.min_notional_quote),
            );
            map.insert(
                "min_quantity".into(),
                canonical_optional_unit(instrument.min_quantity),
            );
            map.insert(
                "native_symbol".into(),
                serde_json::Value::from(instrument.native_symbol.as_ref()),
            );
            map.insert("price_tick".into(), canonical_unit(instrument.price_tick));
            map.insert(
                "quantity_lot".into(),
                canonical_unit(instrument.quantity_lot),
            );
            map.insert(
                "source_key".into(),
                canonical_optional_string(instrument.source_key.as_ref()),
            );
            map.insert(
                "synthetic".into(),
                serde_json::Value::from(instrument.synthetic),
            );
            serde_json::Value::Object(map)
        })
        .collect::<Vec<_>>();
    let mut root = serde_json::Map::new();
    root.insert(
        "captured_at_ns".into(),
        serde_json::Value::from(snapshot.captured_at_ns),
    );
    root.insert("instruments".into(), serde_json::Value::Array(instruments));
    root.insert(
        "schema_version".into(),
        serde_json::Value::from(snapshot.schema_version),
    );
    serde_json::Value::Object(root)
}

impl MetadataSnapshot {
    /// Canonical bytes covered by the digest (the digest field itself is never hashed).
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&canonical_snapshot_value(self)).expect("canonical JSON is encodable")
    }

    pub fn compute_digest(&self) -> [u8; 32] {
        Sha256::digest(self.canonical_bytes()).into()
    }

    /// Recomputes the digest and compares it with the stored one.
    pub fn digest_matches(&self) -> bool {
        self.digest == self.compute_digest()
    }

    /// Finds one entry by the venue's native symbol (and source when recorded).
    ///
    /// Symbol identity is stable across the config and trace namespaces; `asset_no` is only the
    /// consumer-local numbering and differs between them.
    pub fn instrument_by_symbol(
        &self,
        native_symbol: &str,
        source_key: Option<&str>,
    ) -> Option<&MetadataInstrument> {
        self.instruments.iter().find(|instrument| {
            instrument.native_symbol.as_ref() == native_symbol
                && source_key.is_none_or(|source| {
                    instrument
                        .source_key
                        .as_deref()
                        .is_none_or(|recorded| recorded == source)
                })
        })
    }

    /// Finds one entry by asset number, preferring the requested source when the snapshot records it.
    pub fn instrument(
        &self,
        asset_no: u32,
        source_key: Option<&str>,
    ) -> Option<&MetadataInstrument> {
        self.instruments.iter().find(|instrument| {
            instrument.asset_no == asset_no
                && source_key.is_none_or(|source| {
                    instrument
                        .source_key
                        .as_deref()
                        .is_none_or(|recorded| recorded == source)
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> MetadataSnapshot {
        let mut snapshot = MetadataSnapshot {
            schema_version: METADATA_SNAPSHOT_SCHEMA_VERSION,
            captured_at_ns: 1_700_000_000_000,
            instruments: vec![
                MetadataInstrument {
                    asset_no: 1,
                    native_symbol: Arc::from("BTCUSDT"),
                    source_key: Some(Arc::from("hedge")),
                    price_tick: "0.1".parse().unwrap(),
                    quantity_lot: "0.001".parse().unwrap(),
                    contract_multiplier: "1".parse().unwrap(),
                    min_quantity: Some("0.001".parse().unwrap()),
                    min_notional_quote: Some("5".parse().unwrap()),
                    linear_quote_currency: Some(Arc::from("USDT")),
                    synthetic: false,
                },
                MetadataInstrument {
                    asset_no: 0,
                    native_symbol: Arc::from("BTC-USDT-SWAP"),
                    source_key: Some(Arc::from("maker")),
                    price_tick: "0.1".parse().unwrap(),
                    quantity_lot: "0.01".parse().unwrap(),
                    contract_multiplier: "0.01".parse().unwrap(),
                    min_quantity: None,
                    min_notional_quote: None,
                    linear_quote_currency: Some(Arc::from("USDT")),
                    synthetic: false,
                },
            ],
            digest: [0; 32],
        };
        snapshot.digest = snapshot.compute_digest();
        snapshot
    }

    #[test]
    fn digest_is_stable_across_instrument_order_and_detects_edit() {
        let first = snapshot();
        assert!(first.digest_matches());
        let mut reordered = first.clone();
        reordered.instruments.reverse();
        assert_eq!(reordered.compute_digest(), first.digest);
        let mut edited = first.clone();
        edited.instruments[0].price_tick = "0.2".parse().unwrap();
        assert_ne!(edited.compute_digest(), first.digest);
        assert!(!edited.digest_matches());
    }

    #[test]
    fn canonical_bytes_match_the_documented_rule() {
        let first = snapshot();
        let text = String::from_utf8(first.canonical_bytes()).unwrap();
        assert!(text.starts_with(r#"{"captured_at_ns":1700000000000,"#));
        assert!(text.contains(r#""native_symbol":"BTC-USDT-SWAP""#));
        // Sorted by source key: "hedge" sorts before "maker".
        let hedge = text.find("BTCUSDT").unwrap();
        let maker = text.find("BTC-USDT-SWAP").unwrap();
        assert!(hedge < maker, "{text}");
    }

    #[test]
    fn base_delta_and_lookup_follow_the_recorded_identity() {
        let first = snapshot();
        assert_eq!(
            first.instruments[1].base_delta_per_lot(),
            Some("0.0001".parse().unwrap())
        );
        assert!(first.instrument(1, Some("hedge")).is_some());
        assert!(first.instrument(1, Some("maker")).is_none());
        assert!(first.instrument(2, None).is_none());
    }
}
