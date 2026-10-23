//! Public CrossEx futures feeds can retain the underlying venue's contract counts, even while
//! CrossEx orders use base quantities. This multiplier applies to public quantities only.
use super::{GateCrossExClient, rest::*};
use crate::api::ApiError;
use serde_json::Value;
use std::collections::BTreeMap;

impl GateCrossExClient {
    pub fn with_market_quantity_multipliers(
        mut self,
        values: &BTreeMap<String, f64>,
    ) -> Result<Self, ApiError> {
        for (symbol, multiplier) in values {
            symbol_parts(symbol)?;
            if !multiplier.is_finite() || *multiplier <= 0.0 {
                return Err(invalid(
                    "market quantity multiplier must be finite and positive",
                ));
            }
        }
        self.market_quantity_multipliers = values.clone();
        Ok(self)
    }
    pub(super) async fn market_quantity_multiplier(&self, symbol: &str) -> Result<f64, ApiError> {
        let (venue, base, quote) = symbol_parts(symbol)?;
        if let Some(value) = self.market_quantity_multipliers.get(symbol) {
            return Ok(*value);
        }
        if !symbol.contains("_FUTURE_") {
            return Ok(1.0);
        }
        match venue {
            "BINANCE" | "BYBIT" | "HYPERLIQUID" | "LIGHTER" => Ok(1.0),
            "GATE" if matches!(quote, "USDT" | "USDC") => {
                let value = self
                    .get(
                        &format!(
                            "/futures/{}/contracts/{base}_{quote}",
                            quote.to_ascii_lowercase()
                        ),
                        &[],
                        false,
                    )
                    .await?;
                gate_multiplier(&value, &format!("{base}_{quote}"))
            }
            "OKX" if matches!(quote, "USDT" | "USDC") => {
                let instrument = format!("{base}-{quote}-SWAP");
                let response = self
                    .http
                    .get("https://www.okx.com/api/v5/public/instruments")
                    .query(&[("instType", "SWAP"), ("instId", instrument.as_str())])
                    .send()
                    .await
                    .map_err(|e| ApiError::transport(EXCHANGE, e.without_url()))?;
                if !response.status().is_success() {
                    return Err(unsupported(
                        "OKX public contract metadata unavailable; configure a verified market_quantity_multipliers override",
                    ));
                }
                let value: Value = response
                    .json()
                    .await
                    .map_err(|_| protocol("invalid OKX public contract metadata"))?;
                okx_multiplier(&value, &instrument, base)
            }
            _ => Err(unsupported(
                "public futures quantity unit requires a verified market_quantity_multipliers override",
            )),
        }
    }
}
pub(super) fn gate_multiplier(value: &Value, contract: &str) -> Result<f64, ApiError> {
    if value["name"] != contract || value["type"] != "direct" {
        return Err(protocol(
            "Gate public multiplier requires the matching linear contract",
        ));
    }
    positive_multiplier(number(value, "quanto_multiplier")?)
}
pub(super) fn okx_multiplier(value: &Value, instrument: &str, base: &str) -> Result<f64, ApiError> {
    if value["code"] != "0" {
        return Err(protocol("OKX contract metadata was not accepted"));
    }
    let row = rows(&value["data"])?
        .iter()
        .find(|v| v["instId"] == instrument)
        .ok_or_else(|| protocol("missing OKX contract metadata"))?;
    if row["ctType"] != "linear" || row["ctValCcy"] != base {
        return Err(protocol(
            "OKX public multiplier requires a linear base-denominated contract",
        ));
    }
    let multiple = if row.get("ctMult").is_none_or(|v| v.is_null() || v == "") {
        1.0
    } else {
        number(row, "ctMult")?
    };
    positive_multiplier(
        scale_decimal(&text(row, "ctVal")?, multiple)?
            .parse()
            .map_err(|_| protocol("invalid OKX multiplier"))?,
    )
}
fn positive_multiplier(value: f64) -> Result<f64, ApiError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(protocol("invalid public contract multiplier"));
    }
    Ok(value)
}
pub(super) fn scale_quantity(value: f64, multiplier: f64) -> Result<f64, ApiError> {
    if !value.is_finite() || value < 0.0 {
        return Err(protocol("invalid normalized market quantity"));
    }
    let result = scale_decimal(&value.to_string(), multiplier)?
        .parse::<f64>()
        .map_err(|_| protocol("normalized market quantity overflow"))?;
    if !result.is_finite() || result < 0.0 {
        return Err(protocol("invalid normalized market quantity"));
    }
    Ok(result)
}

/// Preserve ordinary decimal quantities for MarketService's exact lot parser. Passing a
/// floating product through to_string can turn 33560 * 0.0001 into 3.3560000000000003.
pub(super) fn scale_decimal(quantity: &str, multiplier: f64) -> Result<String, ApiError> {
    positive_multiplier(multiplier)?;
    fn parts(value: &str) -> Result<(u128, usize), ApiError> {
        let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
        if whole.is_empty()
            || !whole.bytes().all(|c| c.is_ascii_digit())
            || !fraction.bytes().all(|c| c.is_ascii_digit())
        {
            return Err(protocol("ordinary positive decimal quantity required"));
        }
        let fraction = fraction.trim_end_matches('0');
        if fraction.len() > 38 {
            return Err(protocol("market decimal scale exceeds range"));
        }
        let digits = format!("{}{fraction}", whole.trim_start_matches('0'));
        let coefficient = if digits.is_empty() {
            0
        } else {
            digits
                .parse()
                .map_err(|_| protocol("market decimal exceeds range"))?
        };
        Ok((coefficient, fraction.len()))
    }
    let (quantity, qscale) = parts(quantity)?;
    let (multiple, mscale) = parts(&multiplier.to_string())?;
    let coefficient = quantity
        .checked_mul(multiple)
        .ok_or_else(|| protocol("market quantity product overflow"))?;
    if coefficient == 0 {
        return Ok("0".into());
    }
    let scale = qscale + mscale;
    let digits = coefficient.to_string();
    let mut result = if scale == 0 {
        digits
    } else if digits.len() > scale {
        format!(
            "{}.{}",
            &digits[..digits.len() - scale],
            &digits[digits.len() - scale..]
        )
    } else {
        format!("0.{}{digits}", "0".repeat(scale - digits.len()))
    };
    if result.contains('.') {
        result = result
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_owned();
    }
    Ok(result)
}
