//! Shared floating point boundary conversion. Native decimal strings keep their exact parser.
use titan_domain_types::DecimalUnit;

pub(crate) fn integer_units(value: f64, unit: f64, exact: bool) -> Result<i64, &'static str> {
    if !value.is_finite() || !unit.is_finite() || unit <= 0.0 {
        return Err("non-finite value or invalid instrument unit");
    }
    let scaled = value / unit;
    let rounded = scaled.round();
    if !rounded.is_finite() || rounded < i64::MIN as f64 || rounded >= i64::MAX as f64 {
        return Err("decimal exceeds configured integer range");
    }
    if exact && (scaled - rounded).abs() > 1e-7 {
        return Err("decimal is not exactly representable in configured units");
    }
    Ok(rounded as i64)
}
pub(crate) fn from_units(value: i64, unit: DecimalUnit) -> f64 {
    value as f64 * unit.as_f64()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quantities_are_exact_observations_can_round_and_overflow_never_saturates() {
        assert_eq!(integer_units(1.25, 0.01, true), Ok(125));
        assert!(integer_units(1.251, 0.01, true).is_err());
        assert_eq!(integer_units(1.251, 0.01, false), Ok(125));
        assert!(integer_units(i64::MAX as f64, 1.0, true).is_err());
        assert_eq!(integer_units(i64::MIN as f64, 1.0, true), Ok(i64::MIN));
        assert!(integer_units(f64::NAN, 0.01, false).is_err());
    }
}
