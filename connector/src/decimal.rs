//! Exact, allocation-free conversion of borrowed venue decimal quantities into ABI lots.
//! The multiplier is compiled once from venue contract metadata, never guessed from lot size.
use titan_domain_types::DecimalUnit;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecimalMultiplier {
    coefficient: u128,
    scale: u32,
}
fn parts(value: &str) -> Result<(u128, u32), &'static str> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|v| v.is_ascii_digit())
        || !fraction.bytes().all(|v| v.is_ascii_digit())
    {
        return Err("ordinary nonnegative decimal required");
    }
    let fraction = fraction.trim_end_matches('0');
    if fraction.len() > 38 {
        return Err("decimal scale exceeds range");
    }
    let mut coefficient = 0_u128;
    for digit in whole.bytes().chain(fraction.bytes()) {
        coefficient = coefficient
            .checked_mul(10)
            .and_then(|v| v.checked_add(u128::from(digit - b'0')))
            .ok_or("decimal coefficient overflow")?;
    }
    Ok((coefficient, fraction.len() as u32))
}
impl DecimalMultiplier {
    pub const ONE: Self = Self {
        coefficient: 1,
        scale: 0,
    };
    pub fn parse(value: &str) -> Result<Self, &'static str> {
        let (coefficient, scale) = parts(value)?;
        if coefficient == 0 {
            return Err("multiplier must be positive");
        }
        Ok(Self { coefficient, scale })
    }
    /// A cold metadata boundary; no float products are used for ABI quantity conversion.
    pub fn from_f64(value: f64) -> Result<Self, &'static str> {
        if !value.is_finite() || value <= 0.0 {
            return Err("invalid multiplier");
        }
        Self::parse(&value.to_string())
    }
    pub fn as_f64(self) -> f64 {
        self.coefficient as f64 / 10_f64.powi(self.scale as i32)
    }
    pub fn to_lots(self, value: &str, unit: DecimalUnit) -> Result<i64, &'static str> {
        let (coefficient, scale) = parts(value)?;
        if coefficient == 0 {
            return Ok(0);
        }
        let mut product = coefficient
            .checked_mul(self.coefficient)
            .ok_or("scaled decimal overflow")?;
        let scale = scale + self.scale;
        let target = u32::from(unit.scale());
        if scale > target {
            let divisor = 10_u128
                .checked_pow(scale - target)
                .ok_or("decimal scale overflow")?;
            if !product.is_multiple_of(divisor) {
                return Err("quantity is not aligned to lots");
            }
            product /= divisor;
        } else {
            product = product
                .checked_mul(
                    10_u128
                        .checked_pow(target - scale)
                        .ok_or("decimal scale overflow")?,
                )
                .ok_or("scaled decimal overflow")?;
        }
        let divisor = u128::from(unit.coefficient());
        if !product.is_multiple_of(divisor) {
            return Err("quantity is not aligned to lots");
        }
        i64::try_from(product / divisor).map_err(|_| "quantity lots overflow")
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn contract_units_are_exact_for_gate_okx_and_identity() {
        let lots: DecimalUnit = "0.0001".parse().unwrap();
        assert_eq!(
            DecimalMultiplier::parse("0.0001")
                .unwrap()
                .to_lots("33560", lots),
            Ok(33560)
        );
        assert_eq!(
            DecimalMultiplier::parse("0.01")
                .unwrap()
                .to_lots("2264.57", lots),
            Ok(226457)
        );
        assert_eq!(DecimalMultiplier::ONE.to_lots("0.0030", lots), Ok(30));
        assert_eq!(
            DecimalMultiplier::parse("2.5")
                .unwrap()
                .to_lots("3", "0.5".parse().unwrap()),
            Ok(15)
        );
        assert_eq!(
            DecimalMultiplier::ONE.to_lots("9223372036854775807", "1".parse().unwrap()),
            Ok(i64::MAX)
        );
    }
    #[test]
    fn invalid_unaligned_and_overflow_quantities_are_rejected() {
        let unit: DecimalUnit = "0.0001".parse().unwrap();
        for value in ["-1", "NaN", "inf", "1e3", "1.2.3", ""] {
            assert!(DecimalMultiplier::ONE.to_lots(value, unit).is_err());
        }
        assert!(DecimalMultiplier::ONE.to_lots("0.00001", unit).is_err());
        assert!(
            DecimalMultiplier::ONE
                .to_lots("9223372036854775808", "1".parse().unwrap())
                .is_err()
        );
        assert!(DecimalMultiplier::parse("0").is_err());
        assert!(DecimalMultiplier::from_f64(f64::INFINITY).is_err());
    }
}
