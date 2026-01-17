//! Lossless decimal numeric type backed by rust_decimal.
//!
//! Provides canonical parsing from strings and formatting without exponent notation.

use rust_decimal::Decimal as RustDecimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Lossless decimal numeric type for financial calculations.
///
/// Backed by rust_decimal to avoid floating-point drift.
/// Serializes to JSON number (not string) by default.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Decimal(#[serde(with = "rust_decimal::serde::float")] RustDecimal);

impl Decimal {
    /// Create a Decimal from a RustDecimal.
    pub fn new(value: RustDecimal) -> Self {
        Decimal(value)
    }

    /// Parse a Decimal from a string losslessly.
    ///
    /// # Errors
    /// Returns an error if the string is not a valid decimal number.
    pub fn from_str_canonical(s: &str) -> Result<Self, rust_decimal::Error> {
        RustDecimal::from_str(s).map(Decimal)
    }

    /// Format the Decimal as a canonical string (no exponent notation).
    pub fn to_canonical_string(&self) -> String {
        // Use normalize() to remove trailing zeros, then format without exponent
        let normalized = self.0.normalize();
        format!("{}", normalized)
    }

    /// Get the underlying RustDecimal.
    pub fn inner(&self) -> RustDecimal {
        self.0
    }

    /// The additive identity (0).
    pub fn zero() -> Self {
        Decimal(RustDecimal::ZERO)
    }

    /// Returns true if the value is exactly zero.
    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    /// Returns true if the value is > 0.
    pub fn is_positive(&self) -> bool {
        !self.is_zero() && self.0.is_sign_positive()
    }

    /// Returns true if the value is < 0.
    pub fn is_negative(&self) -> bool {
        !self.is_zero() && self.0.is_sign_negative()
    }

    /// Absolute value.
    pub fn abs(&self) -> Self {
        Decimal(self.0.abs())
    }

    /// Returns the value 100.
    pub fn hundred() -> Self {
        Decimal(RustDecimal::ONE_HUNDRED)
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_canonical_string())
    }
}

impl FromStr for Decimal {
    type Err = rust_decimal::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_canonical(s)
    }
}

impl From<RustDecimal> for Decimal {
    fn from(value: RustDecimal) -> Self {
        Decimal(value)
    }
}

impl From<Decimal> for RustDecimal {
    fn from(value: Decimal) -> Self {
        value.0
    }
}

// Arithmetic operations
impl std::ops::Add for Decimal {
    type Output = Decimal;

    fn add(self, rhs: Decimal) -> Decimal {
        Decimal(self.0 + rhs.0)
    }
}

impl std::ops::Sub for Decimal {
    type Output = Decimal;

    fn sub(self, rhs: Decimal) -> Decimal {
        Decimal(self.0 - rhs.0)
    }
}

impl std::ops::Mul for Decimal {
    type Output = Decimal;

    fn mul(self, rhs: Decimal) -> Decimal {
        Decimal(self.0 * rhs.0)
    }
}

impl std::ops::Div for Decimal {
    type Output = Decimal;

    fn div(self, rhs: Decimal) -> Decimal {
        Decimal(self.0 / rhs.0)
    }
}

impl std::ops::Neg for Decimal {
    type Output = Decimal;

    fn neg(self) -> Decimal {
        Decimal(-self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_parse_roundtrip() {
        let test_cases = vec![
            "123.456",
            "0.0001",
            "1000000",
            "-123.456",
            "0",
            "999999999.999999999",
        ];

        for s in test_cases {
            let decimal = Decimal::from_str_canonical(s).expect("parse failed");
            let formatted = decimal.to_canonical_string();
            let reparsed = Decimal::from_str_canonical(&formatted).expect("reparse failed");
            assert_eq!(decimal, reparsed, "roundtrip failed for {}", s);
        }
    }

    #[test]
    fn test_decimal_canonical_no_exponent() {
        // rust_decimal supports scientific notation parsing
        let decimal = Decimal::from_str_canonical("123").expect("parse failed");
        let formatted = decimal.to_canonical_string();
        assert!(
            !formatted.contains('e'),
            "formatted string should not contain exponent"
        );
        assert_eq!(formatted, "123");
    }

    #[test]
    fn test_decimal_arithmetic() {
        let a = Decimal::from_str_canonical("10.5").unwrap();
        let b = Decimal::from_str_canonical("2.5").unwrap();

        let sum = a + b;
        assert_eq!(sum.to_canonical_string(), "13");

        let diff = a - b;
        assert_eq!(diff.to_canonical_string(), "8");

        let prod = a * b;
        assert_eq!(prod.to_canonical_string(), "26.25");
    }

    #[test]
    fn test_decimal_json_serialization() {
        let decimal = Decimal::from_str_canonical("123.456").unwrap();
        let json = serde_json::to_value(decimal).unwrap();
        // Should serialize as a JSON number, not a string
        assert!(json.is_number());
        assert_eq!(json.to_string(), "123.456");
    }

    #[test]
    fn test_decimal_from_rust_decimal() {
        let rust_decimal = RustDecimal::from_str("42.5").unwrap();
        let decimal = Decimal::from(rust_decimal);
        assert_eq!(decimal.to_canonical_string(), "42.5");
    }

    #[test]
    fn test_decimal_to_rust_decimal() {
        let decimal = Decimal::from_str_canonical("42.5").unwrap();
        let rust_decimal: RustDecimal = decimal.into();
        assert_eq!(rust_decimal, RustDecimal::from_str("42.5").unwrap());
    }

    #[test]
    fn test_decimal_display() {
        let decimal = Decimal::from_str_canonical("99.99").unwrap();
        assert_eq!(decimal.to_string(), "99.99");
    }

    #[test]
    fn test_decimal_division() {
        let a = Decimal::from_str_canonical("10").unwrap();
        let b = Decimal::from_str_canonical("2").unwrap();
        let result = a / b;
        assert_eq!(result.to_canonical_string(), "5");
    }

    #[test]
    fn test_decimal_inner() {
        let decimal = Decimal::from_str_canonical("123.456").unwrap();
        let inner = decimal.inner();
        assert_eq!(inner, RustDecimal::from_str("123.456").unwrap());
    }

    #[test]
    fn test_decimal_new() {
        let rust_decimal = RustDecimal::from_str("99.99").unwrap();
        let decimal = Decimal::new(rust_decimal);
        assert_eq!(decimal.to_canonical_string(), "99.99");
    }

    #[test]
    fn test_decimal_ordering() {
        let a = Decimal::from_str_canonical("10").unwrap();
        let b = Decimal::from_str_canonical("20").unwrap();
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, a);
    }
}
