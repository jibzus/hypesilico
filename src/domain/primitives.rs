//! Domain primitives: TimeMs, Address, Coin, Side.

use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Time in milliseconds since Unix epoch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct TimeMs(pub i64);

impl TimeMs {
    /// Create a TimeMs from milliseconds.
    pub fn new(ms: i64) -> Self {
        TimeMs(ms)
    }

    /// Current time in milliseconds since Unix epoch.
    pub fn now() -> Self {
        TimeMs(chrono::Utc::now().timestamp_millis())
    }

    /// Get the underlying milliseconds value.
    pub fn as_i64(&self) -> i64 {
        self.0
    }

    /// Alias for `as_i64`, to match `*_ms` conventions.
    pub fn as_ms(&self) -> i64 {
        self.0
    }
}

/// Wallet address (hex string).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Address(pub String);

impl Address {
    /// Create an Address from a string.
    pub fn new(addr: String) -> Self {
        Address(addr)
    }

    /// Get the address as a string reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Address {
    type Err = AddressParseError;

    /// Parse an address from a string.
    ///
    /// Requires exactly 42 characters: "0x" prefix + 40 hex digits.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 42 {
            return Err(AddressParseError::InvalidLength(s.len()));
        }
        if !s.starts_with("0x") {
            return Err(AddressParseError::MissingPrefix);
        }
        let hex_part = &s[2..];
        if !hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(AddressParseError::InvalidHex);
        }
        Ok(Address(s.to_string()))
    }
}

/// Errors that can occur when parsing an address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressParseError {
    /// Address must be exactly 42 characters (0x + 40 hex digits).
    InvalidLength(usize),
    /// Address must start with "0x".
    MissingPrefix,
    /// Address must contain only hex digits after the "0x" prefix.
    InvalidHex,
}

impl std::fmt::Display for AddressParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AddressParseError::InvalidLength(len) => {
                write!(f, "address must be 42 characters, got {}", len)
            }
            AddressParseError::MissingPrefix => {
                write!(f, "address must start with '0x'")
            }
            AddressParseError::InvalidHex => {
                write!(f, "address must contain only hex digits")
            }
        }
    }
}

impl std::error::Error for AddressParseError {}

/// Coin/asset symbol (e.g., "BTC", "ETH").
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Coin(pub String);

impl Coin {
    /// Create a Coin from a string.
    pub fn new(coin: String) -> Self {
        Coin(coin)
    }

    /// Get the coin as a string reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Coin {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err("coin cannot be empty");
        }
        Ok(Coin::new(s.to_string()))
    }
}

impl std::fmt::Display for Coin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Trade side: Buy or Sell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    /// Buy side (long).
    Buy,
    /// Sell side (short).
    Sell,
}

impl Side {
    /// Get the signed multiplier for this side (+1 for Buy, -1 for Sell).
    pub fn sign(&self) -> i32 {
        match self {
            Side::Buy => 1,
            Side::Sell => -1,
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "buy"),
            Side::Sell => write!(f, "sell"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_side_sign() {
        assert_eq!(Side::Buy.sign(), 1);
        assert_eq!(Side::Sell.sign(), -1);
    }

    #[test]
    fn test_side_serialization() {
        let buy = Side::Buy;
        let json = serde_json::to_string(&buy).unwrap();
        assert_eq!(json, "\"buy\"");

        let sell = Side::Sell;
        let json = serde_json::to_string(&sell).unwrap();
        assert_eq!(json, "\"sell\"");
    }

    #[test]
    fn test_address_display() {
        let addr = Address::new("0x123abc".to_string());
        assert_eq!(addr.to_string(), "0x123abc");
    }

    #[test]
    fn test_coin_display() {
        let coin = Coin::new("BTC".to_string());
        assert_eq!(coin.to_string(), "BTC");
    }

    #[test]
    fn test_timems_ordering() {
        let t1 = TimeMs::new(1000);
        let t2 = TimeMs::new(2000);
        assert!(t1 < t2);
    }
}
