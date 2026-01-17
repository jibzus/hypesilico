//! Domain primitives: TimeMs, Address, Coin, Side.

use serde::{Deserialize, Serialize};

/// Time in milliseconds since Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimeMs(pub i64);

impl TimeMs {
    /// Create a TimeMs from milliseconds.
    pub fn new(ms: i64) -> Self {
        TimeMs(ms)
    }

    /// Get the underlying milliseconds value.
    pub fn as_i64(&self) -> i64 {
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
