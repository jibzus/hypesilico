//! Fill type representing a single trade execution.

use crate::domain::{Address, Attribution, Coin, Decimal, Side, TimeMs};
use serde::{Deserialize, Serialize};

/// A single trade fill/execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    /// Time of the fill in milliseconds since Unix epoch.
    pub time_ms: TimeMs,
    /// User/wallet address.
    pub user: Address,
    /// Coin/asset being traded.
    pub coin: Coin,
    /// Trade side (Buy or Sell).
    pub side: Side,
    /// Price per unit.
    pub px: Decimal,
    /// Size/quantity traded.
    pub sz: Decimal,
    /// Fee paid for this fill.
    pub fee: Decimal,
    /// Closed PnL from this fill (if any).
    pub closed_pnl: Decimal,
    /// Builder fee (if applicable).
    pub builder_fee: Option<Decimal>,
    /// Trade ID (preferred stable key).
    pub tid: Option<i64>,
    /// Order ID.
    pub oid: Option<i64>,
    /// Attribution information (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribution: Option<Attribution>,
}

impl Fill {
    /// Create a new Fill.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        time_ms: TimeMs,
        user: Address,
        coin: Coin,
        side: Side,
        px: Decimal,
        sz: Decimal,
        fee: Decimal,
        closed_pnl: Decimal,
        builder_fee: Option<Decimal>,
        tid: Option<i64>,
        oid: Option<i64>,
    ) -> Self {
        Fill {
            time_ms,
            user,
            coin,
            side,
            px,
            sz,
            fee,
            closed_pnl,
            builder_fee,
            tid,
            oid,
            attribution: None,
        }
    }

    /// Set the attribution for this fill.
    pub fn with_attribution(mut self, attribution: Attribution) -> Self {
        self.attribution = Some(attribution);
        self
    }

    /// Get a stable fill key for ordering/deduplication.
    /// Prefers tid, falls back to oid, then uses a hash of other fields.
    pub fn fill_key(&self) -> String {
        if let Some(tid) = self.tid {
            format!("tid:{}", tid)
        } else if let Some(oid) = self.oid {
            format!("oid:{}", oid)
        } else {
            // Fallback: hash of deterministic fields
            format!(
                "hash:{}:{}:{}:{}:{}",
                self.time_ms.as_i64(),
                self.user,
                self.coin,
                self.side,
                self.px
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fill_creation() {
        let fill = Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str_canonical("50000").unwrap(),
            Decimal::from_str_canonical("1.5").unwrap(),
            Decimal::from_str_canonical("10").unwrap(),
            Decimal::from_str_canonical("0").unwrap(),
            None,
            Some(123),
            Some(456),
        );

        assert_eq!(fill.time_ms.as_i64(), 1000);
        assert_eq!(fill.user.as_str(), "0x123");
        assert_eq!(fill.coin.as_str(), "BTC");
        assert_eq!(fill.side, Side::Buy);
    }

    #[test]
    fn test_fill_key_prefers_tid() {
        let fill = Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str_canonical("50000").unwrap(),
            Decimal::from_str_canonical("1.5").unwrap(),
            Decimal::from_str_canonical("10").unwrap(),
            Decimal::from_str_canonical("0").unwrap(),
            None,
            Some(123),
            Some(456),
        );

        assert_eq!(fill.fill_key(), "tid:123");
    }

    #[test]
    fn test_fill_key_fallback_to_oid() {
        let fill = Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str_canonical("50000").unwrap(),
            Decimal::from_str_canonical("1.5").unwrap(),
            Decimal::from_str_canonical("10").unwrap(),
            Decimal::from_str_canonical("0").unwrap(),
            None,
            None,
            Some(456),
        );

        assert_eq!(fill.fill_key(), "oid:456");
    }

    #[test]
    fn test_fill_serialization() {
        let fill = Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str_canonical("50000").unwrap(),
            Decimal::from_str_canonical("1.5").unwrap(),
            Decimal::from_str_canonical("10").unwrap(),
            Decimal::from_str_canonical("0").unwrap(),
            None,
            Some(123),
            Some(456),
        );

        let json = serde_json::to_string(&fill).unwrap();
        let deserialized: Fill = serde_json::from_str(&json).unwrap();
        assert_eq!(fill, deserialized);
    }

    #[test]
    fn test_fill_with_attribution() {
        let fill = Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str_canonical("50000").unwrap(),
            Decimal::from_str_canonical("1.5").unwrap(),
            Decimal::from_str_canonical("10").unwrap(),
            Decimal::from_str_canonical("0").unwrap(),
            None,
            Some(123),
            Some(456),
        );

        let attribution = Attribution::heuristic(true);
        let fill_with_attr = fill.with_attribution(attribution.clone());

        assert_eq!(fill_with_attr.attribution, Some(attribution));
    }

    #[test]
    fn test_fill_with_builder_fee() {
        let fill = Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Sell,
            Decimal::from_str_canonical("50000").unwrap(),
            Decimal::from_str_canonical("1.5").unwrap(),
            Decimal::from_str_canonical("10").unwrap(),
            Decimal::from_str_canonical("100").unwrap(),
            Some(Decimal::from_str_canonical("5").unwrap()),
            Some(123),
            Some(456),
        );

        assert_eq!(
            fill.builder_fee,
            Some(Decimal::from_str_canonical("5").unwrap())
        );
    }

    #[test]
    fn test_fill_key_hash_fallback() {
        let fill = Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str_canonical("50000").unwrap(),
            Decimal::from_str_canonical("1.5").unwrap(),
            Decimal::from_str_canonical("10").unwrap(),
            Decimal::from_str_canonical("0").unwrap(),
            None,
            None,
            None,
        );

        let key = fill.fill_key();
        assert!(key.starts_with("hash:"));
    }
}
