//! Fill type representing a single trade execution.

use crate::domain::{Address, Attribution, Coin, Decimal, Side, TimeMs};
use serde::{Deserialize, Serialize};

/// A single trade fill/execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    /// Stable unique identifier for this fill.
    pub fill_key: String,
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
        let fill_key = Self::compute_fill_key(
            &user,
            &coin,
            time_ms,
            side,
            &px,
            &sz,
            &fee,
            &closed_pnl,
            tid,
            oid,
        );
        Fill {
            fill_key,
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

    /// Generate a stable unique key for this fill.
    ///
    /// Priority: `tid` (if present) > hash of deterministic fields.
    #[allow(clippy::too_many_arguments)]
    pub fn compute_fill_key(
        user: &Address,
        coin: &Coin,
        time_ms: TimeMs,
        side: Side,
        px: &Decimal,
        sz: &Decimal,
        fee: &Decimal,
        closed_pnl: &Decimal,
        tid: Option<i64>,
        oid: Option<i64>,
    ) -> String {
        if let Some(tid) = tid {
            return format!("tid:{}", tid);
        }

        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(user.as_str());
        hasher.update(coin.as_str());
        hasher.update(time_ms.as_ms().to_le_bytes());
        hasher.update(if side == Side::Buy { b"B" } else { b"S" });
        hasher.update(px.to_canonical_string());
        hasher.update(sz.to_canonical_string());
        hasher.update(fee.to_canonical_string());
        hasher.update(closed_pnl.to_canonical_string());
        if let Some(oid) = oid {
            hasher.update(oid.to_le_bytes());
        }
        let hash = hasher.finalize();
        format!("hash:{}", hex::encode(&hash[..16]))
    }

    /// Borrow the precomputed fill key.
    pub fn fill_key(&self) -> &str {
        &self.fill_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

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
    fn test_fill_key_with_tid() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let px = Decimal::from_str("50000").unwrap();
        let sz = Decimal::from_str("1.5").unwrap();
        let fee = Decimal::from_str("10").unwrap();
        let pnl = Decimal::from_str("0").unwrap();

        let key = Fill::compute_fill_key(
            &user,
            &coin,
            TimeMs::new(1000),
            Side::Buy,
            &px,
            &sz,
            &fee,
            &pnl,
            Some(12345),
            Some(999),
        );
        assert_eq!(key, "tid:12345");
    }

    #[test]
    fn test_fill_key_without_tid_uses_hash() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let px = Decimal::from_str("50000").unwrap();
        let sz = Decimal::from_str("1.5").unwrap();
        let fee = Decimal::from_str("10").unwrap();
        let pnl = Decimal::from_str("0").unwrap();

        let key = Fill::compute_fill_key(
            &user,
            &coin,
            TimeMs::new(1000),
            Side::Buy,
            &px,
            &sz,
            &fee,
            &pnl,
            None,
            Some(999),
        );
        assert!(key.starts_with("hash:"));
        assert_eq!(key.len(), 5 + 32);
    }

    #[test]
    fn test_fill_key_deterministic() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let px = Decimal::from_str("50000").unwrap();
        let sz = Decimal::from_str("1.5").unwrap();
        let fee = Decimal::from_str("10").unwrap();
        let pnl = Decimal::from_str("0").unwrap();

        let key1 = Fill::compute_fill_key(
            &user,
            &coin,
            TimeMs::new(1000),
            Side::Buy,
            &px,
            &sz,
            &fee,
            &pnl,
            None,
            Some(999),
        );
        let key2 = Fill::compute_fill_key(
            &user,
            &coin,
            TimeMs::new(1000),
            Side::Buy,
            &px,
            &sz,
            &fee,
            &pnl,
            None,
            Some(999),
        );
        assert_eq!(key1, key2, "Same inputs must produce same key");
    }

    #[test]
    fn test_fill_key_different_for_different_fills() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let sz = Decimal::from_str("1.5").unwrap();
        let fee = Decimal::from_str("10").unwrap();
        let pnl = Decimal::from_str("0").unwrap();

        let key1 = Fill::compute_fill_key(
            &user,
            &coin,
            TimeMs::new(1000),
            Side::Buy,
            &Decimal::from_str("100").unwrap(),
            &sz,
            &fee,
            &pnl,
            None,
            Some(999),
        );
        let key2 = Fill::compute_fill_key(
            &user,
            &coin,
            TimeMs::new(1000),
            Side::Buy,
            &Decimal::from_str("101").unwrap(),
            &sz,
            &fee,
            &pnl,
            None,
            Some(999),
        );
        assert_ne!(key1, key2);
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
