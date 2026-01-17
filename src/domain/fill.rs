//! Fill type representing a single trade execution.

use crate::domain::{Address, Attribution, Coin, Decimal, Side, TimeMs};
use serde::{Deserialize, Serialize};

/// A single trade fill/execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    /// Stable unique identifier for this fill.
    ///
    /// Invariant: Computed once at construction via `compute_fill_key`.
    /// The DB read path (`Repository::query_fills`) preserves the stored
    /// key rather than recomputing, ensuring idempotency.
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
            builder_fee.as_ref(),
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
        builder_fee: Option<&Decimal>,
        tid: Option<i64>,
        oid: Option<i64>,
    ) -> String {
        if let Some(tid) = tid {
            return format!("{}:{}:tid:{}", user.as_str(), coin.as_str(), tid);
        }

        use sha2::{Digest, Sha256};

        // Helper to write length-prefixed variable-length data
        fn hash_var(hasher: &mut Sha256, data: &str) {
            hasher.update((data.len() as u32).to_le_bytes());
            hasher.update(data.as_bytes());
        }

        let mut hasher = Sha256::new();

        // Variable-length fields: prefix with length to avoid boundary collisions
        hash_var(&mut hasher, user.as_str());
        hash_var(&mut hasher, coin.as_str());

        // Fixed-width fields: no prefix needed
        hasher.update(time_ms.as_ms().to_le_bytes());
        hasher.update(if side == Side::Buy { b"B" } else { b"S" });

        // Decimal strings: length-prefixed
        hash_var(&mut hasher, &px.to_canonical_string());
        hash_var(&mut hasher, &sz.to_canonical_string());
        hash_var(&mut hasher, &fee.to_canonical_string());
        hash_var(&mut hasher, &closed_pnl.to_canonical_string());

        // Optional fields: presence marker (0/1) + value if present
        match builder_fee {
            Some(bf) => {
                hasher.update([1u8]);
                hash_var(&mut hasher, &bf.to_canonical_string());
            }
            None => hasher.update([0u8]),
        }
        match oid {
            Some(o) => {
                hasher.update([1u8]);
                hasher.update(o.to_le_bytes());
            }
            None => hasher.update([0u8]),
        }

        let hash = hasher.finalize();
        // Truncate SHA-256 to 128 bits (16 bytes) for shorter keys.
        // 128-bit collision resistance is sufficient for deduplication;
        // this is an identifier, not a security-sensitive hash.
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
            None,
            Some(12345),
            Some(999),
        );
        assert_eq!(key, "0x123:BTC:tid:12345");
    }

    #[test]
    fn test_fill_key_with_tid_scoped_by_user_and_coin() {
        let px = Decimal::from_str("50000").unwrap();
        let sz = Decimal::from_str("1.5").unwrap();
        let fee = Decimal::from_str("10").unwrap();
        let pnl = Decimal::from_str("0").unwrap();

        // Same tid, different users
        let key1 = Fill::compute_fill_key(
            &Address::new("0xAAA".to_string()),
            &Coin::new("BTC".to_string()),
            TimeMs::new(1000),
            Side::Buy,
            &px,
            &sz,
            &fee,
            &pnl,
            None,
            Some(999),
            None,
        );
        let key2 = Fill::compute_fill_key(
            &Address::new("0xBBB".to_string()),
            &Coin::new("BTC".to_string()),
            TimeMs::new(1000),
            Side::Buy,
            &px,
            &sz,
            &fee,
            &pnl,
            None,
            Some(999),
            None,
        );
        assert_ne!(
            key1, key2,
            "Same tid but different users must produce different keys"
        );

        // Same tid, same user, different coins
        let key3 = Fill::compute_fill_key(
            &Address::new("0xAAA".to_string()),
            &Coin::new("ETH".to_string()),
            TimeMs::new(1000),
            Side::Buy,
            &px,
            &sz,
            &fee,
            &pnl,
            None,
            Some(999),
            None,
        );
        assert_ne!(
            key1, key3,
            "Same tid but different coins must produce different keys"
        );
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
            None,
            Some(999),
        );
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_fill_key_different_for_different_builder_fee() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let px = Decimal::from_str("50000").unwrap();
        let sz = Decimal::from_str("1.5").unwrap();
        let fee = Decimal::from_str("10").unwrap();
        let pnl = Decimal::from_str("0").unwrap();
        let bf1 = Decimal::from_str("5").unwrap();
        let bf2 = Decimal::from_str("10").unwrap();

        let key1 = Fill::compute_fill_key(
            &user,
            &coin,
            TimeMs::new(1000),
            Side::Buy,
            &px,
            &sz,
            &fee,
            &pnl,
            Some(&bf1),
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
            Some(&bf2),
            None,
            Some(999),
        );
        let key3 = Fill::compute_fill_key(
            &user,
            &coin,
            TimeMs::new(1000),
            Side::Buy,
            &px,
            &sz,
            &fee,
            &pnl,
            None,
            None,
            Some(999),
        );

        assert_ne!(
            key1, key2,
            "Different builder fees must produce different keys"
        );
        assert_ne!(
            key1, key3,
            "With vs without builder fee must produce different keys"
        );
        assert_ne!(
            key2, key3,
            "With vs without builder fee must produce different keys"
        );
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

    #[test]
    fn test_fill_key_no_boundary_collision() {
        // Test that variable-length field boundaries are respected.
        // Without length prefixes, "AB" + "C" and "A" + "BC" would hash the same.
        let px = Decimal::from_str("100").unwrap();
        let sz = Decimal::from_str("1").unwrap();
        let fee = Decimal::from_str("0").unwrap();
        let pnl = Decimal::from_str("0").unwrap();

        // user="AB", coin="C"
        let key1 = Fill::compute_fill_key(
            &Address::new("AB".to_string()),
            &Coin::new("C".to_string()),
            TimeMs::new(1000),
            Side::Buy,
            &px,
            &sz,
            &fee,
            &pnl,
            None,
            None,
            None,
        );

        // user="A", coin="BC"
        let key2 = Fill::compute_fill_key(
            &Address::new("A".to_string()),
            &Coin::new("BC".to_string()),
            TimeMs::new(1000),
            Side::Buy,
            &px,
            &sz,
            &fee,
            &pnl,
            None,
            None,
            None,
        );

        assert_ne!(
            key1, key2,
            "Different user/coin boundaries must produce different keys"
        );

        // Also test decimal boundary collisions: px="12", sz="3" vs px="1", sz="23"
        let key3 = Fill::compute_fill_key(
            &Address::new("X".to_string()),
            &Coin::new("Y".to_string()),
            TimeMs::new(1000),
            Side::Buy,
            &Decimal::from_str("12").unwrap(),
            &Decimal::from_str("3").unwrap(),
            &fee,
            &pnl,
            None,
            None,
            None,
        );

        let key4 = Fill::compute_fill_key(
            &Address::new("X".to_string()),
            &Coin::new("Y".to_string()),
            TimeMs::new(1000),
            Side::Buy,
            &Decimal::from_str("1").unwrap(),
            &Decimal::from_str("23").unwrap(),
            &fee,
            &pnl,
            None,
            None,
            None,
        );

        assert_ne!(
            key3, key4,
            "Different px/sz boundaries must produce different keys"
        );
    }
}
