//! Deposit ledger event.

use crate::domain::{Address, Decimal, TimeMs};
use serde::{Deserialize, Serialize};

/// A deposit/withdrawal ledger event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deposit {
    /// Stable unique identifier for this event.
    ///
    /// Priority: `tx_hash` (if present) > hash of deterministic fields.
    pub event_key: String,
    /// User/wallet address.
    pub user: Address,
    /// Time of the event in milliseconds since Unix epoch.
    pub time_ms: TimeMs,
    /// Signed amount (positive deposit, negative withdrawal).
    pub amount: Decimal,
    /// Transaction hash when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
}

impl Deposit {
    /// Create a new Deposit and compute its `event_key`.
    pub fn new(user: Address, time_ms: TimeMs, amount: Decimal, tx_hash: Option<String>) -> Self {
        let tx_hash = normalize_tx_hash(tx_hash);
        let event_key = Self::compute_event_key(&user, time_ms, &amount, tx_hash.as_deref());
        Self {
            event_key,
            user,
            time_ms,
            amount,
            tx_hash,
        }
    }

    /// Compute a stable unique key for this event.
    ///
    /// Priority: `tx_hash` (if present) > hash of deterministic fields (user, time_ms, amount).
    ///
    /// # Hash Collision Resistance
    ///
    /// When `tx_hash` is unavailable, we generate a key by truncating a SHA-256 hash
    /// to 128 bits (16 bytes). This provides approximately 2^64 collision resistance
    /// via the birthday bound, which is sufficient for our expected dataset sizes
    /// (far fewer than 2^32 deposits per user).
    pub fn compute_event_key(
        user: &Address,
        time_ms: TimeMs,
        amount: &Decimal,
        tx_hash: Option<&str>,
    ) -> String {
        if let Some(tx) = tx_hash.filter(|s| !s.trim().is_empty()) {
            return tx.trim().to_lowercase();
        }

        use sha2::{Digest, Sha256};

        fn hash_var(hasher: &mut Sha256, data: &str) {
            hasher.update((data.len() as u32).to_le_bytes());
            hasher.update(data.as_bytes());
        }

        let mut hasher = Sha256::new();
        hash_var(&mut hasher, user.as_str());
        hasher.update(time_ms.as_ms().to_le_bytes());
        hash_var(&mut hasher, &amount.to_canonical_string());

        let hash = hasher.finalize();
        format!("hash:{}", hex::encode(&hash[..16]))
    }
}

fn normalize_tx_hash(tx_hash: Option<String>) -> Option<String> {
    tx_hash
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn event_key_prefers_tx_hash() {
        let deposit = Deposit::new(
            Address::new("0xabc".to_string()),
            TimeMs::new(1000),
            Decimal::from_str("1").unwrap(),
            Some("0xDEADBEEF".to_string()),
        );
        assert_eq!(deposit.event_key, "0xdeadbeef");
        assert_eq!(deposit.tx_hash.as_deref(), Some("0xdeadbeef"));
    }

    #[test]
    fn event_key_falls_back_to_hash() {
        let d1 = Deposit::new(
            Address::new("0xabc".to_string()),
            TimeMs::new(1000),
            Decimal::from_str("1.2300").unwrap(),
            None,
        );
        let d2 = Deposit::new(
            Address::new("0xabc".to_string()),
            TimeMs::new(1000),
            Decimal::from_str("1.23").unwrap(),
            None,
        );
        assert_eq!(d1.event_key, d2.event_key);
        assert!(d1.event_key.starts_with("hash:"));
    }
}
