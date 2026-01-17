//! Stable fill ordering for deterministic processing.

use crate::domain::Fill;

/// Stable ordering key for fills.
///
/// Ensures deterministic ordering of fills with the same timestamp.
/// Ordering: time_ms -> tid -> oid -> fill_key hash
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FillOrderingKey {
    /// Time in milliseconds (primary sort).
    pub time_ms: i64,
    /// Trade ID (secondary sort, if present).
    pub tid: Option<i64>,
    /// Order ID (tertiary sort, if present).
    pub oid: Option<i64>,
    /// Fill key hash (fallback sort).
    pub fill_key: String,
}

impl FillOrderingKey {
    /// Create an ordering key from a Fill.
    pub fn from_fill(fill: &Fill) -> Self {
        FillOrderingKey {
            time_ms: fill.time_ms.as_i64(),
            tid: fill.tid,
            oid: fill.oid,
            fill_key: fill.fill_key().to_string(),
        }
    }

    /// Compare two fills for deterministic ordering.
    ///
    /// Returns true if fill_a should come before fill_b.
    pub fn should_come_before(fill_a: &Fill, fill_b: &Fill) -> bool {
        let key_a = Self::from_fill(fill_a);
        let key_b = Self::from_fill(fill_b);
        key_a < key_b
    }
}

/// Sort fills deterministically.
pub fn sort_fills_deterministic(fills: &mut [Fill]) {
    fills.sort_by(|a, b| {
        let key_a = FillOrderingKey::from_fill(a);
        let key_b = FillOrderingKey::from_fill(b);
        key_a.cmp(&key_b)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Address, Coin, Decimal, Side, TimeMs};

    fn make_fill(time_ms: i64, tid: Option<i64>, oid: Option<i64>) -> Fill {
        Fill::new(
            TimeMs::new(time_ms),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str_canonical("50000").unwrap(),
            Decimal::from_str_canonical("1.5").unwrap(),
            Decimal::from_str_canonical("10").unwrap(),
            Decimal::from_str_canonical("0").unwrap(),
            None,
            tid,
            oid,
        )
    }

    #[test]
    fn test_fill_ordering_by_time() {
        let fill_a = make_fill(1000, Some(1), None);
        let fill_b = make_fill(2000, Some(2), None);

        assert!(FillOrderingKey::should_come_before(&fill_a, &fill_b));
        assert!(!FillOrderingKey::should_come_before(&fill_b, &fill_a));
    }

    #[test]
    fn test_fill_ordering_same_time_by_tid() {
        let fill_a = make_fill(1000, Some(1), None);
        let fill_b = make_fill(1000, Some(2), None);

        assert!(FillOrderingKey::should_come_before(&fill_a, &fill_b));
        assert!(!FillOrderingKey::should_come_before(&fill_b, &fill_a));
    }

    #[test]
    fn test_fill_ordering_same_time_no_tid_by_oid() {
        let fill_a = make_fill(1000, None, Some(1));
        let fill_b = make_fill(1000, None, Some(2));

        assert!(FillOrderingKey::should_come_before(&fill_a, &fill_b));
        assert!(!FillOrderingKey::should_come_before(&fill_b, &fill_a));
    }

    #[test]
    fn test_sort_fills_deterministic() {
        let mut fills = vec![
            make_fill(2000, Some(2), None),
            make_fill(1000, Some(1), None),
            make_fill(1000, Some(3), None),
        ];

        sort_fills_deterministic(&mut fills);

        assert_eq!(fills[0].time_ms.as_i64(), 1000);
        assert_eq!(fills[0].tid, Some(1));
        assert_eq!(fills[1].time_ms.as_i64(), 1000);
        assert_eq!(fills[1].tid, Some(3));
        assert_eq!(fills[2].time_ms.as_i64(), 2000);
        assert_eq!(fills[2].tid, Some(2));
    }

    #[test]
    fn test_fill_ordering_key_determinism() {
        let fill = make_fill(1000, Some(123), Some(456));
        let key1 = FillOrderingKey::from_fill(&fill);
        let key2 = FillOrderingKey::from_fill(&fill);
        assert_eq!(key1, key2);
    }
}
