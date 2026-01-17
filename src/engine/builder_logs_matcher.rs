//! Matching raw fills against Hyperliquid builder logs for attribution.

use crate::domain::{AttributionConfidence, BuilderLogFill, Decimal, Fill, Side};
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchTolerances {
    pub time_ms: i64,
    pub px_abs: Decimal,
    pub sz_abs: Decimal,
}

impl Default for MatchTolerances {
    fn default() -> Self {
        Self {
            time_ms: 1_000,
            px_abs: Decimal::from_str("0.000001").expect("valid decimal"),
            sz_abs: Decimal::from_str("0.000001").expect("valid decimal"),
        }
    }
}

#[derive(Debug)]
pub struct BuilderLogsIndex<'a> {
    by_tid: HashMap<i64, &'a BuilderLogFill>,
    fuzzy: HashMap<(String, String, Side), Vec<&'a BuilderLogFill>>,
}

impl<'a> BuilderLogsIndex<'a> {
    pub fn new(logs: &'a [BuilderLogFill]) -> Self {
        let mut by_tid = HashMap::new();
        let mut fuzzy: HashMap<(String, String, Side), Vec<&BuilderLogFill>> = HashMap::new();

        for row in logs {
            if let Some(tid) = row.tid {
                by_tid.insert(tid, row);
            }

            let key = (
                row.user.as_str().to_ascii_lowercase(),
                row.coin.as_str().to_ascii_uppercase(),
                row.side,
            );
            fuzzy.entry(key).or_default().push(row);
        }

        Self { by_tid, fuzzy }
    }

    pub fn match_fill(
        &self,
        fill: &Fill,
        tolerances: &MatchTolerances,
    ) -> Option<AttributionConfidence> {
        if let Some(tid) = fill.tid {
            if self.by_tid.contains_key(&tid) {
                return Some(AttributionConfidence::Exact);
            }
        }

        self.fuzzy_match(fill, tolerances)
            .then_some(AttributionConfidence::Fuzzy)
    }

    fn fuzzy_match(&self, fill: &Fill, tolerances: &MatchTolerances) -> bool {
        let key = (
            fill.user.as_str().to_ascii_lowercase(),
            fill.coin.as_str().to_ascii_uppercase(),
            fill.side,
        );

        let Some(candidates) = self.fuzzy.get(&key) else {
            return false;
        };

        let mut best: Option<(i64, Decimal, Decimal, i64)> = None;

        for row in candidates {
            let dt = (fill.time_ms.as_ms() - row.time_ms.as_ms()).abs();
            if dt > tolerances.time_ms {
                continue;
            }

            let dpx = (fill.px - row.px).abs();
            if dpx > tolerances.px_abs {
                continue;
            }

            let dsz = (fill.sz - row.sz).abs();
            if dsz > tolerances.sz_abs {
                continue;
            }

            let tid = row.tid.unwrap_or(-1);
            let score = (dt, dpx, dsz, tid);
            if best.as_ref().map(|b| score < *b).unwrap_or(true) {
                best = Some(score);
            }
        }

        best.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Address, Coin, TimeMs};

    fn fill(
        time_ms: i64,
        tid: Option<i64>,
        px: &str,
        sz: &str,
        side: Side,
    ) -> Fill {
        Fill::new(
            TimeMs::new(time_ms),
            Address::new("0xabc".to_string()),
            Coin::new("BTC".to_string()),
            side,
            Decimal::from_str(px).unwrap(),
            Decimal::from_str(sz).unwrap(),
            Decimal::from_str("0").unwrap(),
            Decimal::from_str("0").unwrap(),
            None,
            tid,
            None,
        )
    }

    fn log(time_ms: i64, tid: Option<i64>, px: &str, sz: &str, side: Side) -> BuilderLogFill {
        BuilderLogFill {
            time_ms: TimeMs::new(time_ms),
            user: Address::new("0xAbC".to_string()),
            coin: Coin::new("btc".to_string()),
            side,
            px: Decimal::from_str(px).unwrap(),
            sz: Decimal::from_str(sz).unwrap(),
            tid,
            oid: None,
        }
    }

    #[test]
    fn exact_match_on_tid() {
        let logs = vec![log(1000, Some(42), "100", "1", Side::Buy)];
        let index = BuilderLogsIndex::new(&logs);
        let fill = fill(999, Some(42), "999", "9", Side::Sell);

        assert_eq!(
            index.match_fill(&fill, &MatchTolerances::default()),
            Some(AttributionConfidence::Exact)
        );
    }

    #[test]
    fn fuzzy_match_with_tolerances() {
        let logs = vec![log(1000, Some(1), "100.0000005", "1.0000005", Side::Buy)];
        let index = BuilderLogsIndex::new(&logs);
        let fill = fill(1500, Some(999), "100.0000004", "1.0000004", Side::Buy);

        let tolerances = MatchTolerances {
            time_ms: 1_000,
            px_abs: Decimal::from_str("0.000001").unwrap(),
            sz_abs: Decimal::from_str("0.000001").unwrap(),
        };
        assert_eq!(
            index.match_fill(&fill, &tolerances),
            Some(AttributionConfidence::Fuzzy)
        );
    }

    #[test]
    fn no_match_returns_none() {
        let logs = vec![log(1000, Some(1), "100", "1", Side::Buy)];
        let index = BuilderLogsIndex::new(&logs);
        let fill = fill(9999, Some(2), "100", "1", Side::Buy);
        assert_eq!(index.match_fill(&fill, &MatchTolerances::default()), None);
    }
}

