//! Builder attribution taint logic for lifecycles.

use super::{Effect, Snapshot};
use crate::domain::Attribution;
use std::collections::{BTreeSet, HashMap};

/// Taint information for a lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaintInfo {
    pub is_tainted: bool,
    pub reason: Option<String>,
}

/// Computes taint for lifecycles based on fill attributions.
pub struct TaintComputer {
    /// Map from lifecycle_id to set of fill_keys in that lifecycle.
    /// Uses BTreeSet for deterministic iteration order.
    lifecycle_fills: HashMap<i64, BTreeSet<String>>,

    /// Map from fill_key to attribution.
    fill_attributions: HashMap<String, Attribution>,
}

impl TaintComputer {
    pub fn new() -> Self {
        Self {
            lifecycle_fills: HashMap::new(),
            fill_attributions: HashMap::new(),
        }
    }

    /// Register a fill as belonging to a lifecycle.
    pub fn add_fill_to_lifecycle(&mut self, lifecycle_id: i64, fill_key: String) {
        self.lifecycle_fills
            .entry(lifecycle_id)
            .or_default()
            .insert(fill_key);
    }

    /// Set attribution for a fill.
    pub fn set_attribution(&mut self, fill_key: String, attribution: Attribution) {
        self.fill_attributions.insert(fill_key, attribution);
    }

    /// Compute taint for a specific lifecycle.
    pub fn compute_taint(&self, lifecycle_id: i64) -> TaintInfo {
        let empty = BTreeSet::new();
        let fill_keys = self.lifecycle_fills.get(&lifecycle_id).unwrap_or(&empty);

        for fill_key in fill_keys {
            match self.fill_attributions.get(fill_key) {
                Some(attr) if !attr.attributed => {
                    return TaintInfo {
                        is_tainted: true,
                        reason: Some(format!(
                            "Fill {} not attributed to builder (mode={:?})",
                            fill_key, attr.mode
                        )),
                    };
                }
                None => {
                    return TaintInfo {
                        is_tainted: true,
                        reason: Some(format!("Fill {} has no attribution data", fill_key)),
                    };
                }
                Some(_) => {}
            }
        }

        TaintInfo {
            is_tainted: false,
            reason: None,
        }
    }

    /// Compute taint for all lifecycles.
    pub fn compute_all_taints(&self) -> HashMap<i64, TaintInfo> {
        self.lifecycle_fills
            .keys()
            .map(|&id| (id, self.compute_taint(id)))
            .collect()
    }
}

impl Default for TaintComputer {
    fn default() -> Self {
        Self::new()
    }
}

/// Filters data for builder-only queries.
pub struct BuilderOnlyFilter<'a> {
    taint_infos: &'a HashMap<i64, TaintInfo>,
}

impl<'a> BuilderOnlyFilter<'a> {
    pub fn new(taint_infos: &'a HashMap<i64, TaintInfo>) -> Self {
        Self { taint_infos }
    }

    /// Check if a lifecycle should be included in builder-only output.
    pub fn include_lifecycle(&self, lifecycle_id: i64) -> bool {
        self.taint_infos
            .get(&lifecycle_id)
            .map(|t| !t.is_tainted)
            .unwrap_or(false) // Exclude if no taint info.
    }

    /// Filter snapshots for builder-only output.
    pub fn filter_snapshots(&self, snapshots: &[Snapshot]) -> Vec<Snapshot> {
        snapshots
            .iter()
            .filter(|s| self.include_lifecycle(s.lifecycle_id))
            .cloned()
            .collect()
    }

    /// Filter effects for builder-only output.
    pub fn filter_effects(&self, effects: &[Effect]) -> Vec<Effect> {
        effects
            .iter()
            .filter(|e| self.include_lifecycle(e.lifecycle_id))
            .cloned()
            .collect()
    }

    /// Check if any data was excluded (for tainted flag in response).
    pub fn had_exclusions(&self, lifecycle_ids: &[i64]) -> bool {
        lifecycle_ids
            .iter()
            .any(|id| !self.include_lifecycle(*id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Attribution, Decimal};
    use std::str::FromStr;

    fn attributed() -> Attribution {
        let fee = Decimal::from_str("1").unwrap();
        Attribution::from_heuristic(Some(&fee))
    }

    fn not_attributed() -> Attribution {
        Attribution::from_heuristic(None)
    }

    #[test]
    fn test_lifecycle_with_all_builder_fills_not_tainted() {
        let mut computer = TaintComputer::new();

        computer.add_fill_to_lifecycle(1, "fill_a".into());
        computer.add_fill_to_lifecycle(1, "fill_b".into());

        computer.set_attribution("fill_a".into(), attributed());
        computer.set_attribution("fill_b".into(), attributed());

        let taint = computer.compute_taint(1);
        assert!(!taint.is_tainted);
        assert!(taint.reason.is_none());
    }

    #[test]
    fn test_lifecycle_with_one_non_builder_fill_is_tainted() {
        let mut computer = TaintComputer::new();

        computer.add_fill_to_lifecycle(1, "fill_a".into());
        computer.add_fill_to_lifecycle(1, "fill_b".into());

        computer.set_attribution("fill_a".into(), attributed());
        computer.set_attribution("fill_b".into(), not_attributed());

        let taint = computer.compute_taint(1);
        assert!(taint.is_tainted);
        assert!(taint.reason.as_ref().is_some());
        assert!(taint.reason.unwrap().contains("fill_b"));
    }

    #[test]
    fn test_lifecycle_with_missing_attribution_is_tainted() {
        let mut computer = TaintComputer::new();
        computer.add_fill_to_lifecycle(1, "fill_a".into());

        let taint = computer.compute_taint(1);
        assert!(taint.is_tainted);
        assert!(taint
            .reason
            .unwrap()
            .to_lowercase()
            .contains("no attribution"));
    }

    #[test]
    fn test_taint_resets_on_new_lifecycle() {
        let mut computer = TaintComputer::new();

        computer.add_fill_to_lifecycle(1, "fill_a".into());
        computer.set_attribution("fill_a".into(), not_attributed());

        computer.add_fill_to_lifecycle(2, "fill_b".into());
        computer.set_attribution("fill_b".into(), attributed());

        let taints = computer.compute_all_taints();
        assert!(taints[&1].is_tainted);
        assert!(!taints[&2].is_tainted);
    }

    #[test]
    fn test_filter_excludes_tainted_lifecycles() {
        let taints = HashMap::from([
            (
                1,
                TaintInfo {
                    is_tainted: false,
                    reason: None,
                },
            ),
            (
                2,
                TaintInfo {
                    is_tainted: true,
                    reason: Some("non-builder fill".into()),
                },
            ),
        ]);

        let filter = BuilderOnlyFilter::new(&taints);

        let snapshots = vec![
            Snapshot {
                lifecycle_id: 1,
                ..Default::default()
            },
            Snapshot {
                lifecycle_id: 2,
                ..Default::default()
            },
        ];

        let filtered = filter.filter_snapshots(&snapshots);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].lifecycle_id, 1);
    }

    #[test]
    fn test_had_exclusions_returns_true_when_data_excluded() {
        let taints = HashMap::from([
            (
                1,
                TaintInfo {
                    is_tainted: false,
                    reason: None,
                },
            ),
            (
                2,
                TaintInfo {
                    is_tainted: true,
                    reason: Some("x".into()),
                },
            ),
        ]);

        let filter = BuilderOnlyFilter::new(&taints);

        assert!(filter.had_exclusions(&[1, 2]));
        assert!(!filter.had_exclusions(&[1]));
    }
}

