//! Attribution model for builder-only mode.

use crate::domain::Address;
use serde::{Deserialize, Serialize};

/// Attribution mode for determining builder-attributed fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AttributionMode {
    /// Heuristic mode: builderFee > 0 indicates builder attribution.
    Heuristic,
    /// Log-backed mode: match against builder_fills logs.
    Logs,
}

/// Confidence level for attribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    /// Exact match (e.g., from logs).
    Exact,
    /// Fuzzy match (e.g., heuristic with high confidence).
    Fuzzy,
    /// Low confidence match.
    Low,
}

/// Attribution information for a fill.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attribution {
    /// Whether this fill is attributed to the builder.
    pub attributed: bool,
    /// Mode used for attribution.
    pub mode: AttributionMode,
    /// Confidence level of the attribution.
    pub confidence: Confidence,
    /// Builder address (only set when mode=logs and matched).
    pub builder: Option<Address>,
}

impl Attribution {
    /// Create a new Attribution.
    pub fn new(
        attributed: bool,
        mode: AttributionMode,
        confidence: Confidence,
        builder: Option<Address>,
    ) -> Self {
        Attribution {
            attributed,
            mode,
            confidence,
            builder,
        }
    }

    /// Create a heuristic attribution (builderFee > 0).
    pub fn heuristic(attributed: bool) -> Self {
        Attribution {
            attributed,
            mode: AttributionMode::Heuristic,
            confidence: if attributed {
                Confidence::Fuzzy
            } else {
                Confidence::Low
            },
            builder: None,
        }
    }

    /// Create a log-backed attribution.
    pub fn logs(attributed: bool, builder: Option<Address>) -> Self {
        Attribution {
            attributed,
            mode: AttributionMode::Logs,
            confidence: Confidence::Exact,
            builder,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attribution_heuristic() {
        let attr = Attribution::heuristic(true);
        assert!(attr.attributed);
        assert_eq!(attr.mode, AttributionMode::Heuristic);
        assert_eq!(attr.confidence, Confidence::Fuzzy);
        assert_eq!(attr.builder, None);
    }

    #[test]
    fn test_attribution_logs() {
        let builder = Address::new("0x123".to_string());
        let attr = Attribution::logs(true, Some(builder.clone()));
        assert!(attr.attributed);
        assert_eq!(attr.mode, AttributionMode::Logs);
        assert_eq!(attr.confidence, Confidence::Exact);
        assert_eq!(attr.builder, Some(builder));
    }

    #[test]
    fn test_attribution_serialization() {
        let attr = Attribution::heuristic(true);
        let json = serde_json::to_string(&attr).unwrap();
        let deserialized: Attribution = serde_json::from_str(&json).unwrap();
        assert_eq!(attr, deserialized);
    }
}
