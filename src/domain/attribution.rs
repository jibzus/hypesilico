//! Attribution model for builder-only mode.

use crate::domain::{Address, Decimal};
use serde::{Deserialize, Serialize};

/// How a fill was attributed to a builder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attribution {
    /// Was this fill attributed to our builder?
    pub attributed: bool,

    /// How was attribution determined?
    pub mode: AttributionMode,

    /// How confident are we in this attribution?
    pub confidence: AttributionConfidence,

    /// Which builder was matched (only set if mode=Logs and matched).
    pub builder: Option<Address>,
}

/// How attribution was determined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AttributionMode {
    /// builder_fee > 0 implies builder-attributed.
    Heuristic,

    /// Matched against builder_fills logs.
    Logs,
}

/// Confidence level of attribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AttributionConfidence {
    /// Exact match (e.g., tid matched in logs).
    Exact,

    /// Fuzzy match (e.g., time/size matched within tolerance).
    Fuzzy,

    /// Low confidence (heuristic only).
    Low,
}

// Backward-compatible alias from PR-002 naming.
pub type Confidence = AttributionConfidence;

impl Attribution {
    /// Create attribution using heuristic (builder_fee > 0).
    pub fn from_heuristic(builder_fee: Option<&Decimal>) -> Self {
        let attributed = builder_fee
            .map(|fee| {
                let inner = fee.inner();
                !inner.is_zero() && inner.is_sign_positive()
            })
            .unwrap_or(false);

        Self {
            attributed,
            mode: AttributionMode::Heuristic,
            confidence: AttributionConfidence::Low,
            builder: None,
        }
    }

    /// Create attribution from log match.
    pub fn from_logs_match(
        matched: bool,
        builder: Option<Address>,
        confidence: AttributionConfidence,
    ) -> Self {
        Self {
            attributed: matched,
            mode: AttributionMode::Logs,
            confidence,
            builder,
        }
    }

    /// Convenience wrapper for legacy callsites.
    ///
    /// Uses Low confidence because builder_fee > 0 doesn't verify
    /// the builder address matches TARGET_BUILDER.
    pub fn heuristic(attributed: bool) -> Self {
        Self {
            attributed,
            mode: AttributionMode::Heuristic,
            confidence: AttributionConfidence::Low,
            builder: None,
        }
    }

    /// Convenience wrapper for legacy callsites.
    pub fn logs(attributed: bool, builder: Option<Address>) -> Self {
        Self::from_logs_match(attributed, builder, AttributionConfidence::Exact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_heuristic_attribution_with_fee() {
        let fee = Decimal::from_str("0.5").unwrap();
        let attr = Attribution::from_heuristic(Some(&fee));

        assert!(attr.attributed);
        assert_eq!(attr.mode, AttributionMode::Heuristic);
        assert_eq!(attr.confidence, AttributionConfidence::Low);
        assert!(attr.builder.is_none());
    }

    #[test]
    fn test_heuristic_attribution_without_fee() {
        let attr = Attribution::from_heuristic(None);
        assert!(!attr.attributed);
    }

    #[test]
    fn test_heuristic_attribution_with_zero_fee() {
        let fee = Decimal::from_str("0").unwrap();
        let attr = Attribution::from_heuristic(Some(&fee));
        assert!(!attr.attributed);
    }

    #[test]
    fn test_logs_attribution_exact_match() {
        let builder = Address::new("0x1234".to_string());
        let attr = Attribution::from_logs_match(
            true,
            Some(builder.clone()),
            AttributionConfidence::Exact,
        );

        assert!(attr.attributed);
        assert_eq!(attr.mode, AttributionMode::Logs);
        assert_eq!(attr.confidence, AttributionConfidence::Exact);
        assert_eq!(attr.builder, Some(builder));
    }

    #[test]
    fn test_attribution_serialization_roundtrip() {
        let fee = Decimal::from_str("1").unwrap();
        let attr = Attribution::from_heuristic(Some(&fee));
        let json = serde_json::to_string(&attr).unwrap();
        let deserialized: Attribution = serde_json::from_str(&json).unwrap();
        assert_eq!(attr, deserialized);
    }
}
