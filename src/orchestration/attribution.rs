//! Builder attribution ingestion via Hyperliquid builder logs.

use crate::config::{BuilderAttributionMode, Config};
use crate::datasource::{BuilderLogsError, BuilderLogsSource};
use crate::db::Repository;
use crate::domain::{Address, Attribution, AttributionConfidence, AttributionMode, Coin, Fill, TimeMs};
use crate::engine::{BuilderLogsIndex, MatchTolerances};
use chrono::TimeZone;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AttributionIngestionError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Logs(#[from] BuilderLogsError),
    #[error("invalid target builder address")]
    InvalidTargetBuilder,
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(i64),
}

#[derive(Debug, Clone)]
pub struct AttributionIngestor {
    pub tolerances: MatchTolerances,
}

impl Default for AttributionIngestor {
    fn default() -> Self {
        Self {
            tolerances: MatchTolerances::default(),
        }
    }
}

impl AttributionIngestor {
    pub fn attribute_fill(
        &self,
        mode: BuilderAttributionMode,
        fill: &Fill,
        logs_index: Option<&BuilderLogsIndex<'_>>,
        target_builder: &Address,
    ) -> Attribution {
        match mode {
            BuilderAttributionMode::Heuristic => {
                Attribution::from_heuristic(fill.builder_fee.as_ref())
            }
            BuilderAttributionMode::Logs => {
                let confidence = logs_index.and_then(|idx| idx.match_fill(fill, &self.tolerances));
                match confidence {
                    Some(confidence) => {
                        Attribution::from_logs_match(true, Some(target_builder.clone()), confidence)
                    }
                    None => Attribution::from_logs_match(false, None, AttributionConfidence::Exact),
                }
            }
            BuilderAttributionMode::Auto => {
                let confidence = logs_index.and_then(|idx| idx.match_fill(fill, &self.tolerances));
                match confidence {
                    Some(confidence) => {
                        Attribution::from_logs_match(true, Some(target_builder.clone()), confidence)
                    }
                    None => Attribution::from_heuristic(fill.builder_fee.as_ref()),
                }
            }
        }
    }

    pub async fn ingest_window(
        &self,
        repo: &Repository,
        logs_fetcher: &dyn BuilderLogsSource,
        config: &Config,
        user: &Address,
        coin: Option<&Coin>,
        from_ms: Option<TimeMs>,
        to_ms: Option<TimeMs>,
    ) -> Result<usize, AttributionIngestionError> {
        let from_ms = from_ms.unwrap_or(TimeMs::new(0));
        let to_ms = to_ms.unwrap_or(TimeMs::new(i64::MAX));

        let target_builder = Address::new(config.target_builder.clone());
        if target_builder.as_str().is_empty() {
            return Err(AttributionIngestionError::InvalidTargetBuilder);
        }

        let fills = repo.query_fills(user, coin, Some(from_ms), Some(to_ms)).await?;
        if fills.is_empty() {
            return Ok(0);
        }

        let mut staged = Vec::with_capacity(fills.len());

        // Group fills by UTC day for builder logs fetching.
        let mut current_day: Option<String> = None;
        let mut day_logs: Vec<crate::domain::BuilderLogFill> = Vec::new();
        let mut day_index: Option<BuilderLogsIndex<'_>> = None;

        for fill in &fills {
            let day = yyyymmdd_utc(fill.time_ms.as_ms())?;
            let need_refresh = current_day.as_deref() != Some(&day);

            if need_refresh {
                current_day = Some(day.clone());
                day_index = None; // drop any borrows before mutating logs

                let logs_available = match config.builder_attribution_mode {
                    BuilderAttributionMode::Heuristic => {
                        day_logs.clear();
                        false
                    }
                    BuilderAttributionMode::Logs => {
                        day_logs = logs_fetcher.fetch_and_parse_day(&target_builder, &day).await?;
                        true
                    }
                    BuilderAttributionMode::Auto => match logs_fetcher
                        .fetch_and_parse_day(&target_builder, &day)
                        .await
                    {
                        Ok(logs) => {
                            day_logs = logs;
                            true
                        }
                        Err(e) => {
                            tracing::warn!(builder=%target_builder, yyyymmdd=%day, error=%e, "Failed to fetch builder logs, falling back to heuristic attribution");
                            day_logs.clear();
                            false
                        }
                    },
                };

                if logs_available {
                    day_index = Some(BuilderLogsIndex::new(&day_logs));
                }
            }

            let attribution = self.attribute_fill(
                config.builder_attribution_mode,
                fill,
                day_index.as_ref(),
                &target_builder,
            );

            staged.push((
                fill.fill_key.clone(),
                attribution.attributed,
                match attribution.mode {
                    AttributionMode::Heuristic => "heuristic".to_string(),
                    AttributionMode::Logs => "logs".to_string(),
                },
                match attribution.confidence {
                    AttributionConfidence::Exact => "exact".to_string(),
                    AttributionConfidence::Fuzzy => "fuzzy".to_string(),
                    AttributionConfidence::Low => "low".to_string(),
                },
                attribution.builder.map(|b| b.as_str().to_string()),
            ));
        }

        repo.insert_attributions(&staged).await?;
        Ok(staged.len())
    }
}

fn yyyymmdd_utc(time_ms: i64) -> Result<String, AttributionIngestionError> {
    let Some(dt) = chrono::Utc.timestamp_millis_opt(time_ms).single() else {
        return Err(AttributionIngestionError::InvalidTimestamp(time_ms));
    };
    Ok(dt.format("%Y%m%d").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{BuilderLogFill, Decimal, Side};
    use std::str::FromStr;

    fn fill_with_builder_fee(builder_fee: Option<&str>, tid: i64) -> Fill {
        Fill::new(
            TimeMs::new(1_700_000_000_000),
            Address::new("0xabc".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str("100").unwrap(),
            Decimal::from_str("1").unwrap(),
            Decimal::from_str("0").unwrap(),
            Decimal::from_str("0").unwrap(),
            builder_fee.map(|s| Decimal::from_str(s).unwrap()),
            Some(tid),
            None,
        )
    }

    #[test]
    fn auto_prefers_logs_over_heuristic() {
        let target_builder = Address::new("0xbuilder".to_string());
        let fill = fill_with_builder_fee(None, 42);

        let logs = vec![BuilderLogFill {
            time_ms: fill.time_ms,
            user: fill.user.clone(),
            coin: fill.coin.clone(),
            side: fill.side,
            px: fill.px,
            sz: fill.sz,
            tid: fill.tid,
            oid: fill.oid,
        }];
        let index = BuilderLogsIndex::new(&logs);
        let ingestor = AttributionIngestor::default();
        let attr = ingestor.attribute_fill(
            BuilderAttributionMode::Auto,
            &fill,
            Some(&index),
            &target_builder,
        );

        assert!(attr.attributed);
        assert_eq!(attr.mode, AttributionMode::Logs);
        assert_eq!(attr.confidence, AttributionConfidence::Exact);
        assert_eq!(attr.builder, Some(target_builder));
    }

    #[test]
    fn logs_mode_unmatched_is_not_attributed() {
        let target_builder = Address::new("0xbuilder".to_string());
        let fill = fill_with_builder_fee(Some("1"), 123);
        let logs = vec![];
        let index = BuilderLogsIndex::new(&logs);
        let ingestor = AttributionIngestor::default();

        let attr = ingestor.attribute_fill(
            BuilderAttributionMode::Logs,
            &fill,
            Some(&index),
            &target_builder,
        );

        assert!(!attr.attributed);
        assert_eq!(attr.mode, AttributionMode::Logs);
        assert_eq!(attr.confidence, AttributionConfidence::Exact);
        assert!(attr.builder.is_none());
    }

    #[test]
    fn yyyymmdd_is_utc_day() {
        let day = yyyymmdd_utc(0).unwrap();
        assert_eq!(day, "19700101");
    }

    #[test]
    fn yyyymmdd_rejects_invalid_timestamp() {
        // chrono rejects i64::MAX millis.
        let err = yyyymmdd_utc(i64::MAX).unwrap_err();
        assert!(matches!(err, AttributionIngestionError::InvalidTimestamp(_)));
    }
}
