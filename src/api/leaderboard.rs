use axum::extract::{Query, State};
use axum::Json;
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::str::FromStr;

use crate::api::AppState;
use crate::config::PnlMode;
use crate::db::repo::LeaderboardFillEffect;
use crate::domain::{Address, Coin, Decimal, TimeMs};
use crate::error::AppError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaderboardMetric {
    Volume,
    Pnl,
    ReturnPct,
}

impl FromStr for LeaderboardMetric {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "volume" => Ok(LeaderboardMetric::Volume),
            "pnl" => Ok(LeaderboardMetric::Pnl),
            "returnpct" => Ok(LeaderboardMetric::ReturnPct),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaderboardQuery {
    pub coin: Option<String>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub metric: Option<String>,
    pub builder_only: Option<bool>,
    pub max_start_capital: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaderboardEntry {
    pub rank: i64,
    pub user: String,
    pub metric_value: String,
    pub trade_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tainted: Option<bool>,
}

struct UserMetric {
    user: Address,
    metric_value: Decimal,
    metric_value_str: String,
    trade_count: i64,
    tainted: bool,
}

pub async fn get_leaderboard(
    Query(params): Query<LeaderboardQuery>,
    State(state): State<AppState>,
) -> Result<Json<Vec<LeaderboardEntry>>, AppError> {
    let metric = params
        .metric
        .as_deref()
        .ok_or_else(|| AppError::BadRequest("metric is required".to_string()))?;
    let metric = LeaderboardMetric::from_str(metric).map_err(|_| {
        AppError::BadRequest("metric must be one of: volume, pnl, returnPct".to_string())
    })?;

    let coin = params
        .coin
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| Coin::new(s.to_string()));

    let from_ms = params.from_ms.map(TimeMs::new);
    let to_ms = params.to_ms.map(TimeMs::new);

    if let (Some(from), Some(to)) = (from_ms, to_ms) {
        if from > to {
            return Err(AppError::BadRequest("fromMs must be <= toMs".to_string()));
        }
    }

    let builder_only = params.builder_only.unwrap_or(false);
    let max_start_capital = params
        .max_start_capital
        .as_deref()
        .map(Decimal::from_str_canonical)
        .transpose()
        .map_err(|_| AppError::BadRequest("Invalid maxStartCapital".to_string()))?;

    let users = parse_leaderboard_users(&state.config.leaderboard_users)?;
    if users.is_empty() {
        return Ok(Json(Vec::new()));
    }

    // Process all users in parallel for better performance
    let user_futures = users.into_iter().map(|user| {
        let state = state.clone();
        let coin = coin.clone();
        async move {
            state
                .orchestrator
                .ensure_compiled(&user, coin.as_ref(), from_ms, to_ms)
                .await
                .map_err(|e| {
                    tracing::error!(user=%user, error=%e, "Compilation failed");
                    AppError::Internal(format!("Compilation failed: {}", e))
                })?;

            let effects = state
                .repo
                .query_fill_effects_for_leaderboard(&user, coin.as_ref(), from_ms, to_ms)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;

            let (effects, tainted) = if builder_only {
                filter_effects_builder_only(&state, effects).await?
            } else {
                (effects, false)
            };

            compute_user_metric(
                &state,
                user,
                effects,
                tainted,
                metric,
                from_ms.unwrap_or(TimeMs::new(0)),
                max_start_capital,
            )
            .await
        }
    });

    let mut metrics: Vec<UserMetric> = try_join_all(user_futures).await?;

    metrics.sort_by(|a, b| {
        b.metric_value
            .cmp(&a.metric_value)
            .then_with(|| b.trade_count.cmp(&a.trade_count))
            .then_with(|| a.user.as_str().cmp(b.user.as_str()))
    });

    let entries = metrics
        .into_iter()
        .enumerate()
        .map(|(idx, m)| LeaderboardEntry {
            rank: (idx + 1) as i64,
            user: m.user.as_str().to_string(),
            metric_value: m.metric_value_str,
            trade_count: m.trade_count,
            tainted: builder_only.then_some(m.tainted),
        })
        .collect();

    Ok(Json(entries))
}

fn parse_leaderboard_users(users: &[String]) -> Result<Vec<Address>, AppError> {
    let mut parsed: Vec<Address> = users
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(Address::from_str)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| AppError::Internal("Invalid leaderboard user address in config".to_string()))?;

    parsed.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
    parsed.dedup();
    Ok(parsed)
}

async fn filter_effects_builder_only(
    state: &AppState,
    effects: Vec<LeaderboardFillEffect>,
) -> Result<(Vec<LeaderboardFillEffect>, bool), AppError> {
    if effects.is_empty() {
        return Ok((effects, false));
    }

    let mut lifecycle_ids: Vec<i64> = effects.iter().map(|e| e.lifecycle_id).collect();
    lifecycle_ids.sort_unstable();
    lifecycle_ids.dedup();

    let tainted_ids = state
        .repo
        .query_tainted_lifecycle_ids(&lifecycle_ids)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    if tainted_ids.is_empty() {
        return Ok((effects, false));
    }

    let tainted_set: HashSet<i64> = tainted_ids.into_iter().collect();
    let mut had_exclusions = false;

    let included: Vec<_> = effects
        .into_iter()
        .filter(|e| {
            let keep = !tainted_set.contains(&e.lifecycle_id);
            had_exclusions |= !keep;
            keep
        })
        .collect();

    Ok((included, had_exclusions))
}

async fn compute_user_metric(
    state: &AppState,
    user: Address,
    effects: Vec<LeaderboardFillEffect>,
    tainted: bool,
    metric: LeaderboardMetric,
    equity_at_ms: TimeMs,
    max_start_capital: Option<Decimal>,
) -> Result<UserMetric, AppError> {
    let mut volume = Decimal::zero();
    let mut realized_pnl = Decimal::zero();
    let mut fees_paid = Decimal::zero();
    let mut fill_keys: HashSet<String> = HashSet::new();

    for effect in &effects {
        volume = volume + effect.notional;
        realized_pnl = realized_pnl + effect.closed_pnl;
        fees_paid = fees_paid + effect.fee;
        fill_keys.insert(effect.fill_key.clone());
    }

    if state.config.pnl_mode == PnlMode::Net {
        realized_pnl = realized_pnl - fees_paid;
    }

    let trade_count = fill_keys.len() as i64;

    let metric_value = match metric {
        LeaderboardMetric::Volume => volume,
        LeaderboardMetric::Pnl => realized_pnl,
        LeaderboardMetric::ReturnPct => {
            let equity_at_start = state
                .equity_resolver
                .resolve_equity(&user, equity_at_ms)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;

            let effective_capital = match max_start_capital {
                Some(max) if equity_at_start > max => max,
                _ => equity_at_start,
            };

            if effective_capital.is_zero() {
                Decimal::zero()
            } else {
                (realized_pnl / effective_capital) * Decimal::hundred()
            }
        }
    };

    Ok(UserMetric {
        user,
        metric_value,
        metric_value_str: metric_value.to_canonical_string(),
        trade_count,
        tainted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_metric_accepts_camel_case_return_pct() {
        assert_eq!(
            LeaderboardMetric::from_str("returnPct").unwrap(),
            LeaderboardMetric::ReturnPct
        );
        assert!(LeaderboardMetric::from_str("nope").is_err());
    }

    #[test]
    fn parse_leaderboard_users_sorts_and_dedups() {
        let users = vec![
            " 0x0000000000000000000000000000000000000002 ".to_string(),
            "0x0000000000000000000000000000000000000001".to_string(),
            "0x0000000000000000000000000000000000000002".to_string(),
        ];
        let parsed = parse_leaderboard_users(&users).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].as_str(), "0x0000000000000000000000000000000000000001");
        assert_eq!(parsed[1].as_str(), "0x0000000000000000000000000000000000000002");
    }
}
