use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::api::AppState;
use crate::config::PnlMode;
use crate::domain::{Address, Coin, Decimal, TimeMs};
use crate::error::AppError;

fn parse_user_address(input: &str) -> Result<Address, AppError> {
    if !input.starts_with("0x") || input.len() < 3 {
        return Err(AppError::BadRequest("Invalid user address".to_string()));
    }
    if !input[2..].chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AppError::BadRequest("Invalid user address".to_string()));
    }
    Ok(Address::new(input.to_string()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PnlQuery {
    pub user: String,
    pub coin: Option<String>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub builder_only: Option<bool>,
    pub max_start_capital: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PnlResponse {
    pub realized_pnl: String,
    pub return_pct: String,
    pub fees_paid: String,
    pub trade_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tainted: Option<bool>,
}

pub async fn get_pnl(
    Query(params): Query<PnlQuery>,
    State(state): State<AppState>,
) -> Result<Json<PnlResponse>, AppError> {
    let user = parse_user_address(&params.user)?;

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

    state
        .orchestrator
        .ensure_compiled(&user, coin.as_ref(), from_ms, to_ms)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    let effects = state
        .repo
        .query_fill_effects_for_pnl(&user, coin.as_ref(), from_ms, to_ms)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    let (filtered_effects, tainted) = if builder_only {
        let mut lifecycle_ids: Vec<i64> = effects.iter().map(|e| e.lifecycle_id).collect();
        lifecycle_ids.sort_unstable();
        lifecycle_ids.dedup();

        let tainted_ids = state
            .repo
            .query_tainted_lifecycle_ids(&lifecycle_ids)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;

        let mut tainted_set = std::collections::HashSet::new();
        for id in tainted_ids {
            tainted_set.insert(id);
        }

        let mut had_exclusions = false;
        let included: Vec<_> = effects
            .into_iter()
            .filter(|e| {
                let keep = !tainted_set.contains(&e.lifecycle_id);
                had_exclusions |= !keep;
                keep
            })
            .collect();

        (included, Some(had_exclusions))
    } else {
        (effects, None)
    };

    let mut realized_pnl = Decimal::zero();
    let mut fees_paid = Decimal::zero();

    for effect in &filtered_effects {
        realized_pnl = realized_pnl + effect.closed_pnl;
        fees_paid = fees_paid + effect.fee;
    }

    if state.config.pnl_mode == PnlMode::Net {
        realized_pnl = realized_pnl - fees_paid;
    }

    let equity_at_start = state
        .equity_resolver
        .resolve_equity(&user, from_ms.unwrap_or(TimeMs::new(0)))
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    let effective_capital = match max_start_capital {
        Some(max) if equity_at_start > max => max,
        _ => equity_at_start,
    };

    let return_pct = if effective_capital.is_zero() {
        Decimal::zero()
    } else {
        let hundred = Decimal::from_str_canonical("100").expect("100 is a valid decimal");
        (realized_pnl / effective_capital) * hundred
    };

    Ok(Json(PnlResponse {
        realized_pnl: realized_pnl.to_canonical_string(),
        return_pct: return_pct.to_canonical_string(),
        fees_paid: fees_paid.to_canonical_string(),
        trade_count: filtered_effects.len() as i64,
        tainted,
    }))
}

