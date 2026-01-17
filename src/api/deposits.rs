use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::api::AppState;
use crate::domain::{Address, Decimal, TimeMs};
use crate::error::AppError;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepositsQuery {
    pub user: String,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DepositsResponse {
    pub total_deposits: String,
    pub deposit_count: i64,
    pub deposits: Vec<DepositDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DepositDto {
    pub time_ms: i64,
    pub amount: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
}

pub async fn get_deposits(
    Query(params): Query<DepositsQuery>,
    State(state): State<AppState>,
) -> Result<Json<DepositsResponse>, AppError> {
    let user = Address::from_str(&params.user)
        .map_err(|_| AppError::BadRequest("Invalid user address".into()))?;

    let from_ms = params.from_ms.map(TimeMs::new);
    let to_ms = params.to_ms.map(TimeMs::new);
    if let (Some(from_ms), Some(to_ms)) = (from_ms, to_ms) {
        if from_ms > to_ms {
            return Err(AppError::BadRequest("fromMs must be <= toMs".into()));
        }
    }

    state
        .orchestrator
        .ensure_deposits_ingested(&user, from_ms, to_ms)
        .await
        .map_err(|e| AppError::Internal(format!("Deposit ingestion failed: {}", e)))?;

    let deposits = state
        .repo
        .query_deposits(
            &user,
            from_ms.unwrap_or(TimeMs::new(0)).as_ms(),
            to_ms.unwrap_or(TimeMs::new(i64::MAX)).as_ms(),
        )
        .await?;

    let mut total_deposits = Decimal::zero();
    for d in &deposits {
        total_deposits = total_deposits + d.amount;
    }

    let deposit_count = deposits.len() as i64;
    let deposits = deposits
        .into_iter()
        .map(|d| DepositDto {
            time_ms: d.time_ms.as_ms(),
            amount: d.amount.to_canonical_string(),
            tx_hash: d.tx_hash,
        })
        .collect();

    Ok(Json(DepositsResponse {
        total_deposits: total_deposits.to_canonical_string(),
        deposit_count,
        deposits,
    }))
}

