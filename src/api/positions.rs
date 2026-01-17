use crate::api::AppState;
use crate::domain::{Address, Coin, TimeMs};
use crate::error::AppError;
use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionsHistoryQuery {
    pub user: String,
    pub coin: Option<String>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub builder_only: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionsHistoryResponse {
    pub snapshots: Vec<PositionSnapshotDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tainted: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionSnapshotDto {
    pub time_ms: i64,
    pub coin: String,
    pub net_size: String,
    pub avg_entry_px: String,
    pub lifecycle_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tainted: Option<bool>,
}

pub async fn get_positions_history(
    Query(params): Query<PositionsHistoryQuery>,
    State(state): State<AppState>,
) -> Result<Json<PositionsHistoryResponse>, AppError> {
    let user = Address::from_str(&params.user)
        .map_err(|_| AppError::BadRequest("Invalid user address".into()))?;

    let coin = match params.coin.as_deref() {
        Some(c) => Some(Coin::from_str(c).map_err(|_| AppError::BadRequest("Invalid coin".into()))?),
        None => None,
    };

    let from_ms = params.from_ms.map(TimeMs::new);
    let to_ms = params.to_ms.map(TimeMs::new);
    if let (Some(from_ms), Some(to_ms)) = (from_ms, to_ms) {
        if from_ms > to_ms {
            return Err(AppError::BadRequest("fromMs must be <= toMs".into()));
        }
    }
    let builder_only = params.builder_only.unwrap_or(false);

    state
        .ensure_compiled(&user, coin.as_ref(), from_ms, to_ms)
        .await?;

    let mut snapshots = state
        .repo
        .query_position_snapshots(&user, coin.as_ref(), from_ms, to_ms)
        .await
        .map_err(|e| AppError::Internal(format!("Snapshot query failed: {}", e)))?;

    snapshots.sort_by(|a, b| {
        a.time_ms
            .cmp(&b.time_ms)
            .then_with(|| a.seq.cmp(&b.seq))
            .then_with(|| a.coin.as_str().cmp(b.coin.as_str()))
            .then_with(|| a.lifecycle_id.cmp(&b.lifecycle_id))
    });

    let (filtered_snapshots, tainted) = if builder_only {
        let any_tainted = snapshots.iter().any(|s| s.lifecycle_tainted);
        (
            snapshots
                .into_iter()
                .filter(|s| !s.lifecycle_tainted)
                .collect::<Vec<_>>(),
            Some(any_tainted),
        )
    } else {
        (snapshots, None)
    };

    let snapshot_dtos = filtered_snapshots
        .into_iter()
        .map(|s| PositionSnapshotDto {
            time_ms: s.time_ms.as_ms(),
            coin: s.coin.as_str().to_string(),
            net_size: s.net_size,
            avg_entry_px: s.avg_entry_px,
            lifecycle_id: s.lifecycle_id.to_string(),
            tainted: if builder_only { Some(false) } else { None },
        })
        .collect();

    Ok(Json(PositionsHistoryResponse {
        snapshots: snapshot_dtos,
        tainted,
    }))
}

