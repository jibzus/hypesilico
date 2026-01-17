use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::domain::ordering::sort_fills_deterministic;
use crate::domain::{Address, AttributionMode, Coin, TimeMs};
use crate::error::AppError;
use super::AppState;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradesQuery {
    pub user: String,
    pub coin: Option<String>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub builder_only: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TradesResponse {
    pub trades: Vec<TradeDto>,
    /// Indicates whether any fills were excluded from the response when `builderOnly=true`.
    ///
    /// - `Some(true)`: At least one fill was excluded because it lacked builder attribution.
    /// - `Some(false)`: All fills in the window had builder attribution (none excluded).
    /// - `None`: `builderOnly` was not set (all fills returned regardless of attribution).
    ///
    /// Note: This is a per-fill exclusion flag, not a lifecycle-level taint indicator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tainted: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeDto {
    pub time_ms: i64,
    pub coin: String,
    pub side: String,
    pub px: String,
    pub sz: String,
    pub fee: String,
    pub closed_pnl: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub builder: Option<String>,
}

pub async fn get_trades(
    Query(params): Query<TradesQuery>,
    State(state): State<AppState>,
) -> Result<Json<TradesResponse>, AppError> {
    let user = Address::from_str(&params.user)
        .map_err(|_| AppError::BadRequest("Invalid user address".into()))?;

    let coin = match params.coin.as_deref() {
        Some("") | None => None,
        Some(c) => Some(Coin::new(c.to_string())),
    };
    let from_ms = params.from_ms.map(TimeMs::new);
    let to_ms = params.to_ms.map(TimeMs::new);
    let builder_only = params.builder_only.unwrap_or(false);

    state
        .ingestor
        .ensure_ingested(&user, coin.as_ref(), from_ms, to_ms)
        .await?;

    let mut fills = state
        .repo
        .query_fills(&user, coin.as_ref(), from_ms, to_ms)
        .await?;

    sort_fills_deterministic(&mut fills);

    let fill_keys: Vec<String> = fills.iter().map(|f| f.fill_key.clone()).collect();
    let attributions = state.repo.query_attributions_full(&fill_keys).await?;

    let (fills, tainted) = if builder_only {
        let mut included = Vec::with_capacity(fills.len());
        let mut excluded_any = false;

        for fill in fills {
            let attributed = attributions
                .get(fill.fill_key())
                .map(|a| a.attributed)
                .unwrap_or(false);
            if attributed {
                included.push(fill);
            } else {
                excluded_any = true;
            }
        }

        (included, Some(excluded_any))
    } else {
        (fills, None)
    };

    let trades = fills
        .into_iter()
        .map(|f| {
            let builder = attributions
                .get(f.fill_key())
                .filter(|a| a.attributed && a.mode == AttributionMode::Logs)
                .and_then(|a| a.builder.as_ref())
                .map(|b| b.as_str().to_string());

            TradeDto {
                time_ms: f.time_ms.as_ms(),
                coin: f.coin.as_str().to_string(),
                side: f.side.to_string(),
                px: f.px.to_canonical_string(),
                sz: f.sz.to_canonical_string(),
                fee: f.fee.to_canonical_string(),
                closed_pnl: f.closed_pnl.to_canonical_string(),
                builder,
            }
        })
        .collect();

    Ok(Json(TradesResponse { trades, tainted }))
}
