//! Risk fields endpoint - fetches real-time risk data from Hyperliquid.

use crate::api::AppState;
use crate::error::AppError;
use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RiskQuery {
    pub user: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RiskResponse {
    pub positions: Vec<PositionRisk>,
    pub cross_margin_summary: CrossMarginSummary,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionRisk {
    pub coin: String,
    pub size: String,
    pub entry_px: String,
    pub position_value: String,
    pub unrealized_pnl: String,
    pub liquidation_px: Option<String>,
    pub leverage: Option<String>,
    pub margin_used: String,
    pub max_leverage: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrossMarginSummary {
    pub account_value: String,
    pub total_margin_used: String,
    pub total_ntl_pos: String,
    pub total_raw_usd: String,
    pub withdrawable: String,
}

pub async fn get_risk(
    Query(params): Query<RiskQuery>,
    State(state): State<AppState>,
) -> Result<Json<RiskResponse>, AppError> {
    let user = &params.user;

    // Validate address format
    if !user.starts_with("0x") || user.len() != 42 {
        return Err(AppError::BadRequest("Invalid user address".into()));
    }

    // Fetch live user state from Hyperliquid
    let user_state = fetch_user_state(&state.config.hyperliquid_api_url, user)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to fetch user state: {}", e)))?;

    Ok(Json(user_state))
}

async fn fetch_user_state(base_url: &str, user: &str) -> Result<RiskResponse, String> {
    let client = reqwest::Client::new();
    let url = format!("{}/info", base_url);

    let payload = serde_json::json!({
        "type": "clearinghouseState",
        "user": user
    });

    let response = client
        .post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("Network error: {}", e))?;

    if !response.status().is_success() {
        return Err(format!("HTTP error: {}", response.status()));
    }

    let json: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Parse error: {}", e))?;

    parse_user_state(&json)
}

fn parse_user_state(json: &serde_json::Value) -> Result<RiskResponse, String> {
    // Parse cross margin summary
    let margin_summary = json
        .get("marginSummary")
        .ok_or("Missing marginSummary")?;

    let cross_margin_summary = CrossMarginSummary {
        account_value: margin_summary
            .get("accountValue")
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string(),
        total_margin_used: margin_summary
            .get("totalMarginUsed")
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string(),
        total_ntl_pos: margin_summary
            .get("totalNtlPos")
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string(),
        total_raw_usd: margin_summary
            .get("totalRawUsd")
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string(),
        withdrawable: margin_summary
            .get("withdrawable")
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string(),
    };

    // Parse asset positions
    let mut positions = Vec::new();

    if let Some(asset_positions) = json.get("assetPositions").and_then(|v| v.as_array()) {
        for asset_pos in asset_positions {
            let position = asset_pos.get("position");
            if position.is_none() {
                continue;
            }
            let position = position.unwrap();

            // Skip positions with zero size
            let size = position
                .get("szi")
                .and_then(|v| v.as_str())
                .unwrap_or("0");
            if size == "0" || size == "0.0" {
                continue;
            }

            let coin = position
                .get("coin")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let entry_px = position
                .get("entryPx")
                .and_then(|v| v.as_str())
                .unwrap_or("0")
                .to_string();

            let position_value = position
                .get("positionValue")
                .and_then(|v| v.as_str())
                .unwrap_or("0")
                .to_string();

            let unrealized_pnl = position
                .get("unrealizedPnl")
                .and_then(|v| v.as_str())
                .unwrap_or("0")
                .to_string();

            let liquidation_px = position
                .get("liquidationPx")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let leverage = position
                .get("leverage")
                .and_then(|v| {
                    if v.is_object() {
                        v.get("value").and_then(|lv| lv.as_str())
                    } else {
                        v.as_str()
                    }
                })
                .map(|s| s.to_string());

            let margin_used = position
                .get("marginUsed")
                .and_then(|v| v.as_str())
                .unwrap_or("0")
                .to_string();

            let max_leverage = position
                .get("maxLeverage")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    position.get("maxTradeSzs").and_then(|v| v.as_str())
                })
                .map(|s| s.to_string());

            positions.push(PositionRisk {
                coin,
                size: size.to_string(),
                entry_px,
                position_value,
                unrealized_pnl,
                liquidation_px,
                leverage,
                margin_used,
                max_leverage,
            });
        }
    }

    Ok(RiskResponse {
        positions,
        cross_margin_summary,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_user_state_empty() {
        let json = serde_json::json!({
            "marginSummary": {
                "accountValue": "10000",
                "totalMarginUsed": "500",
                "totalNtlPos": "5000",
                "totalRawUsd": "10000",
                "withdrawable": "9500"
            },
            "assetPositions": []
        });

        let result = parse_user_state(&json).unwrap();
        assert_eq!(result.positions.len(), 0);
        assert_eq!(result.cross_margin_summary.account_value, "10000");
        assert_eq!(result.cross_margin_summary.total_margin_used, "500");
    }

    #[test]
    fn test_parse_user_state_with_position() {
        let json = serde_json::json!({
            "marginSummary": {
                "accountValue": "10000",
                "totalMarginUsed": "500",
                "totalNtlPos": "5000",
                "totalRawUsd": "10000",
                "withdrawable": "9500"
            },
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "0.1",
                        "entryPx": "50000",
                        "positionValue": "5000",
                        "unrealizedPnl": "100",
                        "liquidationPx": "45000",
                        "leverage": { "value": "10" },
                        "marginUsed": "500"
                    }
                }
            ]
        });

        let result = parse_user_state(&json).unwrap();
        assert_eq!(result.positions.len(), 1);
        assert_eq!(result.positions[0].coin, "BTC");
        assert_eq!(result.positions[0].size, "0.1");
        assert_eq!(result.positions[0].liquidation_px, Some("45000".to_string()));
        assert_eq!(result.positions[0].leverage, Some("10".to_string()));
    }
}
