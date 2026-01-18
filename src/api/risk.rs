//! Risk fields endpoint - fetches real-time risk data from Hyperliquid.

use crate::api::AppState;
use crate::domain::Decimal;
use crate::error::AppError;
use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Cache entry with timestamp for TTL-based expiration.
struct CacheEntry {
    response: RiskResponse,
    cached_at: Instant,
}

/// Simple in-memory cache for risk responses.
/// Keyed by user address with configurable TTL.
struct RiskCache {
    entries: RwLock<HashMap<String, CacheEntry>>,
    ttl: Duration,
}

impl RiskCache {
    fn new(ttl_secs: u64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    async fn get(&self, user: &str) -> Option<RiskResponse> {
        let entries = self.entries.read().await;
        if let Some(entry) = entries.get(user) {
            if entry.cached_at.elapsed() < self.ttl {
                return Some(entry.response.clone());
            }
        }
        None
    }

    async fn set(&self, user: String, response: RiskResponse) {
        let mut entries = self.entries.write().await;
        entries.insert(
            user,
            CacheEntry {
                response,
                cached_at: Instant::now(),
            },
        );
        // Cleanup old entries (simple eviction)
        entries.retain(|_, entry| entry.cached_at.elapsed() < self.ttl * 2);
    }
}

/// Global cache instance with 5-second TTL for rate limiting protection.
static RISK_CACHE: OnceLock<RiskCache> = OnceLock::new();

fn get_cache() -> &'static RiskCache {
    RISK_CACHE.get_or_init(|| RiskCache::new(5))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RiskQuery {
    pub user: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RiskResponse {
    pub positions: Vec<PositionRisk>,
    pub cross_margin_summary: CrossMarginSummary,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
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

    // Check cache first for rate limiting protection
    let cache = get_cache();
    if let Some(cached) = cache.get(user).await {
        return Ok(Json(cached));
    }

    // Fetch live user state from Hyperliquid
    let user_state = fetch_user_state(&state.http_client, &state.config.hyperliquid_api_url, user)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch user state for {}: {}", user, e);
            AppError::Internal("Failed to fetch risk data from upstream".into())
        })?;

    // Cache the response
    cache.set(user.clone(), user_state.clone()).await;

    Ok(Json(user_state))
}

async fn fetch_user_state(
    client: &reqwest::Client,
    base_url: &str,
    user: &str,
) -> Result<RiskResponse, String> {
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
            let Some(position) = asset_pos.get("position") else {
                continue;
            };

            // Skip positions with zero size (using Decimal for accurate comparison)
            let size_str = position
                .get("szi")
                .and_then(|v| v.as_str())
                .unwrap_or("0");

            let is_zero = Decimal::from_str_canonical(size_str)
                .map(|d| d.is_zero())
                .unwrap_or(true);

            if is_zero {
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
                .map(|s| s.to_string());

            positions.push(PositionRisk {
                coin,
                size: size_str.to_string(),
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

    #[test]
    fn test_parse_user_state_with_flat_leverage() {
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
                        "coin": "ETH",
                        "szi": "1.5",
                        "entryPx": "3000",
                        "positionValue": "4500",
                        "unrealizedPnl": "50",
                        "liquidationPx": "2700",
                        "leverage": "5",
                        "marginUsed": "900"
                    }
                }
            ]
        });

        let result = parse_user_state(&json).unwrap();
        assert_eq!(result.positions.len(), 1);
        assert_eq!(result.positions[0].coin, "ETH");
        assert_eq!(result.positions[0].leverage, Some("5".to_string()));
    }

    #[test]
    fn test_parse_user_state_skips_zero_positions() {
        let json = serde_json::json!({
            "marginSummary": {
                "accountValue": "10000",
                "totalMarginUsed": "0",
                "totalNtlPos": "0",
                "totalRawUsd": "10000",
                "withdrawable": "10000"
            },
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "0",
                        "entryPx": "0",
                        "positionValue": "0",
                        "unrealizedPnl": "0",
                        "marginUsed": "0"
                    }
                },
                {
                    "position": {
                        "coin": "ETH",
                        "szi": "0.0",
                        "entryPx": "0",
                        "positionValue": "0",
                        "unrealizedPnl": "0",
                        "marginUsed": "0"
                    }
                },
                {
                    "position": {
                        "coin": "SOL",
                        "szi": "0.00",
                        "entryPx": "0",
                        "positionValue": "0",
                        "unrealizedPnl": "0",
                        "marginUsed": "0"
                    }
                },
                {
                    "position": {
                        "coin": "DOGE",
                        "szi": "100",
                        "entryPx": "0.1",
                        "positionValue": "10",
                        "unrealizedPnl": "1",
                        "marginUsed": "5"
                    }
                }
            ]
        });

        let result = parse_user_state(&json).unwrap();
        // Only DOGE should be included (non-zero size)
        assert_eq!(result.positions.len(), 1);
        assert_eq!(result.positions[0].coin, "DOGE");
        assert_eq!(result.positions[0].size, "100");
    }

    #[test]
    fn test_parse_user_state_missing_position_field() {
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
                    // Missing "position" field - should be skipped
                    "type": "someOtherType"
                },
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "0.1",
                        "entryPx": "50000",
                        "positionValue": "5000",
                        "unrealizedPnl": "100",
                        "marginUsed": "500"
                    }
                }
            ]
        });

        let result = parse_user_state(&json).unwrap();
        assert_eq!(result.positions.len(), 1);
        assert_eq!(result.positions[0].coin, "BTC");
    }
}
