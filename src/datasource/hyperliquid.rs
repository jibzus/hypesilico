//! Hyperliquid API client implementation.

use super::{DataSource, DataSourceError, Deposit};
use crate::domain::{Address, Coin, Decimal, Fill, Side, TimeMs};
use async_trait::async_trait;
use backoff::future::retry;
use backoff::ExponentialBackoff;
use reqwest::Client;
use std::time::Duration;
use tracing::{debug, warn};

/// Hyperliquid data source using the public Info API.
#[derive(Debug, Clone)]
pub struct HyperliquidDataSource {
    client: Client,
    base_url: String,
}

impl HyperliquidDataSource {
    /// Create a new Hyperliquid data source.
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }

    /// Create with default Hyperliquid API URL.
    pub fn default_url() -> Self {
        Self::new("https://api.hyperliquid.xyz".to_string())
    }

    async fn post_info(
        &self,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, DataSourceError> {
        let url = format!("{}/info", self.base_url);
        let backoff = ExponentialBackoff {
            max_elapsed_time: Some(Duration::from_secs(30)),
            ..Default::default()
        };

        retry(backoff, || async {
            let response = self
                .client
                .post(&url)
                .json(&payload)
                .send()
                .await
                .map_err(|e| {
                    backoff::Error::transient(DataSourceError::NetworkError(e.to_string()))
                })?;

            let status = response.status();
            if status == 429 {
                return Err(backoff::Error::transient(DataSourceError::RateLimited));
            }
            if status.is_server_error() {
                return Err(backoff::Error::transient(DataSourceError::HttpError {
                    status: status.as_u16(),
                    message: "Server error".to_string(),
                }));
            }
            if !status.is_success() {
                return Err(backoff::Error::permanent(DataSourceError::HttpError {
                    status: status.as_u16(),
                    message: "Client error".to_string(),
                }));
            }

            response
                .json::<serde_json::Value>()
                .await
                .map_err(|e| backoff::Error::permanent(DataSourceError::ParseError(e.to_string())))
        })
        .await
    }
}

#[async_trait]
impl DataSource for HyperliquidDataSource {
    async fn fetch_fills(
        &self,
        user: &str,
        coin: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<Fill>, DataSourceError> {
        debug!(
            "Fetching fills for user={}, coin={}, from_ms={}, to_ms={}",
            user, coin, from_ms, to_ms
        );

        let payload = serde_json::json!({
            "type": "userFillsByTime",
            "user": user,
            "coin": coin,
            "startTime": from_ms,
            "endTime": to_ms,
            "aggregateByTime": false
        });

        let response = self.post_info(payload).await?;

        // Parse response into fills
        let fills_json = response
            .as_array()
            .ok_or_else(|| DataSourceError::ParseError("Expected array response".to_string()))?;

        let mut fills = Vec::new();
        for fill_json in fills_json {
            match parse_fill(fill_json, user, coin) {
                Ok(fill) => fills.push(fill),
                Err(e) => {
                    warn!("Failed to parse fill: {}", e);
                }
            }
        }

        Ok(fills)
    }

    async fn fetch_deposits(
        &self,
        user: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<Deposit>, DataSourceError> {
        debug!(
            "Fetching deposits for user={}, from_ms={}, to_ms={}",
            user, from_ms, to_ms
        );

        let payload = serde_json::json!({
            "type": "userNonFundingLedgerUpdates",
            "user": user,
            "startTime": from_ms,
            "endTime": to_ms
        });

        let response = self.post_info(payload).await?;

        let deposits_json = response
            .as_array()
            .ok_or_else(|| DataSourceError::ParseError("Expected array response".to_string()))?;

        let mut deposits = Vec::new();
        for deposit_json in deposits_json {
            match parse_deposit(deposit_json, user) {
                Ok(deposit) => deposits.push(deposit),
                Err(e) => {
                    warn!("Failed to parse deposit: {}", e);
                }
            }
        }

        Ok(deposits)
    }

    async fn fetch_equity(
        &self,
        user: &str,
        at_ms: i64,
    ) -> Result<Option<Decimal>, DataSourceError> {
        debug!("Fetching equity for user={}, at_ms={}", user, at_ms);

        let payload = serde_json::json!({
            "type": "userState",
            "user": user
        });

        let response = self.post_info(payload).await?;

        // Try to extract equity from response
        if let Some(equity_str) = response.get("equity").and_then(|v| v.as_str()) {
            Decimal::from_str_canonical(equity_str)
                .map(Some)
                .map_err(|e| DataSourceError::ParseError(format!("Invalid equity: {}", e)))
        } else {
            Ok(None)
        }
    }
}

fn parse_fill(
    fill_json: &serde_json::Value,
    user: &str,
    coin: &str,
) -> Result<Fill, DataSourceError> {
    let time_ms = fill_json
        .get("time")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| DataSourceError::ParseError("Missing time field".to_string()))?;

    let side_str = fill_json
        .get("side")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DataSourceError::ParseError("Missing side field".to_string()))?;

    let side = match side_str {
        "A" => Side::Buy,
        "B" => Side::Sell,
        _ => {
            return Err(DataSourceError::ParseError(format!(
                "Invalid side: {}",
                side_str
            )))
        }
    };

    let px_str = fill_json
        .get("px")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DataSourceError::ParseError("Missing px field".to_string()))?;
    let px = Decimal::from_str_canonical(px_str)
        .map_err(|e| DataSourceError::ParseError(format!("Invalid px: {}", e)))?;

    let sz_str = fill_json
        .get("sz")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DataSourceError::ParseError("Missing sz field".to_string()))?;
    let sz = Decimal::from_str_canonical(sz_str)
        .map_err(|e| DataSourceError::ParseError(format!("Invalid sz: {}", e)))?;

    let fee_str = fill_json
        .get("fee")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DataSourceError::ParseError("Missing fee field".to_string()))?;
    let fee = Decimal::from_str_canonical(fee_str)
        .map_err(|e| DataSourceError::ParseError(format!("Invalid fee: {}", e)))?;

    let closed_pnl_str = fill_json
        .get("closedPnl")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DataSourceError::ParseError("Missing closedPnl field".to_string()))?;
    let closed_pnl = Decimal::from_str_canonical(closed_pnl_str)
        .map_err(|e| DataSourceError::ParseError(format!("Invalid closedPnl: {}", e)))?;

    let tid = fill_json.get("tid").and_then(|v| v.as_i64());
    let oid = fill_json.get("oid").and_then(|v| v.as_i64());
    let builder_fee = fill_json
        .get("builderFee")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str_canonical(s).ok());

    Ok(Fill::new(
        TimeMs::new(time_ms),
        Address::new(user.to_string()),
        Coin::new(coin.to_string()),
        side,
        px,
        sz,
        fee,
        closed_pnl,
        builder_fee,
        tid,
        oid,
    ))
}

fn parse_deposit(deposit_json: &serde_json::Value, user: &str) -> Result<Deposit, DataSourceError> {
    let time_ms = deposit_json
        .get("time")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| DataSourceError::ParseError("Missing time field".to_string()))?;

    let amount_str = deposit_json
        .get("amount")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DataSourceError::ParseError("Missing amount field".to_string()))?;
    let amount = Decimal::from_str_canonical(amount_str)
        .map_err(|e| DataSourceError::ParseError(format!("Invalid amount: {}", e)))?;

    let coin = deposit_json
        .get("coin")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DataSourceError::ParseError("Missing coin field".to_string()))?
        .to_string();

    Ok(Deposit {
        user: Address::new(user.to_string()),
        time_ms: TimeMs::new(time_ms),
        amount,
        coin: Coin::new(coin),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_fill_valid() {
        let fill_json = serde_json::json!({
            "time": 1000,
            "side": "A",
            "px": "50000",
            "sz": "1",
            "fee": "10",
            "closedPnl": "0",
            "tid": 123,
            "oid": 456
        });

        let fill = parse_fill(&fill_json, "0x123", "BTC").unwrap();
        assert_eq!(fill.user, Address::new("0x123".to_string()));
        assert_eq!(fill.coin, Coin::new("BTC".to_string()));
        assert_eq!(fill.time_ms, TimeMs::new(1000));
        assert_eq!(fill.side, Side::Buy);
        assert_eq!(fill.tid, Some(123));
        assert_eq!(fill.oid, Some(456));
    }

    #[test]
    fn test_parse_deposit_valid() {
        let deposit_json = serde_json::json!({
            "time": 1000,
            "amount": "1000",
            "coin": "USDC"
        });

        let deposit = parse_deposit(&deposit_json, "0x123").unwrap();
        assert_eq!(deposit.user, Address::new("0x123".to_string()));
        assert_eq!(deposit.time_ms, TimeMs::new(1000));
        assert_eq!(deposit.coin, Coin::new("USDC".to_string()));
    }
}
