//! Data source abstraction for fetching fills, deposits, and equity from external sources.

use crate::domain::{Decimal, Deposit, Fill};
use async_trait::async_trait;
use std::fmt;

pub mod hyperliquid;
pub mod mock;
pub mod builder_logs;

pub use hyperliquid::HyperliquidDataSource;
pub use mock::MockDataSource;
pub use builder_logs::{BuilderLogsError, BuilderLogsFetcher, BuilderLogsSource};

/// Data source trait for fetching fills, deposits, and equity information.
///
/// Implementations must handle pagination, retry/backoff, and rate limiting.
#[async_trait]
pub trait DataSource: Send + Sync + fmt::Debug {
    /// Fetch fills for a user and coin within a time range.
    ///
    /// # Arguments
    /// * `user` - User address
    /// * `coin` - Asset symbol (e.g., "BTC", "ETH")
    /// * `from_ms` - Start time in milliseconds (inclusive)
    /// * `to_ms` - End time in milliseconds (inclusive)
    ///
    /// # Returns
    /// Vector of fills, ordered deterministically by (time_ms, tid, oid, fill_key)
    async fn fetch_fills(
        &self,
        user: &str,
        coin: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<Fill>, DataSourceError>;

    /// Fetch deposits/withdrawals for a user within a time range.
    ///
    /// # Arguments
    /// * `user` - User address
    /// * `from_ms` - Start time in milliseconds (inclusive)
    /// * `to_ms` - End time in milliseconds (inclusive)
    ///
    /// # Returns
    /// Vector of deposits, ordered by time_ms
    async fn fetch_deposits(
        &self,
        user: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<Deposit>, DataSourceError>;

    /// Fetch user's equity at a specific point in time (best-effort).
    ///
    /// # Arguments
    /// * `user` - User address
    /// * `at_ms` - Time in milliseconds
    ///
    /// # Returns
    /// User's equity at the specified time, or None if unavailable
    async fn fetch_equity(
        &self,
        user: &str,
        at_ms: i64,
    ) -> Result<Option<Decimal>, DataSourceError>;
}

/// Error type for data source operations.
#[derive(Debug, Clone)]
pub enum DataSourceError {
    /// Network error (e.g., connection timeout, DNS failure)
    NetworkError(String),
    /// HTTP error (e.g., 429 rate limit, 5xx server error)
    HttpError { status: u16, message: String },
    /// Parsing error (invalid JSON or malformed response)
    ParseError(String),
    /// Rate limit exceeded (caller should implement backoff)
    RateLimited,
    /// Other error
    Other(String),
}

impl fmt::Display for DataSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataSourceError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            DataSourceError::HttpError { status, message } => {
                write!(f, "HTTP error {}: {}", status, message)
            }
            DataSourceError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            DataSourceError::RateLimited => write!(f, "Rate limited"),
            DataSourceError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for DataSourceError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Address, TimeMs};

    #[test]
    fn test_datasource_error_display() {
        let err = DataSourceError::NetworkError("connection timeout".to_string());
        assert_eq!(err.to_string(), "Network error: connection timeout");

        let err = DataSourceError::HttpError {
            status: 429,
            message: "Too many requests".to_string(),
        };
        assert_eq!(err.to_string(), "HTTP error 429: Too many requests");

        let err = DataSourceError::ParseError("invalid JSON".to_string());
        assert_eq!(err.to_string(), "Parse error: invalid JSON");

        let err = DataSourceError::RateLimited;
        assert_eq!(err.to_string(), "Rate limited");
    }

    #[test]
    fn test_deposit_clone_and_eq() {
        let deposit = Deposit::new(
            Address::new("0x123".to_string()),
            TimeMs::new(1000),
            Decimal::from_str_canonical("100").unwrap(),
            None,
        );
        let deposit2 = deposit.clone();
        assert_eq!(deposit, deposit2);
    }
}
