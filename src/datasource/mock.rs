//! Mock data source for testing without network calls.

use super::{DataSource, DataSourceError, Deposit};
use crate::domain::{Address, Decimal, Fill, TimeMs};
use async_trait::async_trait;

/// Mock data source that returns predefined test data.
#[derive(Debug, Clone)]
pub struct MockDataSource {
    fills: Vec<Fill>,
    deposits: Vec<Deposit>,
    equity: Option<Decimal>,
}

impl MockDataSource {
    /// Create a new mock data source with empty data.
    pub fn new() -> Self {
        Self {
            fills: Vec::new(),
            deposits: Vec::new(),
            equity: None,
        }
    }

    /// Add a fill to the mock data source.
    pub fn with_fill(mut self, fill: Fill) -> Self {
        self.fills.push(fill);
        self
    }

    /// Add multiple fills to the mock data source.
    pub fn with_fills(mut self, fills: Vec<Fill>) -> Self {
        self.fills.extend(fills);
        self
    }

    /// Add a deposit to the mock data source.
    pub fn with_deposit(mut self, deposit: Deposit) -> Self {
        self.deposits.push(deposit);
        self
    }

    /// Add multiple deposits to the mock data source.
    pub fn with_deposits(mut self, deposits: Vec<Deposit>) -> Self {
        self.deposits.extend(deposits);
        self
    }

    /// Set the equity value returned by fetch_equity.
    pub fn with_equity(mut self, equity: Decimal) -> Self {
        self.equity = Some(equity);
        self
    }
}

impl Default for MockDataSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataSource for MockDataSource {
    async fn fetch_fills(
        &self,
        user: &str,
        coin: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<Fill>, DataSourceError> {
        let user_addr = Address::new(user.to_string());
        let from_time = TimeMs::new(from_ms);
        let to_time = TimeMs::new(to_ms);

        Ok(self
            .fills
            .iter()
            .filter(|f| {
                f.user == user_addr
                    && (coin.is_empty() || f.coin.as_str() == coin)
                    && f.time_ms >= from_time
                    && f.time_ms <= to_time
            })
            .cloned()
            .collect())
    }

    async fn fetch_deposits(
        &self,
        user: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<Deposit>, DataSourceError> {
        let user_addr = Address::new(user.to_string());
        let from_time = TimeMs::new(from_ms);
        let to_time = TimeMs::new(to_ms);

        Ok(self
            .deposits
            .iter()
            .filter(|d| d.user == user_addr && d.time_ms >= from_time && d.time_ms <= to_time)
            .cloned()
            .collect())
    }

    async fn fetch_equity(
        &self,
        _user: &str,
        _at_ms: i64,
    ) -> Result<Option<Decimal>, DataSourceError> {
        Ok(self.equity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Coin, Side};

    fn make_test_fill() -> Fill {
        Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str_canonical("50000").unwrap(),
            Decimal::from_str_canonical("1").unwrap(),
            Decimal::from_str_canonical("10").unwrap(),
            Decimal::from_str_canonical("0").unwrap(),
            None,
            Some(1),
            None,
        )
    }

    #[tokio::test]
    async fn test_mock_datasource_fetch_fills() {
        let fill = make_test_fill();
        let mock = MockDataSource::new().with_fill(fill.clone());
        let fills = mock.fetch_fills("0x123", "BTC", 0, 2000).await.unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0], fill);
    }

    #[tokio::test]
    async fn test_mock_datasource_fetch_fills_filtered() {
        let fill = make_test_fill();
        let mock = MockDataSource::new().with_fill(fill);
        let fills = mock.fetch_fills("0x123", "ETH", 0, 2000).await.unwrap();
        assert_eq!(fills.len(), 0);
    }

    #[tokio::test]
    async fn test_mock_datasource_fetch_deposits() {
        let deposit = Deposit {
            user: Address::new("0x123".to_string()),
            time_ms: TimeMs::new(1000),
            amount: Decimal::from_str_canonical("1000").unwrap(),
            coin: Coin::new("USDC".to_string()),
        };

        let mock = MockDataSource::new().with_deposit(deposit.clone());
        let deposits = mock.fetch_deposits("0x123", 0, 2000).await.unwrap();
        assert_eq!(deposits.len(), 1);
        assert_eq!(deposits[0], deposit);
    }

    #[tokio::test]
    async fn test_mock_datasource_fetch_equity() {
        let equity = Decimal::from_str_canonical("10000").unwrap();
        let mock = MockDataSource::new().with_equity(equity);
        let result = mock.fetch_equity("0x123", 1000).await.unwrap();
        assert_eq!(result, Some(equity));
    }
}
