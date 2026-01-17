use crate::config::Config;
use crate::datasource::{DataSource, DataSourceError};
use crate::db::Repository;
use crate::domain::{Address, Coin, TimeMs};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone)]
pub struct Ingestor {
    datasource: Arc<dyn DataSource>,
    repo: Arc<Repository>,
    config: Config,
}

impl Ingestor {
    pub fn new(datasource: Arc<dyn DataSource>, repo: Arc<Repository>, config: Config) -> Self {
        Self {
            datasource,
            repo,
            config,
        }
    }

    /// Ensure fills are ingested for the given user/coin/time range.
    ///
    /// Implements window correctness via `LOOKBACK_MS`.
    pub async fn ensure_ingested(
        &self,
        user: &Address,
        coin: Option<&Coin>,
        from_ms: Option<TimeMs>,
        to_ms: Option<TimeMs>,
    ) -> Result<IngestionResult, IngestionError> {
        let fetch_from = self.compute_fetch_start(user, coin, from_ms).await?;
        let fetch_to = to_ms.unwrap_or_else(TimeMs::now);

        // Convert to DataSource signature (string-based)
        let coin_str = coin.map(|c| c.as_str()).unwrap_or("");
        let fills = self
            .datasource
            .fetch_fills(
                user.as_str(),
                coin_str,
                fetch_from.as_ms(),
                fetch_to.as_ms(),
            )
            .await?;

        let fills_fetched = fills.len();
        let fills_new = self.repo.insert_fills_batch(&fills).await?;

        Ok(IngestionResult {
            fills_fetched,
            fills_new,
            fetch_from,
            fetch_to,
        })
    }

    async fn compute_fetch_start(
        &self,
        _user: &Address,      // TODO(PR-XXX): Use for per-user watermark lookups
        _coin: Option<&Coin>, // TODO(PR-XXX): Use for per-coin watermark lookups
        requested_from: Option<TimeMs>,
    ) -> Result<TimeMs, IngestionError> {
        let requested = requested_from.unwrap_or(TimeMs::new(0));
        let lookback = self.config.lookback_ms;
        let fetch_from = TimeMs::new(requested.as_ms().saturating_sub(lookback));

        tracing::info!(
            "Window correctness: requested from {} but fetching from {} (lookback {}ms)",
            requested.as_ms(),
            fetch_from.as_ms(),
            lookback
        );

        Ok(fetch_from)
    }
}

#[derive(Debug)]
pub struct IngestionResult {
    pub fills_fetched: usize,
    pub fills_new: usize,
    pub fetch_from: TimeMs,
    pub fetch_to: TimeMs,
}

#[derive(Debug, Error)]
pub enum IngestionError {
    #[error(transparent)]
    DataSource(#[from] DataSourceError),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::MockDataSource;
    use crate::db::migrations::init_db;
    use crate::domain::{Address, Coin, Decimal, Fill, Side, TimeMs};
    use std::str::FromStr;
    use tempfile::TempDir;

    async fn setup_repo() -> (Arc<Repository>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join("test.db")
            .to_string_lossy()
            .to_string();
        let pool = init_db(&db_path).await.expect("init_db failed");
        (Arc::new(Repository::new(pool)), temp_dir)
    }

    fn test_config(lookback_ms: i64) -> Config {
        Config {
            port: 0,
            database_path: ":memory:".to_string(),
            hyperliquid_api_url: "http://example.invalid".to_string(),
            target_builder: "0x0".to_string(),
            builder_attribution_mode: crate::config::BuilderAttributionMode::Auto,
            pnl_mode: crate::config::PnlMode::Gross,
            lookback_ms,
            leaderboard_users: vec![],
        }
    }

    fn make_test_fill(user: &Address, coin: &Coin, time_ms: i64, tid: i64) -> Fill {
        Fill::new(
            TimeMs::new(time_ms),
            user.clone(),
            coin.clone(),
            Side::Buy,
            Decimal::from_str("100").unwrap(),
            Decimal::from_str("1").unwrap(),
            Decimal::from_str("0.1").unwrap(),
            Decimal::from_str("0").unwrap(),
            None,
            Some(tid),
            None,
        )
    }

    #[tokio::test]
    async fn test_ensure_ingested_fetches_and_stores() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let ds = Arc::new(
            MockDataSource::new()
                .with_fill(make_test_fill(&user, &coin, 1000, 1))
                .with_fill(make_test_fill(&user, &coin, 2000, 2)),
        );

        let (repo, _temp) = setup_repo().await;
        let ingestor = Ingestor::new(ds, repo.clone(), test_config(0));

        let result = ingestor
            .ensure_ingested(&user, None, None, None)
            .await
            .unwrap();
        assert_eq!(result.fills_new, 2);

        let fills = repo.query_fills(&user, None, None, None).await.unwrap();
        assert_eq!(fills.len(), 2);
    }

    #[tokio::test]
    async fn test_ensure_ingested_is_idempotent() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let ds = Arc::new(MockDataSource::new().with_fill(make_test_fill(&user, &coin, 1000, 1)));

        let (repo, _temp) = setup_repo().await;
        let ingestor = Ingestor::new(ds, repo, test_config(0));

        ingestor
            .ensure_ingested(&user, None, None, None)
            .await
            .unwrap();
        let result2 = ingestor
            .ensure_ingested(&user, None, None, None)
            .await
            .unwrap();

        assert_eq!(result2.fills_new, 0, "Second run should insert nothing new");
    }

    #[tokio::test]
    async fn test_lookback_applied_to_fetch_from() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let ds = Arc::new(MockDataSource::new());

        let (repo, _temp) = setup_repo().await;
        let ingestor = Ingestor::new(ds, repo, test_config(100));

        let result = ingestor
            .ensure_ingested(
                &user,
                Some(&coin),
                Some(TimeMs::new(1000)),
                Some(TimeMs::new(2000)),
            )
            .await
            .unwrap();

        // The lookback should have been applied to fetch_from
        assert_eq!(result.fetch_from.as_ms(), 900);
    }
}
