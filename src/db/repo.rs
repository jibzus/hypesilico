//! Repository layer for database operations.

use crate::domain::{Address, Coin, Decimal, Fill, Side, TimeMs};
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use std::str::FromStr;

/// Repository for database operations.
pub struct Repository {
    pool: SqlitePool,
}

impl Repository {
    /// Create a new repository with the given connection pool.
    pub fn new(pool: SqlitePool) -> Self {
        Repository { pool }
    }

    /// Insert a fill into the database idempotently.
    ///
    /// # Errors
    /// Returns an error if the insert fails.
    pub async fn insert_fill(&self, fill: &Fill) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            INSERT INTO raw_fills (
                user, coin, time_ms, side, px, sz, fee, closed_pnl,
                builder_fee, tid, oid, fill_key, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fill_key) DO NOTHING
            "#,
        )
        .bind(fill.user.as_str())
        .bind(fill.coin.as_str())
        .bind(fill.time_ms.as_i64())
        .bind(fill.side.to_string())
        .bind(fill.px.to_canonical_string())
        .bind(fill.sz.to_canonical_string())
        .bind(fill.fee.to_canonical_string())
        .bind(fill.closed_pnl.to_canonical_string())
        .bind(fill.builder_fee.map(|d| d.to_canonical_string()))
        .bind(fill.tid)
        .bind(fill.oid)
        .bind(fill.fill_key.as_str())
        .bind(chrono::Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Query raw fills for a user and coin within a time range.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn query_raw_fills(
        &self,
        user: &Address,
        coin: &Coin,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<Fill>, sqlx::Error> {
        self.query_fills(
            user,
            Some(coin),
            Some(TimeMs::new(from_ms)),
            Some(TimeMs::new(to_ms)),
        )
        .await
    }

    /// Query fills for a user with optional coin and time window.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn query_fills(
        &self,
        user: &Address,
        coin: Option<&Coin>,
        from_ms: Option<TimeMs>,
        to_ms: Option<TimeMs>,
    ) -> Result<Vec<Fill>, sqlx::Error> {
        let from_ms = from_ms.unwrap_or(TimeMs::new(0)).as_ms();
        let to_ms = to_ms.unwrap_or(TimeMs::new(i64::MAX)).as_ms();

        let (sql, binds_coin) = if coin.is_some() {
            (
                r#"
                SELECT user, coin, time_ms, side, px, sz, fee, closed_pnl,
                       builder_fee, tid, oid, fill_key
                FROM raw_fills
                WHERE user = ? AND coin = ? AND time_ms >= ? AND time_ms <= ?
                ORDER BY time_ms ASC, tid ASC, oid ASC, fill_key ASC
                "#,
                true,
            )
        } else {
            (
                r#"
                SELECT user, coin, time_ms, side, px, sz, fee, closed_pnl,
                       builder_fee, tid, oid, fill_key
                FROM raw_fills
                WHERE user = ? AND time_ms >= ? AND time_ms <= ?
                ORDER BY time_ms ASC, tid ASC, oid ASC, fill_key ASC
                "#,
                false,
            )
        };

        let mut query = sqlx::query(sql).bind(user.as_str());
        if binds_coin {
            query = query.bind(coin.expect("binds_coin implies coin is Some").as_str());
        }
        query = query.bind(from_ms).bind(to_ms);

        let rows = query.fetch_all(&self.pool).await?;

        let fills = rows
            .iter()
            .map(|row| {
                let side_str: String = row.get("side");
                let side = match side_str.as_str() {
                    "buy" => Side::Buy,
                    "sell" => Side::Sell,
                    _ => Side::Buy, // Default fallback
                };

                let px_str: String = row.get("px");
                let sz_str: String = row.get("sz");
                let fee_str: String = row.get("fee");
                let closed_pnl_str: String = row.get("closed_pnl");
                let builder_fee_opt: Option<String> = row.get("builder_fee");
                let fill_key: String = row.get("fill_key");

                let mut fill = Fill::new(
                    TimeMs::new(row.get("time_ms")),
                    Address::new(row.get("user")),
                    Coin::new(row.get("coin")),
                    side,
                    Decimal::from_str(&px_str).unwrap_or_default(),
                    Decimal::from_str(&sz_str).unwrap_or_default(),
                    Decimal::from_str(&fee_str).unwrap_or_default(),
                    Decimal::from_str(&closed_pnl_str).unwrap_or_default(),
                    builder_fee_opt.and_then(|s| Decimal::from_str(&s).ok()),
                    row.get("tid"),
                    row.get("oid"),
                );
                fill.fill_key = fill_key;
                fill
            })
            .collect();

        Ok(fills)
    }

    /// Get a raw fill by its fill_key.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn get_raw_fill_by_key(&self, fill_key: &str) -> Result<Option<Fill>, sqlx::Error> {
        let row = sqlx::query(
            r#"
            SELECT user, coin, time_ms, side, px, sz, fee, closed_pnl,
                   builder_fee, tid, oid, fill_key
            FROM raw_fills
            WHERE fill_key = ?
            "#,
        )
        .bind(fill_key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| {
            let side_str: String = r.get("side");
            let side = match side_str.as_str() {
                "buy" => Side::Buy,
                "sell" => Side::Sell,
                _ => Side::Buy,
            };

            let px_str: String = r.get("px");
            let sz_str: String = r.get("sz");
            let fee_str: String = r.get("fee");
            let closed_pnl_str: String = r.get("closed_pnl");
            let builder_fee_opt: Option<String> = r.get("builder_fee");
            let fill_key: String = r.get("fill_key");

            let mut fill = Fill::new(
                TimeMs::new(r.get("time_ms")),
                Address::new(r.get("user")),
                Coin::new(r.get("coin")),
                side,
                Decimal::from_str(&px_str).unwrap_or_default(),
                Decimal::from_str(&sz_str).unwrap_or_default(),
                Decimal::from_str(&fee_str).unwrap_or_default(),
                Decimal::from_str(&closed_pnl_str).unwrap_or_default(),
                builder_fee_opt.and_then(|s| Decimal::from_str(&s).ok()),
                r.get("tid"),
                r.get("oid"),
            );
            fill.fill_key = fill_key;
            fill
        }))
    }

    /// Store compile state for a user and coin.
    ///
    /// # Errors
    /// Returns an error if the insert fails.
    pub async fn store_compile_state(
        &self,
        user: &Address,
        coin: &Coin,
        last_compiled_time_ms: Option<i64>,
        last_compiled_fill_key: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO compile_state (user, coin, last_compiled_time_ms, last_compiled_fill_key, compile_version)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(user, coin) DO UPDATE SET
                last_compiled_time_ms = excluded.last_compiled_time_ms,
                last_compiled_fill_key = excluded.last_compiled_fill_key,
                compile_version = compile_version + 1
            "#,
        )
        .bind(user.as_str())
        .bind(coin.as_str())
        .bind(last_compiled_time_ms)
        .bind(last_compiled_fill_key)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get compile state for a user and coin.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn get_compile_state(
        &self,
        user: &Address,
        coin: &Coin,
    ) -> Result<Option<(Option<i64>, Option<String>)>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT last_compiled_time_ms, last_compiled_fill_key FROM compile_state WHERE user = ? AND coin = ?",
        )
        .bind(user.as_str())
        .bind(coin.as_str())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| {
            (
                r.get("last_compiled_time_ms"),
                r.get("last_compiled_fill_key"),
            )
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::init_db;
    use tempfile::TempDir;

    async fn setup_test_db() -> (Repository, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join("test.db")
            .to_string_lossy()
            .to_string();
        let pool = init_db(&db_path).await.expect("init_db failed");
        (Repository::new(pool), temp_dir)
    }

    #[tokio::test]
    async fn test_insert_and_query_raw_fill() {
        let (repo, _temp) = setup_test_db().await;

        let fill = Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str("50000").unwrap(),
            Decimal::from_str("1.5").unwrap(),
            Decimal::from_str("10").unwrap(),
            Decimal::from_str("0").unwrap(),
            None,
            Some(123),
            Some(456),
        );

        let inserted = repo.insert_fill(&fill).await.expect("insert failed");
        assert!(inserted);

        let fills = repo
            .query_raw_fills(&fill.user, &fill.coin, 0, 2000)
            .await
            .expect("query failed");

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].time_ms.as_i64(), 1000);
    }

    #[tokio::test]
    async fn test_get_raw_fill_by_key() {
        let (repo, _temp) = setup_test_db().await;

        let fill = Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str("50000").unwrap(),
            Decimal::from_str("1.5").unwrap(),
            Decimal::from_str("10").unwrap(),
            Decimal::from_str("0").unwrap(),
            None,
            Some(123),
            None,
        );

        repo.insert_fill(&fill).await.expect("insert failed");

        let retrieved = repo
            .get_raw_fill_by_key(fill.fill_key())
            .await
            .expect("query failed");

        assert!(retrieved.is_some());
        let retrieved_fill = retrieved.unwrap();
        assert_eq!(retrieved_fill.time_ms.as_i64(), 1000);
    }

    #[tokio::test]
    async fn test_store_and_get_compile_state() {
        let (repo, _temp) = setup_test_db().await;

        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());

        repo.store_compile_state(&user, &coin, Some(5000), Some("tid:999"))
            .await
            .expect("store failed");

        let state = repo
            .get_compile_state(&user, &coin)
            .await
            .expect("query failed");

        assert!(state.is_some());
        let (time_ms, fill_key) = state.unwrap();
        assert_eq!(time_ms, Some(5000));
        assert_eq!(fill_key, Some("tid:999".to_string()));
    }

    #[tokio::test]
    async fn test_insert_duplicate_fill_ignored() {
        let (repo, _temp) = setup_test_db().await;

        let fill = Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str("50000").unwrap(),
            Decimal::from_str("1.5").unwrap(),
            Decimal::from_str("10").unwrap(),
            Decimal::from_str("0").unwrap(),
            None,
            Some(123),
            None,
        );

        let inserted1 = repo.insert_fill(&fill).await.expect("first insert failed");
        assert!(inserted1, "First insert should succeed");

        let inserted2 = repo.insert_fill(&fill).await.expect("second insert failed");
        assert!(!inserted2, "Second insert should be ignored");

        let fills = repo
            .query_raw_fills(&fill.user, &fill.coin, 0, 2000)
            .await
            .expect("query failed");

        // Should only have one fill
        assert_eq!(fills.len(), 1);
    }

    #[tokio::test]
    async fn test_query_fills_without_coin() {
        let (repo, _temp) = setup_test_db().await;

        let fill_btc = Fill::new(
            TimeMs::new(1000),
            Address::new("0x123".to_string()),
            Coin::new("BTC".to_string()),
            Side::Buy,
            Decimal::from_str("50000").unwrap(),
            Decimal::from_str("1.5").unwrap(),
            Decimal::from_str("10").unwrap(),
            Decimal::from_str("0").unwrap(),
            None,
            Some(1),
            None,
        );
        let fill_eth = Fill::new(
            TimeMs::new(2000),
            Address::new("0x123".to_string()),
            Coin::new("ETH".to_string()),
            Side::Sell,
            Decimal::from_str("2500").unwrap(),
            Decimal::from_str("2").unwrap(),
            Decimal::from_str("5").unwrap(),
            Decimal::from_str("0").unwrap(),
            None,
            Some(2),
            None,
        );

        repo.insert_fill(&fill_btc).await.unwrap();
        repo.insert_fill(&fill_eth).await.unwrap();

        let fills = repo
            .query_fills(&fill_btc.user, None, None, None)
            .await
            .unwrap();
        assert_eq!(fills.len(), 2);
    }
}
