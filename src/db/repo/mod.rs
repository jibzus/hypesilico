//! Repository layer for database operations.
//!
//! This module provides the `Repository` struct for all database operations.
//! Methods are organized across submodules by domain:
//! - `fills.rs` - Fill and effect operations
//! - `positions.rs` - Lifecycle, snapshot, and attribution operations

mod fills;
mod positions;

use crate::domain::{Address, Coin, Decimal, Deposit, TimeMs};
use crate::engine::{Effect, EffectType, Lifecycle, Snapshot};
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use std::str::FromStr;
use tracing::warn;

/// Position snapshot row with lifecycle taint information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionSnapshotRow {
    pub time_ms: TimeMs,
    pub seq: i32,
    pub coin: Coin,
    pub net_size: String,
    pub avg_entry_px: String,
    pub lifecycle_id: i64,
    pub lifecycle_tainted: bool,
}

/// Minimal fill effect row for PnL aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PnlFillEffect {
    pub lifecycle_id: i64,
    pub fee: Decimal,
    pub closed_pnl: Decimal,
}

/// Minimal fill effect row for leaderboard aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderboardFillEffect {
    pub fill_key: String,
    pub lifecycle_id: i64,
    pub notional: Decimal,
    pub fee: Decimal,
    pub closed_pnl: Decimal,
}

/// Repository for database operations.
pub struct Repository {
    pool: SqlitePool,
}

impl Repository {
    /// Create a new repository with the given connection pool.
    pub fn new(pool: SqlitePool) -> Self {
        Repository { pool }
    }

    // =========================================================================
    // Deposit operations
    // =========================================================================

    /// Insert a deposit into the database idempotently.
    ///
    /// # Errors
    /// Returns an error if the insert fails.
    pub async fn insert_deposit(&self, deposit: &Deposit) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            INSERT INTO deposits (user, time_ms, amount, tx_hash, event_key)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(event_key) DO NOTHING
            "#,
        )
        .bind(deposit.user.as_str())
        .bind(deposit.time_ms.as_i64())
        .bind(deposit.amount.to_canonical_string())
        .bind(deposit.tx_hash.as_deref())
        .bind(deposit.event_key.as_str())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Insert multiple deposits in a single transaction for better performance.
    ///
    /// Returns the number of newly inserted deposits (excludes duplicates).
    ///
    /// # Errors
    /// Returns an error if the transaction fails.
    pub async fn insert_deposits_batch(&self, deposits: &[Deposit]) -> Result<usize, sqlx::Error> {
        if deposits.is_empty() {
            return Ok(0);
        }

        let mut total_inserted = 0usize;
        let mut tx = self.pool.begin().await?;

        for deposit in deposits {
            let result = sqlx::query(
                r#"
                INSERT INTO deposits (user, time_ms, amount, tx_hash, event_key)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(event_key) DO NOTHING
                "#,
            )
            .bind(deposit.user.as_str())
            .bind(deposit.time_ms.as_i64())
            .bind(deposit.amount.to_canonical_string())
            .bind(deposit.tx_hash.as_deref())
            .bind(deposit.event_key.as_str())
            .execute(&mut *tx)
            .await?;

            if result.rows_affected() > 0 {
                total_inserted += 1;
            }
        }

        tx.commit().await?;
        Ok(total_inserted)
    }

    /// Query deposits for a user within a time range.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn query_deposits(
        &self,
        user: &Address,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<Deposit>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT user, time_ms, amount, tx_hash, event_key
            FROM deposits
            WHERE user = ? AND time_ms >= ? AND time_ms <= ?
            ORDER BY time_ms ASC, event_key ASC
            "#,
        )
        .bind(user.as_str())
        .bind(from_ms)
        .bind(to_ms)
        .fetch_all(&self.pool)
        .await?;

        let deposits = rows
            .iter()
            .map(|row| {
                let user: String = row.get("user");
                let time_ms: i64 = row.get("time_ms");
                let amount_str: String = row.get("amount");
                let tx_hash: Option<String> = row.get("tx_hash");
                let event_key: String = row.get("event_key");

                let amount = Decimal::from_str(&amount_str).unwrap_or_else(|e| {
                    warn!(
                        event_key = %event_key,
                        amount = %amount_str,
                        error = %e,
                        "Failed to parse deposit amount decimal, using default"
                    );
                    Decimal::default()
                });

                Deposit {
                    event_key,
                    user: Address::new(user),
                    time_ms: TimeMs::new(time_ms),
                    amount,
                    tx_hash,
                }
            })
            .collect();

        Ok(deposits)
    }

    /// Get the earliest deposit timestamp for a user.
    ///
    /// Returns None if the user has no deposits.
    pub async fn get_earliest_deposit_timestamp(
        &self,
        user: &Address,
    ) -> Result<Option<i64>, sqlx::Error> {
        let row = sqlx::query(
            r#"
            SELECT MIN(time_ms) as min_time
            FROM deposits
            WHERE user = ?
            "#,
        )
        .bind(user.as_str())
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get::<Option<i64>, _>("min_time"))
    }

    // =========================================================================
    // Compile state operations
    // =========================================================================

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

    // =========================================================================
    // Equity operations
    // =========================================================================

    /// Sum deposits up to and including `at_ms`.
    ///
    /// # Implementation Note
    ///
    /// We iterate in Rust to preserve decimal precision. SQLite's SUM aggregate
    /// function returns REAL (float), which would lose precision for financial
    /// calculations. By fetching rows and summing with our Decimal type, we
    /// maintain lossless arithmetic.
    pub async fn sum_deposits_up_to(
        &self,
        user: &Address,
        at_ms: TimeMs,
    ) -> Result<Decimal, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT amount
            FROM deposits
            WHERE user = ? AND time_ms <= ?
            ORDER BY time_ms ASC, id ASC
            "#,
        )
        .bind(user.as_str())
        .bind(at_ms.as_i64())
        .fetch_all(&self.pool)
        .await?;

        let mut sum = Decimal::zero();
        for row in rows {
            let amount_str: String = row.get("amount");
            let amount = Decimal::from_str(&amount_str).unwrap_or_else(|e| {
                warn!(user = %user, amount = %amount_str, error = %e, "Failed to parse deposit amount decimal, using default");
                Decimal::default()
            });
            sum = sum + amount;
        }

        Ok(sum)
    }

    /// Sum realized PnL strictly before `at_ms` from fill effects (excludes funding).
    ///
    /// # Implementation Note
    ///
    /// We iterate in Rust to preserve decimal precision. SQLite's SUM aggregate
    /// function returns REAL (float), which would lose precision for financial
    /// calculations. By fetching rows and summing with our Decimal type, we
    /// maintain lossless arithmetic.
    pub async fn sum_realized_pnl_before(
        &self,
        user: &Address,
        at_ms: TimeMs,
    ) -> Result<Decimal, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT fe.closed_pnl
            FROM fill_effects fe
            JOIN raw_fills rf ON rf.fill_key = fe.fill_key
            JOIN position_lifecycles pl ON pl.id = fe.lifecycle_id
            WHERE pl.user = ? AND rf.time_ms < ?
            ORDER BY rf.time_ms ASC, fe.id ASC
            "#,
        )
        .bind(user.as_str())
        .bind(at_ms.as_i64())
        .fetch_all(&self.pool)
        .await?;

        let mut sum = Decimal::zero();
        for row in rows {
            let pnl_str: String = row.get("closed_pnl");
            let pnl = Decimal::from_str(&pnl_str).unwrap_or_else(|e| {
                warn!(user = %user, closed_pnl = %pnl_str, error = %e, "Failed to parse closed_pnl decimal, using default");
                Decimal::default()
            });
            sum = sum + pnl;
        }

        Ok(sum)
    }

    /// Get the latest equity snapshot at or before `at_ms`.
    pub async fn get_equity_snapshot_at_or_before(
        &self,
        user: &Address,
        at_ms: TimeMs,
    ) -> Result<Option<(TimeMs, Decimal)>, sqlx::Error> {
        let row = sqlx::query(
            r#"
            SELECT time_ms, equity
            FROM equity_snapshots
            WHERE user = ? AND time_ms <= ?
            ORDER BY time_ms DESC, id DESC
            LIMIT 1
            "#,
        )
        .bind(user.as_str())
        .bind(at_ms.as_i64())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| {
            let time_ms: i64 = r.get("time_ms");
            let equity_str: String = r.get("equity");
            let equity = Decimal::from_str(&equity_str).unwrap_or_else(|e| {
                warn!(user = %user, equity = %equity_str, error = %e, "Failed to parse equity snapshot decimal, using default");
                Decimal::default()
            });
            (TimeMs::new(time_ms), equity)
        }))
    }

    /// Upsert an equity snapshot for an exact (user, time_ms) key.
    pub async fn upsert_equity_snapshot(
        &self,
        user: &Address,
        time_ms: TimeMs,
        equity: Decimal,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            DELETE FROM equity_snapshots
            WHERE user = ? AND time_ms = ?
            "#,
        )
        .bind(user.as_str())
        .bind(time_ms.as_i64())
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO equity_snapshots (user, time_ms, equity)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(user.as_str())
        .bind(time_ms.as_i64())
        .bind(equity.to_canonical_string())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    // =========================================================================
    // Transaction coordination (spans multiple domains)
    // =========================================================================

    /// Insert all derived tables (lifecycles, snapshots, effects) atomically in a single transaction.
    ///
    /// This ensures that if any insert fails, the entire operation is rolled back,
    /// preventing partial data from being committed.
    ///
    /// # Arguments
    /// * `user` - User address
    /// * `coin` - Coin/asset symbol
    /// * `lifecycles` - Position lifecycles to insert
    /// * `snapshots` - Position snapshots to insert
    /// * `effects` - Fill effects to insert
    ///
    /// # Errors
    /// Returns an error if any database operation fails.
    pub async fn insert_derived_tables_atomic(
        &self,
        user: &Address,
        coin: &Coin,
        lifecycles: &[Lifecycle],
        snapshots: &[Snapshot],
        effects: &[Effect],
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // Insert lifecycles with explicit IDs from the tracker
        for lifecycle in lifecycles {
            sqlx::query(
                r#"
                INSERT OR REPLACE INTO position_lifecycles
                (id, user, coin, start_time_ms, end_time_ms, is_tainted, taint_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(lifecycle.id)
            .bind(lifecycle.user.as_str())
            .bind(lifecycle.coin.as_str())
            .bind(lifecycle.start_time_ms.as_i64())
            .bind(lifecycle.end_time_ms.map(|t| t.as_i64()))
            .bind(0) // is_tainted - will be updated after taint computation
            .bind::<Option<String>>(None) // taint_reason
            .execute(&mut *tx)
            .await?;
        }

        // Insert snapshots
        for snapshot in snapshots {
            sqlx::query(
                r#"
                INSERT OR REPLACE INTO position_snapshots
                (user, coin, time_ms, seq, net_size, avg_entry_px, lifecycle_id, is_tainted)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(user.as_str())
            .bind(coin.as_str())
            .bind(snapshot.time_ms.as_i64())
            .bind(snapshot.seq)
            .bind(snapshot.net_size.to_canonical_string())
            .bind(snapshot.avg_entry_px.to_canonical_string())
            .bind(snapshot.lifecycle_id)
            .bind(0) // is_tainted
            .execute(&mut *tx)
            .await?;
        }

        // Insert effects
        for effect in effects {
            let effect_type_str = match effect.effect_type {
                EffectType::Open => "open",
                EffectType::Close => "close",
            };

            sqlx::query(
                r#"
                INSERT OR REPLACE INTO fill_effects
                (fill_key, lifecycle_id, effect_type, qty, notional, fee, closed_pnl)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&effect.fill_key)
            .bind(effect.lifecycle_id)
            .bind(effect_type_str)
            .bind(effect.qty.to_canonical_string())
            .bind(effect.notional.to_canonical_string())
            .bind(effect.fee.to_canonical_string())
            .bind(effect.closed_pnl.to_canonical_string())
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
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
    async fn test_insert_and_query_deposits_time_range() {
        let (repo, _temp) = setup_test_db().await;

        let user = Address::new("0x123".to_string());
        let d1 = Deposit::new(
            user.clone(),
            TimeMs::new(1000),
            Decimal::from_str("10").unwrap(),
            Some("0xaaa".to_string()),
        );
        let d2 = Deposit::new(
            user.clone(),
            TimeMs::new(2000),
            Decimal::from_str("20").unwrap(),
            Some("0xbbb".to_string()),
        );
        repo.insert_deposits_batch(&[d1.clone(), d2.clone()])
            .await
            .unwrap();

        let results = repo.query_deposits(&user, 1500, 2500).await.unwrap();
        assert_eq!(results, vec![d2]);
    }

    #[tokio::test]
    async fn test_insert_duplicate_deposit_ignored() {
        let (repo, _temp) = setup_test_db().await;

        let user = Address::new("0x123".to_string());
        let deposit = Deposit::new(
            user,
            TimeMs::new(1000),
            Decimal::from_str("10").unwrap(),
            Some("0xaaa".to_string()),
        );

        let inserted1 = repo.insert_deposit(&deposit).await.unwrap();
        let inserted2 = repo.insert_deposit(&deposit).await.unwrap();

        assert!(inserted1);
        assert!(!inserted2);
    }
}
