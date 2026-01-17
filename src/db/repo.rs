//! Repository layer for database operations.

use crate::domain::{Address, Coin, Decimal, Fill, Side, TimeMs};
use crate::engine::{Effect, EffectType, Lifecycle, Snapshot};
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use std::str::FromStr;
use tracing::warn;

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

    /// Insert multiple fills in a single transaction for better performance.
    ///
    /// Returns the number of newly inserted fills (excludes duplicates).
    ///
    /// # Errors
    /// Returns an error if the transaction fails.
    pub async fn insert_fills_batch(&self, fills: &[Fill]) -> Result<usize, sqlx::Error> {
        if fills.is_empty() {
            return Ok(0);
        }

        let created_at = chrono::Utc::now().timestamp_millis();
        let mut total_inserted = 0usize;

        // Use a transaction for atomicity and better performance
        let mut tx = self.pool.begin().await?;

        for fill in fills {
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
            .bind(created_at)
            .execute(&mut *tx)
            .await?;

            if result.rows_affected() > 0 {
                total_inserted += 1;
            }
        }

        tx.commit().await?;
        Ok(total_inserted)
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

    /// Query fills after a watermark fill_key for incremental compilation.
    ///
    /// # Arguments
    /// * `user` - User address
    /// * `coin` - Coin/asset symbol
    /// * `after_fill_key` - Only return fills with fill_key > this value (None for all)
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn query_fills_after_watermark(
        &self,
        user: &Address,
        coin: &Coin,
        after_fill_key: Option<&str>,
    ) -> Result<Vec<Fill>, sqlx::Error> {
        let sql = if after_fill_key.is_some() {
            r#"
            SELECT user, coin, time_ms, side, px, sz, fee, closed_pnl,
                   builder_fee, tid, oid, fill_key
            FROM raw_fills
            WHERE user = ? AND coin = ? AND fill_key > ?
            ORDER BY fill_key ASC
            "#
        } else {
            r#"
            SELECT user, coin, time_ms, side, px, sz, fee, closed_pnl,
                   builder_fee, tid, oid, fill_key
            FROM raw_fills
            WHERE user = ? AND coin = ?
            ORDER BY fill_key ASC
            "#
        };

        let mut query = sqlx::query(sql).bind(user.as_str()).bind(coin.as_str());

        if let Some(key) = after_fill_key {
            query = query.bind(key);
        }

        let rows = query.fetch_all(&self.pool).await?;

        let fills = rows
            .iter()
            .map(|row| {
                let side_str: String = row.get("side");
                let side = match side_str.as_str() {
                    "buy" => Side::Buy,
                    "sell" => Side::Sell,
                    _ => Side::Buy,
                };

                let px_str: String = row.get("px");
                let sz_str: String = row.get("sz");
                let fee_str: String = row.get("fee");
                let closed_pnl_str: String = row.get("closed_pnl");
                let builder_fee_opt: Option<String> = row.get("builder_fee");
                let fill_key: String = row.get("fill_key");

                // Parse decimals with warning on failure
                let px = Decimal::from_str(&px_str).unwrap_or_else(|e| {
                    warn!(fill_key = %fill_key, px = %px_str, error = %e, "Failed to parse px decimal, using default");
                    Decimal::default()
                });
                let sz = Decimal::from_str(&sz_str).unwrap_or_else(|e| {
                    warn!(fill_key = %fill_key, sz = %sz_str, error = %e, "Failed to parse sz decimal, using default");
                    Decimal::default()
                });
                let fee = Decimal::from_str(&fee_str).unwrap_or_else(|e| {
                    warn!(fill_key = %fill_key, fee = %fee_str, error = %e, "Failed to parse fee decimal, using default");
                    Decimal::default()
                });
                let closed_pnl = Decimal::from_str(&closed_pnl_str).unwrap_or_else(|e| {
                    warn!(fill_key = %fill_key, closed_pnl = %closed_pnl_str, error = %e, "Failed to parse closed_pnl decimal, using default");
                    Decimal::default()
                });
                let builder_fee = builder_fee_opt.and_then(|s| {
                    Decimal::from_str(&s).map_err(|e| {
                        warn!(fill_key = %fill_key, builder_fee = %s, error = %e, "Failed to parse builder_fee decimal, ignoring");
                        e
                    }).ok()
                });

                let mut fill = Fill::new(
                    TimeMs::new(row.get("time_ms")),
                    Address::new(row.get("user")),
                    Coin::new(row.get("coin")),
                    side,
                    px,
                    sz,
                    fee,
                    closed_pnl,
                    builder_fee,
                    row.get("tid"),
                    row.get("oid"),
                );
                fill.fill_key = fill_key;
                fill
            })
            .collect();

        Ok(fills)
    }

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

    /// Insert position lifecycles into the database.
    ///
    /// # Errors
    /// Returns an error if the insert fails.
    pub async fn insert_lifecycles(&self, lifecycles: &[Lifecycle]) -> Result<(), sqlx::Error> {
        if lifecycles.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

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
            .bind(0) // is_tainted - will be updated in Phase 4
            .bind::<Option<String>>(None) // taint_reason
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Insert position snapshots into the database.
    ///
    /// # Arguments
    /// * `user` - User address
    /// * `coin` - Coin/asset symbol
    /// * `snapshots` - Snapshots to insert
    ///
    /// # Errors
    /// Returns an error if the insert fails.
    pub async fn insert_snapshots(
        &self,
        user: &Address,
        coin: &Coin,
        snapshots: &[Snapshot],
    ) -> Result<(), sqlx::Error> {
        if snapshots.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

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

        tx.commit().await?;
        Ok(())
    }

    /// Insert fill effects into the database.
    ///
    /// # Errors
    /// Returns an error if the insert fails.
    pub async fn insert_effects(&self, effects: &[Effect]) -> Result<(), sqlx::Error> {
        if effects.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

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

    /// Query position lifecycles for a user and coin.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn query_lifecycles(
        &self,
        user: &Address,
        coin: &Coin,
    ) -> Result<Vec<(i64, Address, Coin, i64, Option<i64>)>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT id, user, coin, start_time_ms, end_time_ms
            FROM position_lifecycles
            WHERE user = ? AND coin = ?
            ORDER BY start_time_ms ASC
            "#,
        )
        .bind(user.as_str())
        .bind(coin.as_str())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                (
                    row.get::<i64, _>("id"),
                    Address::new(row.get::<String, _>("user")),
                    Coin::new(row.get::<String, _>("coin")),
                    row.get::<i64, _>("start_time_ms"),
                    row.get::<Option<i64>, _>("end_time_ms"),
                )
            })
            .collect())
    }

    /// Query position snapshots for a user and coin.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn query_snapshots(
        &self,
        user: &Address,
        coin: &Coin,
    ) -> Result<Vec<(i64, i64, i64, i32, String, String)>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT id, time_ms, lifecycle_id, seq, net_size, avg_entry_px
            FROM position_snapshots
            WHERE user = ? AND coin = ?
            ORDER BY time_ms ASC, seq ASC
            "#,
        )
        .bind(user.as_str())
        .bind(coin.as_str())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                (
                    row.get::<i64, _>("id"),
                    row.get::<i64, _>("time_ms"),
                    row.get::<i64, _>("lifecycle_id"),
                    row.get::<i32, _>("seq"),
                    row.get::<String, _>("net_size"),
                    row.get::<String, _>("avg_entry_px"),
                )
            })
            .collect())
    }

    /// Update taint flags for lifecycles.
    ///
    /// # Arguments
    /// * `taint_updates` - Vec of (lifecycle_id, is_tainted, taint_reason)
    ///
    /// # Errors
    /// Returns an error if the update fails.
    pub async fn update_lifecycle_taints(
        &self,
        taint_updates: &[(i64, bool, Option<String>)],
    ) -> Result<(), sqlx::Error> {
        if taint_updates.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for (lifecycle_id, is_tainted, taint_reason) in taint_updates {
            sqlx::query(
                r#"
                UPDATE position_lifecycles
                SET is_tainted = ?, taint_reason = ?
                WHERE id = ?
                "#,
            )
            .bind(if *is_tainted { 1 } else { 0 })
            .bind(taint_reason)
            .bind(lifecycle_id)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Insert or update fill attributions.
    ///
    /// # Arguments
    /// * `attributions` - Vec of (fill_key, attributed, mode, confidence, builder)
    ///
    /// # Errors
    /// Returns an error if the insert fails.
    pub async fn insert_attributions(
        &self,
        attributions: &[(String, bool, String, String, Option<String>)],
    ) -> Result<(), sqlx::Error> {
        if attributions.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for (fill_key, attributed, mode, confidence, builder) in attributions {
            sqlx::query(
                r#"
                INSERT OR REPLACE INTO fill_attributions
                (fill_key, attributed, mode, confidence, builder)
                VALUES (?, ?, ?, ?, ?)
                "#,
            )
            .bind(fill_key)
            .bind(if *attributed { 1 } else { 0 })
            .bind(mode)
            .bind(confidence)
            .bind(builder)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Query attributions for a list of fill keys.
    ///
    /// # Arguments
    /// * `fill_keys` - Fill keys to query attributions for
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn query_attributions(
        &self,
        fill_keys: &[String],
    ) -> Result<Vec<(String, bool, String, String, Option<String>)>, sqlx::Error> {
        if fill_keys.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = vec!["?"; fill_keys.len()].join(",");
        let sql = format!(
            r#"
            SELECT fill_key, attributed, mode, confidence, builder
            FROM fill_attributions
            WHERE fill_key IN ({})
            "#,
            placeholders
        );

        let mut query = sqlx::query(&sql);
        for key in fill_keys {
            query = query.bind(key);
        }

        let rows = query.fetch_all(&self.pool).await?;

        Ok(rows
            .iter()
            .map(|row| {
                (
                    row.get::<String, _>("fill_key"),
                    row.get::<i32, _>("attributed") != 0,
                    row.get::<String, _>("mode"),
                    row.get::<String, _>("confidence"),
                    row.get::<Option<String>, _>("builder"),
                )
            })
            .collect())
    }

    /// Query fill effects for a user and coin.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn query_effects(
        &self,
        user: &Address,
        coin: &Coin,
    ) -> Result<Vec<(i64, String, i64, String, String, String, String, String)>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT fe.id, fe.fill_key, fe.lifecycle_id, fe.effect_type, fe.qty, fe.notional, fe.fee, fe.closed_pnl
            FROM fill_effects fe
            JOIN position_lifecycles pl ON fe.lifecycle_id = pl.id
            WHERE pl.user = ? AND pl.coin = ?
            ORDER BY fe.id ASC
            "#,
        )
        .bind(user.as_str())
        .bind(coin.as_str())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                (
                    row.get::<i64, _>("id"),
                    row.get::<String, _>("fill_key"),
                    row.get::<i64, _>("lifecycle_id"),
                    row.get::<String, _>("effect_type"),
                    row.get::<String, _>("qty"),
                    row.get::<String, _>("notional"),
                    row.get::<String, _>("fee"),
                    row.get::<String, _>("closed_pnl"),
                )
            })
            .collect())
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

    #[tokio::test]
    async fn test_insert_fills_batch() {
        let (repo, _temp) = setup_test_db().await;

        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());

        let fills: Vec<Fill> = (1..=5)
            .map(|i| {
                Fill::new(
                    TimeMs::new(i * 1000),
                    user.clone(),
                    coin.clone(),
                    Side::Buy,
                    Decimal::from_str("50000").unwrap(),
                    Decimal::from_str("1").unwrap(),
                    Decimal::from_str("10").unwrap(),
                    Decimal::from_str("0").unwrap(),
                    None,
                    Some(i),
                    None,
                )
            })
            .collect();

        let inserted = repo.insert_fills_batch(&fills).await.unwrap();
        assert_eq!(inserted, 5, "Should insert all 5 fills");

        let stored = repo.query_fills(&user, None, None, None).await.unwrap();
        assert_eq!(stored.len(), 5);
    }

    #[tokio::test]
    async fn test_insert_fills_batch_idempotent() {
        let (repo, _temp) = setup_test_db().await;

        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());

        let fills: Vec<Fill> = (1..=3)
            .map(|i| {
                Fill::new(
                    TimeMs::new(i * 1000),
                    user.clone(),
                    coin.clone(),
                    Side::Buy,
                    Decimal::from_str("50000").unwrap(),
                    Decimal::from_str("1").unwrap(),
                    Decimal::from_str("10").unwrap(),
                    Decimal::from_str("0").unwrap(),
                    None,
                    Some(i),
                    None,
                )
            })
            .collect();

        // First batch insert
        let inserted1 = repo.insert_fills_batch(&fills).await.unwrap();
        assert_eq!(inserted1, 3);

        // Second batch insert with same fills - should insert 0
        let inserted2 = repo.insert_fills_batch(&fills).await.unwrap();
        assert_eq!(inserted2, 0, "Second batch should insert nothing");

        // Verify only 3 fills in DB
        let stored = repo.query_fills(&user, None, None, None).await.unwrap();
        assert_eq!(stored.len(), 3);
    }

    #[tokio::test]
    async fn test_insert_fills_batch_partial_duplicates() {
        let (repo, _temp) = setup_test_db().await;

        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());

        // Insert first 2 fills
        let fills1: Vec<Fill> = (1..=2)
            .map(|i| {
                Fill::new(
                    TimeMs::new(i * 1000),
                    user.clone(),
                    coin.clone(),
                    Side::Buy,
                    Decimal::from_str("50000").unwrap(),
                    Decimal::from_str("1").unwrap(),
                    Decimal::from_str("10").unwrap(),
                    Decimal::from_str("0").unwrap(),
                    None,
                    Some(i),
                    None,
                )
            })
            .collect();
        repo.insert_fills_batch(&fills1).await.unwrap();

        // Insert fills 1-4 (2 duplicates, 2 new)
        let fills2: Vec<Fill> = (1..=4)
            .map(|i| {
                Fill::new(
                    TimeMs::new(i * 1000),
                    user.clone(),
                    coin.clone(),
                    Side::Buy,
                    Decimal::from_str("50000").unwrap(),
                    Decimal::from_str("1").unwrap(),
                    Decimal::from_str("10").unwrap(),
                    Decimal::from_str("0").unwrap(),
                    None,
                    Some(i),
                    None,
                )
            })
            .collect();
        let inserted = repo.insert_fills_batch(&fills2).await.unwrap();
        assert_eq!(inserted, 2, "Should only insert 2 new fills");

        let stored = repo.query_fills(&user, None, None, None).await.unwrap();
        assert_eq!(stored.len(), 4);
    }

    #[tokio::test]
    async fn test_insert_fills_batch_empty() {
        let (repo, _temp) = setup_test_db().await;

        let inserted = repo.insert_fills_batch(&[]).await.unwrap();
        assert_eq!(inserted, 0);
    }
}
