//! Position lifecycle, snapshot, and attribution operations for the repository.

use crate::domain::{Address, Attribution, AttributionConfidence, AttributionMode, Coin, TimeMs};
use crate::engine::{Effect, EffectType, Lifecycle, Snapshot};
use sqlx::Row;
use std::collections::{HashMap, HashSet};

use super::{PositionSnapshotRow, Repository};

impl Repository {
    /// Query position snapshots for a user with optional coin and time window.
    ///
    /// Joins lifecycles to expose lifecycle-level taint flags for builder-only filtering.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn query_position_snapshots(
        &self,
        user: &Address,
        coin: Option<&Coin>,
        from_ms: Option<TimeMs>,
        to_ms: Option<TimeMs>,
    ) -> Result<Vec<PositionSnapshotRow>, sqlx::Error> {
        let from_ms = from_ms.unwrap_or(TimeMs::new(0)).as_ms();
        let to_ms = to_ms.unwrap_or(TimeMs::new(i64::MAX)).as_ms();

        let (sql, binds_coin) = if coin.is_some() {
            (
                r#"
                SELECT ps.time_ms, ps.seq, ps.coin, ps.net_size, ps.avg_entry_px, ps.lifecycle_id, pl.is_tainted
                FROM position_snapshots ps
                JOIN position_lifecycles pl ON ps.lifecycle_id = pl.id
                WHERE ps.user = ? AND ps.coin = ? AND ps.time_ms >= ? AND ps.time_ms <= ?
                ORDER BY ps.time_ms ASC, ps.seq ASC, ps.coin ASC, ps.lifecycle_id ASC
                "#,
                true,
            )
        } else {
            (
                r#"
                SELECT ps.time_ms, ps.seq, ps.coin, ps.net_size, ps.avg_entry_px, ps.lifecycle_id, pl.is_tainted
                FROM position_snapshots ps
                JOIN position_lifecycles pl ON ps.lifecycle_id = pl.id
                WHERE ps.user = ? AND ps.time_ms >= ? AND ps.time_ms <= ?
                ORDER BY ps.time_ms ASC, ps.seq ASC, ps.coin ASC, ps.lifecycle_id ASC
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
        Ok(rows
            .into_iter()
            .map(|row| PositionSnapshotRow {
                time_ms: TimeMs::new(row.get::<i64, _>("time_ms")),
                seq: row.get::<i32, _>("seq"),
                coin: Coin::new(row.get::<String, _>("coin")),
                net_size: row.get::<String, _>("net_size"),
                avg_entry_px: row.get::<String, _>("avg_entry_px"),
                lifecycle_id: row.get::<i64, _>("lifecycle_id"),
                lifecycle_tainted: row.get::<i32, _>("is_tainted") != 0,
            })
            .collect())
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

    /// Return the set of tainted lifecycle IDs from a provided list.
    ///
    /// Uses chunked queries to avoid SQLite's 999 parameter limit.
    /// Results are deduplicated and returned in sorted order for stability.
    pub async fn query_tainted_lifecycle_ids(
        &self,
        lifecycle_ids: &[i64],
    ) -> Result<Vec<i64>, sqlx::Error> {
        if lifecycle_ids.is_empty() {
            return Ok(Vec::new());
        }

        // SQLite has a 999 parameter limit; chunk to 500 for safety margin.
        const CHUNK_SIZE: usize = 500;
        let mut out: HashSet<i64> = HashSet::with_capacity(lifecycle_ids.len());

        for chunk in lifecycle_ids.chunks(CHUNK_SIZE) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let sql = format!(
                r#"
                SELECT id
                FROM position_lifecycles
                WHERE id IN ({}) AND is_tainted = 1
                "#,
                placeholders
            );

            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(id);
            }

            let rows = query.fetch_all(&self.pool).await?;
            out.extend(rows.iter().map(|row| row.get::<i64, _>("id")));
        }

        // Convert to sorted Vec for stable ordering
        let mut result: Vec<i64> = out.into_iter().collect();
        result.sort_unstable();
        Ok(result)
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
    /// Uses chunked queries to avoid SQLite's 999 parameter limit.
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

        // SQLite has a 999 parameter limit; chunk to 500 for safety margin.
        const CHUNK_SIZE: usize = 500;
        let mut out = Vec::with_capacity(fill_keys.len());

        for chunk in fill_keys.chunks(CHUNK_SIZE) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let sql = format!(
                r#"
                SELECT fill_key, attributed, mode, confidence, builder
                FROM fill_attributions
                WHERE fill_key IN ({})
                "#,
                placeholders
            );

            let mut query = sqlx::query(&sql);
            for key in chunk {
                query = query.bind(key);
            }

            let rows = query.fetch_all(&self.pool).await?;

            out.extend(rows.iter().map(|row| {
                (
                    row.get::<String, _>("fill_key"),
                    row.get::<i32, _>("attributed") != 0,
                    row.get::<String, _>("mode"),
                    row.get::<String, _>("confidence"),
                    row.get::<Option<String>, _>("builder"),
                )
            }));
        }

        Ok(out)
    }

    /// Query full attribution records for a list of fill keys.
    ///
    /// Missing attributions are omitted from the returned map.
    ///
    /// Uses chunked queries to avoid SQLite's 999 parameter limit.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn query_attributions_full(
        &self,
        fill_keys: &[String],
    ) -> Result<HashMap<String, Attribution>, sqlx::Error> {
        if fill_keys.is_empty() {
            return Ok(HashMap::new());
        }

        // SQLite has a 999 parameter limit; chunk to 500 for safety margin.
        const CHUNK_SIZE: usize = 500;
        let mut out = HashMap::with_capacity(fill_keys.len());

        for chunk in fill_keys.chunks(CHUNK_SIZE) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let sql = format!(
                r#"
                SELECT fill_key, attributed, mode, confidence, builder
                FROM fill_attributions
                WHERE fill_key IN ({})
                "#,
                placeholders
            );

            let mut query = sqlx::query(&sql);
            for key in chunk {
                query = query.bind(key);
            }

            let rows = query.fetch_all(&self.pool).await?;

            for row in rows {
                let fill_key = row.get::<String, _>("fill_key");
                let attributed = row.get::<i32, _>("attributed") != 0;
                let mode_str = row.get::<String, _>("mode");
                let confidence_str = row.get::<String, _>("confidence");
                let builder_opt = row.get::<Option<String>, _>("builder");

                let mode = match mode_str.as_str() {
                    "heuristic" => AttributionMode::Heuristic,
                    "logs" => AttributionMode::Logs,
                    _ => AttributionMode::Heuristic,
                };
                let confidence = match confidence_str.as_str() {
                    "exact" => AttributionConfidence::Exact,
                    "fuzzy" => AttributionConfidence::Fuzzy,
                    "low" => AttributionConfidence::Low,
                    _ => AttributionConfidence::Low,
                };
                let builder = builder_opt.map(Address::new);

                out.insert(
                    fill_key,
                    Attribution {
                        attributed,
                        mode,
                        confidence,
                        builder,
                    },
                );
            }
        }

        Ok(out)
    }

    /// Upsert full attribution records (including optional builder address).
    ///
    /// # Errors
    /// Returns an error if the insert fails.
    pub async fn upsert_attributions_full(
        &self,
        attributions: &[(String, Attribution)],
    ) -> Result<(), sqlx::Error> {
        if attributions.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for (fill_key, attribution) in attributions {
            let mode = match attribution.mode {
                AttributionMode::Heuristic => "heuristic",
                AttributionMode::Logs => "logs",
            };
            let confidence = match attribution.confidence {
                AttributionConfidence::Exact => "exact",
                AttributionConfidence::Fuzzy => "fuzzy",
                AttributionConfidence::Low => "low",
            };

            sqlx::query(
                r#"
                INSERT OR REPLACE INTO fill_attributions
                (fill_key, attributed, mode, confidence, builder)
                VALUES (?, ?, ?, ?, ?)
                "#,
            )
            .bind(fill_key)
            .bind(if attribution.attributed { 1 } else { 0 })
            .bind(mode)
            .bind(confidence)
            .bind(attribution.builder.as_ref().map(|b| b.as_str()))
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
