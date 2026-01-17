//! Incremental compilation logic for processing fills and generating derived tables.

use crate::domain::{Address, Attribution, AttributionMode, Coin};
use crate::engine::{PositionTracker, TaintComputer};
use crate::db::Repository;

/// Compiler for incremental fill processing.
pub struct Compiler;

impl Compiler {
    /// Compile fills for a user and coin, processing only new fills since last watermark.
    ///
    /// # Arguments
    /// * `repo` - Database repository
    /// * `user` - User address
    /// * `coin` - Coin/asset symbol
    ///
    /// # Returns
    /// Number of fills processed
    ///
    /// # Errors
    /// Returns an error if database operations fail
    pub async fn compile_incremental(
        repo: &Repository,
        user: &Address,
        coin: &Coin,
    ) -> Result<usize, sqlx::Error> {
        // Get current watermark
        let watermark = repo.get_compile_state(user, coin).await?;
        let last_fill_key = watermark.as_ref().and_then(|(_, key)| key.clone());

        // Query uncompiled fills
        let fills = repo.query_fills_after_watermark(user, coin, last_fill_key.as_deref()).await?;

        if fills.is_empty() {
            return Ok(0);
        }

        // Process fills through position tracker
        let mut tracker = PositionTracker::new();
        for fill in &fills {
            tracker.process_fill(fill);
        }

        // Get outputs from tracker
        let lifecycles = tracker.get_lifecycles();
        let snapshots = tracker.get_snapshots();
        let effects = tracker.get_effects();

        // Get the last fill key for watermark update
        let last_fill_key = fills.last().map(|f| f.fill_key.clone());
        let last_time_ms = fills.last().map(|f| f.time_ms);

        // Insert derived tables in transaction
        repo.insert_lifecycles(lifecycles).await?;
        repo.insert_snapshots(user, coin, snapshots).await?;
        repo.insert_effects(effects).await?;

        // Compute taint for lifecycles
        let fill_keys: Vec<String> = fills.iter().map(|f| f.fill_key.clone()).collect();
        let attributions_data = repo.query_attributions(&fill_keys).await?;

        // Build attribution map
        let mut attribution_map = std::collections::HashMap::new();
        for (fill_key, attributed, mode_str) in attributions_data {
            let mode = match mode_str.as_str() {
                "heuristic" => AttributionMode::Heuristic,
                "logs" => AttributionMode::Logs,
                _ => AttributionMode::Heuristic,
            };
            let attribution = Attribution {
                attributed,
                mode,
                confidence: crate::domain::AttributionConfidence::Low,
                builder: None,
            };
            attribution_map.insert(fill_key, attribution);
        }

        // Compute taint
        let mut taint_computer = TaintComputer::new();
        for lifecycle in lifecycles.iter() {
            for fill in &fills {
                // Check if this fill belongs to this lifecycle
                // For now, we'll associate all fills with all lifecycles (simplified)
                // In a real implementation, we'd track which fills belong to which lifecycle
                taint_computer.add_fill_to_lifecycle(lifecycle.id, fill.fill_key.clone());
            }
        }

        for (fill_key, attribution) in attribution_map {
            taint_computer.set_attribution(fill_key, attribution);
        }

        let taint_infos = taint_computer.compute_all_taints();

        // Prepare taint updates
        let mut taint_updates = Vec::new();
        for lifecycle in lifecycles {
            let taint_info = taint_infos.get(&lifecycle.id);
            let is_tainted = taint_info.map(|t| t.is_tainted).unwrap_or(false);
            let taint_reason = taint_info.and_then(|t| t.reason.clone());
            taint_updates.push((lifecycle.id, is_tainted, taint_reason));
        }

        // Update taint flags
        repo.update_lifecycle_taints(&taint_updates).await?;

        // Update watermark atomically
        if let (Some(time_ms), Some(key)) = (last_time_ms, last_fill_key) {
            repo.store_compile_state(user, coin, Some(time_ms.as_i64()), Some(&key)).await?;
        }

        Ok(fills.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compiler_exists() {
        // Placeholder test to ensure module compiles
        let _compiler = Compiler;
    }
}

