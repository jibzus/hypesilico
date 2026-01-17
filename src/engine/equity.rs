use crate::db::Repository;
use crate::domain::{Address, Decimal, TimeMs};
use std::sync::Arc;

/// Resolves account equity at a timestamp using best-effort cached snapshots.
#[derive(Clone)]
pub struct EquityResolver {
    repo: Arc<Repository>,
}

impl EquityResolver {
    pub fn new(repo: Arc<Repository>) -> Self {
        Self { repo }
    }

    pub async fn resolve_equity(&self, user: &Address, at_ms: TimeMs) -> Result<Decimal, sqlx::Error> {
        if let Some((_t, equity)) = self.repo.get_equity_snapshot_at_or_before(user, at_ms).await? {
            return Ok(equity);
        }

        let deposits_sum = self.repo.sum_deposits_up_to(user, at_ms).await?;
        let realized_pnl_before = self.repo.sum_realized_pnl_before(user, at_ms).await?;
        let derived_equity = deposits_sum + realized_pnl_before;

        self.repo
            .upsert_equity_snapshot(user, at_ms, derived_equity)
            .await?;

        Ok(derived_equity)
    }
}

