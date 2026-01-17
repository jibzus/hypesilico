pub mod health;
pub mod positions;

use crate::compile::Compiler;
use crate::domain::{Address, Coin, TimeMs};
use crate::error::AppError;
use crate::orchestration::ensure::Ingestor;
use crate::Repository;
use axum::{routing::get, Router};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub repo: Arc<Repository>,
    pub ingestor: Arc<Ingestor>,
}

impl AppState {
    pub async fn ensure_compiled(
        &self,
        user: &Address,
        coin: Option<&Coin>,
        from_ms: Option<TimeMs>,
        to_ms: Option<TimeMs>,
    ) -> Result<(), AppError> {
        self.ingestor
            .ensure_ingested(user, coin, from_ms, to_ms)
            .await
            .map_err(|e| AppError::Internal(format!("Ingestion failed: {}", e)))?;

        if let Some(coin) = coin {
            Compiler::compile_incremental(&self.repo, user, coin)
                .await
                .map_err(|e| AppError::Internal(format!("Compile failed: {}", e)))?;
            return Ok(());
        }

        let coins = self
            .repo
            .query_distinct_coins(user, from_ms, to_ms)
            .await
            .map_err(|e| AppError::Internal(format!("Coin query failed: {}", e)))?;

        for coin in coins {
            Compiler::compile_incremental(&self.repo, user, &coin)
                .await
                .map_err(|e| AppError::Internal(format!("Compile failed: {}", e)))?;
        }

        Ok(())
    }
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health::health))
        .route("/ready", get(health::ready))
        .route(
            "/v1/positions/history",
            get(positions::get_positions_history),
        )
        .with_state(state)
}
