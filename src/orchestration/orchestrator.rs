use crate::compile::Compiler;
use crate::db::Repository;
use crate::domain::{Address, Coin, TimeMs};
use crate::orchestration::ensure::{Ingestor, IngestionError};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone)]
pub struct Orchestrator {
    ingestor: Ingestor,
    repo: Arc<Repository>,
}

impl Orchestrator {
    pub fn new(ingestor: Ingestor, repo: Arc<Repository>) -> Self {
        Self { ingestor, repo }
    }

    /// Ensure fills are ingested and compiled for the given query window.
    pub async fn ensure_compiled(
        &self,
        user: &Address,
        coin: Option<&Coin>,
        from_ms: Option<TimeMs>,
        to_ms: Option<TimeMs>,
    ) -> Result<(), OrchestrationError> {
        self.ingestor
            .ensure_ingested(user, coin, from_ms, to_ms)
            .await?;

        let coins_to_compile = match coin {
            Some(c) => vec![c.clone()],
            None => self.repo.query_distinct_coins(user, from_ms, to_ms).await?,
        };

        for coin in coins_to_compile {
            Compiler::compile_incremental(&self.repo, user, &coin).await?;
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum OrchestrationError {
    #[error(transparent)]
    Ingestion(#[from] IngestionError),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

