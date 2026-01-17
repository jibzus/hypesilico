pub mod health;
pub mod trades;

use axum::{routing::get, Router};
use std::sync::Arc;

use crate::config::Config;
use crate::datasource::{DataSource, HyperliquidDataSource};
use crate::db::init_db;
use crate::db::Repository;
use crate::orchestration::ensure::Ingestor;
use crate::AppError;

#[derive(Clone)]
pub struct AppState {
    pub repo: Arc<Repository>,
    pub ingestor: Arc<Ingestor>,
}

pub async fn create_state(config: Config) -> Result<AppState, AppError> {
    let pool = init_db(&config.database_path).await?;
    let repo = Arc::new(Repository::new(pool));

    let datasource: Arc<dyn DataSource> =
        Arc::new(HyperliquidDataSource::new(config.hyperliquid_api_url.clone()));
    let ingestor = Arc::new(Ingestor::new(datasource, repo.clone(), config));

    Ok(AppState { repo, ingestor })
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health::health))
        .route("/ready", get(health::ready))
        .route("/v1/trades", get(trades::get_trades))
        .with_state(state)
}
