pub mod health;
pub mod pnl;
pub mod positions;
pub mod trades;

use crate::config::Config;
use crate::db::Repository;
use crate::engine::EquityResolver;
use crate::orchestration::orchestrator::Orchestrator;
use axum::{routing::get, Router};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub repo: Arc<Repository>,
    pub config: Config,
    pub orchestrator: Arc<Orchestrator>,
    pub equity_resolver: Arc<EquityResolver>,
}

impl AppState {
    pub fn new(
        repo: Arc<Repository>,
        config: Config,
        orchestrator: Arc<Orchestrator>,
        equity_resolver: Arc<EquityResolver>,
    ) -> Self {
        Self {
            repo,
            config,
            orchestrator,
            equity_resolver,
        }
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
        .route("/v1/trades", get(trades::get_trades))
        .route("/v1/pnl", get(pnl::get_pnl))
        .with_state(state)
}
