pub mod deposits;
pub mod health;
pub mod leaderboard;
pub mod pnl;
pub mod positions;
pub mod risk;
pub mod trades;

use crate::config::Config;
use crate::db::Repository;
use crate::engine::EquityResolver;
use crate::orchestration::orchestrator::Orchestrator;
use axum::{routing::get, Router};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

#[derive(Clone)]
pub struct AppState {
    pub repo: Arc<Repository>,
    pub config: Config,
    pub orchestrator: Arc<Orchestrator>,
    pub equity_resolver: Arc<EquityResolver>,
    pub http_client: reqwest::Client,
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
            http_client: reqwest::Client::new(),
        }
    }
}

pub fn create_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/health", get(health::health))
        .route("/ready", get(health::ready))
        .route(
            "/v1/positions/history",
            get(positions::get_positions_history),
        )
        .route("/v1/trades", get(trades::get_trades))
        .route("/v1/pnl", get(pnl::get_pnl))
        .route("/v1/deposits", get(deposits::get_deposits))
        .route("/v1/leaderboard", get(leaderboard::get_leaderboard))
        .route("/v1/risk", get(risk::get_risk))
        .layer(cors)
        .with_state(state)
}
