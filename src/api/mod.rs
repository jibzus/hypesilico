pub mod health;

use axum::{routing::get, Router};

pub fn create_router() -> Router {
    Router::new()
        .route("/health", get(health::health))
        .route("/ready", get(health::ready))
}
