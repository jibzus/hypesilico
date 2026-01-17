use axum::http::StatusCode;
use hypesilico::api::{self, AppState};
use hypesilico::config::{BuilderAttributionMode, Config, PnlMode};
use hypesilico::datasource::MockDataSource;
use hypesilico::db::init_db;
use hypesilico::engine::EquityResolver;
use hypesilico::orchestration::ensure::Ingestor;
use hypesilico::orchestration::orchestrator::Orchestrator;
use hypesilico::Repository;
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;

async fn setup_test_app() -> (axum::Router, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("test.db")
        .to_string_lossy()
        .to_string();

    let pool = init_db(&db_path).await.expect("init_db failed");
    let repo = Arc::new(Repository::new(pool));
    let datasource = Arc::new(MockDataSource::new());

    let config = Config {
        port: 0,
        database_path: db_path,
        hyperliquid_api_url: "http://example.invalid".to_string(),
        target_builder: "0x0000000000000000000000000000000000000000".to_string(),
        builder_attribution_mode: BuilderAttributionMode::Auto,
        pnl_mode: PnlMode::Gross,
        lookback_ms: 0,
        leaderboard_users: vec![],
    };

    let ingestor = Ingestor::new(datasource, repo.clone(), config.clone());
    let orchestrator = Arc::new(Orchestrator::new(ingestor, repo.clone()));
    let equity_resolver = Arc::new(EquityResolver::new(repo.clone()));
    let state = AppState::new(repo, config, orchestrator, equity_resolver);

    (api::create_router(state), temp_dir)
}

#[tokio::test]
async fn test_health_endpoint() {
    let (app, _temp) = setup_test_app().await;

    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/health")
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    let body_str = String::from_utf8(body.to_vec()).unwrap();
    assert!(body_str.contains("ok"));
}

#[tokio::test]
async fn test_ready_endpoint() {
    let (app, _temp) = setup_test_app().await;

    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/ready")
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    let body_str = String::from_utf8(body.to_vec()).unwrap();
    assert!(body_str.contains("ready"));
}
