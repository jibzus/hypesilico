use axum::http::{Request, StatusCode};
use hypesilico::datasource::MockDataSource;
use hypesilico::orchestration::ensure::Ingestor;
use hypesilico::{
    api,
    config::{BuilderAttributionMode, Config, PnlMode},
    db::init_db,
    domain::{Address, Coin, Decimal, Fill, Side, TimeMs},
    DataSource, Repository,
};
use std::str::FromStr;
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;

const USER: &str = "0x0000000000000000000000000000000000000123";

struct TestApp {
    app: axum::Router,
    repo: Arc<Repository>,
    _temp: TempDir,
}

async fn setup_test_app() -> TestApp {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("test.db")
        .to_string_lossy()
        .to_string();
    let pool = init_db(&db_path).await.expect("init_db failed");

    let repo = Arc::new(Repository::new(pool));
    let datasource: Arc<dyn DataSource> = Arc::new(MockDataSource::new());
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
    let ingestor = Arc::new(Ingestor::new(datasource, repo.clone(), config));
    let app = api::create_router(api::AppState { repo: repo.clone(), ingestor });

    TestApp {
        app,
        repo,
        _temp: temp_dir,
    }
}

fn fill(time_ms: i64, coin: &str, side: Side, sz: &str, px: &str, tid: i64) -> Fill {
    Fill::new(
        TimeMs::new(time_ms),
        Address::new(USER.to_string()),
        Coin::new(coin.to_string()),
        side,
        Decimal::from_str(px).unwrap(),
        Decimal::from_str(sz).unwrap(),
        Decimal::from_str("0").unwrap(),
        Decimal::from_str("0").unwrap(),
        None,
        Some(tid),
        None,
    )
}

#[tokio::test]
async fn test_positions_history_response_has_required_fields() {
    let TestApp { app, repo, _temp } = setup_test_app().await;

    let f = fill(1705000000000, "BTC", Side::Buy, "0.5", "50000", 1);
    repo.insert_fill(&f).await.unwrap();

    let request = Request::builder()
        .method("GET")
        .uri(format!("/v1/positions/history?user={}", USER))
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(body.get("tainted").is_none(), "tainted must be omitted");
    assert!(body["snapshots"].is_array());

    let snapshot = &body["snapshots"][0];
    assert!(snapshot["timeMs"].as_i64().is_some());
    assert!(snapshot["coin"].as_str().is_some());
    assert!(snapshot["netSize"].as_str().is_some());
    assert!(snapshot["avgEntryPx"].as_str().is_some());
    assert!(snapshot["lifecycleId"].as_str().is_some());
    assert!(snapshot.get("tainted").is_none(), "snapshot.tainted must be omitted");
}

#[tokio::test]
async fn test_flip_produces_two_snapshots_in_correct_order() {
    let TestApp { app, repo, _temp } = setup_test_app().await;

    // Fill 1: Buy 1 BTC (opens long)
    repo.insert_fill(&fill(1000, "BTC", Side::Buy, "1", "50000", 1))
        .await
        .unwrap();
    // Fill 2: Sell 2 BTC (closes long, opens short)
    repo.insert_fill(&fill(2000, "BTC", Side::Sell, "2", "51000", 2))
        .await
        .unwrap();

    let request = Request::builder()
        .method("GET")
        .uri(format!("/v1/positions/history?user={}&coin=BTC", USER))
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let snapshots = body["snapshots"].as_array().unwrap();

    assert_eq!(snapshots.len(), 3);

    let flip_snapshots: Vec<_> = snapshots
        .iter()
        .filter(|s| s["timeMs"].as_i64() == Some(2000))
        .collect();
    assert_eq!(flip_snapshots.len(), 2);
    assert_eq!(flip_snapshots[0]["netSize"], "0"); // Close (seq=0)
    assert_eq!(flip_snapshots[1]["netSize"], "-1"); // Open short (seq=1)
}

#[tokio::test]
async fn test_positions_history_filters_by_coin_and_time() {
    let TestApp { app, repo, _temp } = setup_test_app().await;

    repo.insert_fill(&fill(1000, "BTC", Side::Buy, "1", "50000", 1))
        .await
        .unwrap();
    repo.insert_fill(&fill(2000, "ETH", Side::Buy, "10", "3000", 2))
        .await
        .unwrap();
    repo.insert_fill(&fill(3000, "BTC", Side::Sell, "1", "51000", 3))
        .await
        .unwrap();

    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/v1/positions/history?user={}&coin=BTC&fromMs=2500&toMs=4000",
            USER
        ))
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let snapshots = body["snapshots"].as_array().unwrap();

    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0]["coin"], "BTC");
    assert_eq!(snapshots[0]["timeMs"], 3000);
}

#[tokio::test]
async fn test_positions_history_response_deterministic() {
    let TestApp { app, repo, _temp } = setup_test_app().await;

    repo.insert_fill(&fill(1000, "BTC", Side::Buy, "1", "50000", 1))
        .await
        .unwrap();
    repo.insert_fill(&fill(2000, "BTC", Side::Sell, "1", "51000", 2))
        .await
        .unwrap();

    let request1 = Request::builder()
        .method("GET")
        .uri(format!("/v1/positions/history?user={}&coin=BTC", USER))
        .body(axum::body::Body::empty())
        .unwrap();
    let response1 = app
        .clone()
        .oneshot(request1)
        .await
        .unwrap();
    let bytes1 = axum::body::to_bytes(response1.into_body(), usize::MAX)
        .await
        .unwrap();

    let request2 = Request::builder()
        .method("GET")
        .uri(format!("/v1/positions/history?user={}&coin=BTC", USER))
        .body(axum::body::Body::empty())
        .unwrap();
    let response2 = app.oneshot(request2).await.unwrap();
    let bytes2 = axum::body::to_bytes(response2.into_body(), usize::MAX)
        .await
        .unwrap();

    assert_eq!(bytes1, bytes2, "Responses must be byte-identical");
}

#[tokio::test]
async fn test_builder_only_excludes_tainted_lifecycles() {
    let TestApp { app, repo, _temp } = setup_test_app().await;

    let f1 = fill(1000, "BTC", Side::Buy, "1", "50000", 1);
    let f2 = fill(2000, "BTC", Side::Sell, "1", "51000", 2);
    repo.insert_fill(&f1).await.unwrap();
    repo.insert_fill(&f2).await.unwrap();

    // Mixed attribution taints the lifecycle.
    repo.insert_attributions(&[
        (f1.fill_key.clone(), true, "heuristic".to_string(), "low".to_string()),
        (f2.fill_key.clone(), false, "heuristic".to_string(), "low".to_string()),
    ])
    .await
    .unwrap();

    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/v1/positions/history?user={}&builderOnly=true&coin=BTC",
            USER
        ))
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(body["snapshots"].as_array().unwrap().len(), 0);
    assert_eq!(body["tainted"], true);
}

#[tokio::test]
async fn test_builder_only_includes_tainted_fields_when_untainted() {
    let TestApp { app, repo, _temp } = setup_test_app().await;

    let f1 = fill(1000, "BTC", Side::Buy, "1", "50000", 1);
    repo.insert_fill(&f1).await.unwrap();
    repo.insert_attributions(&[(
        f1.fill_key.clone(),
        true,
        "heuristic".to_string(),
        "low".to_string(),
    )])
    .await
    .unwrap();

    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/v1/positions/history?user={}&builderOnly=true&coin=BTC",
            USER
        ))
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(body["tainted"], false);
    let snapshots = body["snapshots"].as_array().unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0]["tainted"], false);
}

#[tokio::test]
async fn test_positions_history_rejects_invalid_user_address() {
    let TestApp { app, _temp, .. } = setup_test_app().await;

    let request = Request::builder()
        .method("GET")
        .uri("/v1/positions/history?user=0x123")
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
