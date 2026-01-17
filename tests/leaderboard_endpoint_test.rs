use axum::http::StatusCode;
use hypesilico::api::{self, AppState};
use hypesilico::config::{BuilderAttributionMode, Config, PnlMode};
use hypesilico::datasource::MockDataSource;
use hypesilico::db::init_db;
use hypesilico::domain::{Address, Coin, Decimal, Fill, Side, TimeMs};
use hypesilico::engine::EquityResolver;
use hypesilico::orchestration::ensure::Ingestor;
use hypesilico::orchestration::orchestrator::Orchestrator;
use hypesilico::Repository;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;

struct TestApp {
    app: axum::Router,
    state: AppState,
    _temp: TempDir,
}

fn test_config(users: Vec<String>) -> Config {
    Config {
        port: 0,
        database_path: ":memory:".to_string(),
        hyperliquid_api_url: "http://example.invalid".to_string(),
        target_builder: "0x0".to_string(),
        builder_attribution_mode: BuilderAttributionMode::Auto,
        pnl_mode: PnlMode::Gross,
        lookback_ms: 0,
        leaderboard_users: users,
    }
}

async fn setup_test_app(users: Vec<String>) -> TestApp {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("test.db")
        .to_string_lossy()
        .to_string();
    let pool = init_db(&db_path).await.expect("init_db failed");

    let repo = Arc::new(Repository::new(pool));
    let datasource = Arc::new(MockDataSource::new());
    let config = test_config(users);

    let ingestor = Ingestor::new(datasource, repo.clone(), config.clone());
    let orchestrator = Arc::new(Orchestrator::new(ingestor, repo.clone()));
    let equity_resolver = Arc::new(EquityResolver::new(repo.clone()));
    let state = AppState::new(repo, config, orchestrator, equity_resolver);
    let app = api::create_router(state.clone());

    TestApp {
        app,
        state,
        _temp: temp_dir,
    }
}

#[allow(clippy::too_many_arguments)]
fn fill(
    user: &Address,
    coin: &Coin,
    time_ms: i64,
    tid: i64,
    side: Side,
    px: &str,
    sz: &str,
    fee: &str,
    closed_pnl: &str,
    builder_fee: Option<&str>,
) -> Fill {
    Fill::new(
        TimeMs::new(time_ms),
        user.clone(),
        coin.clone(),
        side,
        Decimal::from_str(px).unwrap(),
        Decimal::from_str(sz).unwrap(),
        Decimal::from_str(fee).unwrap(),
        Decimal::from_str(closed_pnl).unwrap(),
        builder_fee.map(|s| Decimal::from_str(s).unwrap()),
        Some(tid),
        None,
    )
}

async fn request(app: axum::Router, uri: &str) -> (StatusCode, axum::body::Bytes) {
    let req = axum::http::Request::builder()
        .method("GET")
        .uri(uri)
        .body(axum::body::Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, body)
}

#[tokio::test]
async fn test_leaderboard_ranking_tie_breakers() {
    let u1 = Address::new("0x0000000000000000000000000000000000000003".to_string());
    let u2 = Address::new("0x0000000000000000000000000000000000000002".to_string());
    let u3 = Address::new("0x0000000000000000000000000000000000000001".to_string());

    let test_app = setup_test_app(vec![
        u1.as_str().to_string(),
        u2.as_str().to_string(),
        u3.as_str().to_string(),
    ])
    .await;

    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_fill(&fill(&u1, &coin, 1000, 1, Side::Buy, "100", "1", "0", "0", Some("1")))
        .await
        .unwrap();

    test_app
        .state
        .repo
        .insert_fill(&fill(&u2, &coin, 1000, 2, Side::Buy, "25", "2", "0", "0", Some("1")))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(&u2, &coin, 2000, 3, Side::Buy, "25", "2", "0", "0", Some("1")))
        .await
        .unwrap();

    test_app
        .state
        .repo
        .insert_fill(&fill(&u3, &coin, 1000, 4, Side::Buy, "25", "2", "0", "0", Some("1")))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(&u3, &coin, 2000, 5, Side::Buy, "25", "2", "0", "0", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(test_app.app.clone(), "/v1/leaderboard?metric=volume").await;
    assert_eq!(status, StatusCode::OK);

    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v.is_array());
    assert_eq!(v.as_array().unwrap().len(), 3);

    assert_eq!(v[0]["rank"], 1);
    assert_eq!(v[0]["user"], u3.as_str());
    assert_eq!(v[0]["metricValue"], "100");
    assert_eq!(v[0]["tradeCount"], 2);

    assert_eq!(v[1]["rank"], 2);
    assert_eq!(v[1]["user"], u2.as_str());
    assert_eq!(v[1]["metricValue"], "100");
    assert_eq!(v[1]["tradeCount"], 2);

    assert_eq!(v[2]["rank"], 3);
    assert_eq!(v[2]["user"], u1.as_str());
    assert_eq!(v[2]["metricValue"], "100");
    assert_eq!(v[2]["tradeCount"], 1);
}

#[tokio::test]
async fn test_leaderboard_metric_correctness_volume_pnl_return_pct() {
    let user = Address::new("0x0000000000000000000000000000000000000123".to_string());
    let coin = Coin::new("BTC".to_string());

    let test_app = setup_test_app(vec![user.as_str().to_string()]).await;

    test_app
        .state
        .repo
        .upsert_equity_snapshot(&user, TimeMs::new(0), Decimal::from_str("100").unwrap())
        .await
        .unwrap();

    test_app
        .state
        .repo
        .insert_fill(&fill(
            &user,
            &coin,
            1000,
            1,
            Side::Buy,
            "10",
            "2",
            "0",
            "5",
            Some("1"),
        ))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app.clone(),
        "/v1/leaderboard?metric=volume&coin=BTC&fromMs=0&toMs=2000",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v[0]["metricValue"], "20");
    assert_eq!(v[0]["tradeCount"], 1);

    let (status, body) = request(
        test_app.app.clone(),
        "/v1/leaderboard?metric=pnl&coin=BTC&fromMs=0&toMs=2000",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v[0]["metricValue"], "5");
    assert_eq!(v[0]["tradeCount"], 1);

    let (status, body) = request(
        test_app.app.clone(),
        "/v1/leaderboard?metric=returnPct&coin=BTC&fromMs=0&toMs=2000&maxStartCapital=50",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v[0]["metricValue"], "10");
}

#[tokio::test]
async fn test_leaderboard_builder_only_excludes_tainted_lifecycles() {
    let user = Address::new("0x0000000000000000000000000000000000000123".to_string());
    let coin = Coin::new("BTC".to_string());

    let test_app = setup_test_app(vec![user.as_str().to_string()]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(
            &user,
            &coin,
            1000,
            1,
            Side::Buy,
            "10",
            "1",
            "0",
            "0",
            Some("1"),
        ))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(
            &user,
            &coin,
            2000,
            2,
            Side::Sell,
            "10",
            "1",
            "0",
            "0",
            None,
        ))
        .await
        .unwrap();

    test_app
        .state
        .repo
        .insert_fill(&fill(
            &user,
            &coin,
            3000,
            3,
            Side::Buy,
            "20",
            "1",
            "0",
            "0",
            Some("1"),
        ))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(
            &user,
            &coin,
            4000,
            4,
            Side::Sell,
            "20",
            "1",
            "0",
            "0",
            Some("1"),
        ))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app.clone(),
        "/v1/leaderboard?metric=volume&builderOnly=true&coin=BTC&fromMs=0&toMs=5000",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v[0]["metricValue"], "40");
    assert_eq!(v[0]["tradeCount"], 2);
    assert_eq!(v[0]["tainted"], true);
}

