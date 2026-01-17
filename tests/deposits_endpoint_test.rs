use axum::http::StatusCode;
use hypesilico::api;
use hypesilico::config::{BuilderAttributionMode, Config, PnlMode};
use hypesilico::datasource::MockDataSource;
use hypesilico::db::init_db;
use hypesilico::domain::{Address, Decimal, Deposit, TimeMs};
use hypesilico::engine::EquityResolver;
use hypesilico::orchestration::ensure::Ingestor;
use hypesilico::orchestration::orchestrator::Orchestrator;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;

struct TestApp {
    app: axum::Router,
    repo: Arc<hypesilico::Repository>,
    _temp: TempDir,
}

async fn setup_test_app(datasource: Arc<MockDataSource>) -> TestApp {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("test.db")
        .to_string_lossy()
        .to_string();
    let pool = init_db(&db_path).await.expect("init_db failed");
    let repo = Arc::new(hypesilico::Repository::new(pool));

    let config = Config {
        port: 0,
        database_path: db_path,
        hyperliquid_api_url: "http://example.invalid".to_string(),
        target_builder: "0x0".to_string(),
        builder_attribution_mode: BuilderAttributionMode::Auto,
        pnl_mode: PnlMode::Gross,
        lookback_ms: 0,
        leaderboard_users: vec![],
    };

    let ingestor = Ingestor::new(datasource, repo.clone(), config.clone());
    let orchestrator = Arc::new(Orchestrator::new(ingestor, repo.clone()));
    let equity_resolver = Arc::new(EquityResolver::new(repo.clone()));
    let state = api::AppState::new(repo.clone(), config, orchestrator, equity_resolver);
    let app = api::create_router(state);

    TestApp {
        app,
        repo,
        _temp: temp_dir,
    }
}

async fn request(app: axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let req = axum::http::Request::builder()
        .method("GET")
        .uri(uri)
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, body)
}

fn deposit(user: &str, time_ms: i64, amount: &str, tx_hash: Option<&str>, event_key: &str) -> Deposit {
    Deposit {
        event_key: event_key.to_string(),
        user: Address::from_str(user).unwrap(),
        time_ms: TimeMs::new(time_ms),
        amount: Decimal::from_str(amount).unwrap(),
        tx_hash: tx_hash.map(|s| s.to_string()),
    }
}

#[tokio::test]
async fn test_deposits_response_has_required_fields() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    let d = deposit(user, 1705000000000, "5000.00", Some("0xabc"), "dep:1");
    test_app.repo.insert_deposit(&d).await.unwrap();

    let (status, body) = request(test_app.app, &format!("/v1/deposits?user={}", user)).await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["totalDeposits"].is_string());
    assert!(json["depositCount"].is_i64());
    assert!(json["deposits"].is_array());

    let deposit = &json["deposits"][0];
    assert!(deposit["timeMs"].is_i64());
    assert!(deposit["amount"].is_string());
}

#[tokio::test]
async fn test_total_deposits_sums_correctly() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    test_app
        .repo
        .insert_deposits_batch(&[
            deposit(user, 1000, "5000", Some("0xabc"), "dep:1"),
            deposit(user, 2000, "3000.0", Some("0xdef"), "dep:2"),
            deposit(user, 3000, "2000.00", None, "dep:3"),
        ])
        .await
        .unwrap();

    let (_status, body) = request(test_app.app, &format!("/v1/deposits?user={}", user)).await;
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(v["totalDeposits"], "10000");
    assert_eq!(v["depositCount"], 3);
    assert_eq!(v["deposits"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_deposits_filtered_by_time_range() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    test_app
        .repo
        .insert_deposits_batch(&[
            deposit(user, 1000, "1000", None, "dep:1"),
            deposit(user, 2000, "2000", None, "dep:2"),
            deposit(user, 3000, "3000", None, "dep:3"),
        ])
        .await
        .unwrap();

    let (_status, body) = request(
        test_app.app,
        &format!("/v1/deposits?user={}&fromMs=1500&toMs=2500", user),
    )
    .await;

    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["depositCount"], 1);
    assert_eq!(v["totalDeposits"], "2000");
    assert_eq!(v["deposits"][0]["timeMs"], 2000);
}

#[tokio::test]
async fn test_deposits_from_ms_filter() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    test_app
        .repo
        .insert_deposits_batch(&[
            deposit(user, 1000, "1000", None, "dep:1"),
            deposit(user, 2000, "2000", None, "dep:2"),
        ])
        .await
        .unwrap();

    let (_status, body) = request(
        test_app.app,
        &format!("/v1/deposits?user={}&fromMs=1500", user),
    )
    .await;

    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["depositCount"], 1);
    assert_eq!(v["deposits"][0]["timeMs"], 2000);
}

#[tokio::test]
async fn test_deposits_to_ms_filter() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    test_app
        .repo
        .insert_deposits_batch(&[
            deposit(user, 1000, "1000", None, "dep:1"),
            deposit(user, 2000, "2000", None, "dep:2"),
        ])
        .await
        .unwrap();

    let (_status, body) = request(
        test_app.app,
        &format!("/v1/deposits?user={}&toMs=1500", user),
    )
    .await;

    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["depositCount"], 1);
    assert_eq!(v["deposits"][0]["timeMs"], 1000);
}

#[tokio::test]
async fn test_deposits_sorted_by_time_ascending() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    test_app
        .repo
        .insert_deposits_batch(&[
            deposit(user, 3000, "3000", None, "dep:3"),
            deposit(user, 1000, "1000", None, "dep:1"),
            deposit(user, 2000, "2000", None, "dep:2"),
        ])
        .await
        .unwrap();

    let (_status, body) = request(test_app.app, &format!("/v1/deposits?user={}", user)).await;
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(v["deposits"][0]["timeMs"], 1000);
    assert_eq!(v["deposits"][1]["timeMs"], 2000);
    assert_eq!(v["deposits"][2]["timeMs"], 3000);
}

#[tokio::test]
async fn test_deposits_response_deterministic() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    test_app
        .repo
        .insert_deposits_batch(&[
            deposit(user, 1000, "1000", None, "dep:1"),
            deposit(user, 2000, "2000", Some("0xabc"), "dep:2"),
        ])
        .await
        .unwrap();

    let uri = format!("/v1/deposits?user={}", user);
    let (_s1, b1) = request(test_app.app.clone(), &uri).await;
    let (_s2, b2) = request(test_app.app, &uri).await;
    assert_eq!(b1, b2, "Responses must be byte-identical");
}

#[tokio::test]
async fn test_deposits_empty_for_unknown_user() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    let (_status, body) = request(test_app.app, &format!("/v1/deposits?user={}", user)).await;
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(v["totalDeposits"], "0");
    assert_eq!(v["depositCount"], 0);
    assert!(v["deposits"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_deposits_rejects_invalid_user_address() {
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    let (status, _body) = request(test_app.app, "/v1/deposits?user=invalid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_deposits_requires_user_parameter() {
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    let (status, _body) = request(test_app.app, "/v1/deposits").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_deposits_rejects_invalid_time_window() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    let (status, _body) = request(
        test_app.app,
        &format!("/v1/deposits?user={}&fromMs=2000&toMs=1000", user),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_deposits_omits_tx_hash_when_missing() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    test_app
        .repo
        .insert_deposit(&deposit(user, 1000, "1000", None, "dep:1"))
        .await
        .unwrap();

    let (_status, body) = request(test_app.app, &format!("/v1/deposits?user={}", user)).await;
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let obj = v["deposits"][0].as_object().unwrap();
    assert!(obj.get("txHash").is_none());
}

