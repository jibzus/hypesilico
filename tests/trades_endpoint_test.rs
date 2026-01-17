use axum::http::StatusCode;
use hypesilico::api;
use hypesilico::config::{BuilderAttributionMode, Config, PnlMode};
use hypesilico::datasource::MockDataSource;
use hypesilico::db::init_db;
use hypesilico::domain::{Address, Attribution, AttributionConfidence, Coin, Decimal, Fill, Side, TimeMs};
use hypesilico::orchestration::ensure::Ingestor;
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

    let ingestor = Arc::new(Ingestor::new(datasource, repo.clone(), config));
    let state = api::AppState { repo: repo.clone(), ingestor };
    let app = api::create_router(state);

    TestApp {
        app,
        repo,
        _temp: temp_dir,
    }
}

fn fill(
    user: &str,
    coin: &str,
    time_ms: i64,
    tid: i64,
    oid: i64,
    side: Side,
) -> Fill {
    Fill::new(
        TimeMs::new(time_ms),
        Address::new(user.to_string()),
        Coin::new(coin.to_string()),
        side,
        Decimal::from_str("50000.00").unwrap(),
        Decimal::from_str("0.1000").unwrap(),
        Decimal::from_str("5.00").unwrap(),
        Decimal::from_str("0").unwrap(),
        None,
        Some(tid),
        Some(oid),
    )
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

#[tokio::test]
async fn test_trades_response_has_required_fields() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    let f = fill(user, "BTC", 1705000000000, 2, 1, Side::Buy);
    test_app.repo.insert_fill(&f).await.unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/trades?user={}", user),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["trades"].is_array());

    let trade = &json["trades"][0];
    assert!(trade["timeMs"].is_i64());
    assert!(trade["coin"].is_string());
    assert!(trade["side"].is_string());
    assert!(trade["px"].is_string());
    assert!(trade["sz"].is_string());
    assert!(trade["fee"].is_string());
    assert!(trade["closedPnl"].is_string());
}

#[tokio::test]
async fn test_trades_response_deterministic() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    let f1 = fill(user, "BTC", 1000, 2, 2, Side::Buy);
    let f2 = fill(user, "BTC", 1000, 1, 1, Side::Sell);
    test_app.repo.insert_fill(&f1).await.unwrap();
    test_app.repo.insert_fill(&f2).await.unwrap();

    let uri = format!("/v1/trades?user={}", user);

    let (_s1, b1) = request(test_app.app.clone(), &uri).await;
    let (_s2, b2) = request(test_app.app, &uri).await;

    assert_eq!(b1, b2, "Responses must be byte-identical");
}

#[tokio::test]
async fn test_builder_only_filters_non_builder_fills_and_sets_tainted() {
    let user = "0x1111111111111111111111111111111111111111";
    let builder = "0x2222222222222222222222222222222222222222";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    let builder_fill = fill(user, "BTC", 1000, 1, 1, Side::Buy);
    let other_fill = fill(user, "BTC", 2000, 2, 2, Side::Sell);
    test_app.repo.insert_fill(&builder_fill).await.unwrap();
    test_app.repo.insert_fill(&other_fill).await.unwrap();

    test_app
        .repo
        .upsert_attributions_full(&[(
            builder_fill.fill_key.clone(),
            Attribution::from_logs_match(
                true,
                Some(Address::new(builder.to_string())),
                AttributionConfidence::Exact,
            ),
        )])
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/trades?user={}&builderOnly=true", user),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["trades"].as_array().unwrap().len(), 1);
    assert_eq!(json["tainted"], serde_json::Value::Bool(true));
    assert_eq!(json["trades"][0]["builder"], serde_json::Value::String(builder.to_string()));
}

#[tokio::test]
async fn test_trades_filters_by_coin_and_time_range() {
    let user = "0x1111111111111111111111111111111111111111";
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    test_app
        .repo
        .insert_fill(&fill(user, "BTC", 1000, 1, 1, Side::Buy))
        .await
        .unwrap();
    test_app
        .repo
        .insert_fill(&fill(user, "ETH", 1500, 2, 2, Side::Buy))
        .await
        .unwrap();
    test_app
        .repo
        .insert_fill(&fill(user, "BTC", 2500, 3, 3, Side::Sell))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/trades?user={}&coin=BTC&fromMs=1200&toMs=2600", user),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let trades = json["trades"].as_array().unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0]["coin"], serde_json::Value::String("BTC".to_string()));
    assert_eq!(trades[0]["timeMs"], serde_json::Value::Number(serde_json::Number::from(2500)));
}

#[tokio::test]
async fn test_trades_rejects_invalid_user() {
    let datasource = Arc::new(MockDataSource::new());
    let test_app = setup_test_app(datasource).await;

    let (status, _body) = request(test_app.app, "/v1/trades?user=not-an-address").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

