use axum::http::StatusCode;
use hypesilico::api::{self, AppState};
use hypesilico::compile::Compiler;
use hypesilico::config::{BuilderAttributionMode, PnlMode};
use hypesilico::datasource::MockDataSource;
use hypesilico::db::init_db;
use hypesilico::domain::{Address, Coin, Decimal, Deposit, Fill, Side, TimeMs};
use hypesilico::engine::EquityResolver;
use hypesilico::orchestration::ensure::Ingestor;
use hypesilico::orchestration::orchestrator::Orchestrator;
use hypesilico::{Config, Repository};
use std::str::FromStr;
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;

struct TestApp {
    app: axum::Router,
    state: AppState,
    _temp: TempDir,
}

fn test_config(pnl_mode: PnlMode) -> Config {
    Config {
        port: 0,
        database_path: ":memory:".to_string(),
        hyperliquid_api_url: "http://example.invalid".to_string(),
        target_builder: "0x0".to_string(),
        builder_attribution_mode: BuilderAttributionMode::Auto,
        pnl_mode,
        lookback_ms: 0,
        leaderboard_users: vec![],
    }
}

async fn setup_test_app(pnl_mode: PnlMode) -> TestApp {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("test.db")
        .to_string_lossy()
        .to_string();
    let pool = init_db(&db_path).await.expect("init_db failed");

    let repo = Arc::new(Repository::new(pool));
    let datasource = Arc::new(MockDataSource::new());
    let config = test_config(pnl_mode);

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
async fn test_pnl_response_has_required_fields() {
    let test_app = setup_test_app(PnlMode::Gross).await;

    let user = Address::new("0x0000000000000000000000000000000000000123".to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_deposit(&Deposit {
            event_key: "dep:1".to_string(),
            user: user.clone(),
            time_ms: TimeMs::new(0),
            amount: Decimal::from_str("10000").unwrap(),
            tx_hash: None,
        })
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
            "50000",
            "1",
            "0",
            "0",
            Some("1"),
        ))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app.clone(),
        "/v1/pnl?user=0x0000000000000000000000000000000000000123",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v["realizedPnl"].is_string());
    assert!(v["returnPct"].is_string());
    assert!(v["feesPaid"].is_string());
    assert!(v["tradeCount"].is_i64());
}

#[tokio::test]
async fn test_pnl_calculation_matches_manual_math_gross_mode() {
    let test_app = setup_test_app(PnlMode::Gross).await;

    let user = Address::new("0x0000000000000000000000000000000000000123".to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_fill(&fill(
            &user,
            &coin,
            1000,
            1,
            Side::Buy,
            "50000",
            "1",
            "5",
            "100",
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
            "50000",
            "1",
            "10",
            "200",
            Some("1"),
        ))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app.clone(),
        "/v1/pnl?user=0x0000000000000000000000000000000000000123",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["realizedPnl"], "300");
    assert_eq!(v["feesPaid"], "15");
    assert_eq!(v["tradeCount"], 2);
}

#[tokio::test]
async fn test_pnl_net_mode_subtracts_fees() {
    let test_app = setup_test_app(PnlMode::Net).await;

    let user = Address::new("0x0000000000000000000000000000000000000123".to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_fill(&fill(
            &user,
            &coin,
            1000,
            1,
            Side::Buy,
            "50000",
            "1",
            "5",
            "100",
            Some("1"),
        ))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app.clone(),
        "/v1/pnl?user=0x0000000000000000000000000000000000000123",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["realizedPnl"], "95");
}

#[tokio::test]
async fn test_return_pct_uses_equity_at_from_ms() {
    let test_app = setup_test_app(PnlMode::Gross).await;

    let user = Address::new("0x0000000000000000000000000000000000000123".to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_deposit(&Deposit {
            event_key: "dep:1".to_string(),
            user: user.clone(),
            time_ms: TimeMs::new(0),
            amount: Decimal::from_str("10000").unwrap(),
            tx_hash: None,
        })
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
            "50000",
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
            "50000",
            "1",
            "0",
            "1500",
            Some("1"),
        ))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app.clone(),
        "/v1/pnl?user=0x0000000000000000000000000000000000000123",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["returnPct"], "15");

    let snapshot = test_app
        .state
        .repo
        .get_equity_snapshot_at_or_before(&user, TimeMs::new(0))
        .await
        .unwrap();
    assert!(snapshot.is_some(), "equity resolver should cache snapshots");
}

#[tokio::test]
async fn test_return_pct_respects_max_start_capital() {
    let test_app = setup_test_app(PnlMode::Gross).await;

    let user = Address::new("0x0000000000000000000000000000000000000123".to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_deposit(&Deposit {
            event_key: "dep:1".to_string(),
            user: user.clone(),
            time_ms: TimeMs::new(0),
            amount: Decimal::from_str("100000").unwrap(),
            tx_hash: None,
        })
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
            "50000",
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
            "50000",
            "1",
            "0",
            "1500",
            Some("1"),
        ))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app.clone(),
        "/v1/pnl?user=0x0000000000000000000000000000000000000123&maxStartCapital=10000",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["returnPct"], "15");
}

#[tokio::test]
async fn test_builder_only_excludes_tainted_lifecycles() {
    let test_app = setup_test_app(PnlMode::Gross).await;

    let user = Address::new("0x0000000000000000000000000000000000000123".to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_fill(&fill(
            &user,
            &coin,
            1000,
            1,
            Side::Buy,
            "50000",
            "1",
            "0",
            "0",
            Some("1"), // attributed
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
            "50000",
            "1",
            "0",
            "100",
            None, // not attributed -> taints lifecycle
        ))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app.clone(),
        "/v1/pnl?user=0x0000000000000000000000000000000000000123&builderOnly=true",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["realizedPnl"], "0");
    assert_eq!(v["tradeCount"], 0);
    assert_eq!(v["tainted"], true);
}

#[tokio::test]
async fn test_pnl_response_deterministic() {
    let test_app = setup_test_app(PnlMode::Gross).await;

    let user = Address::new("0x0000000000000000000000000000000000000123".to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_deposit(&Deposit {
            event_key: "dep:1".to_string(),
            user: user.clone(),
            time_ms: TimeMs::new(0),
            amount: Decimal::from_str("10000").unwrap(),
            tx_hash: None,
        })
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
            "50000",
            "1",
            "0",
            "0",
            Some("1"),
        ))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (_status1, body1) = request(
        test_app.app.clone(),
        "/v1/pnl?user=0x0000000000000000000000000000000000000123",
    )
    .await;
    let (_status2, body2) = request(
        test_app.app.clone(),
        "/v1/pnl?user=0x0000000000000000000000000000000000000123",
    )
    .await;

    assert_eq!(body1, body2, "Responses must be byte-identical");
}
