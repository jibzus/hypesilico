//! Contract and determinism tests for all API endpoints.
//!
//! This module provides comprehensive testing for:
//! - Contract validation: Field names (camelCase), types, required/optional fields
//! - Determinism: Same request twice = identical bytes
//! - Golden tests: Compare against fixture files

use axum::http::StatusCode;
use hypesilico::api::{self, AppState};
use hypesilico::compile::Compiler;
use hypesilico::config::{BuilderAttributionMode, Config, PnlMode};
use hypesilico::datasource::MockDataSource;
use hypesilico::db::init_db;
use hypesilico::domain::{Address, Attribution, AttributionConfidence, Coin, Decimal, Deposit, Fill, Side, TimeMs};
use hypesilico::engine::EquityResolver;
use hypesilico::orchestration::ensure::Ingestor;
use hypesilico::orchestration::orchestrator::Orchestrator;
use hypesilico::Repository;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;

// =============================================================================
// Test Constants
// =============================================================================

const TEST_USER: &str = "0x1111111111111111111111111111111111111111";
const TEST_USER_2: &str = "0x2222222222222222222222222222222222222222";
const TEST_BUILDER: &str = "0x3333333333333333333333333333333333333333";

// =============================================================================
// Test Infrastructure
// =============================================================================

struct TestApp {
    app: axum::Router,
    state: AppState,
    _temp: TempDir,
}

async fn setup_test_app(leaderboard_users: Vec<String>) -> TestApp {
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
        target_builder: TEST_BUILDER.to_string(),
        builder_attribution_mode: BuilderAttributionMode::Auto,
        pnl_mode: PnlMode::Gross,
        lookback_ms: 0,
        leaderboard_users,
    };

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

#[allow(clippy::too_many_arguments)]
fn fill(
    user: &str,
    coin: &str,
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
        Address::new(user.to_string()),
        Coin::new(coin.to_string()),
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

fn deposit(user: &str, time_ms: i64, amount: &str, tx_hash: Option<&str>, event_key: &str) -> Deposit {
    Deposit {
        event_key: event_key.to_string(),
        user: Address::new(user.to_string()),
        time_ms: TimeMs::new(time_ms),
        amount: Decimal::from_str(amount).unwrap(),
        tx_hash: tx_hash.map(|s| s.to_string()),
    }
}

/// Assert all keys in a JSON object are camelCase
fn assert_all_keys_camel_case(value: &serde_json::Value, path: &str) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map {
                // camelCase: starts with lowercase, no underscores
                assert!(
                    key.chars().next().map_or(true, |c| c.is_lowercase()),
                    "Key '{}' at path '{}' should start with lowercase (camelCase)",
                    key,
                    path
                );
                assert!(
                    !key.contains('_'),
                    "Key '{}' at path '{}' should not contain underscores (camelCase)",
                    key,
                    path
                );
                assert_all_keys_camel_case(val, &format!("{}.{}", path, key));
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, val) in arr.iter().enumerate() {
                assert_all_keys_camel_case(val, &format!("{}[{}]", path, i));
            }
        }
        _ => {}
    }
}

// =============================================================================
// Contract Tests - /v1/trades
// =============================================================================

#[tokio::test]
async fn test_contract_trades_field_names_camel_case() {
    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/trades?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_all_keys_camel_case(&json, "root");
}

#[tokio::test]
async fn test_contract_trades_required_fields_present() {
    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "100", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/trades?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Response-level required fields
    assert!(json["trades"].is_array(), "trades must be an array");

    // Trade-level required fields
    let trade = &json["trades"][0];
    assert!(trade["timeMs"].is_i64(), "timeMs must be i64");
    assert!(trade["coin"].is_string(), "coin must be string");
    assert!(trade["side"].is_string(), "side must be string");
    assert!(trade["px"].is_string(), "px must be string (decimal)");
    assert!(trade["sz"].is_string(), "sz must be string (decimal)");
    assert!(trade["fee"].is_string(), "fee must be string (decimal)");
    assert!(trade["closedPnl"].is_string(), "closedPnl must be string (decimal)");
}

#[tokio::test]
async fn test_contract_trades_decimal_fields_are_strings() {
    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000.123", "0.001", "5.5", "100.99", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/trades?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let trade = &json["trades"][0];

    // All decimal values must be strings, not numbers
    assert!(trade["px"].as_str().is_some(), "px must be a string");
    assert!(trade["sz"].as_str().is_some(), "sz must be a string");
    assert!(trade["fee"].as_str().is_some(), "fee must be a string");
    assert!(trade["closedPnl"].as_str().is_some(), "closedPnl must be a string");

    // Verify decimal precision is preserved (no scientific notation)
    let px = trade["px"].as_str().unwrap();
    assert!(!px.contains('e') && !px.contains('E'), "px must not use scientific notation");
}

#[tokio::test]
async fn test_contract_trades_tainted_field_absent_without_builder_only() {
    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/trades?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("tainted").is_none(), "tainted must be absent when builderOnly is not set");
}

#[tokio::test]
async fn test_contract_trades_tainted_field_present_with_builder_only() {
    let test_app = setup_test_app(vec![]).await;

    let f = fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1"));
    test_app.state.repo.insert_fill(&f).await.unwrap();

    // Add attribution
    test_app
        .state
        .repo
        .upsert_attributions_full(&[(
            f.fill_key.clone(),
            Attribution::from_logs_match(true, Some(Address::new(TEST_BUILDER.to_string())), AttributionConfidence::Exact),
        )])
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/trades?user={}&builderOnly=true", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("tainted").is_some(), "tainted must be present when builderOnly=true");
    assert!(json["tainted"].is_boolean(), "tainted must be boolean");
}

#[tokio::test]
async fn test_contract_trades_builder_field_present_when_attributed() {
    let test_app = setup_test_app(vec![]).await;

    let f = fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1"));
    test_app.state.repo.insert_fill(&f).await.unwrap();

    test_app
        .state
        .repo
        .upsert_attributions_full(&[(
            f.fill_key.clone(),
            Attribution::from_logs_match(true, Some(Address::new(TEST_BUILDER.to_string())), AttributionConfidence::Exact),
        )])
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/trades?user={}&builderOnly=true", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let trade = &json["trades"][0];
    assert!(trade["builder"].is_string(), "builder must be string when attributed via logs");
    assert_eq!(trade["builder"].as_str().unwrap(), TEST_BUILDER);
}

// =============================================================================
// Contract Tests - /v1/pnl
// =============================================================================

#[tokio::test]
async fn test_contract_pnl_field_names_camel_case() {
    let test_app = setup_test_app(vec![]).await;

    let user = Address::new(TEST_USER.to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_deposit(&deposit(TEST_USER, 0, "10000", None, "dep:1"))
        .await
        .unwrap();

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "100", Some("1")))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/pnl?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_all_keys_camel_case(&json, "root");
}

#[tokio::test]
async fn test_contract_pnl_required_fields_present() {
    let test_app = setup_test_app(vec![]).await;

    let user = Address::new(TEST_USER.to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "100", Some("1")))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/pnl?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Required fields
    assert!(json["realizedPnl"].is_string(), "realizedPnl must be string (decimal)");
    assert!(json["returnPct"].is_string(), "returnPct must be string (decimal)");
    assert!(json["feesPaid"].is_string(), "feesPaid must be string (decimal)");
    assert!(json["tradeCount"].is_i64(), "tradeCount must be i64");
}

#[tokio::test]
async fn test_contract_pnl_tainted_field_absent_without_builder_only() {
    let test_app = setup_test_app(vec![]).await;

    let user = Address::new(TEST_USER.to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "100", Some("1")))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/pnl?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("tainted").is_none(), "tainted must be absent when builderOnly is not set");
}

#[tokio::test]
async fn test_contract_pnl_tainted_field_present_with_builder_only() {
    let test_app = setup_test_app(vec![]).await;

    let user = Address::new(TEST_USER.to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "100", Some("1")))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/pnl?user={}&builderOnly=true", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("tainted").is_some(), "tainted must be present when builderOnly=true");
    assert!(json["tainted"].is_boolean(), "tainted must be boolean");
}

// =============================================================================
// Contract Tests - /v1/positions/history
// =============================================================================

#[tokio::test]
async fn test_contract_positions_history_field_names_camel_case() {
    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/positions/history?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_all_keys_camel_case(&json, "root");
}

#[tokio::test]
async fn test_contract_positions_history_required_fields_present() {
    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/positions/history?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Response-level required fields
    assert!(json["snapshots"].is_array(), "snapshots must be an array");

    // Snapshot-level required fields
    let snapshot = &json["snapshots"][0];
    assert!(snapshot["timeMs"].is_i64(), "timeMs must be i64");
    assert!(snapshot["coin"].is_string(), "coin must be string");
    assert!(snapshot["netSize"].is_string(), "netSize must be string (decimal)");
    assert!(snapshot["avgEntryPx"].is_string(), "avgEntryPx must be string (decimal)");
    assert!(snapshot["lifecycleId"].is_string(), "lifecycleId must be string");
}

#[tokio::test]
async fn test_contract_positions_history_tainted_field_absent_without_builder_only() {
    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/positions/history?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("tainted").is_none(), "tainted must be absent when builderOnly is not set");

    let snapshot = &json["snapshots"][0];
    assert!(snapshot.get("tainted").is_none(), "snapshot.tainted must be absent when builderOnly is not set");
}

#[tokio::test]
async fn test_contract_positions_history_tainted_field_present_with_builder_only() {
    let test_app = setup_test_app(vec![]).await;

    let f = fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1"));
    test_app.state.repo.insert_fill(&f).await.unwrap();

    // Add attribution
    test_app
        .state
        .repo
        .insert_attributions(&[(
            f.fill_key.clone(),
            true,
            "heuristic".to_string(),
            "low".to_string(),
            None,
        )])
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/positions/history?user={}&builderOnly=true", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("tainted").is_some(), "tainted must be present when builderOnly=true");
    assert!(json["tainted"].is_boolean(), "tainted must be boolean");

    // When untainted, snapshot should have tainted field
    let snapshot = &json["snapshots"][0];
    assert!(snapshot.get("tainted").is_some(), "snapshot.tainted must be present when builderOnly=true");
}

// =============================================================================
// Contract Tests - /v1/leaderboard
// =============================================================================

#[tokio::test]
async fn test_contract_leaderboard_field_names_camel_case() {
    let test_app = setup_test_app(vec![TEST_USER.to_string()]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "100", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(test_app.app, "/v1/leaderboard?metric=volume").await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_all_keys_camel_case(&json, "root");
}

#[tokio::test]
async fn test_contract_leaderboard_required_fields_present() {
    let test_app = setup_test_app(vec![TEST_USER.to_string()]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "100", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(test_app.app, "/v1/leaderboard?metric=volume").await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Response is an array
    assert!(json.is_array(), "leaderboard response must be an array");

    // Entry-level required fields
    let entry = &json[0];
    assert!(entry["rank"].is_i64(), "rank must be i64");
    assert!(entry["user"].is_string(), "user must be string");
    assert!(entry["metricValue"].is_string(), "metricValue must be string (decimal)");
    assert!(entry["tradeCount"].is_i64(), "tradeCount must be i64");
}

#[tokio::test]
async fn test_contract_leaderboard_tainted_field_absent_without_builder_only() {
    let test_app = setup_test_app(vec![TEST_USER.to_string()]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "100", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(test_app.app, "/v1/leaderboard?metric=volume").await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let entry = &json[0];
    assert!(entry.get("tainted").is_none(), "tainted must be absent when builderOnly is not set");
}

#[tokio::test]
async fn test_contract_leaderboard_tainted_field_present_with_builder_only() {
    let test_app = setup_test_app(vec![TEST_USER.to_string()]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "100", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        "/v1/leaderboard?metric=volume&builderOnly=true",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let entry = &json[0];
    assert!(entry.get("tainted").is_some(), "tainted must be present when builderOnly=true");
    assert!(entry["tainted"].is_boolean(), "tainted must be boolean");
}

#[tokio::test]
async fn test_contract_leaderboard_empty_returns_empty_array() {
    let test_app = setup_test_app(vec![]).await;

    let (status, body) = request(test_app.app, "/v1/leaderboard?metric=volume").await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.is_array(), "leaderboard response must be an array");
    assert!(json.as_array().unwrap().is_empty(), "leaderboard with no users must be empty array");
}

// =============================================================================
// Contract Tests - /v1/deposits
// =============================================================================

#[tokio::test]
async fn test_contract_deposits_field_names_camel_case() {
    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_deposit(&deposit(TEST_USER, 1000, "5000", Some("0xabc123"), "dep:1"))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/deposits?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_all_keys_camel_case(&json, "root");
}

#[tokio::test]
async fn test_contract_deposits_required_fields_present() {
    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_deposit(&deposit(TEST_USER, 1000, "5000", Some("0xabc123"), "dep:1"))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/deposits?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Response-level required fields
    assert!(json["totalDeposits"].is_string(), "totalDeposits must be string (decimal)");
    assert!(json["depositCount"].is_i64(), "depositCount must be i64");
    assert!(json["deposits"].is_array(), "deposits must be an array");

    // Deposit-level required fields
    let dep = &json["deposits"][0];
    assert!(dep["timeMs"].is_i64(), "timeMs must be i64");
    assert!(dep["amount"].is_string(), "amount must be string (decimal)");
}

#[tokio::test]
async fn test_contract_deposits_tx_hash_optional() {
    let test_app = setup_test_app(vec![]).await;

    // Deposit without tx_hash
    test_app
        .state
        .repo
        .insert_deposit(&deposit(TEST_USER, 1000, "5000", None, "dep:1"))
        .await
        .unwrap();

    // Deposit with tx_hash
    test_app
        .state
        .repo
        .insert_deposit(&deposit(TEST_USER, 2000, "3000", Some("0xabc123"), "dep:2"))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/deposits?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // First deposit (no tx_hash)
    let dep1 = &json["deposits"][0];
    assert!(dep1.get("txHash").is_none(), "txHash must be absent when not provided");

    // Second deposit (with tx_hash)
    let dep2 = &json["deposits"][1];
    assert!(dep2["txHash"].is_string(), "txHash must be string when provided");
}

#[tokio::test]
async fn test_contract_deposits_empty_returns_zero_values() {
    let test_app = setup_test_app(vec![]).await;

    let (status, body) = request(
        test_app.app,
        &format!("/v1/deposits?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["totalDeposits"], "0", "totalDeposits must be '0' for empty");
    assert_eq!(json["depositCount"], 0, "depositCount must be 0 for empty");
    assert!(json["deposits"].as_array().unwrap().is_empty(), "deposits must be empty array");
}

// =============================================================================
// Contract Tests - /health and /ready
// =============================================================================

#[tokio::test]
async fn test_contract_health_returns_ok() {
    let test_app = setup_test_app(vec![]).await;

    let (status, body) = request(test_app.app, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(String::from_utf8(body).unwrap(), "ok");
}

#[tokio::test]
async fn test_contract_ready_returns_ready() {
    let test_app = setup_test_app(vec![]).await;

    let (status, body) = request(test_app.app, "/ready").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(String::from_utf8(body).unwrap(), "ready");
}

// =============================================================================
// Contract Tests - Error Responses
// =============================================================================

#[tokio::test]
async fn test_contract_error_response_invalid_address() {
    let test_app = setup_test_app(vec![]).await;

    // Invalid address format - handled by our code, returns JSON error
    let (status, body) = request(test_app.app, "/v1/trades?user=invalid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["error"].is_string(), "Error response must have 'error' field");
    assert!(
        json["error"].as_str().unwrap().to_lowercase().contains("address") ||
        json["error"].as_str().unwrap().to_lowercase().contains("user"),
        "Error message should mention address/user validation"
    );
}

#[tokio::test]
async fn test_contract_error_response_invalid_time_window_deposits() {
    let test_app = setup_test_app(vec![]).await;

    // fromMs > toMs is invalid - deposits endpoint validates this
    let (status, body) = request(
        test_app.app,
        &format!("/v1/deposits?user={}&fromMs=2000&toMs=1000", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["error"].is_string(), "Error response must have 'error' field");
}

#[tokio::test]
async fn test_contract_error_response_leaderboard_missing_metric() {
    let test_app = setup_test_app(vec![]).await;

    // Missing 'metric' parameter - leaderboard validates this in handler
    let (status, body) = request(test_app.app, "/v1/leaderboard").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["error"].is_string(), "Error response must have 'error' field");
    assert!(
        json["error"].as_str().unwrap().to_lowercase().contains("metric"),
        "Error message should mention missing metric"
    );
}

#[tokio::test]
async fn test_contract_error_response_leaderboard_invalid_metric() {
    let test_app = setup_test_app(vec![]).await;

    // Invalid metric value
    let (status, body) = request(test_app.app, "/v1/leaderboard?metric=invalid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["error"].is_string(), "Error response must have 'error' field");
}

#[tokio::test]
async fn test_contract_error_response_format_json() {
    let test_app = setup_test_app(vec![]).await;

    // Test error responses that are handled by our handlers (not Axum's query extractor)
    // return proper JSON format with 'error' field
    let error_cases = [
        ("/v1/trades?user=invalid", "invalid address"),
        ("/v1/leaderboard?metric=invalid", "invalid metric"),
        ("/v1/deposits?user=invalid", "invalid address"),
    ];

    for (endpoint, _case) in error_cases {
        let (status, body) = request(test_app.app.clone(), endpoint).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "Endpoint {} should return 400", endpoint);

        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["error"].is_string(), "Error response for {} must have 'error' field", endpoint);
        assert_all_keys_camel_case(&json, &format!("error response for {}", endpoint));
    }
}

// =============================================================================
// Determinism Tests - All Endpoints
// =============================================================================

#[tokio::test]
async fn test_determinism_trades_endpoint() {
    let test_app = setup_test_app(vec![]).await;

    // Insert multiple fills to exercise ordering
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "ETH", 2000, 2, Side::Sell, "3000", "10", "3", "100", Some("1")))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 3000, 3, Side::Sell, "51000", "1", "5", "1000", Some("1")))
        .await
        .unwrap();

    let uri = format!("/v1/trades?user={}", TEST_USER);

    let (_s1, b1) = request(test_app.app.clone(), &uri).await;
    let (_s2, b2) = request(test_app.app, &uri).await;

    assert_eq!(b1, b2, "Trades responses must be byte-identical");
}

#[tokio::test]
async fn test_determinism_pnl_endpoint() {
    let test_app = setup_test_app(vec![]).await;

    let user = Address::new(TEST_USER.to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_deposit(&deposit(TEST_USER, 0, "10000", None, "dep:1"))
        .await
        .unwrap();

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 2000, 2, Side::Sell, "51000", "1", "5", "1000", Some("1")))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let uri = format!("/v1/pnl?user={}", TEST_USER);

    let (_s1, b1) = request(test_app.app.clone(), &uri).await;
    let (_s2, b2) = request(test_app.app, &uri).await;

    assert_eq!(b1, b2, "PnL responses must be byte-identical");
}

#[tokio::test]
async fn test_determinism_positions_history_endpoint() {
    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 2000, 2, Side::Sell, "51000", "1", "5", "1000", Some("1")))
        .await
        .unwrap();

    let uri = format!("/v1/positions/history?user={}", TEST_USER);

    let (_s1, b1) = request(test_app.app.clone(), &uri).await;
    let (_s2, b2) = request(test_app.app, &uri).await;

    assert_eq!(b1, b2, "Positions history responses must be byte-identical");
}

#[tokio::test]
async fn test_determinism_leaderboard_endpoint() {
    let test_app = setup_test_app(vec![
        TEST_USER.to_string(),
        TEST_USER_2.to_string(),
    ])
    .await;

    // Insert fills for both users
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "100", Some("1")))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER_2, "BTC", 1000, 2, Side::Buy, "50000", "2", "10", "200", Some("1")))
        .await
        .unwrap();

    let uri = "/v1/leaderboard?metric=volume";

    let (_s1, b1) = request(test_app.app.clone(), uri).await;
    let (_s2, b2) = request(test_app.app, uri).await;

    assert_eq!(b1, b2, "Leaderboard responses must be byte-identical");
}

#[tokio::test]
async fn test_determinism_deposits_endpoint() {
    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_deposits_batch(&[
            deposit(TEST_USER, 1000, "5000", Some("0xabc"), "dep:1"),
            deposit(TEST_USER, 2000, "3000", None, "dep:2"),
            deposit(TEST_USER, 3000, "2000", Some("0xdef"), "dep:3"),
        ])
        .await
        .unwrap();

    let uri = format!("/v1/deposits?user={}", TEST_USER);

    let (_s1, b1) = request(test_app.app.clone(), &uri).await;
    let (_s2, b2) = request(test_app.app, &uri).await;

    assert_eq!(b1, b2, "Deposits responses must be byte-identical");
}

#[tokio::test]
async fn test_determinism_all_endpoints_cross_check() {
    let test_app = setup_test_app(vec![TEST_USER.to_string()]).await;

    let user = Address::new(TEST_USER.to_string());
    let coin = Coin::new("BTC".to_string());

    // Setup comprehensive data
    test_app
        .state
        .repo
        .insert_deposit(&deposit(TEST_USER, 0, "10000", None, "dep:1"))
        .await
        .unwrap();

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 2000, 2, Side::Sell, "51000", "1", "5", "1000", Some("1")))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    // Test all endpoints in a loop
    let endpoints = [
        format!("/v1/trades?user={}", TEST_USER),
        format!("/v1/pnl?user={}", TEST_USER),
        format!("/v1/positions/history?user={}", TEST_USER),
        format!("/v1/deposits?user={}", TEST_USER),
        "/v1/leaderboard?metric=volume".to_string(),
        "/health".to_string(),
        "/ready".to_string(),
    ];

    for endpoint in &endpoints {
        let (_s1, b1) = request(test_app.app.clone(), endpoint).await;
        let (_s2, b2) = request(test_app.app.clone(), endpoint).await;
        assert_eq!(
            b1, b2,
            "Endpoint {} must return byte-identical responses",
            endpoint
        );
    }
}

// =============================================================================
// Golden Tests - Compare Against Fixtures
// =============================================================================

/// Load fixture file content
fn load_fixture(name: &str) -> Option<String> {
    let path = format!(
        "{}/tests/fixtures/{}",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    std::fs::read_to_string(&path).ok()
}

/// Normalize JSON for comparison (sort keys, format consistently)
fn normalize_json(json: &serde_json::Value) -> String {
    serde_json::to_string_pretty(json).unwrap()
}

#[tokio::test]
async fn test_golden_trades_response() {
    let expected = match load_fixture("trades_response.json") {
        Some(content) => content,
        None => {
            eprintln!("Skipping golden test: trades_response.json not found");
            return;
        }
    };

    let test_app = setup_test_app(vec![]).await;

    // Insert exact data to match fixture
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1705000000000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1705000001000, 2, Side::Sell, "51000", "1", "5", "1000", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/trades?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let actual: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let expected: serde_json::Value = serde_json::from_str(&expected).unwrap();

    assert_eq!(
        normalize_json(&actual),
        normalize_json(&expected),
        "Trades response must match golden fixture"
    );
}

#[tokio::test]
async fn test_golden_pnl_response() {
    let expected = match load_fixture("pnl_response.json") {
        Some(content) => content,
        None => {
            eprintln!("Skipping golden test: pnl_response.json not found");
            return;
        }
    };

    let test_app = setup_test_app(vec![]).await;

    let user = Address::new(TEST_USER.to_string());
    let coin = Coin::new("BTC".to_string());

    test_app
        .state
        .repo
        .insert_deposit(&deposit(TEST_USER, 0, "10000", None, "dep:1"))
        .await
        .unwrap();

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1705000000000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1705000001000, 2, Side::Sell, "51000", "1", "5", "1000", Some("1")))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app.state.repo, &user, &coin)
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/pnl?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let actual: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let expected: serde_json::Value = serde_json::from_str(&expected).unwrap();

    assert_eq!(
        normalize_json(&actual),
        normalize_json(&expected),
        "PnL response must match golden fixture"
    );
}

#[tokio::test]
async fn test_golden_positions_history_response() {
    let expected = match load_fixture("positions_history_response.json") {
        Some(content) => content,
        None => {
            eprintln!("Skipping golden test: positions_history_response.json not found");
            return;
        }
    };

    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1705000000000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1705000001000, 2, Side::Sell, "51000", "1", "5", "1000", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/positions/history?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let actual: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let expected: serde_json::Value = serde_json::from_str(&expected).unwrap();

    // For positions history, compare all fields except lifecycleId (which is generated/hash-based)
    let actual_snapshots = actual["snapshots"].as_array().unwrap();
    let expected_snapshots = expected["snapshots"].as_array().unwrap();

    assert_eq!(
        actual_snapshots.len(),
        expected_snapshots.len(),
        "Positions history response must have same number of snapshots"
    );

    for (i, (actual_snap, expected_snap)) in actual_snapshots.iter().zip(expected_snapshots.iter()).enumerate() {
        assert_eq!(
            actual_snap["timeMs"], expected_snap["timeMs"],
            "Snapshot {} timeMs must match", i
        );
        assert_eq!(
            actual_snap["coin"], expected_snap["coin"],
            "Snapshot {} coin must match", i
        );
        assert_eq!(
            actual_snap["netSize"], expected_snap["netSize"],
            "Snapshot {} netSize must match", i
        );
        assert_eq!(
            actual_snap["avgEntryPx"], expected_snap["avgEntryPx"],
            "Snapshot {} avgEntryPx must match", i
        );
        // lifecycleId is generated (hash-based), so we only verify it exists and is a string
        assert!(
            actual_snap["lifecycleId"].is_string(),
            "Snapshot {} lifecycleId must be a string", i
        );
    }
}

#[tokio::test]
async fn test_golden_leaderboard_response() {
    let expected = match load_fixture("leaderboard_response.json") {
        Some(content) => content,
        None => {
            eprintln!("Skipping golden test: leaderboard_response.json not found");
            return;
        }
    };

    let test_app = setup_test_app(vec![TEST_USER.to_string()]).await;

    test_app
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1705000000000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();

    let (status, body) = request(test_app.app, "/v1/leaderboard?metric=volume").await;
    assert_eq!(status, StatusCode::OK);

    let actual: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let expected: serde_json::Value = serde_json::from_str(&expected).unwrap();

    assert_eq!(
        normalize_json(&actual),
        normalize_json(&expected),
        "Leaderboard response must match golden fixture"
    );
}

#[tokio::test]
async fn test_golden_deposits_response() {
    let expected = match load_fixture("deposits_response.json") {
        Some(content) => content,
        None => {
            eprintln!("Skipping golden test: deposits_response.json not found");
            return;
        }
    };

    let test_app = setup_test_app(vec![]).await;

    test_app
        .state
        .repo
        .insert_deposit(&deposit(TEST_USER, 1705000000000, "5000", Some("0xabc123"), "dep:1"))
        .await
        .unwrap();
    test_app
        .state
        .repo
        .insert_deposit(&deposit(TEST_USER, 1705000001000, "3000", None, "dep:2"))
        .await
        .unwrap();

    let (status, body) = request(
        test_app.app,
        &format!("/v1/deposits?user={}", TEST_USER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let actual: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let expected: serde_json::Value = serde_json::from_str(&expected).unwrap();

    assert_eq!(
        normalize_json(&actual),
        normalize_json(&expected),
        "Deposits response must match golden fixture"
    );
}

// =============================================================================
// Compilation Determinism Tests
// =============================================================================

#[tokio::test]
async fn test_determinism_compilation_same_data_same_tables() {
    // First compilation
    let test_app1 = setup_test_app(vec![]).await;

    let user = Address::new(TEST_USER.to_string());
    let coin = Coin::new("BTC".to_string());

    test_app1
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();
    test_app1
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 2000, 2, Side::Sell, "51000", "1", "5", "1000", Some("1")))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app1.state.repo, &user, &coin)
        .await
        .unwrap();

    let (_, body1) = request(
        test_app1.app,
        &format!("/v1/pnl?user={}", TEST_USER),
    )
    .await;

    // Second compilation with same data
    let test_app2 = setup_test_app(vec![]).await;

    test_app2
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 1000, 1, Side::Buy, "50000", "1", "5", "0", Some("1")))
        .await
        .unwrap();
    test_app2
        .state
        .repo
        .insert_fill(&fill(TEST_USER, "BTC", 2000, 2, Side::Sell, "51000", "1", "5", "1000", Some("1")))
        .await
        .unwrap();

    Compiler::compile_incremental(&test_app2.state.repo, &user, &coin)
        .await
        .unwrap();

    let (_, body2) = request(
        test_app2.app,
        &format!("/v1/pnl?user={}", TEST_USER),
    )
    .await;

    let json1: serde_json::Value = serde_json::from_slice(&body1).unwrap();
    let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();

    assert_eq!(json1["realizedPnl"], json2["realizedPnl"], "PnL must be identical across compilations");
    assert_eq!(json1["feesPaid"], json2["feesPaid"], "Fees must be identical across compilations");
    assert_eq!(json1["tradeCount"], json2["tradeCount"], "Trade count must be identical across compilations");
}
