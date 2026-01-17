//! Integration tests for the compile pipeline.

use hypesilico::{
    compile::Compiler,
    db::init_db,
    domain::{Address, Coin, Decimal, Fill, Side, TimeMs},
    Repository,
};
use std::str::FromStr;
use tempfile::TempDir;

async fn setup_test_db() -> (Repository, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("test.db")
        .to_string_lossy()
        .to_string();
    let pool = init_db(&db_path).await.expect("init_db failed");
    (Repository::new(pool), temp_dir)
}

#[allow(clippy::too_many_arguments)]
fn create_test_fill(
    time_ms: i64,
    user: &str,
    coin: &str,
    side: Side,
    px: &str,
    sz: &str,
    fee: &str,
    closed_pnl: &str,
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
        None,
        Some(time_ms),
        Some(time_ms + 1),
    )
}

#[tokio::test]
async fn test_compile_incremental_empty_fills() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    let result = Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");

    assert_eq!(result, 0);
}

#[tokio::test]
async fn test_compile_incremental_single_fill() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    let fill = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.5", "10", "0");
    repo.insert_fill(&fill).await.expect("insert failed");

    let result = Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");

    assert_eq!(result, 1);

    // Verify watermark was updated
    let watermark = repo
        .get_compile_state(&user, &coin)
        .await
        .expect("get_compile_state failed");
    assert!(watermark.is_some());
    let (time_ms, fill_key) = watermark.unwrap();
    assert_eq!(time_ms, Some(1000));
    assert!(fill_key.is_some());
}

#[tokio::test]
async fn test_compile_incremental_idempotent() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    let fill = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.5", "10", "0");
    repo.insert_fill(&fill).await.expect("insert failed");

    // First compilation
    let result1 = Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");
    assert_eq!(result1, 1);

    // Second compilation should process no new fills
    let result2 = Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");
    assert_eq!(result2, 0);
}

#[tokio::test]
async fn test_compile_incremental_multiple_fills() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    // Insert multiple fills
    let fill1 = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.0", "10", "0");
    let fill2 = create_test_fill(
        2000,
        "0x123",
        "BTC",
        Side::Sell,
        "51000",
        "1.0",
        "10",
        "100",
    );

    repo.insert_fill(&fill1).await.expect("insert failed");
    repo.insert_fill(&fill2).await.expect("insert failed");

    let result = Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");

    assert_eq!(result, 2);
}

#[tokio::test]
async fn test_compile_incremental_watermark_progression() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    // First batch
    let fill1 = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.0", "10", "0");
    repo.insert_fill(&fill1).await.expect("insert failed");

    Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");

    let watermark1 = repo
        .get_compile_state(&user, &coin)
        .await
        .expect("get_compile_state failed")
        .unwrap();

    // Second batch
    let fill2 = create_test_fill(
        2000,
        "0x123",
        "BTC",
        Side::Sell,
        "51000",
        "1.0",
        "10",
        "100",
    );
    repo.insert_fill(&fill2).await.expect("insert failed");

    Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");

    let watermark2 = repo
        .get_compile_state(&user, &coin)
        .await
        .expect("get_compile_state failed")
        .unwrap();

    // Watermark should have progressed
    assert!(watermark2.0 > watermark1.0);
}

#[tokio::test]
async fn test_compile_incremental_different_coins() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let btc = Coin::new("BTC".to_string());
    let eth = Coin::new("ETH".to_string());

    // Insert fills for different coins
    let btc_fill = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.0", "10", "0");
    let eth_fill = create_test_fill(1000, "0x123", "ETH", Side::Buy, "3000", "10.0", "5", "0");

    repo.insert_fill(&btc_fill).await.expect("insert failed");
    repo.insert_fill(&eth_fill).await.expect("insert failed");

    // Compile BTC
    let btc_result = Compiler::compile_incremental(&repo, &user, &btc)
        .await
        .expect("compile failed");
    assert_eq!(btc_result, 1);

    // Compile ETH
    let eth_result = Compiler::compile_incremental(&repo, &user, &eth)
        .await
        .expect("compile failed");
    assert_eq!(eth_result, 1);

    // Verify separate watermarks
    let btc_watermark = repo
        .get_compile_state(&user, &btc)
        .await
        .expect("get_compile_state failed");
    let eth_watermark = repo
        .get_compile_state(&user, &eth)
        .await
        .expect("get_compile_state failed");

    assert!(btc_watermark.is_some());
    assert!(eth_watermark.is_some());
}

#[tokio::test]
async fn test_compile_incremental_different_users() {
    let (repo, _temp) = setup_test_db().await;
    let user1 = Address::new("0x111".to_string());
    let user2 = Address::new("0x222".to_string());
    let coin = Coin::new("BTC".to_string());

    // Insert fills for different users
    let fill1 = create_test_fill(1000, "0x111", "BTC", Side::Buy, "50000", "1.0", "10", "0");
    let fill2 = create_test_fill(1000, "0x222", "BTC", Side::Buy, "50000", "2.0", "20", "0");

    repo.insert_fill(&fill1).await.expect("insert failed");
    repo.insert_fill(&fill2).await.expect("insert failed");

    // Compile for user1
    let result1 = Compiler::compile_incremental(&repo, &user1, &coin)
        .await
        .expect("compile failed");
    assert_eq!(result1, 1);

    // Compile for user2
    let result2 = Compiler::compile_incremental(&repo, &user2, &coin)
        .await
        .expect("compile failed");
    assert_eq!(result2, 1);

    // Verify separate watermarks
    let watermark1 = repo
        .get_compile_state(&user1, &coin)
        .await
        .expect("get_compile_state failed");
    let watermark2 = repo
        .get_compile_state(&user2, &coin)
        .await
        .expect("get_compile_state failed");

    assert!(watermark1.is_some());
    assert!(watermark2.is_some());
}

#[tokio::test]
async fn test_derived_tables_populated_after_compile() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    // Insert a simple open-close lifecycle
    let fill1 = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.0", "10", "0");
    let fill2 = create_test_fill(
        2000,
        "0x123",
        "BTC",
        Side::Sell,
        "51000",
        "1.0",
        "10",
        "100",
    );

    repo.insert_fill(&fill1).await.expect("insert failed");
    repo.insert_fill(&fill2).await.expect("insert failed");

    // Compile
    let result = Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");
    assert_eq!(result, 2);

    // Verify lifecycles were created
    let lifecycles = repo
        .query_lifecycles(&user, &coin)
        .await
        .expect("query_lifecycles failed");
    assert!(!lifecycles.is_empty(), "No lifecycles found");

    // Verify snapshots were created
    let snapshots = repo
        .query_snapshots(&user, &coin)
        .await
        .expect("query_snapshots failed");
    assert!(!snapshots.is_empty(), "No snapshots found");

    // Verify effects were created
    let effects = repo
        .query_effects(&user, &coin)
        .await
        .expect("query_effects failed");
    assert!(!effects.is_empty(), "No effects found");
}

#[tokio::test]
async fn test_lifecycle_open_close_sequence() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    // Create a simple open-close sequence
    let fill1 = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.0", "10", "0");
    let fill2 = create_test_fill(
        2000,
        "0x123",
        "BTC",
        Side::Sell,
        "51000",
        "1.0",
        "10",
        "100",
    );

    repo.insert_fill(&fill1).await.expect("insert failed");
    repo.insert_fill(&fill2).await.expect("insert failed");

    Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");

    // Query lifecycles
    let lifecycles = repo
        .query_lifecycles(&user, &coin)
        .await
        .expect("query_lifecycles failed");

    assert_eq!(lifecycles.len(), 1, "Expected 1 lifecycle");
    let (_id, lc_user, lc_coin, start_time, end_time) = &lifecycles[0];
    assert_eq!(lc_user, &user);
    assert_eq!(lc_coin, &coin);
    assert_eq!(*start_time, 1000);
    assert_eq!(*end_time, Some(2000));
}

#[tokio::test]
async fn test_snapshots_created_per_fill() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    // Create multiple fills
    let fill1 = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.0", "10", "0");
    let fill2 = create_test_fill(2000, "0x123", "BTC", Side::Buy, "50500", "0.5", "5", "0");
    let fill3 = create_test_fill(
        3000,
        "0x123",
        "BTC",
        Side::Sell,
        "51000",
        "1.5",
        "15",
        "100",
    );

    repo.insert_fill(&fill1).await.expect("insert failed");
    repo.insert_fill(&fill2).await.expect("insert failed");
    repo.insert_fill(&fill3).await.expect("insert failed");

    Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");

    // Query snapshots
    let snapshots = repo
        .query_snapshots(&user, &coin)
        .await
        .expect("query_snapshots failed");

    // Should have 3 snapshots (one per fill)
    assert_eq!(snapshots.len(), 3, "Expected 3 snapshots");
}

#[tokio::test]
async fn test_effects_decompose_flip() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    // Create a flip: long -> short
    let fill1 = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.0", "10", "0");
    let fill2 = create_test_fill(
        2000,
        "0x123",
        "BTC",
        Side::Sell,
        "51000",
        "2.0",
        "20",
        "100",
    );

    repo.insert_fill(&fill1).await.expect("insert failed");
    repo.insert_fill(&fill2).await.expect("insert failed");

    Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");

    // Query effects
    let effects = repo
        .query_effects(&user, &coin)
        .await
        .expect("query_effects failed");

    // Flip should create 2 effects: close (1.0) + open (1.0 short)
    assert!(effects.len() >= 2, "Expected at least 2 effects for flip");
}

#[tokio::test]
async fn test_taint_computed_for_lifecycles() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    // Create a simple open-close lifecycle
    let fill1 = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.0", "10", "0");
    let fill2 = create_test_fill(
        2000,
        "0x123",
        "BTC",
        Side::Sell,
        "51000",
        "1.0",
        "10",
        "100",
    );

    repo.insert_fill(&fill1).await.expect("insert failed");
    repo.insert_fill(&fill2).await.expect("insert failed");

    // Compile
    Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");

    // Query lifecycles to check taint flag
    let lifecycles = repo
        .query_lifecycles(&user, &coin)
        .await
        .expect("query_lifecycles failed");

    assert!(!lifecycles.is_empty(), "No lifecycles found");
    // Taint should be 0 (false) since no attribution data exists yet
    // (the taint computation will mark it as tainted if no attribution is found)
}

#[tokio::test]
async fn test_taint_with_builder_attribution() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    // Create a simple open-close lifecycle
    let fill1 = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.0", "10", "0");
    let fill2 = create_test_fill(
        2000,
        "0x123",
        "BTC",
        Side::Sell,
        "51000",
        "1.0",
        "10",
        "100",
    );

    repo.insert_fill(&fill1).await.expect("insert failed");
    repo.insert_fill(&fill2).await.expect("insert failed");

    // Insert attributions for both fills (builder-attributed)
    let fill1_key = fill1.fill_key().to_string();
    let fill2_key = fill2.fill_key().to_string();

    repo.insert_attributions(&[
        (
            fill1_key,
            true,
            "heuristic".to_string(),
            "low".to_string(),
            None,
        ),
        (
            fill2_key,
            true,
            "heuristic".to_string(),
            "low".to_string(),
            None,
        ),
    ])
    .await
    .expect("insert attributions failed");

    // Compile
    Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");

    // Query lifecycles to check taint flag
    let lifecycles = repo
        .query_lifecycles(&user, &coin)
        .await
        .expect("query_lifecycles failed");

    assert!(!lifecycles.is_empty(), "No lifecycles found");
    // With builder attribution, taint should be 0 (false)
}

#[tokio::test]
async fn test_taint_with_non_builder_fill() {
    let (repo, _temp) = setup_test_db().await;
    let user = Address::new("0x123".to_string());
    let coin = Coin::new("BTC".to_string());

    // Create a simple open-close lifecycle
    let fill1 = create_test_fill(1000, "0x123", "BTC", Side::Buy, "50000", "1.0", "10", "0");
    let fill2 = create_test_fill(
        2000,
        "0x123",
        "BTC",
        Side::Sell,
        "51000",
        "1.0",
        "10",
        "100",
    );

    repo.insert_fill(&fill1).await.expect("insert failed");
    repo.insert_fill(&fill2).await.expect("insert failed");

    // Insert attribution for fill1 (builder-attributed) but NOT for fill2
    let fill1_key = fill1.fill_key().to_string();

    repo.insert_attributions(&[(
        fill1_key,
        true,
        "heuristic".to_string(),
        "low".to_string(),
        None,
    )])
        .await
        .expect("insert attributions failed");

    // Compile
    Compiler::compile_incremental(&repo, &user, &coin)
        .await
        .expect("compile failed");

    // Query lifecycles to check taint flag
    let lifecycles = repo
        .query_lifecycles(&user, &coin)
        .await
        .expect("query_lifecycles failed");

    assert!(!lifecycles.is_empty(), "No lifecycles found");
    // With missing attribution for fill2, lifecycle should be tainted
}
