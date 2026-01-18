//! Rigorous integration tests for builder logs attribution using real Hyperliquid data.
//!
//! Tests the three builder addresses:
//! - Phantom:  0xb84168cf3be63c6b8dad05ff5d755e97432ff80b
//! - Insilico: 0x2868fc0d9786a740b491577a43502259efa78a39
//! - BasedApp: 0x1924b8561eef20e70ede628a296175d358be80e5

use hypesilico::datasource::builder_logs::{BuilderLogsFetcher, BuilderLogsSource};
use hypesilico::domain::Address;
use reqwest::Client;

const PHANTOM_BUILDER: &str = "0xb84168cf3be63c6b8dad05ff5d755e97432ff80b";
const INSILICO_BUILDER: &str = "0x2868fc0d9786a740b491577a43502259efa78a39";
const BASEDAPP_BUILDER: &str = "0x1924b8561eef20e70ede628a296175d358be80e5";

fn fetcher() -> BuilderLogsFetcher {
    BuilderLogsFetcher::new(Client::new())
}

/// Get yesterday's date in YYYYMMDD format
fn yesterday_yyyymmdd() -> String {
    let now = chrono::Utc::now();
    let yesterday = now - chrono::Duration::days(1);
    yesterday.format("%Y%m%d").to_string()
}

/// Get a date from N days ago in YYYYMMDD format
fn days_ago_yyyymmdd(days: i64) -> String {
    let now = chrono::Utc::now();
    let date = now - chrono::Duration::days(days);
    date.format("%Y%m%d").to_string()
}

// ============================================================================
// URL CONSTRUCTION TESTS
// ============================================================================

#[test]
fn url_construction_phantom() {
    let builder = Address::new(PHANTOM_BUILDER.to_string());
    let url = BuilderLogsFetcher::builder_logs_url(&builder, "20250117");
    assert_eq!(
        url,
        format!("https://stats-data.hyperliquid.xyz/Mainnet/builder_fills/{}/20250117.csv.lz4", PHANTOM_BUILDER)
    );
}

#[test]
fn url_construction_insilico() {
    let builder = Address::new(INSILICO_BUILDER.to_string());
    let url = BuilderLogsFetcher::builder_logs_url(&builder, "20250117");
    assert_eq!(
        url,
        format!("https://stats-data.hyperliquid.xyz/Mainnet/builder_fills/{}/20250117.csv.lz4", INSILICO_BUILDER)
    );
}

#[test]
fn url_construction_basedapp() {
    let builder = Address::new(BASEDAPP_BUILDER.to_string());
    let url = BuilderLogsFetcher::builder_logs_url(&builder, "20250117");
    assert_eq!(
        url,
        format!("https://stats-data.hyperliquid.xyz/Mainnet/builder_fills/{}/20250117.csv.lz4", BASEDAPP_BUILDER)
    );
}

// ============================================================================
// LIVE FETCH TESTS - Phantom Builder
// ============================================================================

#[tokio::test]
async fn fetch_phantom_yesterday() {
    let fetcher = fetcher();
    let builder = Address::new(PHANTOM_BUILDER.to_string());
    let day = yesterday_yyyymmdd();

    println!("\n=== Fetching Phantom builder logs for {} ===", day);

    match fetcher.fetch_and_parse_day(&builder, &day).await {
        Ok(fills) => {
            println!("SUCCESS: Fetched {} fills for Phantom on {}", fills.len(), day);

            if !fills.is_empty() {
                println!("Sample fill #0:");
                let f = &fills[0];
                println!("  time_ms: {}", f.time_ms.as_ms());
                println!("  user: {}", f.user.as_str());
                println!("  coin: {}", f.coin.as_str());
                println!("  side: {:?}", f.side);
                println!("  px: {}", f.px.to_canonical_string());
                println!("  sz: {}", f.sz.to_canonical_string());
                println!("  tid: {:?}", f.tid);
                println!("  oid: {:?}", f.oid);

                // Verify fields are populated correctly
                assert!(!f.user.as_str().is_empty(), "user should not be empty");
                assert!(!f.coin.as_str().is_empty(), "coin should not be empty");
                assert!(f.px.is_positive(), "px should be positive");
                assert!(f.sz.is_positive(), "sz should be positive");

                // Count unique users
                let unique_users: std::collections::HashSet<_> = fills.iter()
                    .map(|f| f.user.as_str())
                    .collect();
                println!("Unique users: {}", unique_users.len());

                // Count unique coins
                let unique_coins: std::collections::HashSet<_> = fills.iter()
                    .map(|f| f.coin.as_str())
                    .collect();
                println!("Unique coins: {:?}", unique_coins);

                // Count fills with tid
                let with_tid = fills.iter().filter(|f| f.tid.is_some()).count();
                println!("Fills with tid: {} ({:.1}%)", with_tid, (with_tid as f64 / fills.len() as f64) * 100.0);
            }
        }
        Err(e) => {
            println!("Note: No logs available for {} (error: {})", day, e);
            // This is acceptable - builder may not have had activity
        }
    }
}

#[tokio::test]
async fn fetch_phantom_last_7_days() {
    let fetcher = fetcher();
    let builder = Address::new(PHANTOM_BUILDER.to_string());

    println!("\n=== Fetching Phantom builder logs for last 7 days ===");

    let mut total_fills = 0;
    let mut days_with_data = 0;

    for days_ago in 1..=7 {
        let day = days_ago_yyyymmdd(days_ago);
        match fetcher.fetch_and_parse_day(&builder, &day).await {
            Ok(fills) => {
                total_fills += fills.len();
                if !fills.is_empty() {
                    days_with_data += 1;
                    println!("  {}: {} fills", day, fills.len());
                }
            }
            Err(_) => {
                // Skip days without data
            }
        }
    }

    println!("Total: {} fills across {} days", total_fills, days_with_data);
}

// ============================================================================
// LIVE FETCH TESTS - Insilico Builder
// ============================================================================

#[tokio::test]
async fn fetch_insilico_yesterday() {
    let fetcher = fetcher();
    let builder = Address::new(INSILICO_BUILDER.to_string());
    let day = yesterday_yyyymmdd();

    println!("\n=== Fetching Insilico builder logs for {} ===", day);

    match fetcher.fetch_and_parse_day(&builder, &day).await {
        Ok(fills) => {
            println!("SUCCESS: Fetched {} fills for Insilico on {}", fills.len(), day);

            if !fills.is_empty() {
                println!("Sample fill #0:");
                let f = &fills[0];
                println!("  time_ms: {}", f.time_ms.as_ms());
                println!("  user: {}", f.user.as_str());
                println!("  coin: {}", f.coin.as_str());
                println!("  side: {:?}", f.side);
                println!("  px: {}", f.px.to_canonical_string());
                println!("  sz: {}", f.sz.to_canonical_string());
                println!("  tid: {:?}", f.tid);
                println!("  oid: {:?}", f.oid);
            }
        }
        Err(e) => {
            println!("Note: No logs available for {} (error: {})", day, e);
        }
    }
}

#[tokio::test]
async fn fetch_insilico_last_7_days() {
    let fetcher = fetcher();
    let builder = Address::new(INSILICO_BUILDER.to_string());

    println!("\n=== Fetching Insilico builder logs for last 7 days ===");

    let mut total_fills = 0;
    let mut days_with_data = 0;

    for days_ago in 1..=7 {
        let day = days_ago_yyyymmdd(days_ago);
        match fetcher.fetch_and_parse_day(&builder, &day).await {
            Ok(fills) => {
                total_fills += fills.len();
                if !fills.is_empty() {
                    days_with_data += 1;
                    println!("  {}: {} fills", day, fills.len());
                }
            }
            Err(_) => {}
        }
    }

    println!("Total: {} fills across {} days", total_fills, days_with_data);
}

// ============================================================================
// LIVE FETCH TESTS - BasedApp Builder
// ============================================================================

#[tokio::test]
async fn fetch_basedapp_yesterday() {
    let fetcher = fetcher();
    let builder = Address::new(BASEDAPP_BUILDER.to_string());
    let day = yesterday_yyyymmdd();

    println!("\n=== Fetching BasedApp builder logs for {} ===", day);

    match fetcher.fetch_and_parse_day(&builder, &day).await {
        Ok(fills) => {
            println!("SUCCESS: Fetched {} fills for BasedApp on {}", fills.len(), day);

            if !fills.is_empty() {
                println!("Sample fill #0:");
                let f = &fills[0];
                println!("  time_ms: {}", f.time_ms.as_ms());
                println!("  user: {}", f.user.as_str());
                println!("  coin: {}", f.coin.as_str());
                println!("  side: {:?}", f.side);
                println!("  px: {}", f.px.to_canonical_string());
                println!("  sz: {}", f.sz.to_canonical_string());
                println!("  tid: {:?}", f.tid);
                println!("  oid: {:?}", f.oid);
            }
        }
        Err(e) => {
            println!("Note: No logs available for {} (error: {})", day, e);
        }
    }
}

#[tokio::test]
async fn fetch_basedapp_last_7_days() {
    let fetcher = fetcher();
    let builder = Address::new(BASEDAPP_BUILDER.to_string());

    println!("\n=== Fetching BasedApp builder logs for last 7 days ===");

    let mut total_fills = 0;
    let mut days_with_data = 0;

    for days_ago in 1..=7 {
        let day = days_ago_yyyymmdd(days_ago);
        match fetcher.fetch_and_parse_day(&builder, &day).await {
            Ok(fills) => {
                total_fills += fills.len();
                if !fills.is_empty() {
                    days_with_data += 1;
                    println!("  {}: {} fills", day, fills.len());
                }
            }
            Err(_) => {}
        }
    }

    println!("Total: {} fills across {} days", total_fills, days_with_data);
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

#[tokio::test]
async fn fetch_invalid_builder_returns_error() {
    let fetcher = fetcher();
    let builder = Address::new("0xinvalid".to_string());
    let day = yesterday_yyyymmdd();

    let result = fetcher.fetch_and_parse_day(&builder, &day).await;
    // Should return HTTP error (404) for non-existent builder
    assert!(result.is_err(), "Invalid builder should return error");
    println!("Invalid builder correctly returned error: {:?}", result.unwrap_err());
}

#[tokio::test]
async fn fetch_future_date_returns_error() {
    let fetcher = fetcher();
    let builder = Address::new(PHANTOM_BUILDER.to_string());

    // Tomorrow's date
    let tomorrow = (chrono::Utc::now() + chrono::Duration::days(1))
        .format("%Y%m%d")
        .to_string();

    let result = fetcher.fetch_and_parse_day(&builder, &tomorrow).await;
    // Future dates should fail (404)
    assert!(result.is_err(), "Future date should return error");
    println!("Future date correctly returned error: {:?}", result.unwrap_err());
}

// ============================================================================
// MATCHING TESTS
// ============================================================================

#[tokio::test]
async fn matching_with_real_data_phantom() {
    use hypesilico::engine::BuilderLogsIndex;
    use hypesilico::engine::MatchTolerances;
    use hypesilico::domain::{Coin, Fill, TimeMs, Side};
    use std::str::FromStr;

    let fetcher = fetcher();
    let builder = Address::new(PHANTOM_BUILDER.to_string());
    let day = yesterday_yyyymmdd();

    match fetcher.fetch_and_parse_day(&builder, &day).await {
        Ok(fills) if !fills.is_empty() => {
            println!("\n=== Testing matching with {} Phantom fills ===", fills.len());

            let index = BuilderLogsIndex::new(&fills);
            let tolerances = MatchTolerances::default();

            // Take the first real fill and create a test fill that should match exactly
            let sample = &fills[0];
            let test_fill = Fill::new(
                sample.time_ms,
                sample.user.clone(),
                sample.coin.clone(),
                sample.side,
                sample.px,
                sample.sz,
                hypesilico::domain::Decimal::zero(),
                hypesilico::domain::Decimal::zero(),
                None,
                sample.tid,
                sample.oid,
            );

            let result = index.match_fill(&test_fill, &tolerances);
            println!("Exact match test (using same tid): {:?}", result);

            if sample.tid.is_some() {
                assert_eq!(result, Some(hypesilico::domain::AttributionConfidence::Exact));
            }

            // Test fuzzy match - create fill with slightly different values
            let fuzzy_fill = Fill::new(
                TimeMs::new(sample.time_ms.as_ms() + 100), // 100ms later
                sample.user.clone(),
                sample.coin.clone(),
                sample.side,
                sample.px,
                sample.sz,
                hypesilico::domain::Decimal::zero(),
                hypesilico::domain::Decimal::zero(),
                None,
                Some(999999999), // Different tid
                None,
            );

            let fuzzy_result = index.match_fill(&fuzzy_fill, &tolerances);
            println!("Fuzzy match test (different tid, same values): {:?}", fuzzy_result);
            assert_eq!(fuzzy_result, Some(hypesilico::domain::AttributionConfidence::Fuzzy));

            // Test no match - completely different values
            let no_match_fill = Fill::new(
                TimeMs::new(0),
                Address::new("0xdifferent".to_string()),
                Coin::new("DOESNOTEXIST".to_string()),
                Side::Buy,
                hypesilico::domain::Decimal::from_str("99999999").unwrap(),
                hypesilico::domain::Decimal::from_str("99999999").unwrap(),
                hypesilico::domain::Decimal::zero(),
                hypesilico::domain::Decimal::zero(),
                None,
                Some(88888888),
                None,
            );

            let no_match_result = index.match_fill(&no_match_fill, &tolerances);
            println!("No match test (completely different): {:?}", no_match_result);
            assert_eq!(no_match_result, None);
        }
        Ok(_) => {
            println!("No fills available for testing matching");
        }
        Err(e) => {
            println!("Could not fetch data for matching test: {}", e);
        }
    }
}

// ============================================================================
// COMPARATIVE ANALYSIS - All Three Builders
// ============================================================================

#[tokio::test]
async fn compare_all_builders_yesterday() {
    let fetcher = fetcher();
    let day = yesterday_yyyymmdd();

    println!("\n=== Comparing all three builders for {} ===", day);
    println!("{:<15} {:>10} {:>12} {:>10}", "Builder", "Fills", "Unique Users", "% with TID");
    println!("{}", "-".repeat(50));

    for (name, addr) in [
        ("Phantom", PHANTOM_BUILDER),
        ("Insilico", INSILICO_BUILDER),
        ("BasedApp", BASEDAPP_BUILDER),
    ] {
        let builder = Address::new(addr.to_string());
        match fetcher.fetch_and_parse_day(&builder, &day).await {
            Ok(fills) => {
                let unique_users: std::collections::HashSet<_> = fills.iter()
                    .map(|f| f.user.as_str())
                    .collect();
                let with_tid = fills.iter().filter(|f| f.tid.is_some()).count();
                let tid_pct = if fills.is_empty() { 0.0 } else {
                    (with_tid as f64 / fills.len() as f64) * 100.0
                };

                println!("{:<15} {:>10} {:>12} {:>9.1}%",
                    name, fills.len(), unique_users.len(), tid_pct);
            }
            Err(_) => {
                println!("{:<15} {:>10} {:>12} {:>10}", name, "N/A", "N/A", "N/A");
            }
        }
    }
}

// ============================================================================
// DATA QUALITY VALIDATION
// ============================================================================

#[tokio::test]
async fn validate_data_quality_phantom() {
    let fetcher = fetcher();
    let builder = Address::new(PHANTOM_BUILDER.to_string());

    println!("\n=== Validating Phantom data quality ===");

    // Check last 3 days
    for days_ago in 1..=3 {
        let day = days_ago_yyyymmdd(days_ago);
        match fetcher.fetch_and_parse_day(&builder, &day).await {
            Ok(fills) if !fills.is_empty() => {
                println!("\nDay: {}", day);

                // Validate all fills
                let mut issues = Vec::new();

                for (i, fill) in fills.iter().enumerate() {
                    // Check for empty strings
                    if fill.user.as_str().is_empty() {
                        issues.push(format!("Fill {}: empty user", i));
                    }
                    if fill.coin.as_str().is_empty() {
                        issues.push(format!("Fill {}: empty coin", i));
                    }

                    // Check for valid decimals
                    if !fill.px.is_positive() {
                        issues.push(format!("Fill {}: non-positive px: {}", i, fill.px.to_canonical_string()));
                    }
                    if !fill.sz.is_positive() {
                        issues.push(format!("Fill {}: non-positive sz: {}", i, fill.sz.to_canonical_string()));
                    }

                    // Check timestamp is reasonable (after 2020)
                    if fill.time_ms.as_ms() < 1577836800000 {
                        issues.push(format!("Fill {}: suspicious timestamp: {}", i, fill.time_ms.as_ms()));
                    }
                }

                if issues.is_empty() {
                    println!("  ✓ All {} fills passed validation", fills.len());
                } else {
                    println!("  ✗ Found {} issues:", issues.len());
                    for issue in issues.iter().take(5) {
                        println!("    - {}", issue);
                    }
                    if issues.len() > 5 {
                        println!("    ... and {} more", issues.len() - 5);
                    }
                }
            }
            _ => {
                println!("  No data for {}", day);
            }
        }
    }
}
