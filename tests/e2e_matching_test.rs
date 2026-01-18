//! Focused E2E matching test - validates fuzzy matching with real builder logs.
//!
//! This test validates that the matching logic works correctly by:
//! 1. Fetching real builder logs
//! 2. Creating simulated fills from log data
//! 3. Verifying matching works with slight variations

use hypesilico::datasource::builder_logs::{BuilderLogsFetcher, BuilderLogsSource};
use hypesilico::domain::{Address, AttributionConfidence, Coin, Decimal, Fill, Side, TimeMs};
use hypesilico::engine::{BuilderLogsIndex, MatchTolerances};
use reqwest::Client;
use std::str::FromStr;

const INSILICO_BUILDER: &str = "0x2868fc0d9786a740b491577a43502259efa78a39";

fn two_days_ago() -> String {
    let now = chrono::Utc::now();
    let date = now - chrono::Duration::days(2);
    date.format("%Y%m%d").to_string()
}

/// Create a Fill from a BuilderLogFill for testing
fn fill_from_log(log: &hypesilico::domain::BuilderLogFill) -> Fill {
    Fill::new(
        log.time_ms,
        log.user.clone(),
        log.coin.clone(),
        log.side,
        log.px,
        log.sz,
        Decimal::from_str("0").unwrap(),
        Decimal::from_str("0").unwrap(),
        None,
        None, // No tid - simulates real API data
        None,
    )
}

/// Create a Fill with slight variations to test fuzzy matching
fn fill_with_variation(
    log: &hypesilico::domain::BuilderLogFill,
    time_delta_ms: i64,
    px_delta: &str,
    sz_delta: &str,
) -> Fill {
    let new_time = TimeMs::new(log.time_ms.as_ms() + time_delta_ms);
    let new_px = log.px + Decimal::from_str(px_delta).unwrap();
    let new_sz = log.sz + Decimal::from_str(sz_delta).unwrap();

    Fill::new(
        new_time,
        log.user.clone(),
        log.coin.clone(),
        log.side,
        new_px,
        new_sz,
        Decimal::from_str("0").unwrap(),
        Decimal::from_str("0").unwrap(),
        None,
        None,
        None,
    )
}

#[tokio::test]
async fn test_exact_match_without_tid() {
    println!("\n=== Test: Exact Match Without TID ===\n");

    let day = two_days_ago();
    let fetcher = BuilderLogsFetcher::new(Client::new());
    let builder = Address::new(INSILICO_BUILDER.to_string());

    let logs = fetcher.fetch_and_parse_day(&builder, &day).await
        .expect("Failed to fetch builder logs");

    println!("Fetched {} builder log entries", logs.len());
    assert!(!logs.is_empty(), "Should have logs to test");

    // Take first 10 logs and create exact fills (no tid)
    let test_logs: Vec<_> = logs.iter().take(10).collect();
    let index = BuilderLogsIndex::new(&logs);
    let tolerances = MatchTolerances::default();

    let mut matches = 0;
    for log in &test_logs {
        let fill = fill_from_log(log);

        // Since there's no tid, it should fall back to fuzzy matching
        // But with exact same values, it should match
        let result = index.match_fill(&fill, &tolerances);

        if result.is_some() {
            matches += 1;
            println!("✓ Matched: {} {} {:?} px={} sz={}",
                log.user.as_str(),
                log.coin.as_str(),
                log.side,
                log.px.to_canonical_string(),
                log.sz.to_canonical_string()
            );
        } else {
            println!("✗ No match: {} {} {:?}",
                log.user.as_str(),
                log.coin.as_str(),
                log.side
            );
        }
    }

    println!("\nResult: {}/{} matched", matches, test_logs.len());
    assert_eq!(matches, test_logs.len(), "All exact fills should match");
    println!("\n✅ Exact match test PASSED!");
}

#[tokio::test]
async fn test_fuzzy_match_within_tolerances() {
    println!("\n=== Test: Fuzzy Match Within Tolerances ===\n");

    let day = two_days_ago();
    let fetcher = BuilderLogsFetcher::new(Client::new());
    let builder = Address::new(INSILICO_BUILDER.to_string());

    let logs = fetcher.fetch_and_parse_day(&builder, &day).await
        .expect("Failed to fetch builder logs");

    assert!(!logs.is_empty(), "Should have logs to test");

    let test_logs: Vec<_> = logs.iter().take(10).collect();
    let index = BuilderLogsIndex::new(&logs);
    let tolerances = MatchTolerances::default();

    println!("Testing with tolerances:");
    println!("  time_ms: ±{}", tolerances.time_ms);
    println!("  px_abs:  ±{}", tolerances.px_abs.to_canonical_string());
    println!("  sz_abs:  ±{}", tolerances.sz_abs.to_canonical_string());
    println!();

    // Test with variations within tolerance
    let variations = [
        (500, "0.0000005", "0.0000005"),   // Within tolerance
        (-500, "-0.0000005", "-0.0000005"), // Within tolerance (negative)
        (999, "0.0000009", "0.0000009"),    // At edge of tolerance
    ];

    for (time_delta, px_delta, sz_delta) in variations {
        println!("Testing variation: Δtime={}ms, Δpx={}, Δsz={}", time_delta, px_delta, sz_delta);

        let mut matches = 0;
        for log in &test_logs {
            let fill = fill_with_variation(log, time_delta, px_delta, sz_delta);
            let result = index.match_fill(&fill, &tolerances);
            if result.is_some() {
                matches += 1;
            }
        }

        println!("  Result: {}/{} matched", matches, test_logs.len());
        assert_eq!(matches, test_logs.len(),
            "All fills within tolerance should match");
    }

    println!("\n✅ Fuzzy match within tolerances test PASSED!");
}

#[tokio::test]
async fn test_no_match_outside_tolerances() {
    println!("\n=== Test: No Match Outside Tolerances ===\n");

    let day = two_days_ago();
    let fetcher = BuilderLogsFetcher::new(Client::new());
    let builder = Address::new(INSILICO_BUILDER.to_string());

    let logs = fetcher.fetch_and_parse_day(&builder, &day).await
        .expect("Failed to fetch builder logs");

    assert!(!logs.is_empty(), "Should have logs to test");

    let test_logs: Vec<_> = logs.iter().take(10).collect();
    let index = BuilderLogsIndex::new(&logs);
    let tolerances = MatchTolerances::default();

    // Test with variations OUTSIDE tolerance
    let outside_variations = [
        (2000, "0", "0"),        // Time too far
        (0, "0.001", "0"),       // Price too different
        (0, "0", "0.001"),       // Size too different
    ];

    for (time_delta, px_delta, sz_delta) in outside_variations {
        println!("Testing outside tolerance: Δtime={}ms, Δpx={}, Δsz={}",
            time_delta, px_delta, sz_delta);

        let mut no_matches = 0;
        for log in &test_logs {
            let fill = fill_with_variation(log, time_delta, px_delta, sz_delta);
            let result = index.match_fill(&fill, &tolerances);
            if result.is_none() {
                no_matches += 1;
            }
        }

        println!("  Result: {}/{} correctly rejected", no_matches, test_logs.len());
        assert_eq!(no_matches, test_logs.len(),
            "All fills outside tolerance should NOT match");
    }

    println!("\n✅ No match outside tolerances test PASSED!");
}

#[tokio::test]
async fn test_matching_statistics() {
    println!("\n=== Test: Matching Statistics with Real Data ===\n");

    let day = two_days_ago();
    let fetcher = BuilderLogsFetcher::new(Client::new());
    let builder = Address::new(INSILICO_BUILDER.to_string());

    let logs = fetcher.fetch_and_parse_day(&builder, &day).await
        .expect("Failed to fetch builder logs");

    println!("Total builder log entries: {}", logs.len());
    assert!(!logs.is_empty(), "Should have logs to test");

    let index = BuilderLogsIndex::new(&logs);
    let tolerances = MatchTolerances::default();

    // Create fills from ALL logs and test matching
    let mut exact_matches = 0;
    let mut fuzzy_matches = 0;
    let mut no_matches = 0;

    for log in &logs {
        let fill = fill_from_log(log);
        match index.match_fill(&fill, &tolerances) {
            Some(AttributionConfidence::Exact) => exact_matches += 1,
            Some(AttributionConfidence::Fuzzy) => fuzzy_matches += 1,
            None => no_matches += 1,
            _ => {}
        }
    }

    println!("\nMatching Results:");
    println!("  Exact matches:  {} (should be 0 - no tid in simulated fills)", exact_matches);
    println!("  Fuzzy matches:  {}", fuzzy_matches);
    println!("  No matches:     {}", no_matches);

    let total = logs.len();
    let match_rate = (fuzzy_matches as f64 / total as f64) * 100.0;
    println!("\n  Match rate: {:.1}%", match_rate);

    // All fills created from logs should match via fuzzy matching
    assert_eq!(fuzzy_matches, total,
        "All fills created from logs should fuzzy match");
    assert_eq!(no_matches, 0,
        "No fills should fail to match");

    println!("\n✅ Matching statistics test PASSED!");
    println!("\n=== CONCLUSION ===");
    println!("The fuzzy matching system correctly matches {} fills", total);
    println!("against real Hyperliquid builder logs without relying on tid.");
}
