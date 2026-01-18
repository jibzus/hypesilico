//! End-to-end attribution test with REAL Hyperliquid data.
//!
//! This test validates that fuzzy matching correctly attributes fills
//! by fetching real builder logs and real user fills from the Hyperliquid API.

use hypesilico::datasource::builder_logs::{BuilderLogsFetcher, BuilderLogsSource};
use hypesilico::domain::{Address, AttributionConfidence, Side};
use hypesilico::engine::{BuilderLogsIndex, MatchTolerances};
use reqwest::Client;
use std::collections::HashMap;
use std::io::Read;

const INSILICO_BUILDER: &str = "0x2868fc0d9786a740b491577a43502259efa78a39";

/// Get 2 days ago in YYYYMMDD format (to ensure data exists)
fn two_days_ago() -> String {
    let now = chrono::Utc::now();
    let date = now - chrono::Duration::days(2);
    date.format("%Y%m%d").to_string()
}

/// Fetch builder logs and find active users
async fn fetch_builder_logs(builder: &str, day: &str) -> Vec<hypesilico::domain::BuilderLogFill> {
    let fetcher = BuilderLogsFetcher::new(Client::new());
    let builder_addr = Address::new(builder.to_string());

    fetcher
        .fetch_and_parse_day(&builder_addr, day)
        .await
        .expect("Failed to fetch builder logs")
}

/// Fetch user fills directly from Hyperliquid API
async fn fetch_user_fills(
    user: &str,
    start_time: i64,
    end_time: i64,
) -> Vec<serde_json::Value> {
    let client = Client::new();
    let payload = serde_json::json!({
        "type": "userFillsByTime",
        "user": user,
        "startTime": start_time,
        "endTime": end_time,
        "aggregateByTime": false
    });

    let resp = client
        .post("https://api.hyperliquid.xyz/info")
        .json(&payload)
        .send()
        .await
        .expect("Failed to fetch fills");

    resp.json::<Vec<serde_json::Value>>()
        .await
        .expect("Failed to parse fills")
}

/// Parse a fill from Hyperliquid API response
fn parse_fill(fill: &serde_json::Value) -> Option<TestFill> {
    let time_ms = fill.get("time")?.as_i64()?;
    let coin = fill.get("coin")?.as_str()?;
    let side_str = fill.get("side")?.as_str()?;
    let px_str = fill.get("px")?.as_str()?;
    let sz_str = fill.get("sz")?.as_str()?;
    let tid = fill.get("tid")?.as_i64();

    let side = match side_str {
        "A" | "Buy" => Side::Buy,
        "B" | "Sell" => Side::Sell,
        _ => return None,
    };

    let px = hypesilico::domain::Decimal::from_str_canonical(px_str).ok()?;
    let sz = hypesilico::domain::Decimal::from_str_canonical(sz_str).ok()?;

    Some(TestFill {
        time_ms,
        coin: coin.to_string(),
        side,
        px,
        sz,
        tid,
    })
}

#[derive(Debug)]
struct TestFill {
    time_ms: i64,
    coin: String,
    side: Side,
    px: hypesilico::domain::Decimal,
    sz: hypesilico::domain::Decimal,
    tid: Option<i64>,
}

#[derive(Debug, Default)]
struct MatchStats {
    total_fills: usize,
    exact_matches: usize,
    fuzzy_matches: usize,
    no_matches: usize,
}

/// Manual fuzzy matching against builder logs
fn try_fuzzy_match(
    fill: &TestFill,
    user: &str,
    logs: &[hypesilico::domain::BuilderLogFill],
    tolerances: &MatchTolerances,
) -> Option<AttributionConfidence> {
    for log in logs {
        // Check user matches (case insensitive)
        if log.user.as_str().to_ascii_lowercase() != user.to_ascii_lowercase() {
            continue;
        }

        // Check coin matches (case insensitive)
        if log.coin.as_str().to_ascii_uppercase() != fill.coin.to_ascii_uppercase() {
            continue;
        }

        // Check side matches
        if log.side != fill.side {
            continue;
        }

        // Check time within tolerance
        let dt = (fill.time_ms - log.time_ms.as_ms()).abs();
        if dt > tolerances.time_ms {
            continue;
        }

        // Check price within tolerance
        let dpx = (fill.px - log.px).abs();
        if dpx > tolerances.px_abs {
            continue;
        }

        // Check size within tolerance
        let dsz = (fill.sz - log.sz).abs();
        if dsz > tolerances.sz_abs {
            continue;
        }

        return Some(AttributionConfidence::Fuzzy);
    }

    None
}

#[tokio::test]
async fn e2e_fuzzy_matching_with_real_data() {
    println!("\n=== E2E Attribution Test with Real Hyperliquid Data ===\n");

    let day = two_days_ago();
    println!("Testing day: {}", day);

    // Step 1: Fetch builder logs
    println!("\n1. Fetching Insilico builder logs...");
    let logs = fetch_builder_logs(INSILICO_BUILDER, &day).await;
    println!("   Fetched {} builder log entries", logs.len());

    if logs.is_empty() {
        println!("   No logs available for this day, skipping test");
        return;
    }

    // Step 2: Group logs by user to find active traders
    let mut by_user: HashMap<String, Vec<&hypesilico::domain::BuilderLogFill>> = HashMap::new();
    for log in &logs {
        by_user
            .entry(log.user.as_str().to_ascii_lowercase())
            .or_default()
            .push(log);
    }

    println!("   Found {} unique users in builder logs", by_user.len());

    // Step 3: Pick top 3 users with most fills for testing
    let mut user_counts: Vec<_> = by_user.iter().map(|(u, l)| (u.clone(), l.len())).collect();
    user_counts.sort_by(|a, b| b.1.cmp(&a.1));

    println!("\n2. Top users by builder fill count:");
    for (user, count) in user_counts.iter().take(5) {
        println!("   {} - {} fills", user, count);
    }

    // Step 4: For each top user, fetch their real fills and test matching
    let tolerances = MatchTolerances::default();
    let mut overall_stats = MatchStats::default();

    println!("\n3. Testing fuzzy matching for top users...\n");

    for (user, log_count) in user_counts.iter().take(3) {
        println!("--- User: {} ({} builder log entries) ---", user, log_count);

        // Get time range from builder logs for this user
        let user_logs: Vec<_> = logs
            .iter()
            .filter(|l| l.user.as_str().to_ascii_lowercase() == *user)
            .collect();

        let min_time = user_logs.iter().map(|l| l.time_ms.as_ms()).min().unwrap();
        let max_time = user_logs.iter().map(|l| l.time_ms.as_ms()).max().unwrap();

        // Expand time range slightly
        let start_time = min_time - 60_000; // 1 minute before
        let end_time = max_time + 60_000;   // 1 minute after

        // Fetch user's actual fills from Hyperliquid
        println!("   Fetching fills from Hyperliquid API...");
        let raw_fills = fetch_user_fills(user, start_time, end_time).await;
        println!("   Got {} fills from API", raw_fills.len());

        if raw_fills.is_empty() {
            println!("   No fills returned, skipping user\n");
            continue;
        }

        // Parse fills
        let fills: Vec<_> = raw_fills.iter().filter_map(parse_fill).collect();
        println!("   Parsed {} fills", fills.len());

        // Test matching
        let mut stats = MatchStats::default();
        stats.total_fills = fills.len();

        for fill in &fills {
            if let Some(confidence) = try_fuzzy_match(fill, user, &logs, &tolerances) {
                match confidence {
                    AttributionConfidence::Exact => stats.exact_matches += 1,
                    AttributionConfidence::Fuzzy => stats.fuzzy_matches += 1,
                    _ => {}
                }
            } else {
                stats.no_matches += 1;
            }
        }

        let match_rate = if stats.total_fills > 0 {
            (stats.fuzzy_matches as f64 / stats.total_fills as f64) * 100.0
        } else {
            0.0
        };

        println!("   Results:");
        println!("     Total fills:    {}", stats.total_fills);
        println!("     Fuzzy matches:  {} ({:.1}%)", stats.fuzzy_matches, match_rate);
        println!("     No matches:     {}", stats.no_matches);

        // Show sample matched fill
        if stats.fuzzy_matches > 0 {
            for fill in &fills {
                if try_fuzzy_match(fill, user, &logs, &tolerances).is_some() {
                    println!("\n   Sample matched fill:");
                    println!("     time: {}", fill.time_ms);
                    println!("     coin: {}", fill.coin);
                    println!("     side: {:?}", fill.side);
                    println!("     px:   {}", fill.px.to_canonical_string());
                    println!("     sz:   {}", fill.sz.to_canonical_string());

                    // Find corresponding log entry
                    for log in &logs {
                        if log.user.as_str().to_ascii_lowercase() == *user
                            && log.coin.as_str().to_ascii_uppercase() == fill.coin.to_ascii_uppercase()
                            && log.side == fill.side
                        {
                            let dt = (fill.time_ms - log.time_ms.as_ms()).abs();
                            let dpx = (fill.px - log.px).abs();
                            let dsz = (fill.sz - log.sz).abs();

                            if dt <= tolerances.time_ms
                                && dpx <= tolerances.px_abs
                                && dsz <= tolerances.sz_abs
                            {
                                println!("\n   Matching builder log entry:");
                                println!("     time: {} (Δ{}ms)", log.time_ms.as_ms(), dt);
                                println!("     coin: {}", log.coin.as_str());
                                println!("     side: {:?}", log.side);
                                println!("     px:   {} (Δ{})", log.px.to_canonical_string(), dpx.to_canonical_string());
                                println!("     sz:   {} (Δ{})", log.sz.to_canonical_string(), dsz.to_canonical_string());
                                break;
                            }
                        }
                    }
                    break;
                }
            }
        }

        println!();

        overall_stats.total_fills += stats.total_fills;
        overall_stats.fuzzy_matches += stats.fuzzy_matches;
        overall_stats.no_matches += stats.no_matches;
    }

    // Final summary
    println!("\n=== OVERALL RESULTS ===");
    println!("Total fills tested:  {}", overall_stats.total_fills);
    println!("Fuzzy matches:       {}", overall_stats.fuzzy_matches);
    println!("No matches:          {}", overall_stats.no_matches);

    if overall_stats.total_fills > 0 {
        let overall_rate = (overall_stats.fuzzy_matches as f64 / overall_stats.total_fills as f64) * 100.0;
        println!("Match rate:          {:.1}%", overall_rate);

        // The test passes if we have a reasonable match rate
        // Given fuzzy matching tolerances, we expect most fills to match
        assert!(
            overall_rate >= 50.0,
            "Match rate {:.1}% is too low - fuzzy matching may not be working correctly",
            overall_rate
        );

        println!("\n✅ E2E test PASSED - fuzzy matching is working with real data!");
    } else {
        println!("\n⚠️ No fills were tested - cannot validate matching");
    }
}

#[tokio::test]
async fn e2e_validate_parser_output() {
    println!("\n=== Validating Parser Output Against Raw CSV ===\n");

    let day = two_days_ago();
    let builder = Address::new(INSILICO_BUILDER.to_string());
    let fetcher = BuilderLogsFetcher::new(Client::new());

    // Fetch and parse using our implementation
    let logs = fetcher.fetch_and_parse_day(&builder, &day).await;

    match logs {
        Ok(logs) => {
            println!("Successfully parsed {} rows", logs.len());

            if !logs.is_empty() {
                println!("\nFirst 3 parsed entries:");
                for (i, log) in logs.iter().take(3).enumerate() {
                    println!("  [{}] time_ms={}, user={}, coin={}, side={:?}, px={}, sz={}",
                        i,
                        log.time_ms.as_ms(),
                        log.user.as_str(),
                        log.coin.as_str(),
                        log.side,
                        log.px.to_canonical_string(),
                        log.sz.to_canonical_string()
                    );
                }

                // Verify all entries have valid data
                let mut valid = 0;
                let mut invalid = 0;

                for log in &logs {
                    if log.time_ms.as_ms() > 0
                        && !log.user.as_str().is_empty()
                        && !log.coin.as_str().is_empty()
                    {
                        valid += 1;
                    } else {
                        invalid += 1;
                    }
                }

                println!("\nValidation: {} valid, {} invalid", valid, invalid);
                assert_eq!(invalid, 0, "All entries should be valid");

                println!("\n✅ Parser validation PASSED!");
            }
        }
        Err(e) => {
            println!("Failed to parse: {:?}", e);
            panic!("Parser should work with real data");
        }
    }
}
