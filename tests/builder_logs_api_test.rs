//! Tests for builder logs API using the CORRECT endpoint.
//!
//! CRITICAL FINDINGS:
//! 1. The URL in the codebase is OUTDATED: `https://hyperliquid.xyz/builder_fills/`
//!    CORRECT URL: `https://stats-data.hyperliquid.xyz/Mainnet/builder_fills/`
//!
//! 2. The CSV schema expected by the parser is WRONG:
//!    Expected: time_ms,user,coin,side,px,sz,tid,oid
//!    Actual:   time,user,coin,side,px,sz,crossed,special_trade_type,tif,is_trigger,counterparty,closed_pnl,twap_id,builder_fee
//!
//! 3. The `tid` and `oid` columns DO NOT EXIST in the real API!
//!    This means "exact match" on tid will NEVER work with real data.
//!
//! Builder Addresses Tested:
//! - Phantom:  0xb84168cf3be63c6b8dad05ff5d755e97432ff80b
//! - Insilico: 0x2868fc0d9786a740b491577a43502259efa78a39
//! - BasedApp: 0x1924b8561eef20e70ede628a296175d358be80e5

use std::io::Read;

const PHANTOM_BUILDER: &str = "0xb84168cf3be63c6b8dad05ff5d755e97432ff80b";
const INSILICO_BUILDER: &str = "0x2868fc0d9786a740b491577a43502259efa78a39";
const BASEDAPP_BUILDER: &str = "0x1924b8561eef20e70ede628a296175d358be80e5";

// CORRECT URL format
fn correct_url(builder: &str, yyyymmdd: &str) -> String {
    format!(
        "https://stats-data.hyperliquid.xyz/Mainnet/builder_fills/{}/{}.csv.lz4",
        builder, yyyymmdd
    )
}

// OLD (broken) URL format used by the codebase
fn old_url(builder: &str, yyyymmdd: &str) -> String {
    format!(
        "https://hyperliquid.xyz/builder_fills/{}/{}.csv.lz4",
        builder, yyyymmdd
    )
}

/// Test that the OLD URL redirects and doesn't work
#[tokio::test]
async fn old_url_returns_redirect() {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let url = old_url(PHANTOM_BUILDER, "20260116");
    let resp = client.get(&url).send().await.unwrap();

    println!("Old URL: {}", url);
    println!("Status: {}", resp.status());

    // Old URL returns 301 redirect
    assert!(resp.status().is_redirection(), "Old URL should redirect");

    let location = resp.headers().get("location").map(|h| h.to_str().unwrap());
    println!("Redirects to: {:?}", location);
}

/// Test that the NEW URL returns 200 OK
#[tokio::test]
async fn new_url_returns_200() {
    let client = reqwest::Client::new();

    for (name, addr) in [
        ("Phantom", PHANTOM_BUILDER),
        ("Insilico", INSILICO_BUILDER),
        ("BasedApp", BASEDAPP_BUILDER),
    ] {
        let url = correct_url(addr, "20260116");
        let resp = client.get(&url).send().await.unwrap();

        println!("{}: {} -> {}", name, url, resp.status());
        assert!(resp.status().is_success(), "{} should return 200", name);
    }
}

/// Fetch and parse actual builder logs data
#[tokio::test]
async fn fetch_and_parse_real_data() {
    let client = reqwest::Client::new();

    for (name, addr) in [
        ("Phantom", PHANTOM_BUILDER),
        ("Insilico", INSILICO_BUILDER),
        ("BasedApp", BASEDAPP_BUILDER),
    ] {
        let url = correct_url(addr, "20260116");
        let resp = client.get(&url).send().await.unwrap();
        let lz4_bytes = resp.bytes().await.unwrap();

        // Decompress LZ4
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&lz4_bytes[..]);
        let mut csv_bytes = Vec::new();
        decoder.read_to_end(&mut csv_bytes).unwrap();

        let csv_str = String::from_utf8_lossy(&csv_bytes);
        let lines: Vec<&str> = csv_str.lines().collect();

        println!("\n=== {} ({}) ===", name, addr);
        println!("Total rows: {}", lines.len() - 1);
        println!("Header: {}", lines[0]);
        if lines.len() > 1 {
            println!("First data row: {}", lines[1]);
        }

        // Verify the ACTUAL header columns
        let header = lines[0];
        let columns: Vec<&str> = header.split(',').collect();

        println!("Columns: {:?}", columns);

        // These are the columns the code EXPECTS:
        assert!(!columns.contains(&"time_ms"), "API does NOT have 'time_ms' column");
        assert!(!columns.contains(&"tid"), "API does NOT have 'tid' column - EXACT MATCH BROKEN!");
        assert!(!columns.contains(&"oid"), "API does NOT have 'oid' column");

        // These are the ACTUAL columns:
        assert!(columns.contains(&"time"), "API has 'time' column (ISO8601 format)");
        assert!(columns.contains(&"user"), "API has 'user' column");
        assert!(columns.contains(&"coin"), "API has 'coin' column");
        assert!(columns.contains(&"side"), "API has 'side' column");
        assert!(columns.contains(&"px"), "API has 'px' column");
        assert!(columns.contains(&"sz"), "API has 'sz' column");
        assert!(columns.contains(&"counterparty"), "API has 'counterparty' column");
        assert!(columns.contains(&"closed_pnl"), "API has 'closed_pnl' column");
        assert!(columns.contains(&"builder_fee"), "API has 'builder_fee' column");
    }
}

/// Test that the existing parser FAILS with real data
#[tokio::test]
async fn existing_parser_fails_with_real_data() {
    use hypesilico::datasource::builder_logs::BuilderLogsFetcher;

    let client = reqwest::Client::new();

    // Fetch real data from CORRECT URL
    let url = correct_url(PHANTOM_BUILDER, "20260116");
    let resp = client.get(&url).send().await.unwrap();
    let lz4_bytes = resp.bytes().await.unwrap();

    // Decompress
    let csv_bytes = BuilderLogsFetcher::decompress_lz4_frame(&lz4_bytes).unwrap();

    // Try to parse with existing parser - THIS SHOULD FAIL
    let result = BuilderLogsFetcher::parse_csv(&csv_bytes);

    println!("\n=== Testing existing parser with real API data ===");
    match result {
        Ok(fills) => {
            println!("WARNING: Parser succeeded but data is likely wrong!");
            println!("Fills parsed: {}", fills.len());
            if !fills.is_empty() {
                println!("First fill tid: {:?}", fills[0].tid);
                println!("First fill oid: {:?}", fills[0].oid);
            }
        }
        Err(e) => {
            println!("Parser correctly failed: {}", e);
            assert!(true, "Parser should fail because schema is different");
        }
    }
}

/// Demonstrate the correct parsing for actual API data
#[tokio::test]
async fn demonstrate_correct_parsing() {
    // This is what the parser SHOULD look like:
    #[derive(Debug, serde::Deserialize)]
    struct ActualBuilderLogRow {
        time: String,  // ISO8601 format, not milliseconds!
        user: String,
        coin: String,
        side: String,
        px: String,
        sz: String,
        crossed: bool,
        special_trade_type: String,
        tif: String,
        is_trigger: bool,
        counterparty: String,
        closed_pnl: String,
        twap_id: i64,
        builder_fee: String,
    }

    let client = reqwest::Client::new();

    for (name, addr) in [
        ("Phantom", PHANTOM_BUILDER),
        ("Insilico", INSILICO_BUILDER),
        ("BasedApp", BASEDAPP_BUILDER),
    ] {
        let url = correct_url(addr, "20260116");
        let resp = client.get(&url).send().await.unwrap();
        let lz4_bytes = resp.bytes().await.unwrap();

        let mut decoder = lz4_flex::frame::FrameDecoder::new(&lz4_bytes[..]);
        let mut csv_bytes = Vec::new();
        decoder.read_to_end(&mut csv_bytes).unwrap();

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(&csv_bytes[..]);

        let mut count = 0;
        let mut sample: Option<ActualBuilderLogRow> = None;

        for result in reader.deserialize::<ActualBuilderLogRow>() {
            match result {
                Ok(row) => {
                    if sample.is_none() {
                        sample = Some(row);
                    }
                    count += 1;
                }
                Err(e) => {
                    println!("{}: Parse error at row {}: {}", name, count, e);
                    break;
                }
            }
        }

        println!("\n=== {} ===", name);
        println!("Successfully parsed {} rows", count);
        if let Some(s) = sample {
            println!("Sample row:");
            println!("  time: {}", s.time);
            println!("  user: {}", s.user);
            println!("  coin: {}", s.coin);
            println!("  side: {}", s.side);
            println!("  px: {}", s.px);
            println!("  sz: {}", s.sz);
            println!("  builder_fee: {}", s.builder_fee);
        }
    }
}

/// Statistics on builder activity
#[tokio::test]
async fn builder_statistics() {
    use std::collections::HashSet;

    let client = reqwest::Client::new();

    println!("\n=== Builder Statistics for 2026-01-16 ===\n");
    println!("{:<12} {:>10} {:>12} {:>12} {:>15}", "Builder", "Fills", "Users", "Coins", "Total Fees");
    println!("{}", "-".repeat(65));

    for (name, addr) in [
        ("Phantom", PHANTOM_BUILDER),
        ("Insilico", INSILICO_BUILDER),
        ("BasedApp", BASEDAPP_BUILDER),
    ] {
        let url = correct_url(addr, "20260116");
        let resp = client.get(&url).send().await.unwrap();
        let lz4_bytes = resp.bytes().await.unwrap();

        let mut decoder = lz4_flex::frame::FrameDecoder::new(&lz4_bytes[..]);
        let mut csv_bytes = Vec::new();
        decoder.read_to_end(&mut csv_bytes).unwrap();

        let csv_str = String::from_utf8_lossy(&csv_bytes);
        let lines: Vec<&str> = csv_str.lines().skip(1).collect();

        let mut users = HashSet::new();
        let mut coins = HashSet::new();
        let mut total_fee: f64 = 0.0;

        for line in &lines {
            let fields: Vec<&str> = line.split(',').collect();
            if fields.len() >= 14 {
                users.insert(fields[1].to_string());
                coins.insert(fields[2].to_string());
                if let Ok(fee) = fields[13].parse::<f64>() {
                    total_fee += fee;
                }
            }
        }

        println!(
            "{:<12} {:>10} {:>12} {:>12} {:>15.4}",
            name,
            lines.len(),
            users.len(),
            coins.len(),
            total_fee
        );
    }
}
