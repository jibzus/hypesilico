use hypesilico::config::{BuilderAttributionMode, Config, PnlMode};
use hypesilico::datasource::{BuilderLogsError, BuilderLogsSource};
use hypesilico::db::migrations::init_db;
use hypesilico::domain::{Address, BuilderLogFill, Coin, Decimal, Fill, Side, TimeMs};
use hypesilico::orchestration::attribution::AttributionIngestor;
use std::collections::HashMap;
use std::str::FromStr;
use tempfile::TempDir;
use chrono::TimeZone;

#[derive(Debug)]
struct MockLogsSource {
    by_day: HashMap<String, Vec<BuilderLogFill>>,
}

#[async_trait::async_trait]
impl BuilderLogsSource for MockLogsSource {
    async fn fetch_and_parse_day(
        &self,
        _builder: &Address,
        yyyymmdd: &str,
    ) -> Result<Vec<BuilderLogFill>, BuilderLogsError> {
        Ok(self
            .by_day
            .get(yyyymmdd)
            .cloned()
            .unwrap_or_default())
    }
}

async fn setup_repo() -> (hypesilico::Repository, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("test.db")
        .to_string_lossy()
        .to_string();
    let pool = init_db(&db_path).await.expect("init_db failed");
    (hypesilico::Repository::new(pool), temp_dir)
}

fn cfg(mode: BuilderAttributionMode) -> Config {
    Config {
        port: 0,
        database_path: ":memory:".to_string(),
        hyperliquid_api_url: "http://example.invalid".to_string(),
        target_builder: "0xbuilder".to_string(),
        builder_attribution_mode: mode,
        pnl_mode: PnlMode::Gross,
        lookback_ms: 0,
        leaderboard_users: vec![],
    }
}

fn day_of(ms: i64) -> String {
    chrono::Utc
        .timestamp_millis_opt(ms)
        .single()
        .unwrap()
        .format("%Y%m%d")
        .to_string()
}

fn make_fill(time_ms: i64, tid: i64, px: &str, sz: &str, builder_fee: Option<&str>) -> Fill {
    Fill::new(
        TimeMs::new(time_ms),
        Address::new("0xabc".to_string()),
        Coin::new("BTC".to_string()),
        Side::Buy,
        Decimal::from_str(px).unwrap(),
        Decimal::from_str(sz).unwrap(),
        Decimal::from_str("0").unwrap(),
        Decimal::from_str("0").unwrap(),
        builder_fee.map(|s| Decimal::from_str(s).unwrap()),
        Some(tid),
        None,
    )
}

#[tokio::test]
async fn logs_mode_persists_exact_and_fuzzy_matches() {
    let (repo, _tmp) = setup_repo().await;

    let t0 = 1_700_000_000_000;
    let day = day_of(t0);

    let fill_exact = make_fill(t0, 42, "100", "1", None);
    let fill_fuzzy = make_fill(t0 + 500, 999, "100.0000004", "1.0000004", None);
    let fill_unmatched = make_fill(t0 + 5_000, 77, "101", "2", None);

    repo.insert_fill(&fill_exact).await.unwrap();
    repo.insert_fill(&fill_fuzzy).await.unwrap();
    repo.insert_fill(&fill_unmatched).await.unwrap();

    let logs = vec![
        BuilderLogFill {
            time_ms: fill_exact.time_ms,
            user: fill_exact.user.clone(),
            coin: fill_exact.coin.clone(),
            side: fill_exact.side,
            px: fill_exact.px,
            sz: fill_exact.sz,
            tid: fill_exact.tid,
            oid: None,
        },
        BuilderLogFill {
            time_ms: TimeMs::new(t0 + 450),
            user: fill_fuzzy.user.clone(),
            coin: fill_fuzzy.coin.clone(),
            side: fill_fuzzy.side,
            px: Decimal::from_str("100.0000005").unwrap(),
            sz: Decimal::from_str("1.0000005").unwrap(),
            tid: Some(1),
            oid: None,
        },
    ];

    let logs_source = MockLogsSource {
        by_day: HashMap::from([(day, logs)]),
    };

    let ingestor = AttributionIngestor::default();
    let inserted = ingestor
        .ingest_window(
            &repo,
            &logs_source,
            &cfg(BuilderAttributionMode::Logs),
            &fill_exact.user,
            Some(&fill_exact.coin),
            Some(TimeMs::new(t0 - 1)),
            Some(TimeMs::new(t0 + 10_000)),
        )
        .await
        .unwrap();
    assert_eq!(inserted, 3);

    let keys = vec![
        fill_exact.fill_key.clone(),
        fill_fuzzy.fill_key.clone(),
        fill_unmatched.fill_key.clone(),
    ];
    let rows = repo.query_attributions(&keys).await.unwrap();
    let map: HashMap<String, (bool, String, String, Option<String>)> = rows
        .into_iter()
        .map(|(k, a, m, c, b)| (k, (a, m, c, b)))
        .collect();

    assert_eq!(
        map.get(&fill_exact.fill_key).unwrap(),
        &(true, "logs".to_string(), "exact".to_string(), Some("0xbuilder".to_string()))
    );
    assert_eq!(
        map.get(&fill_fuzzy.fill_key).unwrap(),
        &(true, "logs".to_string(), "fuzzy".to_string(), Some("0xbuilder".to_string()))
    );
    assert_eq!(
        map.get(&fill_unmatched.fill_key).unwrap(),
        &(false, "logs".to_string(), "exact".to_string(), None)
    );
}

#[tokio::test]
async fn auto_mode_prefers_logs_but_falls_back_to_heuristic() {
    let (repo, _tmp) = setup_repo().await;

    let t0 = 1_700_000_000_000;
    let day = day_of(t0);

    let fill_logs = make_fill(t0, 42, "100", "1", None);
    let fill_heuristic = make_fill(t0 + 5_000, 77, "101", "2", Some("0.5"));

    repo.insert_fill(&fill_logs).await.unwrap();
    repo.insert_fill(&fill_heuristic).await.unwrap();

    let logs_source = MockLogsSource {
        by_day: HashMap::from([(
            day,
            vec![BuilderLogFill {
                time_ms: fill_logs.time_ms,
                user: fill_logs.user.clone(),
                coin: fill_logs.coin.clone(),
                side: fill_logs.side,
                px: fill_logs.px,
                sz: fill_logs.sz,
                tid: fill_logs.tid,
                oid: None,
            }],
        )]),
    };

    let ingestor = AttributionIngestor::default();
    ingestor
        .ingest_window(
            &repo,
            &logs_source,
            &cfg(BuilderAttributionMode::Auto),
            &fill_logs.user,
            Some(&fill_logs.coin),
            Some(TimeMs::new(t0 - 1)),
            Some(TimeMs::new(t0 + 10_000)),
        )
        .await
        .unwrap();

    let rows = repo
        .query_attributions(&vec![fill_logs.fill_key.clone(), fill_heuristic.fill_key.clone()])
        .await
        .unwrap();
    let map: HashMap<String, (bool, String, String, Option<String>)> = rows
        .into_iter()
        .map(|(k, a, m, c, b)| (k, (a, m, c, b)))
        .collect();

    assert_eq!(
        map.get(&fill_logs.fill_key).unwrap(),
        &(true, "logs".to_string(), "exact".to_string(), Some("0xbuilder".to_string()))
    );
    assert_eq!(
        map.get(&fill_heuristic.fill_key).unwrap(),
        &(true, "heuristic".to_string(), "low".to_string(), None)
    );
}
