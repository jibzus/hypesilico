//! Fetching and parsing Hyperliquid builder fills logs.

use crate::domain::{Address, BuilderLogFill, Coin, Decimal, Side, TimeMs};
use async_trait::async_trait;
use std::io::Read;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BuilderLogsError {
    #[error("http error: {0}")]
    Http(String),
    #[error("unexpected http status: {0}")]
    HttpStatus(u16),
    #[error("lz4 decode error: {0}")]
    Lz4(String),
    #[error("csv parse error: {0}")]
    Csv(String),
}

#[derive(Debug, Clone)]
pub struct BuilderLogsFetcher {
    client: reqwest::Client,
}

#[async_trait]
pub trait BuilderLogsSource: Send + Sync {
    async fn fetch_and_parse_day(
        &self,
        builder: &Address,
        yyyymmdd: &str,
    ) -> Result<Vec<BuilderLogFill>, BuilderLogsError>;
}

impl BuilderLogsFetcher {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub fn builder_logs_url(builder: &Address, yyyymmdd: &str) -> String {
        format!(
            "https://stats-data.hyperliquid.xyz/Mainnet/builder_fills/{}/{}.csv.lz4",
            builder.as_str(),
            yyyymmdd
        )
    }

    pub async fn fetch_lz4_bytes(
        &self,
        builder: &Address,
        yyyymmdd: &str,
    ) -> Result<Vec<u8>, BuilderLogsError> {
        let url = Self::builder_logs_url(builder, yyyymmdd);
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| BuilderLogsError::Http(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            return Err(BuilderLogsError::HttpStatus(status.as_u16()));
        }

        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| BuilderLogsError::Http(e.to_string()))
    }

    pub fn decompress_lz4_frame(lz4_bytes: &[u8]) -> Result<Vec<u8>, BuilderLogsError> {
        let mut decoder = lz4_flex::frame::FrameDecoder::new(lz4_bytes);
        let mut out = Vec::new();
        decoder
            .read_to_end(&mut out)
            .map_err(|e| BuilderLogsError::Lz4(e.to_string()))?;
        Ok(out)
    }

    pub fn parse_csv(csv_bytes: &[u8]) -> Result<Vec<BuilderLogFill>, BuilderLogsError> {
        // Actual API schema:
        // time,user,coin,side,px,sz,crossed,special_trade_type,tif,is_trigger,counterparty,closed_pnl,twap_id,builder_fee
        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct Row {
            time: String, // ISO8601 format: "2024-12-15T10:30:45.123Z"
            user: String,
            coin: String,
            side: String,
            px: String,
            sz: String,
            // Additional columns from actual API (we ignore them but need to handle them)
            #[serde(default)]
            crossed: Option<String>,
            #[serde(default)]
            special_trade_type: Option<String>,
            #[serde(default)]
            tif: Option<String>,
            #[serde(default)]
            is_trigger: Option<String>,
            #[serde(default)]
            counterparty: Option<String>,
            #[serde(default)]
            closed_pnl: Option<String>,
            #[serde(default)]
            twap_id: Option<String>,
            #[serde(default)]
            builder_fee: Option<String>,
        }

        fn parse_side(s: &str) -> Option<Side> {
            match s.trim().to_ascii_lowercase().as_str() {
                "a" | "buy" | "bid" => Some(Side::Buy),
                "b" | "sell" | "ask" => Some(Side::Sell),
                _ => None,
            }
        }

        /// Parse ISO8601 time string to milliseconds since epoch.
        /// Expects format: "2024-12-15T10:30:45.123Z" or similar.
        fn parse_time_to_ms(time_str: &str) -> Result<i64, BuilderLogsError> {
            use chrono::{DateTime, Utc};
            let dt: DateTime<Utc> = time_str
                .parse()
                .map_err(|e| BuilderLogsError::Csv(format!("invalid time '{}': {}", time_str, e)))?;
            Ok(dt.timestamp_millis())
        }

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(csv_bytes);

        let mut fills = Vec::new();
        for record in reader.deserialize::<Row>() {
            let row = record.map_err(|e| BuilderLogsError::Csv(e.to_string()))?;
            let time_ms = parse_time_to_ms(&row.time)?;
            let side = parse_side(&row.side)
                .ok_or_else(|| BuilderLogsError::Csv(format!("invalid side: {}", row.side)))?;
            let px = Decimal::from_str_canonical(&row.px)
                .map_err(|e| BuilderLogsError::Csv(format!("invalid px: {}", e)))?;
            let sz = Decimal::from_str_canonical(&row.sz)
                .map_err(|e| BuilderLogsError::Csv(format!("invalid sz: {}", e)))?;

            fills.push(BuilderLogFill {
                time_ms: TimeMs::new(time_ms),
                user: Address::new(row.user),
                coin: Coin::new(row.coin),
                side,
                px,
                sz,
                tid: None, // Not available in actual API
                oid: None, // Not available in actual API
            });
        }

        Ok(fills)
    }

    async fn fetch_and_parse_day_impl(
        &self,
        builder: &Address,
        yyyymmdd: &str,
    ) -> Result<Vec<BuilderLogFill>, BuilderLogsError> {
        let lz4 = self.fetch_lz4_bytes(builder, yyyymmdd).await?;
        let csv = Self::decompress_lz4_frame(&lz4)?;
        Self::parse_csv(&csv)
    }
}

impl Default for BuilderLogsFetcher {
    fn default() -> Self {
        Self::new(reqwest::Client::new())
    }
}

#[async_trait]
impl BuilderLogsSource for BuilderLogsFetcher {
    async fn fetch_and_parse_day(
        &self,
        builder: &Address,
        yyyymmdd: &str,
    ) -> Result<Vec<BuilderLogFill>, BuilderLogsError> {
        self.fetch_and_parse_day_impl(builder, yyyymmdd).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn compress_lz4_frame(input: &[u8]) -> Vec<u8> {
        let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
        encoder.write_all(input).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn lz4_decompress_fixture_roundtrip() {
        let csv = b"time,user,coin,side,px,sz,crossed,special_trade_type,tif,is_trigger,counterparty,closed_pnl,twap_id,builder_fee\n\
            2023-11-14T16:53:20.000Z,0xabc,BTC,buy,100,1,false,,Gtc,false,0xdef,0,,0.01\n";
        let lz4 = compress_lz4_frame(csv);

        let out = BuilderLogsFetcher::decompress_lz4_frame(&lz4).unwrap();
        assert_eq!(out, csv);
    }

    #[test]
    fn csv_parsing_valid_row() {
        // Schema: time,user,coin,side,px,sz,crossed,special_trade_type,tif,is_trigger,counterparty,closed_pnl,twap_id,builder_fee
        let csv = b"time,user,coin,side,px,sz,crossed,special_trade_type,tif,is_trigger,counterparty,closed_pnl,twap_id,builder_fee\n\
            2023-11-14T16:53:20.000Z,0xabc,BTC,A,100,1,false,,Gtc,false,0xdef,0,,0.01\n";
        let fills = BuilderLogsFetcher::parse_csv(csv).unwrap();
        assert_eq!(fills.len(), 1);
        // 2023-11-14T16:53:20.000Z = 1699980800000 ms
        assert_eq!(fills[0].time_ms, TimeMs::new(1699980800000));
        assert_eq!(fills[0].user.as_str(), "0xabc");
        assert_eq!(fills[0].coin.as_str(), "BTC");
        assert_eq!(fills[0].side, Side::Buy);
        assert_eq!(fills[0].px.to_canonical_string(), "100");
        assert_eq!(fills[0].sz.to_canonical_string(), "1");
        // tid and oid are not available in the real API
        assert_eq!(fills[0].tid, None);
        assert_eq!(fills[0].oid, None);
    }

    #[test]
    fn csv_parsing_invalid_side_errors() {
        let csv = b"time,user,coin,side,px,sz,crossed,special_trade_type,tif,is_trigger,counterparty,closed_pnl,twap_id,builder_fee\n\
            2023-11-14T16:53:20.000Z,0xabc,BTC,wat,100,1,false,,Gtc,false,0xdef,0,,0.01\n";
        let err = BuilderLogsFetcher::parse_csv(csv).unwrap_err();
        assert!(matches!(err, BuilderLogsError::Csv(_)));
    }

    #[test]
    fn csv_parsing_uppercase_side() {
        // Verify uppercase "A" -> Buy and "B" -> Sell are handled correctly
        let csv = b"time,user,coin,side,px,sz,crossed,special_trade_type,tif,is_trigger,counterparty,closed_pnl,twap_id,builder_fee\n\
            2023-11-14T16:53:20.000Z,0xabc,BTC,A,100,1,false,,Gtc,false,0xdef,0,,0.01\n\
            2023-11-14T16:53:21.000Z,0xdef,ETH,B,200,2,false,,Gtc,false,0xabc,0,,0.02\n";
        let fills = BuilderLogsFetcher::parse_csv(csv).unwrap();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].side, Side::Buy, "uppercase 'A' should be Buy");
        assert_eq!(fills[1].side, Side::Sell, "uppercase 'B' should be Sell");
    }

    #[test]
    fn csv_parsing_minimal_columns() {
        // Test parsing with only the required columns (extra columns marked as optional)
        let csv = b"time,user,coin,side,px,sz\n\
            2023-11-14T16:53:20.000Z,0xabc,BTC,buy,100.5,1.25\n";
        let fills = BuilderLogsFetcher::parse_csv(csv).unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].px.to_canonical_string(), "100.5");
        assert_eq!(fills[0].sz.to_canonical_string(), "1.25");
    }

    #[test]
    fn csv_parsing_invalid_time_format_errors() {
        let csv = b"time,user,coin,side,px,sz\n\
            not-a-valid-time,0xabc,BTC,buy,100,1\n";
        let err = BuilderLogsFetcher::parse_csv(csv).unwrap_err();
        assert!(matches!(err, BuilderLogsError::Csv(_)));
    }
}
