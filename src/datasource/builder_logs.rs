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
            "https://hyperliquid.xyz/builder_fills/{}/{}.csv.lz4",
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
        #[derive(Debug, serde::Deserialize)]
        struct Row {
            time_ms: i64,
            user: String,
            coin: String,
            side: String,
            px: String,
            sz: String,
            tid: Option<i64>,
            oid: Option<i64>,
        }

        fn parse_side(s: &str) -> Option<Side> {
            match s.trim().to_ascii_lowercase().as_str() {
                "a" | "buy" => Some(Side::Buy),
                "b" | "sell" => Some(Side::Sell),
                _ => None,
            }
        }

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(csv_bytes);

        let mut fills = Vec::new();
        for record in reader.deserialize::<Row>() {
            let row = record.map_err(|e| BuilderLogsError::Csv(e.to_string()))?;
            let side = parse_side(&row.side)
                .ok_or_else(|| BuilderLogsError::Csv(format!("invalid side: {}", row.side)))?;
            let px = Decimal::from_str_canonical(&row.px)
                .map_err(|e| BuilderLogsError::Csv(format!("invalid px: {}", e)))?;
            let sz = Decimal::from_str_canonical(&row.sz)
                .map_err(|e| BuilderLogsError::Csv(format!("invalid sz: {}", e)))?;

            fills.push(BuilderLogFill {
                time_ms: TimeMs::new(row.time_ms),
                user: Address::new(row.user),
                coin: Coin::new(row.coin),
                side,
                px,
                sz,
                tid: row.tid,
                oid: row.oid,
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
        let csv = b"time_ms,user,coin,side,px,sz,tid,oid\n1700000000000,0xabc,BTC,buy,100,1,42,1001\n";
        let lz4 = compress_lz4_frame(csv);

        let out = BuilderLogsFetcher::decompress_lz4_frame(&lz4).unwrap();
        assert_eq!(out, csv);
    }

    #[test]
    fn csv_parsing_valid_row() {
        let csv = b"time_ms,user,coin,side,px,sz,tid,oid\n1700000000000,0xabc,BTC,A,100,1,42,1001\n";
        let fills = BuilderLogsFetcher::parse_csv(csv).unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].time_ms, TimeMs::new(1_700_000_000_000));
        assert_eq!(fills[0].user.as_str(), "0xabc");
        assert_eq!(fills[0].coin.as_str(), "BTC");
        assert_eq!(fills[0].side, Side::Buy);
        assert_eq!(fills[0].px.to_canonical_string(), "100");
        assert_eq!(fills[0].sz.to_canonical_string(), "1");
        assert_eq!(fills[0].tid, Some(42));
        assert_eq!(fills[0].oid, Some(1001));
    }

    #[test]
    fn csv_parsing_invalid_side_errors() {
        let csv = b"time_ms,user,coin,side,px,sz,tid,oid\n1700000000000,0xabc,BTC,wat,100,1,42,1001\n";
        let err = BuilderLogsFetcher::parse_csv(csv).unwrap_err();
        assert!(matches!(err, BuilderLogsError::Csv(_)));
    }
}
