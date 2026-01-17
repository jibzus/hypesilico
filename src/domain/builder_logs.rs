//! Types for builder fill logs published by Hyperliquid.

use crate::domain::{Address, Coin, Decimal, Side, TimeMs};

/// A single builder fill log row.
///
/// Source: `https://hyperliquid.xyz/builder_fills/BUILDER/YYYYMMDD.csv.lz4`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuilderLogFill {
    pub time_ms: TimeMs,
    pub user: Address,
    pub coin: Coin,
    pub side: Side,
    pub px: Decimal,
    pub sz: Decimal,
    pub tid: Option<i64>,
    pub oid: Option<i64>,
}

