//! Pure computation engine(s) for deterministic ledger logic.

use crate::domain::{Address, Coin, Decimal, TimeMs};

pub mod builder_logs_matcher;
pub mod equity;
pub mod position_tracker;
pub mod taint;

pub use builder_logs_matcher::{BuilderLogsIndex, MatchTolerances};
pub use equity::EquityResolver;
pub use position_tracker::{PositionState, PositionTracker};
pub use taint::{BuilderOnlyFilter, TaintComputer, TaintInfo};

/// A lifecycle from position open to close.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lifecycle {
    pub id: i64,
    pub user: Address,
    pub coin: Coin,
    pub start_time_ms: TimeMs,
    pub end_time_ms: Option<TimeMs>, // None if still open
}

/// A snapshot of position state after a fill.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Snapshot {
    pub time_ms: TimeMs,
    pub seq: i32, // Tie-breaker for same time_ms
    pub net_size: Decimal,
    pub avg_entry_px: Decimal,
    pub lifecycle_id: i64,
}

/// An effect of a fill on a lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Effect {
    pub fill_key: String,
    pub lifecycle_id: i64,
    pub effect_type: EffectType,
    pub qty: Decimal,      // Absolute quantity
    pub notional: Decimal, // px * qty
    pub fee: Decimal,      // Allocated fee
    pub closed_pnl: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EffectType {
    /// Increasing position.
    #[default]
    Open,
    /// Decreasing position.
    Close,
}
