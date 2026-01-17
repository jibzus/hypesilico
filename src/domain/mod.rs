//! Domain types and determinism layer for Hyperliquid Trade Ledger API.
//!
//! This module provides:
//! - Lossless numeric handling via Decimal wrapper
//! - Domain primitives: TimeMs, Address, Coin, Side
//! - Fill and Attribution types with canonical JSON serialization
//! - Stable fill ordering key helper for deterministic processing

pub mod attribution;
pub mod builder_logs;
pub mod decimal;
pub mod deposit;
pub mod fill;
pub mod ordering;
pub mod primitives;

pub use attribution::{Attribution, AttributionConfidence, AttributionMode, Confidence};
pub use builder_logs::BuilderLogFill;
pub use decimal::Decimal;
pub use deposit::Deposit;
pub use fill::Fill;
pub use ordering::FillOrderingKey;
pub use primitives::{Address, AddressParseError, Coin, Side, TimeMs};
