//! Compile pipeline for transforming normalized fills into derived tables.
//!
//! This module provides:
//! - Watermark-based incremental compilation
//! - Position lifecycle tracking and snapshots
//! - Fill effect decomposition (flip handling)
//! - Taint flag computation for builder-only filtering

use crate::domain::{Address, Coin, TimeMs};
use serde::{Deserialize, Serialize};

pub mod incremental;

pub use incremental::Compiler;

/// Compile state tracking for watermark-based incremental processing.
///
/// Stores the last compiled fill_key and timestamp for a (user, coin) pair,
/// enabling resumption after crashes/restarts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompileState {
    /// User address
    pub user: Address,
    /// Coin/asset symbol
    pub coin: Coin,
    /// Last compilation timestamp in milliseconds
    pub last_compiled_ms: Option<TimeMs>,
    /// Last processed fill_key (deterministic fill identifier)
    pub last_fill_key: Option<String>,
}

impl CompileState {
    /// Create a new compile state for a user and coin.
    pub fn new(user: Address, coin: Coin) -> Self {
        Self {
            user,
            coin,
            last_compiled_ms: None,
            last_fill_key: None,
        }
    }

    /// Create a compile state with initial watermark values.
    pub fn with_watermark(
        user: Address,
        coin: Coin,
        last_compiled_ms: Option<TimeMs>,
        last_fill_key: Option<String>,
    ) -> Self {
        Self {
            user,
            coin,
            last_compiled_ms,
            last_fill_key,
        }
    }

    /// Check if this is the first compilation (no watermark set).
    pub fn is_first_compilation(&self) -> bool {
        self.last_fill_key.is_none()
    }

    /// Update the watermark with new values.
    pub fn update_watermark(&mut self, last_compiled_ms: TimeMs, last_fill_key: String) {
        self.last_compiled_ms = Some(last_compiled_ms);
        self.last_fill_key = Some(last_fill_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_state_new() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let state = CompileState::new(user.clone(), coin.clone());

        assert_eq!(state.user, user);
        assert_eq!(state.coin, coin);
        assert!(state.last_compiled_ms.is_none());
        assert!(state.last_fill_key.is_none());
    }

    #[test]
    fn test_compile_state_is_first_compilation() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let state = CompileState::new(user, coin);

        assert!(state.is_first_compilation());
    }

    #[test]
    fn test_compile_state_with_watermark() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let time_ms = TimeMs::new(1000);
        let fill_key = "fill_key_123".to_string();

        let state = CompileState::with_watermark(
            user.clone(),
            coin.clone(),
            Some(time_ms),
            Some(fill_key.clone()),
        );

        assert_eq!(state.user, user);
        assert_eq!(state.coin, coin);
        assert_eq!(state.last_compiled_ms, Some(time_ms));
        assert_eq!(state.last_fill_key, Some(fill_key));
        assert!(!state.is_first_compilation());
    }

    #[test]
    fn test_compile_state_update_watermark() {
        let user = Address::new("0x123".to_string());
        let coin = Coin::new("BTC".to_string());
        let mut state = CompileState::new(user, coin);

        let time_ms = TimeMs::new(2000);
        let fill_key = "fill_key_456".to_string();
        state.update_watermark(time_ms, fill_key.clone());

        assert_eq!(state.last_compiled_ms, Some(time_ms));
        assert_eq!(state.last_fill_key, Some(fill_key));
    }
}
