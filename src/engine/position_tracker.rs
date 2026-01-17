use crate::domain::{Decimal, Fill, Side};

use super::{Effect, EffectType, Lifecycle, Snapshot};

/// Current state of a position for a user+coin.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PositionState {
    /// Current net size: positive = long, negative = short, zero = flat.
    pub net_size: Decimal,

    /// Average entry price (only meaningful when net_size != 0).
    pub avg_entry_px: Decimal,

    /// Current lifecycle ID (None if flat).
    pub lifecycle_id: Option<i64>,
}

impl PositionState {
    pub fn new() -> Self {
        Self {
            net_size: Decimal::zero(),
            avg_entry_px: Decimal::zero(),
            lifecycle_id: None,
        }
    }

    pub fn is_flat(&self) -> bool {
        self.net_size.is_zero()
    }

    pub fn is_long(&self) -> bool {
        self.net_size.is_positive()
    }

    pub fn is_short(&self) -> bool {
        self.net_size.is_negative()
    }
}

pub struct PositionTracker {
    pub state: PositionState,
    next_lifecycle_id: i64,

    // Outputs accumulated during processing.
    lifecycles: Vec<Lifecycle>,
    snapshots: Vec<Snapshot>,
    effects: Vec<Effect>,
}

impl PositionTracker {
    pub fn new() -> Self {
        Self {
            state: PositionState::new(),
            next_lifecycle_id: 1,
            lifecycles: Vec::new(),
            snapshots: Vec::new(),
            effects: Vec::new(),
        }
    }

    /// Process a single fill, updating state and emitting outputs.
    ///
    /// # Panics
    /// Panics if fills are processed out of order (e.g., close before open).
    /// Callers must ensure fills are sorted by (time_ms, tid).
    pub fn process_fill(&mut self, fill: &Fill) {
        let signed_qty = self.compute_signed_qty(fill);
        let old_size = self.state.net_size;
        let new_size = old_size + signed_qty;

        if self.is_flip(old_size, new_size) {
            self.handle_flip(fill, old_size, new_size);
        } else if old_size.is_zero() && !new_size.is_zero() {
            self.handle_open(fill, new_size);
        } else if !old_size.is_zero() && new_size.is_zero() {
            self.handle_close(fill);
        } else {
            self.handle_adjustment(fill, old_size, new_size);
        }
    }

    /// Compute signed quantity: Buy = +sz, Sell = -sz.
    fn compute_signed_qty(&self, fill: &Fill) -> Decimal {
        match fill.side {
            Side::Buy => fill.sz,
            Side::Sell => -fill.sz,
        }
    }

    /// Check if this is a flip (crossing from long to short or vice versa).
    fn is_flip(&self, old_size: Decimal, new_size: Decimal) -> bool {
        if old_size.is_zero() || new_size.is_zero() {
            return false;
        }
        old_size.is_positive() != new_size.is_positive()
    }

    /// Handle opening a new position from flat.
    fn handle_open(&mut self, fill: &Fill, new_size: Decimal) {
        let lifecycle_id = self.next_lifecycle_id;
        self.next_lifecycle_id += 1;

        self.lifecycles.push(Lifecycle {
            id: lifecycle_id,
            user: fill.user.clone(),
            coin: fill.coin.clone(),
            start_time_ms: fill.time_ms,
            end_time_ms: None,
        });

        self.state.net_size = new_size;
        self.state.avg_entry_px = fill.px;
        self.state.lifecycle_id = Some(lifecycle_id);

        self.effects.push(Effect {
            fill_key: fill.fill_key(),
            lifecycle_id,
            effect_type: EffectType::Open,
            qty: fill.sz,
            notional: fill.px * fill.sz,
            fee: fill.fee,
            closed_pnl: fill.closed_pnl,
        });

        self.snapshots.push(Snapshot {
            time_ms: fill.time_ms,
            seq: 0,
            net_size: new_size,
            avg_entry_px: fill.px,
            lifecycle_id,
        });
    }

    /// Handle closing a position to flat.
    fn handle_close(&mut self, fill: &Fill) {
        let lifecycle_id = self
            .state
            .lifecycle_id
            .expect("close fill requires an open lifecycle");

        if let Some(lifecycle) = self.lifecycles.iter_mut().find(|l| l.id == lifecycle_id) {
            lifecycle.end_time_ms = Some(fill.time_ms);
        }

        self.effects.push(Effect {
            fill_key: fill.fill_key(),
            lifecycle_id,
            effect_type: EffectType::Close,
            qty: fill.sz,
            notional: fill.px * fill.sz,
            fee: fill.fee,
            closed_pnl: fill.closed_pnl,
        });

        self.snapshots.push(Snapshot {
            time_ms: fill.time_ms,
            seq: 0,
            net_size: Decimal::zero(),
            avg_entry_px: Decimal::zero(),
            lifecycle_id,
        });

        self.state = PositionState::new();
    }

    /// Handle a flip (long to short or short to long).
    fn handle_flip(&mut self, fill: &Fill, old_size: Decimal, new_size: Decimal) {
        let old_lifecycle_id = self
            .state
            .lifecycle_id
            .expect("flip fill requires an open lifecycle");

        let close_qty = old_size.abs();
        let open_qty = new_size.abs();
        let total_qty = fill.sz;

        let close_ratio = close_qty / total_qty;
        let close_fee = fill.fee * close_ratio;
        let close_pnl = fill.closed_pnl;
        let open_fee = fill.fee - close_fee;

        if let Some(lifecycle) = self
            .lifecycles
            .iter_mut()
            .find(|lifecycle| lifecycle.id == old_lifecycle_id)
        {
            lifecycle.end_time_ms = Some(fill.time_ms);
        }

        self.effects.push(Effect {
            fill_key: fill.fill_key(),
            lifecycle_id: old_lifecycle_id,
            effect_type: EffectType::Close,
            qty: close_qty,
            notional: fill.px * close_qty,
            fee: close_fee,
            closed_pnl: close_pnl,
        });

        self.snapshots.push(Snapshot {
            time_ms: fill.time_ms,
            seq: 0,
            net_size: Decimal::zero(),
            avg_entry_px: Decimal::zero(),
            lifecycle_id: old_lifecycle_id,
        });

        let new_lifecycle_id = self.next_lifecycle_id;
        self.next_lifecycle_id += 1;

        self.lifecycles.push(Lifecycle {
            id: new_lifecycle_id,
            user: fill.user.clone(),
            coin: fill.coin.clone(),
            start_time_ms: fill.time_ms,
            end_time_ms: None,
        });

        self.effects.push(Effect {
            fill_key: fill.fill_key(),
            lifecycle_id: new_lifecycle_id,
            effect_type: EffectType::Open,
            qty: open_qty,
            notional: fill.px * open_qty,
            fee: open_fee,
            closed_pnl: Decimal::zero(),
        });

        self.snapshots.push(Snapshot {
            time_ms: fill.time_ms,
            seq: 1,
            net_size: new_size,
            avg_entry_px: fill.px,
            lifecycle_id: new_lifecycle_id,
        });

        self.state.net_size = new_size;
        self.state.avg_entry_px = fill.px;
        self.state.lifecycle_id = Some(new_lifecycle_id);
    }

    /// Handle adjustment (increase or decrease without flip/flat).
    fn handle_adjustment(&mut self, fill: &Fill, old_size: Decimal, new_size: Decimal) {
        let lifecycle_id = self
            .state
            .lifecycle_id
            .expect("adjustment fill requires an open lifecycle");
        let old_abs = old_size.abs();
        let new_abs = new_size.abs();

        if new_abs > old_abs {
            let added_qty = (new_size - old_size).abs();
            let old_value = old_abs * self.state.avg_entry_px;
            let new_value = added_qty * fill.px;
            self.state.avg_entry_px = (old_value + new_value) / new_abs;

            self.effects.push(Effect {
                fill_key: fill.fill_key(),
                lifecycle_id,
                effect_type: EffectType::Open,
                qty: fill.sz,
                notional: fill.px * fill.sz,
                fee: fill.fee,
                closed_pnl: fill.closed_pnl,
            });
        } else {
            self.effects.push(Effect {
                fill_key: fill.fill_key(),
                lifecycle_id,
                effect_type: EffectType::Close,
                qty: fill.sz,
                notional: fill.px * fill.sz,
                fee: fill.fee,
                closed_pnl: fill.closed_pnl,
            });
        }

        self.state.net_size = new_size;

        self.snapshots.push(Snapshot {
            time_ms: fill.time_ms,
            seq: 0,
            net_size: new_size,
            avg_entry_px: self.state.avg_entry_px,
            lifecycle_id,
        });
    }

    /// Get the accumulated outputs.
    pub fn into_outputs(self) -> (Vec<Lifecycle>, Vec<Snapshot>, Vec<Effect>) {
        (self.lifecycles, self.snapshots, self.effects)
    }
}

impl Default for PositionTracker {
    fn default() -> Self {
        Self::new()
    }
}

