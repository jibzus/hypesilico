use hypesilico::engine::{EffectType, PositionTracker};
use hypesilico::{Address, Coin, Decimal, Fill, Side, TimeMs};

fn d(s: &str) -> Decimal {
    Decimal::from_str_canonical(s).unwrap()
}

fn fill(side: Side, sz: &str, px: &str, time_ms: i64, tid: i64, fee: &str, closed_pnl: &str) -> Fill {
    Fill::new(
        TimeMs::new(time_ms),
        Address::new("0x123".to_string()),
        Coin::new("BTC".to_string()),
        side,
        d(px),
        d(sz),
        d(fee),
        d(closed_pnl),
        None,
        Some(tid),
        None,
    )
}

fn buy(sz: &str, px: &str, time_ms: i64, tid: i64) -> Fill {
    fill(Side::Buy, sz, px, time_ms, tid, "0", "0")
}

fn sell(sz: &str, px: &str, time_ms: i64, tid: i64) -> Fill {
    fill(Side::Sell, sz, px, time_ms, tid, "0", "0")
}

#[test]
fn test_simple_open_close_long() {
    let mut tracker = PositionTracker::new();

    tracker.process_fill(&buy("1", "50000", 1000, 1));
    assert_eq!(tracker.state.net_size, d("1"));
    assert_eq!(tracker.state.avg_entry_px, d("50000"));
    assert_eq!(tracker.state.lifecycle_id, Some(1));

    tracker.process_fill(&sell("1", "55000", 2000, 2));
    assert!(tracker.state.is_flat());
    assert_eq!(tracker.state.lifecycle_id, None);

    let (lifecycles, snapshots, effects) = tracker.into_outputs();
    assert_eq!(lifecycles.len(), 1);
    assert_eq!(lifecycles[0].start_time_ms, TimeMs::new(1000));
    assert_eq!(lifecycles[0].end_time_ms, Some(TimeMs::new(2000)));

    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0].net_size, d("1"));
    assert_eq!(snapshots[0].avg_entry_px, d("50000"));
    assert_eq!(snapshots[1].net_size, Decimal::zero());
    assert_eq!(snapshots[1].avg_entry_px, Decimal::zero());

    assert_eq!(effects.len(), 2);
    assert_eq!(effects[0].effect_type, EffectType::Open);
    assert_eq!(effects[1].effect_type, EffectType::Close);
}

#[test]
fn test_partial_close_preserves_avg_entry() {
    let mut tracker = PositionTracker::new();

    tracker.process_fill(&buy("2", "50000", 1000, 1));
    assert_eq!(tracker.state.avg_entry_px, d("50000"));

    tracker.process_fill(&sell("1", "55000", 2000, 2));
    assert_eq!(tracker.state.net_size, d("1"));
    assert_eq!(tracker.state.avg_entry_px, d("50000"));
    assert_eq!(tracker.state.lifecycle_id, Some(1));
}

#[test]
fn test_flip_long_to_short_emits_two_effects_and_snapshots() {
    let mut tracker = PositionTracker::new();

    tracker.process_fill(&buy("1", "50000", 1000, 1));
    assert_eq!(tracker.state.lifecycle_id, Some(1));

    // Sell 2 to flip from +1 to -1.
    let flip_fill = fill(Side::Sell, "2", "55000", 2000, 2, "10", "123.45");
    tracker.process_fill(&flip_fill);

    assert_eq!(tracker.state.net_size, d("-1"));
    assert_eq!(tracker.state.avg_entry_px, d("55000"));
    assert_eq!(tracker.state.lifecycle_id, Some(2));

    let (lifecycles, snapshots, effects) = tracker.into_outputs();
    assert_eq!(lifecycles.len(), 2);
    assert_eq!(lifecycles[0].end_time_ms, Some(TimeMs::new(2000)));
    assert_eq!(lifecycles[1].end_time_ms, None);

    let flip_snapshots: Vec<_> = snapshots
        .iter()
        .filter(|s| s.time_ms == TimeMs::new(2000))
        .collect();
    assert_eq!(flip_snapshots.len(), 2);
    assert_eq!(flip_snapshots[0].seq, 0);
    assert_eq!(flip_snapshots[0].net_size, Decimal::zero());
    assert_eq!(flip_snapshots[0].lifecycle_id, 1);
    assert_eq!(flip_snapshots[1].seq, 1);
    assert_eq!(flip_snapshots[1].net_size, d("-1"));
    assert_eq!(flip_snapshots[1].lifecycle_id, 2);

    let flip_effects: Vec<_> = effects
        .iter()
        .filter(|e| e.fill_key == "tid:2")
        .collect();
    assert_eq!(flip_effects.len(), 2);
    assert_eq!(flip_effects[0].effect_type, EffectType::Close);
    assert_eq!(flip_effects[0].lifecycle_id, 1);
    assert_eq!(flip_effects[0].qty, d("1"));
    assert_eq!(flip_effects[0].fee, d("5"));
    assert_eq!(flip_effects[0].closed_pnl, d("123.45"));
    assert_eq!(flip_effects[1].effect_type, EffectType::Open);
    assert_eq!(flip_effects[1].lifecycle_id, 2);
    assert_eq!(flip_effects[1].qty, d("1"));
    assert_eq!(flip_effects[1].fee, d("5"));
    assert_eq!(flip_effects[1].closed_pnl, Decimal::zero());
}

#[test]
fn test_avg_entry_weighted_on_add() {
    let mut tracker = PositionTracker::new();

    tracker.process_fill(&buy("1", "50000", 1000, 1));
    tracker.process_fill(&buy("1", "60000", 2000, 2));

    assert_eq!(tracker.state.net_size, d("2"));
    assert_eq!(tracker.state.avg_entry_px, d("55000"));
}

#[test]
fn test_short_open_add_partial_close_then_close() {
    let mut tracker = PositionTracker::new();

    tracker.process_fill(&sell("1", "100", 1000, 1));
    assert_eq!(tracker.state.net_size, d("-1"));
    assert_eq!(tracker.state.avg_entry_px, d("100"));
    assert_eq!(tracker.state.lifecycle_id, Some(1));

    tracker.process_fill(&sell("1", "90", 2000, 2));
    assert_eq!(tracker.state.net_size, d("-2"));
    assert_eq!(tracker.state.avg_entry_px, d("95"));

    tracker.process_fill(&buy("0.5", "80", 3000, 3));
    assert_eq!(tracker.state.net_size, d("-1.5"));
    assert_eq!(tracker.state.avg_entry_px, d("95"));

    tracker.process_fill(&buy("1.5", "70", 4000, 4));
    assert!(tracker.state.is_flat());
    assert_eq!(tracker.state.lifecycle_id, None);

    let (lifecycles, snapshots, effects) = tracker.into_outputs();
    assert_eq!(lifecycles.len(), 1);
    assert_eq!(lifecycles[0].end_time_ms, Some(TimeMs::new(4000)));
    assert_eq!(snapshots.len(), 4);
    assert_eq!(effects.len(), 4);
    assert_eq!(effects[2].effect_type, EffectType::Close);
    assert_eq!(effects[3].effect_type, EffectType::Close);
}

