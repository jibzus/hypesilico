# Hypesilico - Hyperliquid Trade Ledger API

## Project Overview

A dockerized Rust service that provides trade history, position history, and cumulative PnL APIs for Hyperliquid wallets. Includes optional builder-only mode for filtering trades attributed to a specific builder (for Insilico users or competitions).

## Tech Stack

- **Language:** Rust
- **Framework:** Axum (HTTP server)
- **Database:** SQLite (WAL mode, TEXT for lossless numerics)
- **Numerics:** Decimal/fixed-point (no f64 in engine math to avoid drift)
- **Container:** Docker / docker-compose

## Required API Endpoints

| Endpoint | Method | Required Params | Optional Params |
|----------|--------|-----------------|-----------------|
| `/v1/trades` | GET | `user` | `coin`, `fromMs`, `toMs`, `builderOnly` |
| `/v1/positions/history` | GET | `user` | `coin`, `fromMs`, `toMs`, `builderOnly` |
| `/v1/pnl` | GET | `user` | `coin`, `fromMs`, `toMs`, `builderOnly` |
| `/v1/leaderboard` | GET | - | `coin`, `fromMs`, `toMs`, `metric`, `builderOnly`, `maxStartCapital` |
| `/v1/deposits` | GET | `user` | `fromMs`, `toMs` | (Bonus)
| `/health` | GET | - | - |
| `/ready` | GET | - | - |

## Environment Variables

```bash
TARGET_BUILDER=0x...              # Builder address for attribution
BUILDER_ATTRIBUTION_MODE=auto     # auto|heuristic|logs
PNL_MODE=gross                    # gross|net (for validation)
DATABASE_PATH=/data/ledger.db     # SQLite database path
HL_URL=https://api.hyperliquid.xyz # Hyperliquid API base URL
LEADERBOARD_USERS=...             # Comma-separated user addresses
LEADERBOARD_USERS_FILE=/data/users.txt # Or file path for user universe
LOOKBACK_MS=...                   # Fallback lookback for window reconstruction
```

## Core Concepts

### Position Lifecycle
- **Start:** netSize moves from 0 → non-zero
- **End:** netSize returns to 0
- **Flip:** Long → Short or vice versa (treated as close + open in single fill)

### Builder-Only Mode
When `builderOnly=true`:
1. **Heuristic mode:** `builderFee > 0` → treat as builder-attributed
2. **Log-backed mode:** Ingest `builder_fills/{TARGET_BUILDER}/{YYYYMMDD}.csv.lz4`
3. **Auto mode (default):** Use logs when available, fallback to heuristic

### Taint Rules
- If any non-builder fill affects the same position lifecycle → `tainted=true`
- Tainted lifecycles are excluded from builder-only aggregates/leaderboards

### PnL Calculation
```
realizedPnl = sum(closedPnl)           # Trading PnL only, excludes funding
feesPaid = sum(fee)
effectiveCapital = min(equityAtFromMs, maxStartCapital)
returnPct = realizedPnl / effectiveCapital * 100
```

## Database Schema (Key Tables)

- `raw_fills` - Raw fill data from Hyperliquid
- `normalized_fills` - Processed fills with attribution
- `position_lifecycles` - Lifecycle tracking with taint flags
- `position_snapshots` - Per-fill state snapshots
- `fill_effects` - Flip close/open decomposition
- `equity_snapshots` - Cached equity values
- `deposits` - Deposit ledger updates
- `compile_state` - Watermark tracking for incremental compilation

## Data Models

### Fill
```rust
struct Fill {
    time_ms: i64,
    coin: String,
    side: String,        // "buy" | "sell"
    px: Decimal,         // Lossless numeric
    sz: Decimal,
    fee: Decimal,
    closed_pnl: Decimal,
    builder_fee: Option<Decimal>,
    attribution: Option<Attribution>,
    raw_id: String,      // tid/oid or deterministic hash
}
```

### Position Snapshot
```rust
struct PositionSnapshot {
    time_ms: i64,
    coin: String,
    net_size: Decimal,
    avg_entry_px: Decimal,
    tainted: Option<bool>,  // Only when builderOnly=true
}
```

## Architecture (5 Workstreams)

1. **Platform Owner:** Service scaffold, Docker, config, documentation
2. **Data Owner:** Ingestion, storage, normalization, compile pipeline
3. **Engine Owner:** Position tracking, lifecycle segmentation, PnL calculation
4. **Builder-Only Owner:** Attribution detection, taint rules
5. **QA/Validation Owner:** Golden harness, regression tests, correctness verification

## Judging Criteria

| Criteria | Weight |
|----------|--------|
| Correctness (PnL + tradeCount match validation wallets) | 50% |
| Completeness (all required endpoints work) | 20% |
| Builder-only handling (taint detection, exclusion rules) | 20% |
| Demo clarity (README, video, curl examples) | 10% |

## Key Technical Decisions

1. **Position history:** Snapshot emitted at every fill (not aggregated)
2. **Builder attribution:** Best-effort with heuristic + optional log-backed
3. **Funding:** Excluded from realizedPnl (trading PnL only)
4. **Aggregation:** Default `aggregateByTime=false`
5. **Numeric precision:** Decimal/fixed-point, no f64 in engine math
6. **Storage:** SQLite with WAL mode, TEXT for lossless numerics
7. **Flip handling:** Explicit close/open effect decomposition

## Development Commands

```bash
# Build and run
docker compose up

# Health check
curl http://localhost:8080/health

# Example queries
curl "http://localhost:8080/v1/trades?user=0x..."
curl "http://localhost:8080/v1/pnl?user=0x...&coin=BTC"
curl "http://localhost:8080/v1/positions/history?user=0x...&builderOnly=true"
curl "http://localhost:8080/v1/leaderboard?metric=pnl&builderOnly=true"
```

## Data Source

- Uses public Hyperliquid APIs (Info/WS)
- Must implement DataSource abstraction for future swap to Insilico-HL / HyperServe
- Respects rate limits (1200 weight/min)
- Implements pagination with retry/backoff

## Validation

5 test wallet addresses provided with expected trade counts and PnL values. The golden harness compares outputs against `validation/expected.json`.
