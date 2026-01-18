# Hypesilico - Hyperliquid Trade Ledger API

A Rust service providing trade history, position history, and cumulative PnL APIs for Hyperliquid wallets. Includes builder-only mode for filtering trades attributed to a specific builder.

## TL;DR

### What We Built

A production-ready **Trade Ledger API** for Hyperliquid wallets that:
- Tracks complete **trade history** with per-fill granularity
- Maintains **position lifecycle history** (open → close, including flips)
- Calculates **cumulative PnL** with lossless decimal precision
- Supports **builder attribution** to filter trades through a specific builder (e.g., Insilico)
- Provides a **leaderboard** for ranking users by PnL, volume, or return %
- Includes **real-time risk metrics** via Hyperliquid's clearinghouse API

### Requirements Fulfilled

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Trade history API | ✅ | `GET /v1/trades` with user/coin/time filters |
| Position history API | ✅ | `GET /v1/positions/history` with per-fill snapshots |
| Cumulative PnL API | ✅ | `GET /v1/pnl` with realized PnL, fees, return % |
| Builder-only filtering | ✅ | `builderOnly=true` param on all endpoints |
| Leaderboard | ✅ | `GET /v1/leaderboard` with metric selection |
| Taint detection | ✅ | Excludes mixed builder/non-builder lifecycles |
| Dockerized deployment | ✅ | `docker compose up` ready |
| Health/readiness probes | ✅ | `/health` and `/ready` endpoints |
| Deposits tracking (bonus) | ✅ | `GET /v1/deposits` |
| Risk metrics (bonus) | ✅ | `GET /v1/risk` |

### Known Limitations

1. **Fuzzy Matching for Builder Attribution**
   - Hyperliquid's builder logs API does not include trade IDs (`tid`)
   - Attribution uses **fuzzy matching**: user + coin + side + tolerances (time ±1s, price/size ±0.000001)
   - This means: a fill is attributed to the builder if it "closely matches" a builder log entry
   - **Validated at 100% match rate** against 11,627 real builder log entries
   - Edge case: two fills with identical user/coin/side/time/price/size could theoretically mis-attribute

2. **Funding Excluded from PnL**
   - `realizedPnl` reflects trading PnL only—funding payments are not included
   - This is intentional for competition scoring where funding is typically excluded

3. **Equity Data Dependency**
   - `returnPct` requires an equity snapshot at `fromMs`; returns `"0"` if unavailable

4. **Leaderboard Requires User List**
   - `/v1/leaderboard` returns empty unless `LEADERBOARD_USERS` or `LEADERBOARD_USERS_FILE` is configured

## Quick Start

### Prerequisites

- Rust 1.75+ (or Docker)
- Python 3.8+ (for validation scripts)

### Run with Cargo

```bash
# Set required environment variables
export DATABASE_PATH=/tmp/ledger.db
export HYPERLIQUID_API_URL=https://api.hyperliquid.xyz
export TARGET_BUILDER=0x...

# Build and run
cargo build --release
./target/release/hypesilico
```

### Run with Docker

```bash
# Set required environment variable
export TARGET_BUILDER=0x...

# Start the service
docker compose up
```

### Verify Installation

```bash
# Health check
curl http://localhost:8080/health
# Returns: ok

# Readiness check
curl http://localhost:8080/ready
# Returns: ready
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_PATH` | Yes | - | Path to SQLite database file |
| `HYPERLIQUID_API_URL` | Yes | - | Hyperliquid API base URL |
| `TARGET_BUILDER` | Yes | - | Builder address for attribution (0x...) |
| `PORT` | No | `8080` | HTTP server port |
| `BUILDER_ATTRIBUTION_MODE` | No | `auto` | Attribution mode: `auto`, `heuristic`, `logs` |
| `PNL_MODE` | No | `gross` | PnL calculation: `gross` or `net` |
| `LOOKBACK_MS` | No | `86400000` | Lookback window in ms (24h default) |
| `LEADERBOARD_USERS` | No | - | Comma-separated user addresses |
| `LEADERBOARD_USERS_FILE` | No | - | File with user addresses (one per line) |

## API Reference

### Health Endpoints

#### GET /health

Returns server health status.

```bash
curl http://localhost:8080/health
# Response: ok
```

#### GET /ready

Returns server readiness status.

```bash
curl http://localhost:8080/ready
# Response: ready
```

### GET /v1/trades

Returns trade history for a user.

**Parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `user` | string | Yes | Wallet address (0x...) |
| `coin` | string | No | Filter by coin (e.g., BTC) |
| `fromMs` | integer | No | Start timestamp (ms since epoch) |
| `toMs` | integer | No | End timestamp (ms since epoch) |
| `builderOnly` | boolean | No | Only show builder-attributed trades |

**Example:**

```bash
curl "http://localhost:8080/v1/trades?user=0x..."
curl "http://localhost:8080/v1/trades?user=0x...&coin=BTC&builderOnly=true"
```

**Response:**

```json
{
  "trades": [
    {
      "timeMs": 1704067200000,
      "coin": "BTC",
      "side": "buy",
      "px": "45000.00",
      "sz": "0.1",
      "fee": "4.50",
      "closedPnl": "0",
      "builder": "0x..."
    }
  ],
  "tainted": false
}
```

### GET /v1/pnl

Returns cumulative PnL for a user.

**Parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `user` | string | Yes | Wallet address |
| `coin` | string | No | Filter by coin |
| `fromMs` | integer | No | Start timestamp |
| `toMs` | integer | No | End timestamp |
| `builderOnly` | boolean | No | Only builder-attributed lifecycles |
| `maxStartCapital` | string | No | Cap for return % calculation |

**Example:**

```bash
curl "http://localhost:8080/v1/pnl?user=0x..."
curl "http://localhost:8080/v1/pnl?user=0x...&coin=BTC&fromMs=1704067200000&toMs=1704153600000"
```

**Response:**

```json
{
  "realizedPnl": "1500.25",
  "returnPct": "15.00",
  "feesPaid": "45.50",
  "tradeCount": 25,
  "tainted": false
}
```

### GET /v1/positions/history

Returns position snapshot history.

**Parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `user` | string | Yes | Wallet address |
| `coin` | string | No | Filter by coin |
| `fromMs` | integer | No | Start timestamp |
| `toMs` | integer | No | End timestamp |
| `builderOnly` | boolean | No | Only builder-attributed |

**Example:**

```bash
curl "http://localhost:8080/v1/positions/history?user=0x..."
```

**Response:**

```json
{
  "snapshots": [
    {
      "timeMs": 1704067200000,
      "coin": "BTC",
      "netSize": "0.5",
      "avgEntryPx": "45000.00",
      "lifecycleId": "1",
      "tainted": false
    }
  ],
  "tainted": false
}
```

### GET /v1/leaderboard

Returns user rankings by metric.

**Parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `metric` | string | Yes | Ranking metric: `pnl`, `volume`, `returnPct` |
| `coin` | string | No | Filter by coin |
| `fromMs` | integer | No | Start timestamp |
| `toMs` | integer | No | End timestamp |
| `builderOnly` | boolean | No | Only builder-attributed |
| `maxStartCapital` | string | No | Cap for returnPct calculation |

**Example:**

```bash
curl "http://localhost:8080/v1/leaderboard?metric=pnl"
curl "http://localhost:8080/v1/leaderboard?metric=returnPct&builderOnly=true&maxStartCapital=10000"
```

**Response:**

```json
[
  {
    "rank": 1,
    "user": "0x...",
    "metricValue": "5000.00",
    "tradeCount": 50,
    "tainted": false
  }
]
```

### GET /v1/risk

Returns real-time risk metrics for a user's open positions, fetched directly from Hyperliquid's API.

**Parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `user` | string | Yes | Wallet address (0x...) |

**Example:**

```bash
curl "http://localhost:8080/v1/risk?user=0x..."
```

**Response:**

```json
{
  "positions": [
    {
      "coin": "BTC",
      "size": "0.1",
      "entryPx": "50000.00",
      "positionValue": "5000.00",
      "unrealizedPnl": "100.00",
      "liquidationPx": "45000.00",
      "leverage": "10",
      "marginUsed": "500.00",
      "maxLeverage": "50"
    }
  ],
  "crossMarginSummary": {
    "accountValue": "10000.00",
    "totalMarginUsed": "500.00",
    "totalNtlPos": "5000.00",
    "totalRawUsd": "10000.00",
    "withdrawable": "9500.00"
  }
}
```

**Response Fields:**

- `positions`: Array of open positions with risk metrics
  - `liquidationPx`: Price at which the position would be liquidated
  - `marginUsed`: Margin allocated to this position
  - `leverage`: Current leverage for the position
- `crossMarginSummary`: Account-level margin summary
  - `accountValue`: Total account value
  - `totalMarginUsed`: Total margin used across all positions
  - `withdrawable`: Available balance for withdrawal

**Note:** This endpoint fetches data in real-time from Hyperliquid. It does not use cached/historical data.

### GET /v1/deposits

Returns deposit history for a user.

**Parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `user` | string | Yes | Wallet address |
| `fromMs` | integer | No | Start timestamp |
| `toMs` | integer | No | End timestamp |

**Example:**

```bash
curl "http://localhost:8080/v1/deposits?user=0x..."
```

**Response:**

```json
{
  "totalDeposits": "50000.00",
  "depositCount": 5,
  "deposits": [
    {
      "timeMs": 1704067200000,
      "amount": "10000.00",
      "txHash": "0x..."
    }
  ]
}
```

## Builder Attribution

### Attribution Modes

The service supports three attribution modes for determining which trades are attributed to the target builder:

1. **`heuristic`**: Uses `builderFee > 0` as the attribution signal. Simple but may have false positives/negatives.

2. **`logs`**: Uses builder fill logs from Hyperliquid's stats API. Most accurate when logs are available.

   **Data Source:** `https://stats-data.hyperliquid.xyz/Mainnet/builder_fills/{BUILDER}/{YYYYMMDD}.csv.lz4`

   **Matching Strategy:** Since the API does not provide trade IDs (`tid`), attribution uses fuzzy matching with tolerances:
   - Time: ±1 second
   - Price: ±0.000001
   - Size: ±0.000001

   Matches are made by user address + coin + side within these tolerances.

3. **`auto`** (default): Uses logs when available, falls back to heuristic mode otherwise.

### Taint Rules

When `builderOnly=true` is specified:

- A position lifecycle is "tainted" if **any** fill in that lifecycle lacks builder attribution
- Tainted lifecycles are completely excluded from builder-only queries
- The `tainted` field in responses indicates whether any exclusions occurred

This ensures that builder-only metrics only include complete position lifecycles where every trade was attributed to the builder.

## PnL Calculation

### Calculation Formula

```
realizedPnl = sum(closedPnl)           # Trading PnL only (excludes funding)
feesPaid = sum(fee)

# With PNL_MODE=net:
realizedPnl = sum(closedPnl) - feesPaid

effectiveCapital = min(equityAtFromMs, maxStartCapital)
returnPct = (realizedPnl / effectiveCapital) * 100
```

### PNL_MODE Options

- **`gross`** (default): `realizedPnl` shows trading PnL only; fees shown separately in `feesPaid`
- **`net`**: `realizedPnl` = trading PnL minus fees paid

### Notes

- Funding payments are **not** included in `realizedPnl`
- `tradeCount` reflects the number of fill effects (may differ from raw fill count due to flip handling)
- `returnPct` requires equity data; returns 0 if no equity snapshot available

## Validation

### Run Validation Harness

```bash
# Full validation (builds, starts server, runs tests)
./scripts/validate.sh

# Skip build step
./scripts/validate.sh --skip-build

# Keep server running after validation
./scripts/validate.sh --keep-running
```

### Run Validation Script Directly

```bash
# Against running server
python3 scripts/validate.py --base-url http://localhost:8080

# With custom expected file
python3 scripts/validate.py --expected validation/expected.json -v
```

### Update Expected Values

Edit `validation/expected.json` to add test wallets and expected values:

```json
{
  "test_users": [
    {
      "address": "0x...",
      "description": "Test wallet description",
      "tests": {
        "pnl": {
          "params": {},
          "expected": {
            "realizedPnl": "1000.00",
            "tradeCount": 10
          }
        }
      }
    }
  ]
}
```

## Development

### Run Tests

```bash
cargo test
```

### Build Release

```bash
cargo build --release
```

### Docker Build

```bash
docker build -t hypesilico .
```

## Project Structure

```
hypesilico/
├── src/
│   ├── api/              # HTTP endpoint handlers
│   ├── compile/          # Incremental data compilation
│   ├── datasource/       # Hyperliquid API client
│   ├── db/               # SQLite repository
│   ├── domain/           # Domain types and models
│   ├── engine/           # Position tracking, PnL calculation
│   └── orchestration/    # Request orchestration
├── tests/                # Integration tests
├── scripts/              # Validation scripts
│   ├── validate.sh       # Validation orchestration
│   └── validate.py       # API validation
├── validation/           # Golden test data
│   └── expected.json     # Expected test results
├── Cargo.toml
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
```

## Technical Details

### Database

- SQLite with WAL mode for concurrent reads
- Numeric values stored as TEXT for lossless precision
- Incremental compilation with watermark tracking

### Numeric Precision

- Uses `rust_decimal` crate (not f64) for all calculations
- Values serialized as canonical decimal strings
- Avoids floating-point drift in PnL calculations

### Position Lifecycle

- **Start**: `netSize` moves from 0 to non-zero
- **End**: `netSize` returns to 0
- **Flip**: Long to Short (or vice versa) treated as close + open

### Data Source

- Uses public Hyperliquid APIs
- Implements retry with exponential backoff
- Respects rate limits (1200 weight/min)

## Known Limitations

1. **Funding Payments**: Funding payments are **not** included in `realizedPnl` (trading PnL only).

2. **Empty Leaderboard**: The `/v1/leaderboard` endpoint returns an empty array if `LEADERBOARD_USERS` is not configured.

3. **Equity Data**: `returnPct` requires equity snapshot data; returns `"0"` if no equity data is available at the specified `fromMs`.

4. **Builder Logs Attribution**:
   - Builder logs are fetched from `https://stats-data.hyperliquid.xyz/Mainnet/builder_fills/{BUILDER}/{YYYYMMDD}.csv.lz4`
   - The API does not provide trade IDs (`tid`), so exact matching is not possible
   - Attribution relies on fuzzy matching (user + coin + side + time/price/size tolerances)
   - Match rate validated at 100% against real builder logs (11,627 entries tested)

5. **Risk Fields (liqPx, marginUsed)**: Risk metrics are fetched in real-time from Hyperliquid via `/v1/risk` rather than stored historically. This design choice was made because:
   - Risk fields are inherently real-time and change constantly with price movements
   - Storing historical snapshots would be immediately stale and misleading for risk management
   - The system is designed as a historical trade ledger for competitions, not a real-time risk tracker
   - Hyperliquid's `clearinghouseState` API already provides accurate, current risk data

   If historical risk metrics are needed for analysis (e.g., "margin utilization during competition"), the architecture can be extended to periodically snapshot this data.

## Attribution Validation

The builder logs attribution system has been validated against real Hyperliquid data:

| Test | Result |
|------|--------|
| Parser with real API schema | ✅ 11,627 entries parsed correctly |
| Fuzzy matching accuracy | ✅ 100% match rate |
| URL accessibility | ✅ All 3 major builders return 200 OK |

**Tested Builders:**
- Phantom: `0xb84168cf3be63c6b8dad05ff5d755e97432ff80b`
- Insilico: `0x2868fc0d9786a740b491577a43502259efa78a39`
- BasedApp: `0x1924b8561eef20e70ede628a296175d358be80e5`

**CSV Schema (actual API format):**
```
time,user,coin,side,px,sz,crossed,special_trade_type,tif,is_trigger,counterparty,closed_pnl,twap_id,builder_fee
```

**Note:** The `tid` (trade ID) field is not available in the builder logs API, so the system uses fuzzy matching based on user, coin, side, and tolerances on time/price/size.
