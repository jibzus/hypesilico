-- Raw fills from Hyperliquid API
CREATE TABLE IF NOT EXISTS raw_fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user TEXT NOT NULL,
    coin TEXT NOT NULL,
    time_ms INTEGER NOT NULL,
    side TEXT NOT NULL,
    px TEXT NOT NULL,
    sz TEXT NOT NULL,
    fee TEXT NOT NULL,
    closed_pnl TEXT NOT NULL,
    builder_fee TEXT,
    tid INTEGER,
    oid INTEGER,
    fill_key TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_fills_user_coin_time ON raw_fills(user, coin, time_ms);
CREATE INDEX IF NOT EXISTS idx_raw_fills_user_coin_tid ON raw_fills(user, coin, tid);

-- Fill attributions (builder-only mode)
CREATE TABLE IF NOT EXISTS fill_attributions (
    fill_key TEXT PRIMARY KEY,
    attributed INTEGER NOT NULL,
    mode TEXT NOT NULL,
    confidence TEXT NOT NULL,
    builder TEXT,
    FOREIGN KEY(fill_key) REFERENCES raw_fills(fill_key)
);

-- Position lifecycles
CREATE TABLE IF NOT EXISTS position_lifecycles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user TEXT NOT NULL,
    coin TEXT NOT NULL,
    start_time_ms INTEGER NOT NULL,
    end_time_ms INTEGER,
    is_tainted INTEGER NOT NULL,
    taint_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_lifecycles_user_coin ON position_lifecycles(user, coin);

-- Position snapshots (one per fill)
CREATE TABLE IF NOT EXISTS position_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user TEXT NOT NULL,
    coin TEXT NOT NULL,
    time_ms INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    net_size TEXT NOT NULL,
    avg_entry_px TEXT NOT NULL,
    lifecycle_id INTEGER NOT NULL,
    is_tainted INTEGER NOT NULL,
    FOREIGN KEY(lifecycle_id) REFERENCES position_lifecycles(id)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_user_coin_time ON position_snapshots(user, coin, time_ms);

-- Index for position_snapshots JOIN on lifecycle_id
CREATE INDEX IF NOT EXISTS idx_snapshots_lifecycle_id ON position_snapshots(lifecycle_id);

-- Fill effects (flip decomposition)
CREATE TABLE IF NOT EXISTS fill_effects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fill_key TEXT NOT NULL,
    lifecycle_id INTEGER NOT NULL,
    effect_type TEXT NOT NULL,
    qty TEXT NOT NULL,
    notional TEXT NOT NULL,
    fee TEXT NOT NULL,
    closed_pnl TEXT NOT NULL,
    FOREIGN KEY(fill_key) REFERENCES raw_fills(fill_key),
    FOREIGN KEY(lifecycle_id) REFERENCES position_lifecycles(id)
);

CREATE INDEX IF NOT EXISTS idx_effects_fill_key ON fill_effects(fill_key);

-- Index for fill_effects JOIN on lifecycle_id (PnL and leaderboard queries)
CREATE INDEX IF NOT EXISTS idx_effects_lifecycle_id ON fill_effects(lifecycle_id);

-- Deposits
CREATE TABLE IF NOT EXISTS deposits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user TEXT NOT NULL,
    time_ms INTEGER NOT NULL,
    amount TEXT NOT NULL,
    tx_hash TEXT,
    event_key TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_deposits_user_time ON deposits(user, time_ms);

-- Equity snapshots
CREATE TABLE IF NOT EXISTS equity_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user TEXT NOT NULL,
    time_ms INTEGER NOT NULL,
    equity TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_equity_user_time ON equity_snapshots(user, time_ms);

-- Builder logs cache
CREATE TABLE IF NOT EXISTS builder_logs_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    builder TEXT NOT NULL,
    yyyymmdd TEXT NOT NULL,
    fetched_at_ms INTEGER NOT NULL,
    content_hash TEXT,
    parsed INTEGER NOT NULL,
    UNIQUE(builder, yyyymmdd)
);

-- Compile state (watermark tracking)
CREATE TABLE IF NOT EXISTS compile_state (
    user TEXT NOT NULL,
    coin TEXT NOT NULL,
    last_compiled_time_ms INTEGER,
    last_compiled_fill_key TEXT,
    compile_version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(user, coin)
);

