# setup_db.py — run once to create ALL database tables
# python setup_db.py

import sqlite3
from pathlib import Path

Path("data").mkdir(exist_ok=True)
conn = sqlite3.connect("data/sniper.db")

conn.executescript("""
    -- ── Token filter cache ──────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS checked_tokens (
        mint        TEXT PRIMARY KEY,
        result      TEXT NOT NULL,
        risk_score  INTEGER DEFAULT 0,
        fail_reason TEXT,
        checked_at  TEXT NOT NULL,
        source      TEXT
    );

    CREATE TABLE IF NOT EXISTS rugcheck_cache (
        mint        TEXT PRIMARY KEY,
        score       INTEGER,
        risks_json  TEXT,
        cached_at   TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS wallet_cache (
        address     TEXT PRIMARY KEY,
        age_days    REAL,
        tx_count    INTEGER,
        is_bad      INTEGER DEFAULT 0,
        bad_reason  TEXT,
        cached_at   TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS domain_cache (
        domain      TEXT PRIMARY KEY,
        resolves    INTEGER DEFAULT 0,
        age_hours   REAL,
        cached_at   TEXT NOT NULL
    );

    -- ── Trade records ───────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS trades (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        mint            TEXT NOT NULL,
        token_name      TEXT,
        token_symbol    TEXT,
        source          TEXT,
        entry_price_usd REAL,
        entry_time      TEXT,
        sol_spent       REAL,
        usd_spent       REAL,
        tokens_received REAL,
        risk_score      INTEGER,
        status          TEXT DEFAULT 'open',
        exit_reason     TEXT,
        exit_price_usd  REAL,
        exit_time       TEXT,
        pnl_usd         REAL,
        pnl_pct         REAL,
        paper_trade     INTEGER DEFAULT 0
    );

    -- ── Token analytics (filter funnel data) ───────────────────────────────
    CREATE TABLE IF NOT EXISTS token_analytics (
        mint                        TEXT PRIMARY KEY,
        source                      TEXT,
        detected_at                 TEXT,
        liquidity_usd               REAL DEFAULT 0,
        market_cap_usd              REAL DEFAULT 0,
        risk_score                  INTEGER DEFAULT 0,
        verdict                     TEXT,
        rejection_stage             TEXT,
        rejection_reason            TEXT,
        flags_json                  TEXT DEFAULT '[]',
        deployer_age_days           REAL DEFAULT 0,
        deployer_tx_count           INTEGER DEFAULT 0,
        holder_concentration_pct    REAL DEFAULT 0,
        wallet_cluster_signals      INTEGER DEFAULT 0,
        lp_locked                   INTEGER DEFAULT 0,
        mint_authority_revoked      INTEGER DEFAULT 1,
        freeze_authority_revoked    INTEGER DEFAULT 1,
        outcome                     TEXT DEFAULT 'unknown',
        peak_multiplier             REAL DEFAULT 0,
        outcome_recorded_at         TEXT
    );

    CREATE TABLE IF NOT EXISTS daily_stats (
        date            TEXT PRIMARY KEY,
        tokens_detected INTEGER DEFAULT 0,
        tokens_passed   INTEGER DEFAULT 0,
        tokens_rejected INTEGER DEFAULT 0,
        trades_opened   INTEGER DEFAULT 0,
        trades_closed   INTEGER DEFAULT 0,
        gross_pnl_usd   REAL DEFAULT 0,
        fees_usd        REAL DEFAULT 0
    );

    -- ── AI Training tables ──────────────────────────────────────────────────
    -- One row per trade: entry conditions (features) + outcome (label)
    CREATE TABLE IF NOT EXISTS trade_log (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        mint                    TEXT NOT NULL,
        symbol                  TEXT,
        source                  TEXT,

        entry_time              TEXT,
        entry_price_usd         REAL,
        entry_mc_usd            REAL,
        entry_liq_usd           REAL,
        entry_age_min           REAL,
        entry_risk_score        INTEGER,
        entry_verdict           TEXT,
        entry_flags_json        TEXT,
        entry_snipers_pct       REAL,
        entry_bundles_pct       REAL,
        entry_dev_pct           REAL,
        entry_fake_vol_pct      REAL,
        entry_holders           INTEGER,
        entry_fake_holders_pct  REAL,
        entry_security_ok       INTEGER,
        entry_sol_spent         REAL,
        entry_usd_spent         REAL,

        exit_time               TEXT,
        exit_price_usd          REAL,
        exit_reason             TEXT,
        hold_hours              REAL,
        peak_multiplier         REAL,
        pnl_usd                 REAL,
        pnl_pct                 REAL,
        tp_levels_hit_json      TEXT,
        outcome_label           TEXT
    );

    -- Price snapshot every 5 min during hold (time-series)
    CREATE TABLE IF NOT EXISTS trade_snapshots (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        mint          TEXT NOT NULL,
        symbol        TEXT,
        snapshot_time TEXT,
        age_min       REAL,
        price_usd     REAL,
        multiplier    REAL,
        sol_price     REAL
    );

    -- Every token the bot SEES — bought or not (full funnel view)
    CREATE TABLE IF NOT EXISTS token_seen_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        mint            TEXT NOT NULL,
        source          TEXT,
        seen_at         TEXT,
        mc_usd          REAL,
        liq_usd         REAL,
        age_min         REAL,
        risk_score      INTEGER,
        verdict         TEXT,
        reject_reason   TEXT,
        flags_json      TEXT,
        was_bought      INTEGER DEFAULT 0,
        outcome_label   TEXT
    );
""")

conn.commit()
conn.close()
print("✓ All database tables created successfully")
print("  Now run: python main.py")
