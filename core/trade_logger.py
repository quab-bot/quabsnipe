# core/trade_logger.py
# Comprehensive trade data logger for AI/ML training dataset
#
# Every trade is recorded with:
#  - Entry conditions (all filter signals at time of buy)
#  - Price snapshots every 5 minutes during hold
#  - Final outcome: exit reason, P&L, peak multiplier
#
# After a few weeks of live trading, export with:
#   python scripts/export_training_data.py
#
# The resulting CSV / JSON can train a classifier to predict:
#   "Given these entry conditions → will this token reach 2x, 5x, or rug?"

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DB_PATH = Path("data/sniper.db")
DB_PATH.parent.mkdir(exist_ok=True)


def init_trade_tables():
    """Create the trade_log and trade_snapshots tables if they don't exist."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript("""
        -- One row per trade (entry conditions + final outcome)
        CREATE TABLE IF NOT EXISTS trade_log (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            mint                    TEXT NOT NULL,
            symbol                  TEXT,
            source                  TEXT,        -- pumpfun / raydium / telegram_signal

            -- Entry conditions (features for ML)
            entry_time              TEXT,
            entry_price_usd         REAL,
            entry_mc_usd            REAL,
            entry_liq_usd           REAL,
            entry_age_min           REAL,
            entry_risk_score        INTEGER,
            entry_verdict           TEXT,        -- clean / caution
            entry_flags_json        TEXT,        -- JSON list of filter flags
            entry_snipers_pct       REAL,
            entry_bundles_pct       REAL,
            entry_dev_pct           REAL,
            entry_fake_vol_pct      REAL,
            entry_holders           INTEGER,
            entry_fake_holders_pct  REAL,
            entry_security_ok       INTEGER,     -- 0/1
            entry_sol_spent         REAL,
            entry_usd_spent         REAL,

            -- Outcome (labels for ML)
            exit_time               TEXT,
            exit_price_usd          REAL,
            exit_reason             TEXT,        -- trailing_stop / stop_loss / 24hr_check / etc.
            hold_hours              REAL,
            peak_multiplier         REAL,        -- highest price / entry price
            pnl_usd                 REAL,
            pnl_pct                 REAL,
            tp_levels_hit_json      TEXT,        -- e.g. ["2x_income_lock","5x"]

            -- Derived ML label
            outcome_label           TEXT         -- rug / small_win / 2x / 5x / 10x+ / ongoing
        );

        -- Price snapshots every 5 min during the hold (time-series features)
        CREATE TABLE IF NOT EXISTS trade_snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            mint          TEXT NOT NULL,
            symbol        TEXT,
            snapshot_time TEXT,
            age_min       REAL,      -- minutes since entry
            price_usd     REAL,
            multiplier    REAL,      -- price / entry_price
            sol_price     REAL
        );

        -- Token analysis at the point it was SEEN (rejected or passed)
        -- This is the raw funnel data — useful for training a "should I buy?" model
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
            was_bought      INTEGER DEFAULT 0,   -- 1 if it passed and was traded
            outcome_label   TEXT                 -- filled in later if bought
        );
    """)
    conn.commit()
    conn.close()


def log_entry(
    mint: str,
    symbol: str,
    source: str,
    entry_price_usd: float,
    entry_mc_usd: float,
    entry_liq_usd: float,
    entry_age_min: float,
    risk_score: int,
    verdict: str,
    flags: list,
    snipers_pct: float = 0,
    bundles_pct: float = 0,
    dev_pct: float = 0,
    fake_vol_pct: float = 0,
    holders: int = 0,
    fake_holders_pct: float = 0,
    security_ok: bool = False,
    sol_spent: float = 0,
    usd_spent: float = 0,
):
    """
    Record entry conditions when a trade is opened.
    Called by TradeCoordinator right after a successful buy.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("""
            INSERT INTO trade_log (
                mint, symbol, source, entry_time, entry_price_usd,
                entry_mc_usd, entry_liq_usd, entry_age_min,
                entry_risk_score, entry_verdict, entry_flags_json,
                entry_snipers_pct, entry_bundles_pct, entry_dev_pct,
                entry_fake_vol_pct, entry_holders, entry_fake_holders_pct,
                entry_security_ok, entry_sol_spent, entry_usd_spent,
                outcome_label
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            mint, symbol, source, _now(), entry_price_usd,
            entry_mc_usd, entry_liq_usd, entry_age_min,
            risk_score, verdict, json.dumps(flags),
            snipers_pct, bundles_pct, dev_pct,
            fake_vol_pct, holders, fake_holders_pct,
            int(security_ok), sol_spent, usd_spent,
            "ongoing",
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[trade_logger] log_entry error: {e}")


def log_trade_snapshot(
    mint: str,
    symbol: str,
    age_min: float,
    multiplier: float,
    price_usd: float,
    sol_price: float,
):
    """Record a 5-minute price snapshot during a live position."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("""
            INSERT INTO trade_snapshots (mint, symbol, snapshot_time, age_min,
                                         price_usd, multiplier, sol_price)
            VALUES (?,?,?,?,?,?,?)
        """, (mint, symbol, _now(), age_min, price_usd, multiplier, sol_price))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[trade_logger] snapshot error: {e}")


def finalize_trade(
    mint: str,
    symbol: str,
    source: str,
    entry_price: float,
    exit_price: float,
    entry_usd: float,
    exit_usd: float,
    peak_multiplier: float,
    exit_reason: str,
    hold_hours: float,
    tp_levels: list,
    pnl_usd: float,
    pnl_pct: float,
):
    """
    Fill in the outcome columns when a trade closes.
    Also assigns an outcome_label for ML training.
    """
    label = _outcome_label(peak_multiplier, pnl_pct, exit_reason)
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("""
            UPDATE trade_log
            SET exit_time          = ?,
                exit_price_usd     = ?,
                exit_reason        = ?,
                hold_hours         = ?,
                peak_multiplier    = ?,
                pnl_usd            = ?,
                pnl_pct            = ?,
                tp_levels_hit_json = ?,
                outcome_label      = ?
            WHERE mint = ? AND exit_time IS NULL
        """, (
            _now(), exit_price, exit_reason, hold_hours,
            peak_multiplier, pnl_usd, pnl_pct,
            json.dumps(tp_levels), label, mint,
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[trade_logger] finalize_trade error: {e}")


def log_seen_token(
    mint: str,
    source: str,
    mc_usd: float,
    liq_usd: float,
    age_min: float,
    risk_score: int,
    verdict: str,
    reject_reason: str,
    flags: list,
    was_bought: bool = False,
):
    """
    Record every token the bot saw — whether it bought or rejected.
    This gives a full picture of what the funnel looked like,
    and lets you later compare rejected tokens' outcomes vs traded ones.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("""
            INSERT OR IGNORE INTO token_seen_log
                (mint, source, seen_at, mc_usd, liq_usd, age_min, risk_score,
                 verdict, reject_reason, flags_json, was_bought)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            mint, source, _now(), mc_usd, liq_usd, age_min, risk_score,
            verdict, reject_reason, json.dumps(flags), int(was_bought),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[trade_logger] log_seen_token error: {e}")


def export_training_data(output_path: str = "data/training_data.json") -> int:
    """
    Export all completed trades as a JSON file for ML training.
    Returns the number of records exported.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT * FROM trade_log
            WHERE exit_time IS NOT NULL
              AND outcome_label != 'ongoing'
            ORDER BY entry_time
        """).fetchall()
        conn.close()

        records = [dict(r) for r in rows]
        # Parse JSON columns back to lists
        for r in records:
            r["entry_flags"]     = json.loads(r.get("entry_flags_json") or "[]")
            r["tp_levels_hit"]   = json.loads(r.get("tp_levels_hit_json") or "[]")

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(records, f, indent=2)

        return len(records)
    except Exception as e:
        print(f"[trade_logger] export error: {e}")
        return 0


def _outcome_label(peak_multiplier: float, pnl_pct: float, exit_reason: str) -> str:
    """
    Assign a clean label for ML classification.
    These are the classes your model will predict.
    """
    if exit_reason in ("stop_loss_phase1",) and pnl_pct <= -30:
        return "rug"
    if peak_multiplier >= 10:
        return "10x+"
    elif peak_multiplier >= 5:
        return "5x"
    elif peak_multiplier >= 2:
        return "2x"
    elif pnl_pct >= 0:
        return "small_win"
    else:
        return "loss"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
