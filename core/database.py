# core/database.py
# SQLite database for caching checked tokens, wallets, RugCheck results
# Prevents redundant API calls and speeds up repeated checks

import sqlite3
import json
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

DB_PATH = Path("data/sniper.db")
DB_PATH.parent.mkdir(exist_ok=True)

_lock = threading.Lock()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # WAL mode prevents lock conflicts
    return conn


def init_db():
    """Create all tables if they don't exist."""
    with _lock:
        conn = get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS checked_tokens (
                mint        TEXT PRIMARY KEY,
                result      TEXT NOT NULL,       -- 'pass', 'reject', 'caution'
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
        """)
        conn.commit()
        conn.close()


# ── Token result cache ─────────────────────────────────────────────────────────

def cache_token_result(mint: str, result: str, risk_score: int, fail_reason: str = "", source: str = ""):
    with _lock:
        conn = get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO checked_tokens
            (mint, result, risk_score, fail_reason, checked_at, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (mint, result, risk_score, fail_reason, _now(), source))
        conn.commit()
        conn.close()


def get_cached_token(mint: str, max_age_minutes: int = 60) -> dict | None:
    """Return cached token result if fresh enough."""
    with _lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM checked_tokens WHERE mint = ?", (mint,)
        ).fetchone()
        conn.close()
    if not row:
        return None
    if _is_stale(row["checked_at"], max_age_minutes):
        return None
    return dict(row)


# ── RugCheck cache ─────────────────────────────────────────────────────────────

def cache_rugcheck(mint: str, score: int, risks: list):
    with _lock:
        conn = get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO rugcheck_cache
            (mint, score, risks_json, cached_at)
            VALUES (?, ?, ?, ?)
        """, (mint, score, json.dumps(risks), _now()))
        conn.commit()
        conn.close()


def get_cached_rugcheck(mint: str, max_age_minutes: int = 30) -> dict | None:
    with _lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM rugcheck_cache WHERE mint = ?", (mint,)
        ).fetchone()
        conn.close()
    if not row:
        return None
    if _is_stale(row["cached_at"], max_age_minutes):
        return None
    return {"score": row["score"], "risks": json.loads(row["risks_json"])}


# ── Wallet cache ───────────────────────────────────────────────────────────────

def cache_wallet(address: str, age_days: float, tx_count: int, is_bad: bool = False, bad_reason: str = ""):
    with _lock:
        conn = get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO wallet_cache
            (address, age_days, tx_count, is_bad, bad_reason, cached_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (address, age_days, tx_count, int(is_bad), bad_reason, _now()))
        conn.commit()
        conn.close()


def get_cached_wallet(address: str, max_age_hours: int = 24) -> dict | None:
    with _lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM wallet_cache WHERE address = ?", (address,)
        ).fetchone()
        conn.close()
    if not row:
        return None
    if _is_stale(row["cached_at"], max_age_hours * 60):
        return None
    return dict(row)


# ── Domain cache ───────────────────────────────────────────────────────────────

def cache_domain(domain: str, resolves: bool, age_hours: float | None):
    with _lock:
        conn = get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO domain_cache
            (domain, resolves, age_hours, cached_at)
            VALUES (?, ?, ?, ?)
        """, (domain, int(resolves), age_hours, _now()))
        conn.commit()
        conn.close()


def get_cached_domain(domain: str, max_age_hours: int = 6) -> dict | None:
    with _lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM domain_cache WHERE domain = ?", (domain,)
        ).fetchone()
        conn.close()
    if not row:
        return None
    if _is_stale(row["cached_at"], max_age_hours * 60):
        return None
    return dict(row)


# ── Trade logging ──────────────────────────────────────────────────────────────

def log_trade_open(mint: str, name: str, symbol: str, source: str,
                   entry_price: float, sol_spent: float, usd_spent: float,
                   tokens_received: float, risk_score: int, paper: bool = False) -> int:
    with _lock:
        conn = get_conn()
        cur = conn.execute("""
            INSERT INTO trades
            (mint, token_name, token_symbol, source, entry_price_usd,
             entry_time, sol_spent, usd_spent, tokens_received, risk_score, paper_trade)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (mint, name, symbol, source, entry_price, _now(),
              sol_spent, usd_spent, tokens_received, risk_score, int(paper)))
        conn.commit()
        trade_id = cur.lastrowid
        conn.close()
        return trade_id


def log_trade_close(trade_id: int, exit_reason: str, exit_price: float, pnl_usd: float, pnl_pct: float):
    with _lock:
        conn = get_conn()
        conn.execute("""
            UPDATE trades SET
                status = 'closed',
                exit_reason = ?,
                exit_price_usd = ?,
                exit_time = ?,
                pnl_usd = ?,
                pnl_pct = ?
            WHERE id = ?
        """, (exit_reason, exit_price, _now(), pnl_usd, pnl_pct, trade_id))
        conn.commit()
        conn.close()


def get_daily_pnl(date: str | None = None) -> dict:
    if not date:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with _lock:
        conn = get_conn()
        row = conn.execute("""
            SELECT
                COUNT(*) as total_trades,
                SUM(CASE WHEN status='closed' THEN 1 ELSE 0 END) as closed,
                SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pnl_usd < 0 THEN 1 ELSE 0 END) as losses,
                SUM(COALESCE(pnl_usd, 0)) as total_pnl,
                AVG(CASE WHEN pnl_usd IS NOT NULL THEN pnl_pct END) as avg_pnl_pct
            FROM trades
            WHERE DATE(entry_time) = ? AND paper_trade = 0
        """, (date,)).fetchone()
        conn.close()
    return dict(row) if row else {}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_stale(cached_at: str, max_age_minutes: int) -> bool:
    try:
        cached = datetime.fromisoformat(cached_at)
        if cached.tzinfo is None:
            cached = cached.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - cached) > timedelta(minutes=max_age_minutes)
    except Exception:
        return True


def init_analytics_table():
    """Add analytics table — called at startup."""
    with _lock:
        conn = get_conn()
        conn.executescript("""
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
        """)
        conn.commit()
        conn.close()
