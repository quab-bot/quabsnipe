# core/channel_tracker.py
# Per-channel win rate tracking and position size multiplier
# Channels that perform well get bigger position sizes over time

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from utils.logger import get_logger

logger = get_logger("channel_tracker")

DB_PATH = Path("data/sniper.db")

MIN_TRADES_FOR_SIZING = 10   # Need at least 10 completed trades before adjusting size


def init_channel_tables():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS channel_signals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            channel     TEXT NOT NULL,
            mint        TEXT NOT NULL,
            seen_at     TEXT NOT NULL,
            passed      INTEGER DEFAULT 0,
            traded      INTEGER DEFAULT 0,
            pnl_usd     REAL,
            won         INTEGER,
            UNIQUE(channel, mint)
        );
    """)
    conn.commit()
    conn.close()


def record_signal_seen(channel: str, mint: str, passed: bool):
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.execute("""
            INSERT OR IGNORE INTO channel_signals (channel, mint, seen_at, passed)
            VALUES (?, ?, ?, ?)
        """, (channel, mint, _now(), int(passed)))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"record_signal_seen error: {e}")


def record_signal_outcome(mint: str, pnl_usd: float, won: bool):
    """Update outcome for all channels that called this token."""
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.execute("""
            UPDATE channel_signals
            SET pnl_usd = ?, won = ?, traded = 1
            WHERE mint = ?
        """, (pnl_usd, int(won), mint))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"record_signal_outcome error: {e}")


def get_leaderboard() -> list:
    """Returns all channels sorted by total P&L."""
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        rows = conn.execute("""
            SELECT
                channel,
                COUNT(*)                                            AS total_signals,
                SUM(passed)                                         AS passed,
                SUM(CASE WHEN traded=1 THEN 1 ELSE 0 END)           AS traded,
                SUM(CASE WHEN won=1 THEN 1 ELSE 0 END)              AS wins,
                SUM(CASE WHEN won=0 AND traded=1 THEN 1 ELSE 0 END) AS losses,
                COALESCE(SUM(pnl_usd), 0)                           AS total_pnl,
                MIN(seen_at)                                        AS first_seen
            FROM channel_signals
            GROUP BY channel
            ORDER BY total_pnl DESC
        """).fetchall()
        conn.close()
        return [
            {
                "channel":       r[0],
                "total_signals": r[1],
                "passed":        r[2] or 0,
                "traded":        r[3] or 0,
                "wins":          r[4] or 0,
                "losses":        r[5] or 0,
                "pnl":           r[6] or 0.0,
                "first_seen":    r[7] or "",
            }
            for r in rows
        ]
    except Exception as e:
        logger.debug(f"get_leaderboard error: {e}")
        return []


def get_size_multiplier(channel: str) -> float:
    """
    Returns a position size multiplier based on the channel's historical win rate.
    Requires MIN_TRADES_FOR_SIZING completed trades before adjusting.

    Win rate >= 70% → 1.3x size
    Win rate 55–69% → 1.0x (neutral)
    Win rate 40–54% → 0.75x
    Win rate <  40% → 0.5x
    """
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        row  = conn.execute("""
            SELECT
                SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN traded=1 THEN 1 ELSE 0 END) AS traded
            FROM channel_signals
            WHERE channel = ?
        """, (channel,)).fetchone()
        conn.close()

        if not row or not row[1] or row[1] < MIN_TRADES_FOR_SIZING:
            return 1.0  # Not enough data yet

        win_rate = row[0] / row[1]
        if win_rate >= 0.70: return 1.3
        if win_rate >= 0.55: return 1.0
        if win_rate >= 0.40: return 0.75
        return 0.5

    except Exception as e:
        logger.debug(f"get_size_multiplier error: {e}")
        return 1.0


def how_many_channels_called(mint: str) -> int:
    """Return how many different channels have sent a signal for this CA."""
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        row  = conn.execute(
            "SELECT COUNT(DISTINCT channel) FROM channel_signals WHERE mint = ?",
            (mint,)
        ).fetchone()
        conn.close()
        return row[0] if row else 0
    except Exception:
        return 0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
