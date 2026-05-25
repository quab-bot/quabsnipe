# core/rejected_tracker.py
# Track what happens to rejected tokens at 30min, 1hr, 2hr after rejection.
# This fills the biggest gap in AI training data — outcomes for tokens NOT traded.

import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from utils.logger import get_logger

logger = get_logger("rejected_tracker")

DB_PATH = Path("data/sniper.db")


def init_rejected_tables():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS rejected_outcomes (
            mint            TEXT PRIMARY KEY,
            source          TEXT,
            rejected_at     TEXT NOT NULL,
            price_at_entry  REAL DEFAULT 0,
            price_at_30m    REAL,
            price_at_1h     REAL,
            price_at_2h     REAL,
            outcome_30m     TEXT,
            outcome_1h      TEXT,
            outcome_2h      TEXT,
            reject_reason   TEXT,
            risk_score      INTEGER DEFAULT 0
        );
    """)
    conn.commit()
    conn.close()


def schedule_rejected_token(mint: str, source: str, reject_reason: str,
                             risk_score: int, entry_price: float = 0):
    """Queue a rejected token for outcome tracking."""
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.execute("""
            INSERT OR IGNORE INTO rejected_outcomes
                (mint, source, rejected_at, price_at_entry, reject_reason, risk_score)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (mint, source, _now(), entry_price, reject_reason, risk_score))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"schedule_rejected_token error: {e}")


async def check_pending_outcomes():
    """
    Called periodically from main loop.
    Checks price at 30m / 1h / 2h for queued rejected tokens.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT mint, rejected_at, price_at_entry,
                   price_at_30m, price_at_1h, price_at_2h
            FROM rejected_outcomes
            WHERE price_at_2h IS NULL
              AND rejected_at >= datetime('now', '-3 hours')
            LIMIT 15
        """).fetchall()
        conn.close()
    except Exception as e:
        logger.debug(f"check_pending: DB error: {e}")
        return

    if not rows:
        return

    now_ts = datetime.now(timezone.utc).timestamp()

    for row in rows:
        try:
            rejected_ts = datetime.fromisoformat(row["rejected_at"])
            if rejected_ts.tzinfo is None:
                rejected_ts = rejected_ts.replace(tzinfo=timezone.utc)
            age_min = (now_ts - rejected_ts.timestamp()) / 60

            if age_min < 30:
                continue  # Not ready yet

            price = await _get_price(row["mint"])
            if price is None:
                await asyncio.sleep(0.5)
                continue

            entry = row["price_at_entry"] or price
            updates = {}

            if age_min >= 30 and row["price_at_30m"] is None:
                mult = price / entry if entry > 0 else 1.0
                updates["price_at_30m"] = price
                updates["outcome_30m"]  = _label(mult)

            if age_min >= 60 and row["price_at_1h"] is None:
                mult = price / entry if entry > 0 else 1.0
                updates["price_at_1h"] = price
                updates["outcome_1h"]  = _label(mult)

            if age_min >= 120 and row["price_at_2h"] is None:
                mult = price / entry if entry > 0 else 1.0
                updates["price_at_2h"] = price
                updates["outcome_2h"]  = _label(mult)

            if updates:
                set_clause = ", ".join(f"{k} = ?" for k in updates)
                values     = list(updates.values()) + [row["mint"]]
                conn2 = sqlite3.connect(str(DB_PATH), check_same_thread=False)
                conn2.execute(
                    f"UPDATE rejected_outcomes SET {set_clause} WHERE mint = ?",
                    values,
                )
                conn2.commit()
                conn2.close()
                logger.debug(
                    f"Outcome: {row['mint'][:16]}... age={age_min:.0f}m {updates}"
                )

            await asyncio.sleep(0.4)

        except Exception as e:
            logger.debug(f"Outcome row error {row['mint'][:16]}: {e}")


def get_rejected_outcome_stats(hours: int = 24) -> dict:
    """Summary of what happened to rejected tokens (for /analysis)."""
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        r = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN outcome_1h LIKE 'pumped%' THEN 1 ELSE 0 END) as pumped,
                SUM(CASE WHEN outcome_1h IN ('dumped','rugged') THEN 1 ELSE 0 END) as dumped,
                SUM(CASE WHEN outcome_1h='flat' THEN 1 ELSE 0 END) as flat,
                SUM(CASE WHEN outcome_1h IS NULL THEN 1 ELSE 0 END) as pending
            FROM rejected_outcomes
            WHERE rejected_at >= datetime('now', ?)
        """, (f"-{hours} hours",)).fetchone()
        conn.close()
        return dict(r) if r else {}
    except Exception as e:
        logger.debug(f"get_rejected_outcome_stats error: {e}")
        return {}


async def _get_price(mint: str) -> float | None:
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status != 200:
                    return None
                data  = await resp.json()
                pairs = data.get("pairs", [])
                if not pairs:
                    return None
                p = pairs[0].get("priceUsd") or pairs[0].get("priceNative")
                return float(p) if p else None
    except Exception:
        return None


def _label(mult: float) -> str:
    if mult >= 2.0:  return "pumped_2x+"
    if mult >= 1.3:  return "pumped_30%+"
    if mult >= 0.85: return "flat"
    if mult >= 0.50: return "dumped"
    return "rugged"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
