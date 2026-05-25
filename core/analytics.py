# core/analytics.py
# Analytics — records every analyzed token + query helpers for Telegram commands

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from utils.logger import get_logger

logger = get_logger("analytics")

DB_PATH = Path("data/sniper.db")


def log_token_analysis(
    mint: str,
    source: str,
    liquidity_usd: float,
    market_cap_usd: float,
    risk_score: int,
    verdict: str,
    rejection_stage: str,
    rejection_reason: str,
    flags: list,
    deployer_age_days: float = 0,
    deployer_tx_count: int = 0,
    holder_concentration_pct: float = 0,
    wallet_cluster_signals: int = 0,
    lp_locked: bool = False,
    mint_authority_revoked: bool = True,
    freeze_authority_revoked: bool = True,
):
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.execute("""
            INSERT OR IGNORE INTO token_analytics (
                mint, source, detected_at,
                liquidity_usd, market_cap_usd,
                risk_score, verdict,
                rejection_stage, rejection_reason,
                flags_json,
                deployer_age_days, deployer_tx_count,
                holder_concentration_pct,
                wallet_cluster_signals,
                lp_locked, mint_authority_revoked, freeze_authority_revoked
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            mint, source, _now(),
            liquidity_usd, market_cap_usd,
            risk_score, verdict,
            rejection_stage, rejection_reason,
            json.dumps(flags),
            deployer_age_days, deployer_tx_count,
            holder_concentration_pct,
            wallet_cluster_signals,
            int(lp_locked), int(mint_authority_revoked), int(freeze_authority_revoked),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"Analytics log error: {e}")


def update_token_outcome(mint: str, outcome: str, peak_multiplier: float = 0):
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.execute("""
            UPDATE token_analytics
            SET outcome = ?, peak_multiplier = ?, outcome_recorded_at = ?
            WHERE mint = ?
        """, (outcome, peak_multiplier, _now(), mint))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"Outcome update error: {e}")


def get_analytics_summary(hours: int = 24) -> dict:
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN verdict='reject' THEN 1 ELSE 0 END) as rejected,
                SUM(CASE WHEN verdict IN ('clean','caution') THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN rejection_stage='stage1' THEN 1 ELSE 0 END) as s1_rejects,
                SUM(CASE WHEN rejection_stage='stage2' THEN 1 ELSE 0 END) as s2_rejects,
                SUM(CASE WHEN rejection_stage='stage3' THEN 1 ELSE 0 END) as s3_rejects,
                AVG(liquidity_usd) as avg_liquidity,
                AVG(risk_score) as avg_risk_score,
                SUM(CASE WHEN outcome='rugged' THEN 1 ELSE 0 END) as rugged,
                SUM(CASE WHEN outcome='graduated' THEN 1 ELSE 0 END) as graduated
            FROM token_analytics
            WHERE detected_at >= datetime('now', ?)
        """, (f"-{hours} hours",)).fetchone()
        conn.close()
        return dict(rows) if rows else {}
    except Exception as e:
        logger.debug(f"Analytics summary error: {e}")
        return {}


def get_rejection_breakdown(hours: int = 24) -> list:
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        rows = conn.execute("""
            SELECT rejection_reason, COUNT(*) as count
            FROM token_analytics
            WHERE detected_at >= datetime('now', ?) AND verdict = 'reject'
            GROUP BY rejection_reason
            ORDER BY count DESC
            LIMIT 15
        """, (f"-{hours} hours",)).fetchall()
        conn.close()
        return [{"reason": r[0], "count": r[1]} for r in rows]
    except Exception:
        return []


def get_all_time_pnl() -> dict:
    """All-time trade summary for /pnl and /stats."""
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        r = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='closed' THEN 1 ELSE 0 END) as closed,
                SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pnl_usd <= 0 THEN 1 ELSE 0 END) as losses,
                SUM(COALESCE(pnl_usd, 0)) as total_pnl,
                MAX(COALESCE(pnl_usd, 0)) as best_pnl,
                MIN(COALESCE(pnl_usd, 0)) as worst_pnl,
                AVG(CASE WHEN pnl_usd IS NOT NULL THEN pnl_pct END) as avg_pnl_pct
            FROM trades
            WHERE paper_trade = 0 AND status = 'closed'
        """).fetchone()
        conn.close()
        return dict(r) if r else {}
    except Exception as e:
        logger.debug(f"All-time PnL error: {e}")
        return {}


def get_weekly_pnl() -> list:
    """Per-day P&L for the last 7 days."""
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        rows = conn.execute("""
            SELECT
                DATE(entry_time) as day,
                COUNT(*) as trades,
                SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pnl_usd <= 0 THEN 1 ELSE 0 END) as losses,
                SUM(COALESCE(pnl_usd, 0)) as pnl
            FROM trades
            WHERE paper_trade = 0
              AND status = 'closed'
              AND entry_time >= datetime('now', '-7 days')
            GROUP BY DATE(entry_time)
            ORDER BY day DESC
        """).fetchall()
        conn.close()
        return [{"day": r[0], "trades": r[1], "wins": r[2],
                 "losses": r[3], "pnl": r[4]} for r in rows]
    except Exception as e:
        logger.debug(f"Weekly PnL error: {e}")
        return []


def get_top_trades(n: int = 5) -> list:
    """Best N closed trades by peak multiplier."""
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        rows = conn.execute("""
            SELECT token_symbol, pnl_usd, pnl_pct, exit_reason,
                   entry_time, sol_spent
            FROM trades
            WHERE paper_trade = 0
              AND status = 'closed'
              AND pnl_usd IS NOT NULL
            ORDER BY pnl_usd DESC
            LIMIT ?
        """, (n,)).fetchall()
        conn.close()
        return [{"symbol": r[0], "pnl_usd": r[1], "pnl_pct": r[2],
                 "exit_reason": r[3], "entry_time": r[4],
                 "sol_spent": r[5]} for r in rows]
    except Exception as e:
        logger.debug(f"Top trades error: {e}")
        return []


def get_filter_stage_breakdown(hours: int = 24) -> dict:
    """How many tokens each filter stage rejects, plus pass rate."""
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        r = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN rejection_stage='stage1' THEN 1 ELSE 0 END) as s1,
                SUM(CASE WHEN rejection_stage='stage2' THEN 1 ELSE 0 END) as s2,
                SUM(CASE WHEN rejection_stage='stage3' THEN 1 ELSE 0 END) as s3,
                SUM(CASE WHEN verdict IN ('clean','caution') THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN verdict='caution' THEN 1 ELSE 0 END) as caution,
                SUM(CASE WHEN source='telegram_signal' THEN 1 ELSE 0 END) as sig_total,
                SUM(CASE WHEN source='telegram_signal'
                    AND verdict IN ('clean','caution') THEN 1 ELSE 0 END) as sig_passed,
                AVG(CASE WHEN verdict IN ('clean','caution') THEN risk_score END) as avg_pass_score,
                AVG(CASE WHEN verdict='reject' THEN risk_score END) as avg_rej_score,
                AVG(CASE WHEN verdict IN ('clean','caution') THEN liquidity_usd END) as avg_pass_liq
            FROM token_analytics
            WHERE detected_at >= datetime('now', ?)
        """, (f"-{hours} hours",)).fetchone()
        conn.close()
        return dict(r) if r else {}
    except Exception as e:
        logger.debug(f"Stage breakdown error: {e}")
        return {}


def get_signal_channel_stats(hours: int = 168) -> dict:
    """Signal channel stats for the last N hours (default 7 days)."""
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        r = conn.execute("""
            SELECT
                COUNT(*) as seen,
                SUM(CASE WHEN verdict IN ('clean','caution') THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN verdict='reject' THEN 1 ELSE 0 END) as rejected,
                AVG(risk_score) as avg_score
            FROM token_analytics
            WHERE source='telegram_signal'
              AND detected_at >= datetime('now', ?)
        """, (f"-{hours} hours",)).fetchone()
        conn.close()
        return dict(r) if r else {}
    except Exception as e:
        logger.debug(f"Signal channel stats error: {e}")
        return {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
