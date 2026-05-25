# core/risk_guard.py
# Daily loss limit + low balance guard
# Called every 5 minutes from main loop

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import config
from utils.logger import get_logger

logger = get_logger("risk_guard")

DB_PATH = Path("data/sniper.db")


def get_today_pnl_usd() -> float:
    """Return net P&L in USD for today's closed live trades."""
    try:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        row  = conn.execute("""
            SELECT COALESCE(SUM(pnl_usd), 0)
            FROM trades
            WHERE paper_trade = 0
              AND status = 'closed'
              AND DATE(exit_time) = ?
        """, (date,)).fetchone()
        conn.close()
        return row[0] if row else 0.0
    except Exception as e:
        logger.debug(f"get_today_pnl error: {e}")
        return 0.0


def check_daily_loss(wallet_sol: float, sol_price: float) -> tuple[bool, str]:
    """
    Returns (should_pause, reason).
    Pauses if today's losses exceed DAILY_LOSS_LIMIT_PCT of current wallet value.
    """
    if wallet_sol <= 0 or sol_price <= 0:
        return False, ""

    wallet_usd  = wallet_sol * sol_price
    pnl_usd     = get_today_pnl_usd()

    if pnl_usd >= 0:
        return False, ""

    loss_pct = abs(pnl_usd) / wallet_usd
    limit    = config.DAILY_LOSS_LIMIT_PCT

    if loss_pct >= limit:
        return True, (
            f"Daily loss limit hit — lost ${abs(pnl_usd):.2f} "
            f"({loss_pct*100:.0f}% of wallet, limit is {limit*100:.0f}%)"
        )
    return False, ""


def check_low_balance(wallet_sol: float, sol_price: float) -> tuple[bool, str]:
    """
    Returns (should_pause, reason).
    Pauses if wallet drops below LOW_BALANCE_USD to preserve gas.
    """
    if sol_price <= 0:
        return False, ""

    wallet_usd = wallet_sol * sol_price
    threshold  = config.LOW_BALANCE_USD

    if wallet_usd < threshold:
        return True, (
            f"Low balance — wallet is ${wallet_usd:.2f} "
            f"(below ${threshold:.0f} safety floor)"
        )
    return False, ""
