# paper/paper_engine.py
# Full paper trading simulation — mirrors live position_manager.py exactly.
# Uses SQLite database for persistence — survives Railway redeploys.
# Prices fetched live from DexScreener every 10s.

import asyncio
import json
from datetime import datetime, timezone

import aiohttp

import config
from utils.logger import get_logger
from core.database import (
    paper_get_trades_needing_check, paper_update_post_check, paper_record_refill,
    paper_get_balance, paper_update_balance, paper_reset as db_reset,
    paper_open_trade, paper_update_trade, paper_close_trade,
    paper_get_open_trades, paper_get_stats,
    paper_is_rugged, paper_add_rug,
    paper_get_starting,
)

logger = get_logger("paper_engine")

PAPER_STARTING_BALANCE = 40.0
PAPER_POSITION_USD     = config.SIGNAL_POSITION_USD


def _dynamic_trail_pct(multiplier: float) -> float:
    if multiplier >= 10:    return 0.10
    elif multiplier >= 5:   return 0.13
    elif multiplier >= 2:   return 0.18
    elif multiplier >= 1.5: return 0.22
    elif multiplier >= 1.3: return 0.25
    else:                   return 0.28


class PaperEngine:
    def __init__(self, on_notify=None):
        self._on_notify = on_notify
        self._running   = False
        # In-memory cache of open positions {mint: {db_row fields}}
        self._open: dict[str, dict] = {}
        # Session-level traded set (also backed by DB open trades)
        self._traded: set[str] = set()
        self._load_open_positions()

    def _load_open_positions(self):
        """Load open positions from DB into memory on startup."""
        rows = paper_get_open_trades()
        for row in rows:
            self._open[row["mint"]] = row
            self._traded.add(row["mint"])
        if rows:
            logger.info(f"Paper engine: {len(rows)} open position(s) restored from DB")

    # ── Balance ────────────────────────────────────────────────────────────────

    def balance(self) -> float:
        return paper_get_balance()

    def reset(self):
        self._open.clear()
        self._traded.clear()
        db_reset(PAPER_STARTING_BALANCE)
        logger.info(f"Paper trading reset — balance: ${PAPER_STARTING_BALANCE:.2f}")

    # ── Open a paper trade ─────────────────────────────────────────────────────

    async def open_trade(self, mint: str, name: str, symbol: str,
                         channel: str, score: int, ai_prob: float = None) -> dict:

        if mint in self._traded:
            return {"success": False, "reason": "Already traded this token in this session"}

        if paper_is_rugged(mint):
            logger.info(f"🚫 PAPER RUG MEMORY: {mint[:16]} — previously rugged, skipping")
            return {"success": False, "reason": "Token previously rugged — skipped by rug memory"}

        if len(self._open) >= config.MAX_OPEN_TRADES:
            return {"success": False, "reason": f"Max {config.MAX_OPEN_TRADES} paper positions open"}

        bal = paper_get_balance()
        # Drawdown protection — if balance drops below 75% of starting, pause and notify
        import os as _dpos
        _drawdown_limit = float(_dpos.getenv("PAPER_DRAWDOWN_LIMIT", "0.75"))
        _drawdown_floor = PAPER_STARTING_BALANCE * _drawdown_limit
        if bal < _drawdown_floor and bal > 0:
            logger.warning(
                f"⛔ PAPER DRAWDOWN LIMIT | Balance ${bal:.2f} below "
                f"${_drawdown_floor:.2f} ({_drawdown_limit*100:.0f}% floor) — pausing"
            )
            if self._on_notify:
                await self._on_notify("paper_drawdown", {
                    "balance": bal,
                    "floor": _drawdown_floor,
                    "starting": PAPER_STARTING_BALANCE,
                })
            return {"success": False, "reason": f"Drawdown limit hit (${bal:.2f})"}

        if bal < PAPER_POSITION_USD:
            # Auto-refill — top up to $40 so data collection never stops
            logger.info(f"📋 PAPER auto-refill: ${bal:.2f} → ${PAPER_STARTING_BALANCE:.2f}")
            paper_record_refill()
            paper_update_balance(PAPER_STARTING_BALANCE)
            bal = PAPER_STARTING_BALANCE
            if self._on_notify:
                await self._on_notify("paper_refill", {
                    "old_balance": bal,
                    "new_balance": PAPER_STARTING_BALANCE,
                })

        entry_price = await self._get_price(mint)
        if not entry_price or entry_price <= 0:
            return {"success": False, "reason": "Could not fetch entry price from DexScreener"}

        tokens_held      = PAPER_POSITION_USD / entry_price
        fixed_stop_price = entry_price * (1 - config.STOP_LOSS_PHASE1_PCT)

        trade_id = paper_open_trade(
            mint=mint, name=name, symbol=symbol,
            channel=channel, score=score,
            entry_price=entry_price,
            usd_spent=PAPER_POSITION_USD,
            tokens_held=tokens_held,
            fixed_stop_price=fixed_stop_price,
            ai_prob=ai_prob,
        )

        new_bal = bal - PAPER_POSITION_USD
        paper_update_balance(new_bal)

        row = {
            "id": trade_id, "mint": mint, "name": name, "symbol": symbol,
            "channel": channel, "score": score,
            "entry_price_usd": entry_price,
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "usd_spent": PAPER_POSITION_USD,
            "tokens_held": tokens_held,
            "phase": 1,
            "highest_price_usd": entry_price,
            "fixed_stop_price": fixed_stop_price,
            "trailing_stop_price": 0.0,
            "tp_levels_hit": [],
            "total_realised_usd": 0.0,
            "extended_at": None,
            "peak_multiplier": 1.0,
        }
        self._open[mint]  = row
        self._traded.add(mint)

        logger.info(
            f"📋 PAPER OPEN | {name} | Entry: ${entry_price:.8f} | "
            f"Spent: ${PAPER_POSITION_USD:.2f} | Balance: ${new_bal:.2f}"
        )

        if self._on_notify:
            await self._on_notify("paper_opened", {
                "mint": mint, "name": name, "symbol": symbol,
                "entry_price": entry_price, "usd_spent": PAPER_POSITION_USD,
                "channel": channel, "score": score, "balance": new_bal,
            })

        return {"success": True, "entry_price": entry_price}

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def start(self):
        self._running = True
        logger.info("Paper engine started — checking every 10s")
        asyncio.create_task(self._post_check_loop())
        while self._running:
            try:
                if self._open:
                    for pos in list(self._open.values()):
                        try:
                            await self._check_position(pos)
                        except Exception as e:
                            logger.error(f"Paper check error {pos['symbol']}: {e}")
            except Exception as e:
                logger.error(f"Paper engine error: {e}")
            await asyncio.sleep(10)

    async def _post_check_loop(self):
        """Check closed trades at 1h, 2h, 24h after close to see where price went."""
        while self._running:
            try:
                trades = paper_get_trades_needing_check()
                for t in trades:
                    await self._run_post_check(t)
            except Exception as e:
                logger.debug(f"Post-check loop error: {e}")
            await asyncio.sleep(300)  # check every 5 minutes

    async def _run_post_check(self, trade: dict):
        mint        = trade["mint"]
        close_price = trade["close_price_usd"] or 0
        if close_price <= 0:
            return

        price = await self._get_price(mint)
        if not price or price <= 0:
            return

        pct_change = ((price - close_price) / close_price * 100) if close_price > 0 else 0

        closed_at = datetime.fromisoformat(trade["closed_at"])
        if closed_at.tzinfo is None:
            closed_at = closed_at.replace(tzinfo=timezone.utc)
        hours_since = (datetime.now(timezone.utc) - closed_at).total_seconds() / 3600

        # Record at the appropriate checkpoint
        if trade.get("price_1h_after") is None and hours_since >= 1:
            paper_update_post_check(trade["id"], 1, price, pct_change)
            logger.info(
                f"📊 POST-CHECK 1h | {trade['symbol']} | "
                f"Exit: ${close_price:.8f} → Now: ${price:.8f} | {pct_change:+.1f}%"
            )
            if self._on_notify:
                await self._on_notify("paper_post_check", {
                    "name": trade["name"], "symbol": trade["symbol"],
                    "hours": 1, "exit_price": close_price,
                    "current_price": price, "pct_change": pct_change,
                    "close_reason": trade["close_reason"],
                    "original_pnl_pct": trade["final_pnl_pct"] or 0,
                })

        elif trade.get("price_2h_after") is None and hours_since >= 2:
            paper_update_post_check(trade["id"], 2, price, pct_change)
            logger.info(
                f"📊 POST-CHECK 2h | {trade['symbol']} | {pct_change:+.1f}% from exit"
            )

        elif trade.get("price_24h_after") is None and hours_since >= 24:
            paper_update_post_check(trade["id"], 24, price, pct_change)
            logger.info(
                f"📊 POST-CHECK 24h | {trade['symbol']} | {pct_change:+.1f}% from exit"
            )
            if self._on_notify:
                await self._on_notify("paper_post_check", {
                    "name": trade["name"], "symbol": trade["symbol"],
                    "hours": 24, "exit_price": close_price,
                    "current_price": price, "pct_change": pct_change,
                    "close_reason": trade["close_reason"],
                    "original_pnl_pct": trade["final_pnl_pct"] or 0,
                })

    async def stop(self):
        self._running = False

    # ── Check position ─────────────────────────────────────────────────────────

    async def _check_position(self, pos: dict):
        mint  = pos["mint"]
        price = await self._get_price(mint)
        if not price or price <= 0:
            return

        now        = datetime.now(timezone.utc)
        entry_time = datetime.fromisoformat(pos["entry_time"])
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)
        age_sec    = (now - entry_time).total_seconds()
        age_hours  = age_sec / 3600
        entry      = pos["entry_price_usd"]
        multiplier = price / entry
        gain_pct   = (price - entry) / entry

        # Track peak
        if price > pos["highest_price_usd"]:
            pos["highest_price_usd"]    = price
            pos["peak_multiplier"]      = multiplier
            trail_pct                   = _dynamic_trail_pct(multiplier)
            pos["trailing_stop_price"]  = price * (1 - trail_pct)
            paper_update_trade(pos["id"],
                highest_price_usd=price,
                peak_multiplier=multiplier,
                trailing_stop_price=pos["trailing_stop_price"],
            )

        # ── 2-minute momentum checkpoint ──────────────────────────────────────
        # At 2min: +20% = confirmed momentum (leave alone)
        #          <20% = no momentum (tight -5% exit from peak)
        if pos["phase"] == 1 and 110 <= age_sec <= 180:
            if gain_pct >= 0.20:
                if not pos.get("momentum_confirmed"):
                    pos["momentum_confirmed"] = True
                    logger.info(
                        f"🚀 PAPER MOMENTUM | {pos['symbol']} | "
                        f"+{gain_pct*100:.0f}% at 2min — confirmed, letting run"
                    )
            elif not pos.get("tight_exit") and not pos.get("momentum_confirmed"):
                pos["tight_exit"]          = True
                tight_stop                 = pos["highest_price_usd"] * 0.95
                pos["trailing_stop_price"] = tight_stop
                paper_update_trade(pos["id"], trailing_stop_price=tight_stop)
                logger.info(
                    f"⚠️ PAPER TIGHT EXIT | {pos['symbol']} | "
                    f"Only +{gain_pct*100:.1f}% at 2min — tight stop: ${tight_stop:.8f}"
                )
                if self._on_notify:
                    await self._on_notify("paper_tight_exit", {
                        "name": pos["name"], "symbol": pos["symbol"],
                        "gain_pct": round(gain_pct * 100, 1), "age_sec": int(age_sec),
                    })

        # Phase 1 → 2
        if pos["phase"] == 1:
            if age_sec >= config.PHASE1_DURATION_SECONDS or gain_pct >= config.PHASE2_TRIGGER_GAIN_PCT:
                pos["phase"]               = 2
                trail_pct                  = _dynamic_trail_pct(multiplier)
                pos["trailing_stop_price"] = pos["highest_price_usd"] * (1 - trail_pct)
                paper_update_trade(pos["id"],
                    phase=2,
                    trailing_stop_price=pos["trailing_stop_price"],
                )
                reason = "5min" if age_sec >= config.PHASE1_DURATION_SECONDS else f"+{gain_pct*100:.0f}%"
                logger.info(f"📈 PAPER {pos['symbol']} → Phase 2 ({reason})")

        # 72hr max
        if age_hours >= config.MAX_HOLD_HOURS:
            await self._close(pos, price, "max_hold_72hr", multiplier)
            return

        # 24hr check
        if age_hours >= config.DAILY_CHECK_HOURS and not pos.get("extended_at"):
            await self._run_24hr_check(pos, price, multiplier)
            return

        if pos.get("extended_at"):
            ext = datetime.fromisoformat(pos["extended_at"])
            if ext.tzinfo is None:
                ext = ext.replace(tzinfo=timezone.utc)
            if (now - ext).total_seconds() / 3600 >= config.EXTEND_HOLD_HOURS:
                await self._close(pos, price, "extended_hold_expired", multiplier)
                return

        # Stop loss
        if pos["phase"] == 1:
            if price <= pos["fixed_stop_price"]:
                await self._close(pos, price, "stop_loss_phase1", multiplier)
                return
        elif pos["phase"] == 2:
            if pos["trailing_stop_price"] > 0 and price <= pos["trailing_stop_price"]:
                await self._close(pos, price, "trailing_stop", multiplier)
                return

        # Take profits
        await self._check_take_profits(pos, price, multiplier)

    # ── Take profits ───────────────────────────────────────────────────────────

    async def _check_take_profits(self, pos: dict, price: float, multiplier: float):
        tp_hit = pos["tp_levels_hit"]
        if isinstance(tp_hit, str):
            tp_hit = json.loads(tp_hit)

        # 2x income lock
        if multiplier >= config.INCOME_LOCK_MULTIPLIER and "2x_income_lock" not in tp_hit:
            tokens_to_sell          = pos["tokens_held"] * config.INCOME_LOCK_SELL_PCT
            usd_received            = tokens_to_sell * price
            pos["tokens_held"]     -= tokens_to_sell
            pos["total_realised_usd"] += usd_received
            tp_hit.append("2x_income_lock")
            pos["tp_levels_hit"]    = tp_hit
            new_bal = paper_get_balance() + usd_received
            paper_update_balance(new_bal)
            paper_update_trade(pos["id"],
                tokens_held=pos["tokens_held"],
                total_realised_usd=pos["total_realised_usd"],
                tp_levels_hit=json.dumps(tp_hit),
            )
            sell_pct = int(config.INCOME_LOCK_SELL_PCT * 100)
            logger.info(f"💰 PAPER TP 2x | {pos['symbol']} | +${usd_received:.2f}")
            if self._on_notify:
                await self._on_notify("paper_tp", {
                    "name": pos["name"], "symbol": pos["symbol"],
                    "level": f"2x income lock ({sell_pct}%)",
                    "usd_received": usd_received, "multiplier": multiplier,
                    "balance": new_bal,
                })
            return

        # Standard ladder
        tp_levels = list(config.TAKE_PROFIT_LEVELS)
        last = 20
        while last < 200:
            last += config.TP_STEP_AFTER_20X
            tp_levels.append(last)

        for level in tp_levels:
            key = f"{level}x"
            if multiplier >= level and key not in tp_hit:
                tokens_to_sell          = pos["tokens_held"] * config.TAKE_PROFIT_SELL_PCT
                usd_received            = tokens_to_sell * price
                pos["tokens_held"]     -= tokens_to_sell
                pos["total_realised_usd"] += usd_received
                tp_hit.append(key)
                pos["tp_levels_hit"]    = tp_hit
                new_bal = paper_get_balance() + usd_received
                paper_update_balance(new_bal)
                paper_update_trade(pos["id"],
                    tokens_held=pos["tokens_held"],
                    total_realised_usd=pos["total_realised_usd"],
                    tp_levels_hit=json.dumps(tp_hit),
                )
                logger.info(f"💰 PAPER TP {key} | {pos['symbol']} | +${usd_received:.2f}")
                if self._on_notify:
                    await self._on_notify("paper_tp", {
                        "name": pos["name"], "symbol": pos["symbol"],
                        "level": key, "usd_received": usd_received,
                        "multiplier": multiplier, "balance": new_bal,
                    })
                break

    # ── Close ──────────────────────────────────────────────────────────────────

    async def _close(self, pos: dict, price: float, reason: str, multiplier: float):
        tp_hit = pos["tp_levels_hit"]
        if isinstance(tp_hit, str):
            tp_hit = json.loads(tp_hit)

        entry_time = datetime.fromisoformat(pos["entry_time"])
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)

        remaining_usd = pos["tokens_held"] * price
        total_out     = pos["total_realised_usd"] + remaining_usd
        pnl_usd       = total_out - pos["usd_spent"]
        pnl_pct       = (pnl_usd / pos["usd_spent"] * 100) if pos["usd_spent"] > 0 else 0
        hold_hours    = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600

        new_bal = paper_get_balance() + remaining_usd
        paper_update_balance(new_bal)

        paper_close_trade(
            trade_id=pos["id"],
            close_reason=reason,
            close_price=price,
            pnl_usd=pnl_usd,
            pnl_pct=pnl_pct,
            hold_hours=hold_hours,
            peak_multiplier=pos["peak_multiplier"],
            total_realised_usd=pos["total_realised_usd"],
            tp_levels_hit=tp_hit,
        )

        # Add to rug memory if loss
        if pnl_usd < 0 or reason in ("stop_loss_phase1", "24hr_check_failed"):
            paper_add_rug(pos["mint"], reason)

        # Remove from in-memory open positions
        self._open.pop(pos["mint"], None)

        emoji = "✅" if pnl_usd > 0 else "❌"
        logger.info(
            f"{emoji} PAPER CLOSED | {pos['symbol']} | {reason} | "
            f"Peak: {pos['peak_multiplier']:.2f}x | P&L: ${pnl_usd:+.2f} ({pnl_pct:+.1f}%) | "
            f"Balance: ${new_bal:.2f}"
        )

        if self._on_notify:
            await self._on_notify("paper_closed", {
                "name": pos["name"], "symbol": pos["symbol"],
                "reason": reason, "pnl_usd": pnl_usd, "pnl_pct": pnl_pct,
                "peak_mult": pos["peak_multiplier"], "tp_levels": tp_hit,
                "balance": new_bal, "multiplier": multiplier,
            })

    # ── 24hr check ─────────────────────────────────────────────────────────────

    async def _run_24hr_check(self, pos: dict, price: float, multiplier: float):
        signals  = await self._get_hold_signals(pos["mint"])
        positive = sum(1 for v in signals.values() if v)
        logger.info(f"⏰ PAPER 24hr check: {pos['symbol']} — {positive}/4 signals")

        if positive >= 3:
            now_str = datetime.now(timezone.utc).isoformat()
            pos["extended_at"] = now_str
            paper_update_trade(pos["id"], extended_at=now_str)
            if self._on_notify:
                await self._on_notify("paper_24hr_extend", {
                    "name": pos["name"], "symbol": pos["symbol"],
                    "signals": signals, "positive": positive,
                })
        else:
            await self._close(pos, price, "24hr_check_failed", multiplier)

    async def _get_hold_signals(self, mint: str) -> dict:
        signals = {
            "volume_increasing": False,
            "holders_growing":   False,
            "higher_lows":       False,
            "social_momentum":   False,
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as resp:
                    if resp.status == 200:
                        data  = await resp.json()
                        pairs = data.get("pairs", [])
                        if pairs:
                            p       = pairs[0]
                            vol_24h = p.get("volume", {}).get("h24", 0) or 0
                            vol_6h  = p.get("volume", {}).get("h6", 0) or 0
                            p_ch    = p.get("priceChange", {}).get("h24", 0) or 0
                            buys_1h = p.get("txns", {}).get("h1", {}).get("buys", 0) or 0
                            signals["volume_increasing"] = vol_6h > (vol_24h * 0.25) if vol_24h > 0 else False
                            signals["higher_lows"]       = p_ch > -15
                            signals["social_momentum"]   = buys_1h > 5
        except Exception as e:
            logger.debug(f"Paper hold signals error: {e}")
        return signals

    # ── Price fetch ────────────────────────────────────────────────────────────

    async def _get_price(self, mint: str) -> float | None:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
                    timeout=aiohttp.ClientTimeout(total=6)
                ) as resp:
                    if resp.status == 200:
                        data  = await resp.json()
                        pairs = data.get("pairs") or []
                        if pairs:
                            return float(pairs[0].get("priceUsd") or 0)
        except Exception as e:
            logger.debug(f"Paper price fetch {mint[:16]}: {e}")
        return None

    # ── Stats (for /paper command) ─────────────────────────────────────────────

    def get_stats(self) -> dict:
        s = paper_get_stats()
        s["balance"]  = paper_get_balance()
        s["starting"] = paper_get_starting()
        return s

    def get_open_positions(self) -> list:
        return list(self._open.values())
