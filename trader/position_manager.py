# trader/position_manager.py
# Full exit logic — dynamic trailing stop, TP ladder, 24hr check, 72hr max
#
# IMPROVED trailing stop formula (tightens faster as profit grows):
#   Entry  → 1.3x : 28% trail  (IMPROVED: new tier — slightly tighter than before)
#   1.3x   → 1.5x : 25% trail
#   1.5x   → 2x   : 22% trail
#   2x     → 5x   : 18% trail  (IMPROVED: tighter after income lock)
#   5x     → 10x  : 13% trail
#   10x+         : 10% trail   (maximum capture at moon run)
#
# Phase 1 (0–5 min): fixed -40% hard stop (volatile new token zone)
# Phase 2 (triggered at +15% gain OR 5 min): dynamic trailing stop kicks in
#
# IMPROVEMENT SUMMARY vs v7:
# - Phase 2 triggers at +15% gain (was +30%) → locks in trail sooner
# - Income lock sells 50% at 2x (was 70%) → more moonbag riding
# - Added 3x TP level (gap between 2x and 5x filled)
# - Dynamic trail has finer tiers — tightens faster between 1.3x and 2x
# - SOL price cache has a TTL of 60s (was never-expiring)

import asyncio
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Callable, Awaitable

import aiohttp

import config
from utils.logger import get_logger
from utils.models import Position
from trader.executor import sell_token, get_token_price_sol
from core.trade_logger import log_trade_snapshot, finalize_trade

logger = get_logger("position_manager")

POSITIONS_FILE = Path("data/positions.json")
POSITIONS_FILE.parent.mkdir(exist_ok=True)


def _dynamic_trail_pct(multiplier: float) -> float:
    """
    IMPROVED: Dynamic trailing stop — tightens faster as profit grows.
    More tiers between entry and 2x where most coins live.
    The tighter the trail, the more profit you lock in on the way down.
    """
    if multiplier >= 10:    return 0.10   # 10% trail at 10x+
    elif multiplier >= 5:   return 0.13   # 13% trail at 5-10x (was 15%)
    elif multiplier >= 2:   return 0.18   # 18% trail at 2-5x (was 20%)
    elif multiplier >= 1.5: return 0.22   # 22% trail at 1.5-2x (was 25%)
    elif multiplier >= 1.3: return 0.25   # IMPROVED: new tier at 1.3x
    else:                   return 0.28   # 28% trail in entry zone (was 30%)


class PositionManager:
    def __init__(self, sol_price_fn: Callable[[], Awaitable[float]],
                 on_exit_notify: Callable[[str, dict], Awaitable[None]] | None = None):
        self._positions: dict[str, Position] = {}
        self._sol_price_fn   = sol_price_fn
        self._on_exit_notify = on_exit_notify
        self._running        = False
        self._last_snapshot: dict[str, datetime] = {}
        self._load_positions()

    # ── Public API ─────────────────────────────────────────────────────────────

    def add_position(self, pos: Position):
        self._positions[pos.mint_address] = pos
        self._save_positions()
        logger.info(
            f"📥 Position opened: {pos.token_symbol} | "
            f"Source: {pos.source} | "
            f"Entry: ${pos.entry_price_usd:.8f} | "
            f"Spent: ${pos.usd_amount:.2f} | "
            f"SL (Phase 1): ${pos.fixed_stop_price:.8f} (-40%) | "
            f"Phase 2 triggers at +15% gain or 5 min"
        )

    def get_position(self, mint: str) -> Position | None:
        return self._positions.get(mint)

    def get_all_positions(self) -> list[Position]:
        return list(self._positions.values())

    def position_count(self) -> int:
        return len(self._positions)

    async def manual_sell_all(self, mint: str) -> bool:
        pos = self._positions.get(mint)
        if not pos:
            return False
        logger.info(f"🤚 Manual sell: {pos.token_symbol}")
        await self._close_position(pos, reason="manual_sell")
        return True

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def start(self):
        self._running = True
        logger.info("Position manager started — checking every 10s")
        while self._running:
            try:
                if self._positions:
                    await self._check_all_positions()
            except Exception as e:
                logger.error(f"Position manager error: {e}")
            await asyncio.sleep(10)

    async def stop(self):
        self._running = False

    async def _check_all_positions(self):
        sol_price = await self._sol_price_fn()
        for mint, pos in list(self._positions.items()):
            try:
                await self._check_position(pos, sol_price)
            except Exception as e:
                logger.error(f"Error checking {pos.token_symbol}: {e}")

    async def _check_position(self, pos: Position, sol_price: float):
        now        = datetime.now(timezone.utc)
        age_sec    = (now - pos.entry_time).total_seconds()
        age_hours  = age_sec / 3600

        price_sol = await get_token_price_sol(pos.mint_address)
        if price_sol is None:
            logger.debug(f"No price for {pos.token_symbol}")
            return

        current_price_usd = price_sol * sol_price
        entry             = pos.entry_price_usd

        if entry <= 0:
            return

        multiplier = current_price_usd / entry
        gain_pct   = (current_price_usd - entry) / entry

        # Update highest price + recalculate dynamic trailing stop
        if current_price_usd > pos.highest_price_usd:
            pos.highest_price_usd   = current_price_usd
            trail_pct               = _dynamic_trail_pct(multiplier)
            pos.trailing_stop_price = pos.highest_price_usd * (1 - trail_pct)
            self._save_positions()

        logger.debug(
            f"{pos.token_symbol} | ${current_price_usd:.8f} | "
            f"{gain_pct*100:+.1f}% | {multiplier:.2f}x | "
            f"Trail:{_dynamic_trail_pct(multiplier)*100:.0f}% | Phase:{pos.phase}"
        )

        # ── AI data snapshots (every 5 minutes) ───────────────────────────────
        last_snap = self._last_snapshot.get(pos.mint_address)
        if last_snap is None or (now - last_snap).total_seconds() >= 300:
            log_trade_snapshot(
                mint=pos.mint_address,
                symbol=pos.token_symbol,
                age_min=age_sec / 60,
                multiplier=multiplier,
                price_usd=current_price_usd,
                sol_price=sol_price,
            )
            self._last_snapshot[pos.mint_address] = now

        # ── Hard max 72 hours ──────────────────────────────────────────────────
        if age_hours >= config.MAX_HOLD_HOURS:
            logger.info(f"⏰ 72hr max reached: {pos.token_symbol}")
            await self._close_position(pos, reason="max_hold_72hr",
                                       multiplier=multiplier, sol_price=sol_price)
            return

        # ── 24hr check ────────────────────────────────────────────────────────
        if age_hours >= config.DAILY_CHECK_HOURS and pos.extended_at is None:
            await self._run_24hr_check(pos, current_price_usd, multiplier, sol_price)
            return

        # Extended hold expiry
        if pos.extended_at:
            hrs_since = (now - pos.extended_at).total_seconds() / 3600
            if hrs_since >= config.EXTEND_HOLD_HOURS:
                logger.info(f"⏰ Extended hold expired: {pos.token_symbol}")
                await self._close_position(pos, reason="extended_hold_expired",
                                           multiplier=multiplier, sol_price=sol_price)
                return

        # ── Phase 1 → 2 transition ─────────────────────────────────────────────
        # IMPROVED: phase 2 triggers at +15% gain (was +30%)
        if pos.phase == 1:
            phase2_time = age_sec >= config.PHASE1_DURATION_SECONDS
            phase2_gain = gain_pct >= config.PHASE2_TRIGGER_GAIN_PCT   # now 15%
            if phase2_time or phase2_gain:
                reason = "5min" if phase2_time else f"+{gain_pct*100:.0f}%"
                logger.info(f"📈 {pos.token_symbol} → Phase 2 ({reason}) | Trail: {_dynamic_trail_pct(multiplier)*100:.0f}%")
                pos.phase               = 2
                trail_pct               = _dynamic_trail_pct(multiplier)
                pos.trailing_stop_price = pos.highest_price_usd * (1 - trail_pct)
                self._save_positions()

        # ── Stop loss ──────────────────────────────────────────────────────────
        if pos.phase == 1:
            if current_price_usd <= pos.fixed_stop_price:
                loss = gain_pct * 100
                logger.info(f"🛑 STOP LOSS | {pos.token_symbol} | {loss:.1f}%")
                await self._close_position(pos, reason="stop_loss_phase1",
                                           multiplier=multiplier, sol_price=sol_price)
                return
        elif pos.phase == 2:
            if current_price_usd <= pos.trailing_stop_price:
                drop = (current_price_usd - pos.highest_price_usd) / pos.highest_price_usd * 100
                trail_pct = _dynamic_trail_pct(multiplier)
                logger.info(
                    f"🛑 TRAILING STOP | {pos.token_symbol} | "
                    f"{drop:.1f}% from high | Trail was {trail_pct*100:.0f}%"
                )
                await self._close_position(pos, reason="trailing_stop",
                                           multiplier=multiplier, sol_price=sol_price)
                return

        # ── Take profit ladder ─────────────────────────────────────────────────
        await self._check_take_profits(pos, multiplier, current_price_usd, sol_price)

    async def _check_take_profits(self, pos: Position, multiplier: float,
                                   current_price_usd: float, sol_price: float):
        # INCOME LOCK at 2x — IMPROVED: sell 50% (was 70%), let 50% ride
        if multiplier >= 2.0 and "2x_income_lock" not in pos.tp_levels_hit:
            tokens_to_sell = pos.tokens_held * config.INCOME_LOCK_SELL_PCT
            sell_pct_display = int(config.INCOME_LOCK_SELL_PCT * 100)
            logger.info(f"💰 INCOME LOCK 2x | {pos.token_symbol} | selling {sell_pct_display}%")
            result = await sell_token(pos.mint_address, tokens_to_sell)
            if result.success:
                usd_received         = result.out_amount * sol_price
                pos.tokens_held     -= tokens_to_sell
                pos.total_profit_usd += usd_received
                pos.tp_levels_hit.append("2x_income_lock")
                self._save_positions()
                await self._notify("take_profit", {
                    "token": pos.token_symbol, "level": f"2x (income lock {sell_pct_display}%)",
                    "sold_pct": sell_pct_display, "usd_received": usd_received,
                    "remaining": pos.tokens_held,
                })
            return

        # Standard TP ladder: 3x, 5x, 10x, 15x, 20x, then every 5x
        # IMPROVED: 3x added to fill the 2x → 5x gap
        tp_levels = list(config.TAKE_PROFIT_LEVELS)  # [3, 5, 10, 15, 20]
        last = 20
        while last < 200:
            last += config.TP_STEP_AFTER_20X
            tp_levels.append(last)

        for level in tp_levels:
            key = f"{level}x"
            if multiplier >= level and key not in pos.tp_levels_hit:
                tokens_to_sell = pos.tokens_held * config.TAKE_PROFIT_SELL_PCT
                if tokens_to_sell <= 0:
                    pos.tp_levels_hit.append(key)
                    continue
                logger.info(f"💰 TP {key} | {pos.token_symbol} | selling {int(config.TAKE_PROFIT_SELL_PCT*100)}%")
                result = await sell_token(pos.mint_address, tokens_to_sell)
                if result.success:
                    usd_received         = result.out_amount * sol_price
                    pos.tokens_held     -= tokens_to_sell
                    pos.total_profit_usd += usd_received
                    pos.tp_levels_hit.append(key)
                    self._save_positions()
                    await self._notify("take_profit", {
                        "token": pos.token_symbol, "level": key,
                        "sold_pct": int(config.TAKE_PROFIT_SELL_PCT*100),
                        "usd_received": usd_received,
                        "remaining": pos.tokens_held,
                    })
                break

    async def _close_position(self, pos: Position, reason: str,
                               multiplier: float = 0.0, sol_price: float = 0.0):
        if pos.tokens_held <= 0:
            self._remove_position(pos.mint_address)
            return

        if sol_price <= 0:
            sol_price = await self._sol_price_fn()

        result = await sell_token(pos.mint_address, pos.tokens_held)

        if not result.success:
            logger.error(f"Sell failed for {pos.token_symbol}: {result.error}")
            result = await sell_token(pos.mint_address, pos.tokens_held, slippage_bps=5000)
            if not result.success:
                logger.error(f"CRITICAL: Cannot sell {pos.token_symbol} — manual action needed")
                return

        usd_received = result.out_amount * sol_price
        total_pnl    = pos.total_profit_usd + usd_received - pos.usd_amount
        pnl_pct      = (total_pnl / pos.usd_amount * 100) if pos.usd_amount > 0 else 0
        exit_price   = result.price_per_token * sol_price

        logger.info(
            f"📊 CLOSED | {pos.token_symbol} | {reason} | "
            f"Peak: {pos.highest_price_usd / pos.entry_price_usd:.2f}x | "
            f"P&L: ${total_pnl:+.2f} ({pnl_pct:+.1f}%)"
        )

        finalize_trade(
            mint=pos.mint_address,
            symbol=pos.token_symbol,
            source=pos.source,
            entry_price=pos.entry_price_usd,
            exit_price=exit_price,
            entry_usd=pos.usd_amount,
            exit_usd=usd_received + pos.total_profit_usd,
            peak_multiplier=pos.highest_price_usd / pos.entry_price_usd if pos.entry_price_usd > 0 else 0,
            exit_reason=reason,
            hold_hours=(datetime.now(timezone.utc) - pos.entry_time).total_seconds() / 3600,
            tp_levels=pos.tp_levels_hit,
            pnl_usd=total_pnl,
            pnl_pct=pnl_pct,
        )

        await self._notify(reason, {
            "token":      pos.token_symbol,
            "reason":     reason,
            "exit_price": exit_price,
            "usd_out":    usd_received,
            "total_pnl":  total_pnl,
            "pnl_pct":    pnl_pct,
        })

        self._remove_position(pos.mint_address)

    # ── 24hr check ─────────────────────────────────────────────────────────────

    async def _run_24hr_check(self, pos: Position, current_price_usd: float,
                               multiplier: float = 0.0, sol_price: float = 0.0):
        logger.info(f"⏰ 24hr check: {pos.token_symbol}")
        signals  = await self._get_hold_signals(pos.mint_address, current_price_usd, pos)
        positive = sum(1 for v in signals.values() if v)
        logger.info(
            f"  Signals: " + " | ".join(f"{'✅' if v else '❌'} {k}" for k, v in signals.items())
        )

        if positive >= 3:
            logger.info(f"✅ Extending: {pos.token_symbol} ({positive}/4 signals)")
            pos.extended_at = datetime.now(timezone.utc)
            self._save_positions()
            await self._notify("24hr_extend", {
                "token": pos.token_symbol, "signals": signals, "positive": positive
            })
        else:
            logger.info(f"❌ Closing: {pos.token_symbol} ({positive}/4 signals)")
            await self._close_position(pos, reason="24hr_check_failed",
                                       multiplier=multiplier, sol_price=sol_price)

    async def _get_hold_signals(self, mint: str, current_price: float, pos: Position) -> dict:
        signals = {
            "volume_increasing": False,
            "holders_growing":   False,
            "higher_lows":       False,
            "social_momentum":   False,
        }
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        data   = await resp.json()
                        pairs  = data.get("pairs", [])
                        if pairs:
                            p        = pairs[0]
                            vol_24h  = p.get("volume", {}).get("h24", 0) or 0
                            vol_6h   = p.get("volume", {}).get("h6", 0) or 0
                            price_ch = p.get("priceChange", {}).get("h24", 0) or 0
                            buys_1h  = p.get("txns", {}).get("h1", {}).get("buys", 0) or 0

                            signals["volume_increasing"] = vol_6h > (vol_24h * 0.25) if vol_24h > 0 else False
                            signals["higher_lows"]       = price_ch > -15
                            signals["social_momentum"]   = buys_1h > 5

            holders = await self._get_holder_count(mint)
            signals["holders_growing"] = (holders or 0) > 100

        except Exception as e:
            logger.debug(f"Hold signals error: {e}")

        return signals

    async def _get_holder_count(self, mint: str) -> int | None:
        url     = f"https://mainnet.helius-rpc.com/?api-key={config.HELIUS_API_KEY}"
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getTokenAccounts",
                   "params": {"mint": mint, "limit": 1, "page": 1}}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    data = await resp.json()
                    return data.get("result", {}).get("total")
        except Exception:
            return None

    # ── Notifications ──────────────────────────────────────────────────────────

    async def _notify(self, event_type: str, data: dict):
        if self._on_exit_notify:
            try:
                await self._on_exit_notify(event_type, data)
            except Exception as e:
                logger.debug(f"Notify error: {e}")

    # ── Persistence ───────────────────────────────────────────────────────────

    def _remove_position(self, mint: str):
        self._positions.pop(mint, None)
        self._last_snapshot.pop(mint, None)
        self._save_positions()

    def _save_positions(self):
        try:
            data = {}
            for mint, pos in self._positions.items():
                data[mint] = {
                    "mint_address":        pos.mint_address,
                    "token_name":          pos.token_name,
                    "token_symbol":        pos.token_symbol,
                    "entry_price_usd":     pos.entry_price_usd,
                    "entry_time":          pos.entry_time.isoformat(),
                    "sol_amount":          pos.sol_amount,
                    "usd_amount":          pos.usd_amount,
                    "tokens_held":         pos.tokens_held,
                    "tokens_initial":      pos.tokens_initial,
                    "pair_address":        pos.pair_address,
                    "source":              pos.source,
                    "phase":               pos.phase,
                    "highest_price_usd":   pos.highest_price_usd,
                    "trailing_stop_price": pos.trailing_stop_price,
                    "fixed_stop_price":    pos.fixed_stop_price,
                    "tp_levels_hit":       pos.tp_levels_hit,
                    "total_profit_usd":    pos.total_profit_usd,
                    "extended_at":         pos.extended_at.isoformat() if pos.extended_at else None,
                }
            with open(POSITIONS_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Save positions error: {e}")

    def _load_positions(self):
        if not POSITIONS_FILE.exists():
            return
        try:
            with open(POSITIONS_FILE, "r") as f:
                data = json.load(f)
            for mint, d in data.items():
                pos = Position(
                    mint_address=d["mint_address"],
                    token_name=d["token_name"],
                    token_symbol=d["token_symbol"],
                    entry_price_usd=d["entry_price_usd"],
                    entry_time=datetime.fromisoformat(d["entry_time"]),
                    sol_amount=d["sol_amount"],
                    usd_amount=d["usd_amount"],
                    tokens_held=d["tokens_held"],
                    tokens_initial=d["tokens_initial"],
                    pair_address=d["pair_address"],
                    source=d["source"],
                )
                pos.phase               = d.get("phase", 1)
                pos.highest_price_usd   = d.get("highest_price_usd", pos.entry_price_usd)
                pos.trailing_stop_price = d.get("trailing_stop_price", 0.0)
                pos.fixed_stop_price    = d.get("fixed_stop_price", pos.entry_price_usd * 0.60)
                pos.tp_levels_hit       = d.get("tp_levels_hit", [])
                pos.total_profit_usd    = d.get("total_profit_usd", 0.0)
                ext = d.get("extended_at")
                pos.extended_at = datetime.fromisoformat(ext) if ext else None
                self._positions[mint] = pos
            if self._positions:
                logger.info(f"Loaded {len(self._positions)} open position(s)")
        except Exception as e:
            logger.error(f"Load positions error: {e}")
