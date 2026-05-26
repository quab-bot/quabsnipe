# paper/paper_engine.py
# Full paper trading simulation — mirrors live position_manager.py exactly.
# Uses DexScreener for live prices. Fake starting balance of $40.
# Every signal that passes the filter gets a virtual trade.
# All exit logic (phase 1 stop, trailing stop, TP ladder, 24hr check, 72hr max) is identical.

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

import config
from utils.logger import get_logger

logger = get_logger("paper_engine")

PAPER_FILE             = Path("data/paper_positions.json")
PAPER_LEDGER           = Path("data/paper_ledger.json")
PAPER_RUG_MEMORY       = Path("data/paper_rug_memory.json")
PAPER_FILE.parent.mkdir(exist_ok=True)

PAPER_STARTING_BALANCE = 40.0
PAPER_POSITION_USD     = config.SIGNAL_POSITION_USD   # same as live


def _dynamic_trail_pct(multiplier: float) -> float:
    """Mirrors _dynamic_trail_pct from position_manager.py exactly."""
    if multiplier >= 10:    return 0.10
    elif multiplier >= 5:   return 0.13
    elif multiplier >= 2:   return 0.18
    elif multiplier >= 1.5: return 0.22
    elif multiplier >= 1.3: return 0.25
    else:                   return 0.28


class PaperPosition:
    def __init__(self, mint: str, name: str, symbol: str, entry_price_usd: float,
                 usd_spent: float, channel: str, score: int):
        self.mint              = mint
        self.name              = name
        self.symbol            = symbol
        self.entry_price_usd   = entry_price_usd
        self.usd_spent         = usd_spent
        self.channel           = channel
        self.score             = score
        self.entry_time        = datetime.now(timezone.utc)
        self.tokens_held       = usd_spent / entry_price_usd if entry_price_usd > 0 else 0
        self.tokens_initial    = self.tokens_held
        self.phase             = 1
        self.highest_price_usd = entry_price_usd
        self.fixed_stop_price  = entry_price_usd * (1 - config.STOP_LOSS_PHASE1_PCT)
        self.trailing_stop_price = 0.0
        self.tp_levels_hit: list[str] = []
        self.total_realised_usd = 0.0
        self.extended_at       = None
        self.closed            = False
        self.close_reason      = None
        self.close_price_usd   = None
        self.final_pnl_usd     = None
        self.peak_multiplier   = 1.0

    def to_dict(self) -> dict:
        return {
            "mint": self.mint, "name": self.name, "symbol": self.symbol,
            "entry_price_usd": self.entry_price_usd, "usd_spent": self.usd_spent,
            "channel": self.channel, "score": self.score,
            "entry_time": self.entry_time.isoformat(),
            "tokens_held": self.tokens_held, "tokens_initial": self.tokens_initial,
            "phase": self.phase, "highest_price_usd": self.highest_price_usd,
            "fixed_stop_price": self.fixed_stop_price,
            "trailing_stop_price": self.trailing_stop_price,
            "tp_levels_hit": self.tp_levels_hit,
            "total_realised_usd": self.total_realised_usd,
            "extended_at": self.extended_at.isoformat() if self.extended_at else None,
            "closed": self.closed, "close_reason": self.close_reason,
            "close_price_usd": self.close_price_usd,
            "final_pnl_usd": self.final_pnl_usd,
            "peak_multiplier": self.peak_multiplier,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PaperPosition":
        p = cls(
            mint=d["mint"], name=d["name"], symbol=d["symbol"],
            entry_price_usd=d["entry_price_usd"], usd_spent=d["usd_spent"],
            channel=d["channel"], score=d["score"],
        )
        p.entry_time           = datetime.fromisoformat(d["entry_time"])
        p.tokens_held          = d["tokens_held"]
        p.tokens_initial       = d["tokens_initial"]
        p.phase                = d["phase"]
        p.highest_price_usd    = d["highest_price_usd"]
        p.fixed_stop_price     = d["fixed_stop_price"]
        p.trailing_stop_price  = d["trailing_stop_price"]
        p.tp_levels_hit        = d["tp_levels_hit"]
        p.total_realised_usd   = d["total_realised_usd"]
        ext = d.get("extended_at")
        p.extended_at          = datetime.fromisoformat(ext) if ext else None
        p.closed               = d.get("closed", False)
        p.close_reason         = d.get("close_reason")
        p.close_price_usd      = d.get("close_price_usd")
        p.final_pnl_usd        = d.get("final_pnl_usd")
        p.peak_multiplier      = d.get("peak_multiplier", 1.0)
        return p


class PaperEngine:
    def __init__(self, on_notify=None):
        self._positions: dict[str, PaperPosition] = {}
        self._on_notify  = on_notify
        self._running    = False
        self._ledger     = self._load_ledger()
        self._load_positions()
        # Track all mints traded this session to prevent double entry
        self._traded: set[str] = {m for m in self._positions}
        # Persistent rug memory — never re-trade a token that closed at a loss
        self._rug_memory: set[str] = self._load_rug_memory()

    # ── Ledger ─────────────────────────────────────────────────────────────────

    def _load_ledger(self) -> dict:
        if PAPER_LEDGER.exists():
            try:
                with open(PAPER_LEDGER) as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            "balance":    PAPER_STARTING_BALANCE,
            "starting":   PAPER_STARTING_BALANCE,
            "trades":     [],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    def _save_ledger(self):
        try:
            with open(PAPER_LEDGER, "w") as f:
                json.dump(self._ledger, f, indent=2)
        except Exception as e:
            logger.error(f"Save ledger error: {e}")

    def balance(self) -> float:
        return self._ledger["balance"]

    def reset(self):
        self._positions.clear()
        self._traded.clear()
        self._rug_memory.clear()
        self._save_rug_memory()
        self._ledger = {
            "balance":    PAPER_STARTING_BALANCE,
            "starting":   PAPER_STARTING_BALANCE,
            "trades":     [],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save_ledger()
        self._save_positions()
        logger.info(f"Paper trading reset — balance: ${PAPER_STARTING_BALANCE:.2f}")

    # ── Open a paper trade ─────────────────────────────────────────────────────

    async def open_trade(self, mint: str, name: str, symbol: str,
                         channel: str, score: int) -> dict:
        open_positions = [p for p in self._positions.values() if not p.closed]

        if mint in self._traded:
            return {"success": False, "reason": "Already traded this token in this session"}

        if mint in self._rug_memory:
            logger.info(f"🚫 PAPER RUG MEMORY: {mint[:16]} — previously rugged, skipping")
            return {"success": False, "reason": "Token previously rugged — skipped by rug memory"}

        if len(open_positions) >= config.MAX_OPEN_TRADES:
            return {"success": False, "reason": f"Max {config.MAX_OPEN_TRADES} paper positions open"}

        if self._ledger["balance"] < PAPER_POSITION_USD:
            return {"success": False, "reason": f"Paper balance too low (${self._ledger['balance']:.2f})"}

        entry_price = await self._get_price(mint)
        if not entry_price or entry_price <= 0:
            return {"success": False, "reason": "Could not fetch entry price from DexScreener"}

        pos = PaperPosition(
            mint=mint, name=name, symbol=symbol,
            entry_price_usd=entry_price,
            usd_spent=PAPER_POSITION_USD,
            channel=channel, score=score,
        )

        self._ledger["balance"] -= PAPER_POSITION_USD
        self._positions[mint]    = pos
        self._traded.add(mint)
        self._save_positions()
        self._save_ledger()

        logger.info(
            f"📋 PAPER OPEN | {name} | Entry: ${entry_price:.8f} | "
            f"Spent: ${PAPER_POSITION_USD:.2f} | Balance: ${self._ledger['balance']:.2f}"
        )

        if self._on_notify:
            await self._on_notify("paper_opened", {
                "mint": mint, "name": name, "symbol": symbol,
                "entry_price": entry_price, "usd_spent": PAPER_POSITION_USD,
                "channel": channel, "score": score,
                "balance": self._ledger["balance"],
            })

        return {"success": True, "entry_price": entry_price}

    # ── Main price-check loop ──────────────────────────────────────────────────

    async def start(self):
        self._running = True
        logger.info("Paper engine started — checking every 10s")
        while self._running:
            try:
                open_pos = [p for p in self._positions.values() if not p.closed]
                if open_pos:
                    await self._check_all(open_pos)
            except Exception as e:
                logger.error(f"Paper engine error: {e}")
            await asyncio.sleep(10)

    async def stop(self):
        self._running = False

    async def _check_all(self, positions: list):
        for pos in positions:
            try:
                await self._check_position(pos)
            except Exception as e:
                logger.error(f"Paper check error {pos.symbol}: {e}")

    async def _check_position(self, pos: PaperPosition):
        price = await self._get_price(pos.mint)
        if not price or price <= 0:
            return

        now        = datetime.now(timezone.utc)
        age_sec    = (now - pos.entry_time).total_seconds()
        age_hours  = age_sec / 3600
        multiplier = price / pos.entry_price_usd
        gain_pct   = (price - pos.entry_price_usd) / pos.entry_price_usd

        # Track peak + update trailing stop
        if price > pos.highest_price_usd:
            pos.highest_price_usd    = price
            pos.peak_multiplier      = multiplier
            trail_pct                = _dynamic_trail_pct(multiplier)
            pos.trailing_stop_price  = pos.highest_price_usd * (1 - trail_pct)
            self._save_positions()

        # Phase 1 → 2 (same thresholds as live)
        if pos.phase == 1:
            phase2_time = age_sec >= config.PHASE1_DURATION_SECONDS
            phase2_gain = gain_pct >= config.PHASE2_TRIGGER_GAIN_PCT
            if phase2_time or phase2_gain:
                pos.phase               = 2
                trail_pct               = _dynamic_trail_pct(multiplier)
                pos.trailing_stop_price = pos.highest_price_usd * (1 - trail_pct)
                self._save_positions()
                logger.info(
                    f"📈 PAPER {pos.symbol} → Phase 2 "
                    f"({'5min' if phase2_time else f'+{gain_pct*100:.0f}%'})"
                )

        # 72hr max
        if age_hours >= config.MAX_HOLD_HOURS:
            await self._close(pos, price, "max_hold_72hr", multiplier)
            return

        # 24hr check
        if age_hours >= config.DAILY_CHECK_HOURS and pos.extended_at is None:
            await self._run_24hr_check(pos, price, multiplier)
            return

        if pos.extended_at:
            if (now - pos.extended_at).total_seconds() / 3600 >= config.EXTEND_HOLD_HOURS:
                await self._close(pos, price, "extended_hold_expired", multiplier)
                return

        # Stop loss
        if pos.phase == 1:
            if price <= pos.fixed_stop_price:
                await self._close(pos, price, "stop_loss_phase1", multiplier)
                return
        elif pos.phase == 2:
            if pos.trailing_stop_price > 0 and price <= pos.trailing_stop_price:
                await self._close(pos, price, "trailing_stop", multiplier)
                return

        # Take profit ladder
        await self._check_take_profits(pos, price, multiplier)

    async def _check_take_profits(self, pos: PaperPosition, price: float, multiplier: float):
        # Income lock at 2x — sell 50%
        if multiplier >= config.INCOME_LOCK_MULTIPLIER and "2x_income_lock" not in pos.tp_levels_hit:
            tokens_to_sell          = pos.tokens_held * config.INCOME_LOCK_SELL_PCT
            usd_received            = tokens_to_sell * price
            pos.tokens_held        -= tokens_to_sell
            pos.total_realised_usd += usd_received
            pos.tp_levels_hit.append("2x_income_lock")
            self._ledger["balance"] += usd_received
            self._save_positions()
            self._save_ledger()
            sell_pct = int(config.INCOME_LOCK_SELL_PCT * 100)
            logger.info(f"💰 PAPER TP 2x | {pos.symbol} | +${usd_received:.2f} ({sell_pct}% sold)")
            if self._on_notify:
                await self._on_notify("paper_tp", {
                    "mint": pos.mint, "name": pos.name, "symbol": pos.symbol,
                    "level": f"2x income lock ({sell_pct}%)",
                    "usd_received": usd_received, "multiplier": multiplier,
                    "balance": self._ledger["balance"],
                })
            return

        # Standard TP ladder: 3x, 5x, 10x, 15x, 20x then every 5x
        tp_levels = list(config.TAKE_PROFIT_LEVELS)
        last = 20
        while last < 200:
            last += config.TP_STEP_AFTER_20X
            tp_levels.append(last)

        for level in tp_levels:
            key = f"{level}x"
            if multiplier >= level and key not in pos.tp_levels_hit:
                tokens_to_sell          = pos.tokens_held * config.TAKE_PROFIT_SELL_PCT
                usd_received            = tokens_to_sell * price
                pos.tokens_held        -= tokens_to_sell
                pos.total_realised_usd += usd_received
                pos.tp_levels_hit.append(key)
                self._ledger["balance"] += usd_received
                self._save_positions()
                self._save_ledger()
                sell_pct = int(config.TAKE_PROFIT_SELL_PCT * 100)
                logger.info(f"💰 PAPER TP {key} | {pos.symbol} | +${usd_received:.2f}")
                if self._on_notify:
                    await self._on_notify("paper_tp", {
                        "mint": pos.mint, "name": pos.name, "symbol": pos.symbol,
                        "level": key, "usd_received": usd_received,
                        "multiplier": multiplier, "balance": self._ledger["balance"],
                    })
                break

    async def _close(self, pos: PaperPosition, price: float, reason: str, multiplier: float):
        remaining_usd           = pos.tokens_held * price
        total_out               = pos.total_realised_usd + remaining_usd
        pnl_usd                 = total_out - pos.usd_spent
        pnl_pct                 = (pnl_usd / pos.usd_spent * 100) if pos.usd_spent > 0 else 0

        pos.closed              = True
        pos.close_reason        = reason
        pos.close_price_usd     = price
        pos.final_pnl_usd       = pnl_usd
        self._ledger["balance"] += remaining_usd

        self._ledger["trades"].append({
            "mint": pos.mint, "name": pos.name, "symbol": pos.symbol,
            "channel": pos.channel, "score": pos.score,
            "entry_price": pos.entry_price_usd, "exit_price": price,
            "usd_spent": pos.usd_spent, "total_out": total_out,
            "pnl_usd": pnl_usd, "pnl_pct": pnl_pct,
            "peak_mult": pos.peak_multiplier,
            "tp_levels": pos.tp_levels_hit, "reason": reason,
            "hold_hours": (datetime.now(timezone.utc) - pos.entry_time).total_seconds() / 3600,
            "closed_at": datetime.now(timezone.utc).isoformat(),
        })

        self._save_positions()
        self._save_ledger()

        # Remember rugged tokens — don't re-trade them
        if pnl_usd < 0 or reason in ("stop_loss_phase1", "24hr_check_failed"):
            self._rug_memory.add(pos.mint)
            self._save_rug_memory()

        emoji = "✅" if pnl_usd > 0 else "❌"
        logger.info(
            f"{emoji} PAPER CLOSED | {pos.symbol} | {reason} | "
            f"Peak: {pos.peak_multiplier:.2f}x | P&L: ${pnl_usd:+.2f} ({pnl_pct:+.1f}%) | "
            f"Balance: ${self._ledger['balance']:.2f}"
        )

        if self._on_notify:
            await self._on_notify("paper_closed", {
                "mint": pos.mint, "name": pos.name, "symbol": pos.symbol,
                "reason": reason, "pnl_usd": pnl_usd, "pnl_pct": pnl_pct,
                "peak_mult": pos.peak_multiplier, "tp_levels": pos.tp_levels_hit,
                "balance": self._ledger["balance"], "multiplier": multiplier,
            })

    # ── 24hr check ─────────────────────────────────────────────────────────────

    async def _run_24hr_check(self, pos: PaperPosition, price: float, multiplier: float):
        signals  = await self._get_hold_signals(pos.mint)
        positive = sum(1 for v in signals.values() if v)
        logger.info(f"⏰ PAPER 24hr check: {pos.symbol} — {positive}/4 signals")

        if positive >= 3:
            pos.extended_at = datetime.now(timezone.utc)
            self._save_positions()
            if self._on_notify:
                await self._on_notify("paper_24hr_extend", {
                    "name": pos.name, "symbol": pos.symbol,
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

    # ── Stats ──────────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        trades   = self._ledger.get("trades", [])
        open_pos = [p for p in self._positions.values() if not p.closed]
        wins     = [t for t in trades if t["pnl_usd"] > 0]
        losses   = [t for t in trades if t["pnl_usd"] <= 0]
        total_pnl= sum(t["pnl_usd"] for t in trades)
        win_rate = len(wins) / len(trades) * 100 if trades else 0

        by_channel: dict = {}
        for t in trades:
            ch = t.get("channel", "unknown")
            if ch not in by_channel:
                by_channel[ch] = {"trades": 0, "wins": 0, "pnl": 0.0}
            by_channel[ch]["trades"] += 1
            by_channel[ch]["pnl"]    += t["pnl_usd"]
            if t["pnl_usd"] > 0:
                by_channel[ch]["wins"] += 1

        return {
            "balance":        self._ledger["balance"],
            "starting":       self._ledger["starting"],
            "total_pnl":      total_pnl,
            "total_trades":   len(trades),
            "wins":           len(wins),
            "losses":         len(losses),
            "win_rate":       win_rate,
            "open_positions": len(open_pos),
            "by_channel":     by_channel,
            "best_trade":     max(trades, key=lambda t: t["pnl_pct"], default=None),
            "worst_trade":    min(trades, key=lambda t: t["pnl_pct"], default=None),
        }

    def get_open_positions(self) -> list:
        return [p for p in self._positions.values() if not p.closed]

    # ── Persistence ────────────────────────────────────────────────────────────

    def _load_rug_memory(self) -> set:
        if PAPER_RUG_MEMORY.exists():
            try:
                with open(PAPER_RUG_MEMORY) as f:
                    data = json.load(f)
                    mints = set(data.get("rugged", []))
                    if mints:
                        logger.info(f"Paper rug memory: {len(mints)} token(s) blocked")
                    return mints
            except Exception:
                pass
        return set()

    def _save_rug_memory(self):
        try:
            with open(PAPER_RUG_MEMORY, "w") as f:
                json.dump({"rugged": list(self._rug_memory)}, f, indent=2)
        except Exception as e:
            logger.error(f"Save rug memory error: {e}")

    def _save_positions(self):
        try:
            with open(PAPER_FILE, "w") as f:
                json.dump({m: p.to_dict() for m, p in self._positions.items()}, f, indent=2)
        except Exception as e:
            logger.error(f"Save paper positions error: {e}")

    def _load_positions(self):
        if not PAPER_FILE.exists():
            return
        try:
            with open(PAPER_FILE) as f:
                data = json.load(f)
            for mint, d in data.items():
                self._positions[mint] = PaperPosition.from_dict(d)
            open_count = sum(1 for p in self._positions.values() if not p.closed)
            if self._positions:
                logger.info(f"Paper engine: {open_count} open paper position(s) loaded")
        except Exception as e:
            logger.error(f"Load paper positions error: {e}")