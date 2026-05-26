# main.py — Quabsnipe Signal Bot
# Signal channels → Filter → Risk Score → Trade Execution → Telegram
# Sniper (token detector) removed — signal bot only

import asyncio
import os
import sys
from datetime import datetime, timezone

import config
from utils.logger import get_logger
from utils.models import FilterResult
from trader.trade_coordinator import TradeCoordinator
from trader.position_manager import PositionManager
from trader.wallet_manager import get_sol_balance
from core.database import init_db, get_daily_pnl, init_analytics_table, init_paper_tables, paper_export_csv
from core.analytics import get_analytics_summary, get_rejection_breakdown
from core.trade_logger import init_trade_tables
from core.channel_tracker import init_channel_tables
from core.rejected_tracker import init_rejected_tables, check_pending_outcomes
from core.risk_guard import check_daily_loss, check_low_balance
from core import runtime_config
from telegram_bot.bot import TelegramBot
from telegram_signals.session_client import SessionClient
from telegram_signals.channel_monitor import ChannelMonitor
from telegram_signals.signal_bridge import SignalBridge
from paper.paper_engine import PaperEngine

logger = get_logger("main")

# ── Trading mode ───────────────────────────────────────────────────────────────
# Set in .env:
#   SIGNAL_TRADING=off  → receives signals, paper trades only (safe default)
#   SIGNAL_TRADING=on   → receives signals, executes real trades with real money
_signal_raw    = os.getenv("SIGNAL_TRADING", "off").strip().lower()
SIGNAL_LIVE    = (_signal_raw == "on")   # True = live trades
SIGNAL_TRADING = not SIGNAL_LIVE         # True = paper mode (used in display logic)


class SignalBot:
    def __init__(self):
        init_db()
        init_analytics_table()
        init_paper_tables()
        init_trade_tables()
        init_channel_tables()
        init_rejected_tables()

        self._paused       = False
        self._pause_reason = ""
        self._monitors: list[ChannelMonitor] = []

        self._stats = {
            "started_at": datetime.now(timezone.utc),
        }

        self.position_manager = PositionManager(
            sol_price_fn=self._get_sol_price,
            on_exit_notify=self._on_position_event,
        )
        self.coordinator  = TradeCoordinator(self.position_manager)
        self.paper_engine = PaperEngine(on_notify=self._on_paper_event)

        self.telegram = TelegramBot(
            get_positions_fn = self._get_positions,
            get_balance_fn   = get_sol_balance,
            sell_all_fn      = self.position_manager.manual_sell_all,
            set_stop_fn      = self._set_trailing_stop,
            pause_fn         = self._pause,
            resume_fn        = self._resume,
            is_paused_fn     = self._is_paused,
            manual_trade_fn  = self._manual_trade,
            paper_stats_fn   = self.paper_engine.get_stats,
            paper_reset_fn   = self._paper_reset,
            paper_export_fn  = self._paper_export,
        )

    # ── Signal monitor ─────────────────────────────────────────────────────────

    async def _start_signal_monitor(self):
        if not config.SIGNAL_ENABLED:
            logger.info("Signal monitor disabled")
            return
        if not config.TELEGRAM_API_ID or not config.TELEGRAM_PHONE:
            logger.warning("Signal monitor: TELEGRAM_API_ID or TELEGRAM_PHONE not set")
            return
        try:
            self._session = SessionClient(
                api_id=config.TELEGRAM_API_ID,
                api_hash=config.TELEGRAM_API_HASH,
                phone=config.TELEGRAM_PHONE,
            )
            await self._session.start()
            self._signal_bridge = SignalBridge(self.coordinator, self.telegram, self.paper_engine)

            channels = runtime_config.get_channels()
            if not channels:
                channels = config.SIGNAL_CHANNELS

            for ch in channels:
                monitor = ChannelMonitor(
                    client=self._session.get_client(),
                    channel_username=ch,
                    on_signal=self._signal_bridge.on_signal,
                )
                self._monitors.append(monitor)
                asyncio.create_task(monitor.start())
                logger.info(f"✅ Signal monitor active → @{ch}")

        except Exception as e:
            logger.error(f"Signal monitor failed: {e}")
            await self.telegram.notify_error(f"Signal monitor failed to start: {e}")

    # ── Background loops ───────────────────────────────────────────────────────

    async def _heartbeat_loop(self):
        interval = config.HEARTBEAT_INTERVAL_HOURS * 3600
        await asyncio.sleep(interval)
        while True:
            try:
                sol    = await get_sol_balance()
                price  = await self.coordinator.get_sol_price()
                usd    = sol * price if price > 0 else 0
                pos    = self.position_manager.position_count()
                uptime = _uptime_str(self._stats["started_at"])
                paused = await self._is_paused()
                await self.telegram.notify_heartbeat(
                    uptime=uptime, sol=sol, usd=usd,
                    open_positions=pos, paused=paused,
                )
            except Exception as e:
                logger.debug(f"Heartbeat error: {e}")
            await asyncio.sleep(interval)

    async def _risk_guard_loop(self):
        while True:
            await asyncio.sleep(300)
            if self._paused and self._pause_reason == "manual":
                continue
            try:
                sol   = await get_sol_balance()
                price = await self.coordinator.get_sol_price()

                tripped, reason = check_daily_loss(sol, price)
                if tripped:
                    if not self._paused:
                        self._paused       = True
                        self._pause_reason = "daily_loss"
                        logger.warning(f"RISK GUARD: {reason}")
                        await self.telegram.notify_risk_paused(reason)
                    continue

                tripped, reason = check_low_balance(sol, price)
                if tripped:
                    if not self._paused:
                        self._paused       = True
                        self._pause_reason = "low_balance"
                        logger.warning(f"RISK GUARD: {reason}")
                        await self.telegram.notify_risk_paused(reason)
                    continue

                if self._paused and self._pause_reason in ("daily_loss", "low_balance"):
                    self._paused       = False
                    self._pause_reason = ""
                    logger.info("Risk guard: conditions cleared — resuming")
                    await self.telegram.notify_risk_resumed()

            except Exception as e:
                logger.debug(f"Risk guard error: {e}")

    async def _rejected_tracker_loop(self):
        await asyncio.sleep(60)
        while True:
            try:
                await check_pending_outcomes()
            except Exception as e:
                logger.debug(f"Rejected tracker error: {e}")
            await asyncio.sleep(300)

    async def _daily_summary_loop(self):
        while True:
            now  = datetime.now(timezone.utc)
            secs = (24 - now.hour) * 3600 - now.minute * 60 - now.second
            await asyncio.sleep(max(secs, 60))
            await self.telegram.notify_daily_summary(get_daily_pnl())

    async def _auto_export_loop(self):
        await asyncio.sleep(86400)
        while True:
            try:
                from core.trade_logger import export_training_data
                ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
                out = f"data/training/trades_{ts}.json"
                n   = export_training_data(output_path=out)
                logger.info(f"Auto-export: {n} records → {out}")
                await self.telegram.notify_auto_export(n, out)
            except Exception as e:
                logger.error(f"Auto-export failed: {e}")
            await asyncio.sleep(86400)

    # ── Position events → Telegram ─────────────────────────────────────────────

    async def _on_position_event(self, event_type: str, data: dict):
        token = data.get("token", "?")
        if event_type == "take_profit":
            await self.telegram.notify_take_profit(
                symbol=token, level=data.get("level", "?"),
                sold_pct=data.get("sold_pct", 0),
                usd_received=data.get("usd_received", 0),
                tokens_remaining=data.get("remaining", 0),
            )
        elif event_type in ("stop_loss_phase1", "trailing_stop", "max_hold_72hr",
                            "24hr_check_failed", "extended_hold_expired", "manual_sell"):
            await self.telegram.notify_stop_loss(
                symbol=token, reason=event_type,
                exit_price=data.get("exit_price", 0),
                pnl_usd=data.get("total_pnl", 0),
                pnl_pct=data.get("pnl_pct", 0),
            )
        elif event_type == "24hr_extend":
            await self.telegram.notify_24hr_check(
                symbol=token, signals=data.get("signals", {}),
                positive=data.get("positive", 0), action="extend",
            )
        logger.info(f"[{event_type.upper()}] {token} | {data}")

    # ── Bot controls ───────────────────────────────────────────────────────────

    async def _manual_trade(self, ca: str, amount_usd: float) -> dict:
        """Handle /trade command — works in both paper and live mode."""
        try:
            sol_price = await self.coordinator.get_sol_price()
            if sol_price <= 0:
                sol_price = 150.0
            sol_amount = amount_usd / sol_price

            # Read mode dynamically so /setmode takes effect immediately
            import os as _os
            _live_now = _os.getenv("SIGNAL_TRADING", "off").strip().lower() == "on"
            if not _live_now:
                # Paper mode — just log it
                logger.info(
                    f"\n{'='*55}\n"
                    f"MANUAL PAPER TRADE\n"
                    f"  CA:     {ca}\n"
                    f"  Amount: ${amount_usd:.2f} ({sol_amount:.4f} SOL)\n"
                    f"{'='*55}"
                )
                return {"success": True, "mode": "paper", "sol": sol_amount}

            # Live mode — build a FilterResult and send to coordinator
            from utils.models import NewTokenEvent, FilterResult
            from datetime import datetime, timezone

            event = NewTokenEvent(
                mint_address=ca,
                pair_address=ca,
                base_mint=ca,
                quote_mint=config.WSOL_MINT,
                source="manual",
                detected_at=datetime.now(timezone.utc),
                initial_liquidity_sol=sol_amount,
                market_cap_usd=0.0,
            )
            filter_res = FilterResult(
                passed=True,
                token=event,
                filter_details={
                    "risk_score":      0,
                    "verdict":         "manual",
                    "size_multiplier": amount_usd / (await get_sol_balance() * sol_price * config.POSITION_SIZE_PCT)
                    if (await get_sol_balance()) > 0 else 1.0,
                    "flags":           [],
                    "source":          "manual",
                }
            )
            result = await self.coordinator.on_buy_signal(filter_res)
            # on_buy_signal returns None on success (trade placed) or a dict on failure
            if result and not result.get("success", True):
                return {"success": False, "error": result.get("error", "Trade rejected")}
            return {"success": True, "mode": "live", "sol": sol_amount}

        except Exception as e:
            logger.error(f"Manual trade error: {e}")
            return {"success": False, "error": str(e)}

    async def _paper_reset(self):
        self.paper_engine.reset()

    async def _paper_export(self) -> str:
        return paper_export_csv()

    async def _on_paper_event(self, event_type: str, data: dict):
        """Forward paper trading events to Telegram."""
        if not self.telegram:
            return
        try:
            if event_type == "paper_opened":
                await self.telegram.notify_paper_opened(
                    mint=data["mint"], name=data["name"], symbol=data["symbol"],
                    entry_price=data["entry_price"], usd_spent=data["usd_spent"],
                    channel=data["channel"], score=data["score"],
                    balance=data["balance"],
                )
            elif event_type == "paper_tp":
                await self.telegram.notify_paper_tp(
                    name=data["name"], symbol=data["symbol"],
                    level=data["level"], usd_received=data["usd_received"],
                    multiplier=data["multiplier"], balance=data["balance"],
                )
            elif event_type == "paper_closed":
                await self.telegram.notify_paper_closed(
                    name=data["name"], symbol=data["symbol"],
                    reason=data["reason"], pnl_usd=data["pnl_usd"],
                    pnl_pct=data["pnl_pct"], peak_mult=data["peak_mult"],
                    tp_levels=data["tp_levels"], balance=data["balance"],
                )
            elif event_type == "paper_refill":
                await self.telegram.notify_paper_refill(
                    old_balance=data["old_balance"],
                    new_balance=data["new_balance"],
                )
            elif event_type == "paper_post_check":
                await self.telegram.notify_paper_post_check(
                    name=data["name"], symbol=data["symbol"],
                    hours=data["hours"], exit_price=data["exit_price"],
                    current_price=data["current_price"],
                    pct_change=data["pct_change"],
                    close_reason=data["close_reason"],
                    original_pnl_pct=data["original_pnl_pct"],
                )
        except Exception as e:
            logger.debug(f"Paper event notify error: {e}")

    async def _pause(self):
        self._paused       = True
        self._pause_reason = "manual"
        logger.info("Bot PAUSED (manual)")

    async def _resume(self):
        self._paused       = False
        self._pause_reason = ""
        logger.info("Bot RESUMED")

    async def _set_trailing_stop(self, pct: float):
        config.TRAILING_STOP_PCT = pct
        logger.info(f"Trailing stop → {pct*100:.0f}%")

    async def _get_sol_price(self) -> float:
        return await self.coordinator.get_sol_price()

    async def _get_positions(self):
        return self.position_manager.get_all_positions()

    async def _is_paused(self) -> bool:
        return self._paused

    # ── Start / Stop ───────────────────────────────────────────────────────────

    async def start(self):
        mode = "LIVE TRADING" if SIGNAL_LIVE else "PAPER TRADING"
        channels = runtime_config.get_channels() or config.SIGNAL_CHANNELS
        logger.info(
            f"\n{'='*60}\n"
            f"  QUABSNIPE — Signal Bot\n"
            f"  Mode: {mode}\n"
            f"  Channels: {', '.join('@' + c for c in channels)}\n"
            f"{'='*60}"
        )
        try:
            config.validate()
        except EnvironmentError as e:
            logger.error(f"Config error: {e}")
            sys.exit(1)

        asyncio.create_task(self._start_signal_monitor())
        asyncio.create_task(self._send_restart_notice())

        await asyncio.gather(
            self.position_manager.start(),
            self.paper_engine.start(),
            self.telegram.start(),
            self._daily_summary_loop(),
            self._auto_export_loop(),
            self._heartbeat_loop(),
            self._risk_guard_loop(),
            self._rejected_tracker_loop(),
        )

    async def _send_restart_notice(self):
        await asyncio.sleep(3)
        try:
            sol      = await get_sol_balance()
            price    = await self.coordinator.get_sol_price()
            usd      = sol * price if price > 0 else 0
            pos      = self.position_manager.position_count()
            channels = runtime_config.get_channels() or config.SIGNAL_CHANNELS
            await self.telegram.notify_restart(
                sol=sol, usd=usd,
                open_positions=pos,
                mode="LIVE" if SIGNAL_LIVE else "PAPER",
                channels=channels,
            )
        except Exception as e:
            logger.debug(f"Restart notice error: {e}")

    async def stop(self):
        await self.position_manager.stop()
        await self.paper_engine.stop()
        await self.telegram.stop()
        logger.info("Bot stopped cleanly.")


def _uptime_str(started_at: datetime) -> str:
    delta = datetime.now(timezone.utc) - started_at
    h, r  = divmod(int(delta.total_seconds()), 3600)
    m, _  = divmod(r, 60)
    return f"{h}h {m}m"


async def main():
    bot = SignalBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested...")
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())