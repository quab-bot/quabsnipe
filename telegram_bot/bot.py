# telegram_bot/bot.py — Quabsnipe v10
# All commands, clean formatting, HTML parse_mode
# New in v10: /channels, /blacklist, /addchannel, /removechannel,
#             /setsignal, /risk, /setloss, /setminbal, /settings
#             Heartbeat, restart, risk-pause, error notifications

import asyncio
from datetime import datetime, timezone
from typing import Optional

import config
from utils.logger import get_logger
from core.database import get_daily_pnl
from core.analytics import (
    get_analytics_summary,
    get_rejection_breakdown,
    get_all_time_pnl,
    get_weekly_pnl,
    get_top_trades,
    get_filter_stage_breakdown,
    get_signal_channel_stats,
)
from core import channel_tracker, runtime_config

logger = get_logger("telegram_bot")

DEXSCREENER = "https://dexscreener.com/solana"
SOLSCAN     = "https://solscan.io/token"

_ROUTINE_TTL = 120
_IMPORTANT_KEYWORDS = (
    "SIGNAL PASSED", "SIGNAL ACCEPTED", "24H STATS", "P&L", "TRADE OPENED",
    "TAKE PROFIT", "STOP LOSS", "TRAILING STOP", "MANUAL CLOSE", "Online",
    "RESTARTED", "PAUSED", "HEARTBEAT",
)


def _t() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M UTC")


def _mc(mc: float) -> str:
    if mc >= 1_000_000: return f"${mc/1_000_000:.2f}M"
    if mc >= 1_000:     return f"${mc/1_000:.1f}K"
    return f"${mc:.0f}"


def _pct(num, den) -> str:
    if not den: return "—"
    return f"{num/den*100:.0f}%"


def _bar(wins: int, losses: int, width: int = 10) -> str:
    total = wins + losses
    if not total: return "·" * width
    w = round(wins / total * width)
    return "█" * w + "░" * (width - w)


def _short_reason(reason: str) -> str:
    mapping = {
        "honeypot_detected":          "honeypot",
        "honeypot":                   "honeypot",
        "honeypot_simulation_fail":   "honeypot",
        "liq_too_low":                "low liquidity",
        "liquidity_auto_reject":      "low liquidity",
        "mc_too_low":                 "low MC",
        "mc_too_high":                "MC too high",
        "token_too_old":              "too old",
        "fake_vol":                   "fake volume",
        "snipers_holding":            "snipers holding",
        "dev_holding":                "dev holding",
        "dev_holds_too_much":         "dev holding",
        "dev_sold_too_much":          "dev dumped",
        "dev_previous_rugs":          "dev rugged before",
        "dev_funded_by_rugger":       "funded by rugger",
        "fake_holders":               "fake holders",
        "bundles_high":               "high bundles",
        "first20_concentration_high": "concentrated",
        "security_check_failed":      "security failed",
        "no_ca_found":                "no CA found",
        "mint_authority_enabled":     "mint enabled",
        "freeze_authority_enabled":   "freeze enabled",
        "lp_not_locked":              "LP unlocked",
        "bundled_launch":             "bundled launch",
        "top10_holders_high":         "top10 concentrated",
        "no_rugcheck_data":           "not on rugcheck",
        "score":                      "risk score",
        "rugcheck_score":             "rugcheck risk",
        "name_auto_reject":           "banned name",
    }
    r = reason.lower()
    for key, label in mapping.items():
        if key in r:
            return label
    return r.replace("_", " ").split("(")[0][:28]


def _esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class TelegramBot:
    def __init__(self, get_positions_fn, get_balance_fn, sell_all_fn,
                 set_stop_fn, pause_fn, resume_fn, is_paused_fn,
                 manual_trade_fn=None, paper_stats_fn=None, paper_reset_fn=None, paper_export_fn=None):
        self._get_positions  = get_positions_fn
        self._get_balance    = get_balance_fn
        self._sell_all       = sell_all_fn
        self._set_stop       = set_stop_fn
        self._pause          = pause_fn
        self._resume         = resume_fn
        self._is_paused      = is_paused_fn
        self._manual_trade   = manual_trade_fn
        self._paper_stats    = paper_stats_fn
        self._paper_reset    = paper_reset_fn
        self._paper_export   = paper_export_fn
        self._bot           = None
        self._app           = None
        self._enabled       = False
        self._offset        = 0

        self._signal_msg_id:  Optional[int]  = None
        self._signal_history: list[dict]     = []
        self._cleanup_tasks:  list[asyncio.Task] = []

    async def start(self):
        token = config.TELEGRAM_BOT_TOKEN
        if not token or token in ("placeholder", "your_telegram_bot_token"):
            logger.warning("Telegram token not set — disabled")
            return

        try:
            from telegram.ext import Application, CommandHandler
            app = Application.builder().token(token).build()
            self._bot = app.bot
            self._app = app

            cmds = {
                # Info
                "start":          self._cmd_start,
                "help":           self._cmd_help,
                "status":         self._cmd_status,
                "balance":        self._cmd_balance,
                "pnl":            self._cmd_pnl,
                "stats":          self._cmd_stats,
                "analysis":       self._cmd_analysis,
                "signals":        self._cmd_signals,
                "top":            self._cmd_top,
                "week":           self._cmd_week,
                "filters":        self._cmd_filters,
                "export":         self._cmd_export,
                "mode":           self._cmd_mode,
                "channels":       self._cmd_channels,
                "blacklisted":    self._cmd_blacklisted,
                "settings":       self._cmd_settings,
                # Controls
                "pause":          self._cmd_pause,
                "resume":         self._cmd_resume,
                "setstop":        self._cmd_setstop,
                "sellall":        self._cmd_sellall,
                "blacklist":      self._cmd_blacklist,
                "unblacklist":    self._cmd_unblacklist,
                "addchannel":     self._cmd_addchannel,
                "removechannel":  self._cmd_removechannel,
                "setsignal":      self._cmd_setsignal,
                "risk":           self._cmd_risk,
                "setloss":        self._cmd_setloss,
                "setminbal":      self._cmd_setminbal,
                "trade":          self._cmd_trade,
                "ca":             self._cmd_ca,
                "solprice":       self._cmd_solprice,
                "setmode":        self._cmd_setmode,
                "paper":          self._cmd_paper,
                "aistats":        self._cmd_aistats,
                "clear":          self._cmd_clear,
            }
            for cmd, fn in cmds.items():
                app.add_handler(CommandHandler(cmd, fn))

            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            self._enabled = True
            logger.info("Telegram bot started ✓ (full mode)")

        except Exception as e:
            logger.warning(f"Full mode failed ({e}) — simple mode")
            try:
                from telegram import Bot
                self._bot     = Bot(token=token)
                self._enabled = True
                asyncio.create_task(self._poll_loop())
                logger.info("Telegram bot started ✓ (simple mode)")
            except Exception as e2:
                logger.error(f"Telegram failed: {e2}")
                return

    # ── Polling fallback ────────────────────────────────────────────────────────

    async def _poll_loop(self):
        logger.info("Manual command polling active")
        while self._enabled:
            try:
                updates = await self._bot.get_updates(
                    offset=self._offset, timeout=10,
                    allowed_updates=["message"]
                )
                for upd in updates:
                    self._offset = upd.update_id + 1
                    if upd.message and upd.message.text:
                        await self._handle_cmd(
                            upd.message.text,
                            str(upd.message.chat_id),
                            upd.message,
                        )
            except Exception as e:
                logger.debug(f"Poll: {e}")
                await asyncio.sleep(5)
            await asyncio.sleep(1)

    async def _handle_cmd(self, text: str, chat_id: str, msg):
        if chat_id != str(config.TELEGRAM_CHAT_ID):
            await self._to(chat_id, "⛔ Unauthorised")
            return
        try:
            task = asyncio.create_task(
                self._auto_delete(chat_id, msg.message_id, _ROUTINE_TTL)
            )
            self._cleanup_tasks.append(task)
            self._cleanup_tasks = [t for t in self._cleanup_tasks if not t.done()]
        except Exception:
            pass
        parts = text.strip().split()
        cmd   = parts[0].lower().lstrip("/").split("@")[0]
        args  = parts[1:]
        fn = {
            "start":         lambda: self._reply_start(chat_id),
            "help":          lambda: self._reply_help(chat_id),
            "status":        lambda: self._reply_status(chat_id),
            "balance":       lambda: self._reply_balance(chat_id),
            "pnl":           lambda: self._reply_pnl(chat_id),
            "stats":         lambda: self._reply_stats(chat_id),
            "analysis":      lambda: self._reply_analysis(chat_id),
            "signals":       lambda: self._reply_signals(chat_id),
            "top":           lambda: self._reply_top(chat_id),
            "week":          lambda: self._reply_week(chat_id),
            "filters":       lambda: self._reply_filters(chat_id),
            "export":        lambda: self._reply_export(chat_id),
            "pause":         lambda: self._reply_pause(chat_id),
            "resume":        lambda: self._reply_resume(chat_id),
            "mode":          lambda: self._reply_mode(chat_id),
            "channels":      lambda: self._reply_channels(chat_id),
            "blacklisted":   lambda: self._reply_blacklisted(chat_id),
            "settings":      lambda: self._reply_settings(chat_id),
            "setstop":       lambda: self._reply_setstop(chat_id, args),
            "sellall":       lambda: self._reply_sellall(chat_id, args),
            "blacklist":     lambda: self._reply_blacklist(chat_id, args),
            "unblacklist":   lambda: self._reply_unblacklist(chat_id, args),
            "addchannel":    lambda: self._reply_addchannel(chat_id, args),
            "removechannel": lambda: self._reply_removechannel(chat_id, args),
            "setsignal":     lambda: self._reply_setsignal(chat_id, args),
            "risk":          lambda: self._reply_risk(chat_id, args),
            "setloss":       lambda: self._reply_setloss(chat_id, args),
            "setminbal":     lambda: self._reply_setminbal(chat_id, args),
            "trade":         lambda: self._reply_trade(chat_id, args),
            "ca":            lambda: self._reply_ca(chat_id, args),
            "solprice":      lambda: self._reply_solprice(chat_id),
            "setmode":       lambda: self._reply_setmode(chat_id, args),
            "paper":         lambda: self._reply_paper(chat_id, args),
            "aistats":       lambda: self._reply_aistats(chat_id),
            "clear":         lambda: self._reply_clear(chat_id),
        }.get(cmd)
        if fn:
            try:
                await fn()
            except Exception as e:
                await self._to(chat_id, f"Error: {e}")

    # ── App-mode command wrappers ──────────────────────────────────────────────

    async def _cmd_start(self, u, c):
        if not await self._auth(u): return
        await self._reply_start(str(u.effective_chat.id))
    async def _cmd_help(self, u, c):
        if not await self._auth(u): return
        await self._reply_help(str(u.effective_chat.id))
    async def _cmd_status(self, u, c):
        if not await self._auth(u): return
        await self._reply_status(str(u.effective_chat.id))
    async def _cmd_balance(self, u, c):
        if not await self._auth(u): return
        await self._reply_balance(str(u.effective_chat.id))
    async def _cmd_pnl(self, u, c):
        if not await self._auth(u): return
        await self._reply_pnl(str(u.effective_chat.id))
    async def _cmd_stats(self, u, c):
        if not await self._auth(u): return
        await self._reply_stats(str(u.effective_chat.id))
    async def _cmd_analysis(self, u, c):
        if not await self._auth(u): return
        await self._reply_analysis(str(u.effective_chat.id))
    async def _cmd_signals(self, u, c):
        if not await self._auth(u): return
        await self._reply_signals(str(u.effective_chat.id))
    async def _cmd_top(self, u, c):
        if not await self._auth(u): return
        await self._reply_top(str(u.effective_chat.id))
    async def _cmd_week(self, u, c):
        if not await self._auth(u): return
        await self._reply_week(str(u.effective_chat.id))
    async def _cmd_filters(self, u, c):
        if not await self._auth(u): return
        await self._reply_filters(str(u.effective_chat.id))
    async def _cmd_export(self, u, c):
        if not await self._auth(u): return
        await self._reply_export(str(u.effective_chat.id))
    async def _cmd_pause(self, u, c):
        if not await self._auth(u): return
        await self._reply_pause(str(u.effective_chat.id))
    async def _cmd_resume(self, u, c):
        if not await self._auth(u): return
        await self._reply_resume(str(u.effective_chat.id))
    async def _cmd_mode(self, u, c):
        if not await self._auth(u): return
        await self._reply_mode(str(u.effective_chat.id))
    async def _cmd_channels(self, u, c):
        if not await self._auth(u): return
        await self._reply_channels(str(u.effective_chat.id))
    async def _cmd_blacklisted(self, u, c):
        if not await self._auth(u): return
        await self._reply_blacklisted(str(u.effective_chat.id))
    async def _cmd_settings(self, u, c):
        if not await self._auth(u): return
        await self._reply_settings(str(u.effective_chat.id))
    async def _cmd_setstop(self, u, c):
        if not await self._auth(u): return
        await self._reply_setstop(str(u.effective_chat.id), c.args or [])
    async def _cmd_sellall(self, u, c):
        if not await self._auth(u): return
        await self._reply_sellall(str(u.effective_chat.id), c.args or [])
    async def _cmd_blacklist(self, u, c):
        if not await self._auth(u): return
        await self._reply_blacklist(str(u.effective_chat.id), c.args or [])
    async def _cmd_unblacklist(self, u, c):
        if not await self._auth(u): return
        await self._reply_unblacklist(str(u.effective_chat.id), c.args or [])
    async def _cmd_addchannel(self, u, c):
        if not await self._auth(u): return
        await self._reply_addchannel(str(u.effective_chat.id), c.args or [])
    async def _cmd_removechannel(self, u, c):
        if not await self._auth(u): return
        await self._reply_removechannel(str(u.effective_chat.id), c.args or [])
    async def _cmd_setsignal(self, u, c):
        if not await self._auth(u): return
        await self._reply_setsignal(str(u.effective_chat.id), c.args or [])
    async def _cmd_risk(self, u, c):
        if not await self._auth(u): return
        await self._reply_risk(str(u.effective_chat.id), c.args or [])
    async def _cmd_setloss(self, u, c):
        if not await self._auth(u): return
        await self._reply_setloss(str(u.effective_chat.id), c.args or [])
    async def _cmd_setminbal(self, u, c):
        if not await self._auth(u): return
        await self._reply_setminbal(str(u.effective_chat.id), c.args or [])

    async def _cmd_trade(self, u, c):
        if not await self._auth(u): return
        await self._reply_trade(str(u.effective_chat.id), c.args or [])

    async def _cmd_ca(self, u, c):
        if not await self._auth(u): return
        await self._reply_ca(str(u.effective_chat.id), c.args or [])

    async def _cmd_solprice(self, u, c):
        if not await self._auth(u): return
        await self._reply_solprice(str(u.effective_chat.id))

    async def _cmd_setmode(self, u, c):
        if not await self._auth(u): return
        await self._reply_setmode(str(u.effective_chat.id), c.args or [])

    async def _cmd_paper(self, u, c):
        if not await self._auth(u): return
        await self._reply_paper(str(u.effective_chat.id), c.args or [])

    async def _cmd_aistats(self, u, c):
        if not await self._auth(u): return
        await self._reply_aistats(str(u.effective_chat.id))

    async def _cmd_clear(self, u, c):
        if not await self._auth(u): return
        await self._reply_clear(str(u.effective_chat.id))


    # ── Reply functions ────────────────────────────────────────────────────────

    async def _reply_start(self, chat_id):
        await self._to(chat_id,
            "👋 <b>Quabsnipe Signal Bot</b>\n"
            "Automated Solana signal trader — running.\n"
            "/help — all commands",
        )

    async def _reply_help(self, chat_id):
        await self._to(chat_id,
            "<b>QUABSNIPE SIGNAL BOT — COMMANDS</b>\n"
            "──────────────────────────────────────\n"
            "<b>Info</b>\n"
            "/status              — open positions\n"
            "/balance             — wallet balance\n"
            "/pnl                 — today + all-time P&amp;L\n"
            "/stats               — 24h overview\n"
            "/signals             — recent signal history\n"
            "/channels            — channel leaderboard\n"
            "/analysis            — rejection breakdown\n"
            "/top                 — best 5 trades ever\n"
            "/week                — 7-day P&amp;L breakdown\n"
            "/filters             — active signal thresholds\n"
            "/settings            — current live settings\n"
            "/mode                — current paper or live mode\n"
            "/blacklisted         — blacklisted tokens\n"
            "/export              — save AI training data\n\n"
            "<b>Research</b>\n"
            "/ca [CA]             — token info from DexScreener + RugCheck\n"
            "/solprice            — current SOL/USD price\n\n"
            "<b>Trading</b>\n"
            "/trade [CA] [amount] — manual buy (e.g. /trade CA 5 = spend $5)\n"
            "/sellall [CA]        — close a position\n\n"
            "<b>Control</b>\n"
            "/pause               — stop new trades\n"
            "/resume              — resume trading\n"
            "/setmode [paper|live]— switch trading mode live\n"
            "/paper               — paper trading stats + P&amp;L\n"
            "/paper reset         — reset paper balance to $40\n"
            "/paper export        — download all paper trades as CSV\n"
            "/aistats             — AI model stats + current threshold\n"
            "/clear              — clear bot messages from chat\n"
            "/setstop [%]         — set trailing stop % (e.g. /setstop 25)\n"
            "/blacklist [CA]      — block a token forever\n"
            "/unblacklist [CA]    — remove from blacklist\n\n"
            "<b>Configure (live, no restart)</b>\n"
            "/addchannel [name]   — add a signal channel\n"
            "/removechannel [name]— remove a signal channel\n"
            "/setsignal [SOL]     — signal position size in SOL\n"
            "/risk [%]            — position size % of wallet\n"
            "/setloss [%]         — daily loss limit %\n"
            "/setminbal [$]       — minimum balance floor ($)\n",
        )

    async def _reply_status(self, chat_id):
        positions = await self._get_positions()
        paused    = await self._is_paused()
        state     = "⏸ PAUSED" if paused else "▶️ ACTIVE"
        if not positions:
            await self._to(chat_id, f"📋 {state} — No open positions")
            return
        lines = [f"<b>📋 {len(positions)} OPEN — {state}</b>"]
        for p in positions:
            now       = datetime.now(timezone.utc)
            age_m     = (now - p.entry_time).total_seconds() / 60
            pnl       = p.total_profit_usd
            phase_lbl = f"Ph{p.phase}"
            e         = "🟢" if pnl >= 0 else "🔴"
            tps       = ", ".join(p.tp_levels_hit) if p.tp_levels_hit else "none"
            lines.append(
                f"\n{e} <b>{_esc(p.token_symbol)}</b>  {pnl:+.3f} SOL  {age_m:.0f}m\n"
                f"   In: {p.sol_amount:.3f} SOL · {phase_lbl}\n"
                f"   TPs hit: {tps}\n"
                f"   <code>{p.mint_address}</code>"
            )
        await self._to(chat_id, "\n".join(lines), important=True)

    async def _reply_balance(self, chat_id):
        sol    = await self._get_balance()
        paused = await self._is_paused()
        pos    = await self._get_positions()
        in_pos = sum(p.sol_amount for p in pos)
        free   = sol - in_pos
        await self._to(chat_id,
            "<b>💼 WALLET</b>\n"
            "──────────────────────────\n"
            f"Total:       {sol:.4f} SOL\n"
            f"Free:        {free:.4f} SOL\n"
            f"In trades:   {in_pos:.4f} SOL\n"
            f"Positions:   {len(pos)} open\n"
            f"Status:  {'⏸ Paused' if paused else '▶️ Active'}",
        )

    async def _reply_pnl(self, chat_id):
        today   = get_daily_pnl()
        alltime = get_all_time_pnl()

        tw   = today.get("wins", 0) or 0
        tl   = today.get("losses", 0) or 0
        tpnl = today.get("total_pnl", 0) or 0
        tavg = today.get("avg_pnl_pct", 0) or 0
        twr  = _pct(tw, tw + tl)
        te   = "🟢" if tpnl >= 0 else "🔴"

        aw    = alltime.get("wins", 0) or 0
        al    = alltime.get("losses", 0) or 0
        apnl  = alltime.get("total_pnl", 0) or 0
        abest = alltime.get("best_pnl", 0) or 0
        awrst = alltime.get("worst_pnl", 0) or 0
        awr   = _pct(aw, aw + al)
        ae    = "🟢" if apnl >= 0 else "🔴"

        await self._to(chat_id,
            "<b>📊 P&amp;L REPORT</b>\n"
            "──────────────────────────\n"
            "<b>Today</b>\n"
            f"Trades:   {tw+tl}  (W:{tw} L:{tl})\n"
            f"Win rate: {twr}  {_bar(tw, tl)}\n"
            f"P&amp;L:      {te} {tpnl:+.3f} SOL\n"
            f"Avg:      {tavg:+.1f}%\n"
            "──────────────────────────\n"
            "<b>All-Time</b>\n"
            f"Trades:   {aw+al}  (W:{aw} L:{al})\n"
            f"Win rate: {awr}  {_bar(aw, al)}\n"
            f"Total:    {ae} {apnl:+.3f} SOL\n"
            f"Best:     +{abest:.3f} SOL\n"
            f"Worst:    {awrst:.3f} SOL",
            important=True,
        )

    async def _reply_stats(self, chat_id):
        s24  = get_analytics_summary(hours=24)
        pnl  = get_daily_pnl()
        atp  = get_all_time_pnl()
        sigs = get_signal_channel_stats(hours=24)

        total  = s24.get("total", 0) or 0
        passed = s24.get("passed", 0) or 0
        rej    = s24.get("rejected", 0) or 0
        s1     = s24.get("s1_rejects", 0) or 0
        s2     = s24.get("s2_rejects", 0) or 0
        s3     = s24.get("s3_rejects", 0) or 0

        tw   = pnl.get("wins", 0) or 0
        tl   = pnl.get("losses", 0) or 0
        tpnl = pnl.get("total_pnl", 0) or 0
        te   = "🟢" if tpnl >= 0 else "🔴"

        aw = atp.get("wins", 0) or 0
        al = atp.get("losses", 0) or 0

        sv = sigs.get("seen", 0) or 0
        sp = sigs.get("passed", 0) or 0

        channels = runtime_config.get_channels() or config.SIGNAL_CHANNELS
        ch_str   = ", ".join(f"@{c}" for c in channels)

        await self._to(chat_id,
            "<b>📈 24H STATS</b>\n"
            "──────────────────────────\n"
            "<b>Filter Funnel</b>\n"
            f"Tokens seen:  {total}\n"
            f"Passed:       {passed} ({_pct(passed, total)})\n"
            f"Rejected:     {rej}\n"
            f"  Stage 1:    {s1}  Stage 2: {s2}  Stage 3: {s3}\n"
            "──────────────────────────\n"
            "<b>Signals (24h)</b>\n"
            f"Channels: {ch_str}\n"
            f"Seen: {sv}  Passed: {sp} ({_pct(sp, sv)})\n"
            "──────────────────────────\n"
            "<b>Trades</b>\n"
            f"Today:    {tw+tl} trades  {te} {tpnl:+.3f} SOL\n"
            f"Win rate: {_pct(tw, tw+tl)}  {_bar(tw, tl)}\n"
            f"All-time: {aw+al} trades · {_pct(aw, aw+al)} wins\n"
            f"Time: {_t()}",
            important=True,
        )

    async def _reply_analysis(self, chat_id):
        bd   = get_filter_stage_breakdown(hours=24)
        rej  = get_rejection_breakdown(hours=24)
        sigs = get_signal_channel_stats(hours=168)

        total   = bd.get("total", 0) or 0
        passed  = bd.get("passed", 0) or 0
        s1      = bd.get("s1", 0) or 0
        s2      = bd.get("s2", 0) or 0
        s3      = bd.get("s3", 0) or 0
        caution = bd.get("caution", 0) or 0
        avg_ps  = bd.get("avg_pass_score") or 0
        avg_rs  = bd.get("avg_rej_score") or 0
        avg_pl  = bd.get("avg_pass_liq") or 0

        sv7 = sigs.get("seen", 0) or 0
        sp7 = sigs.get("passed", 0) or 0
        sr7 = sigs.get("avg_score") or 0

        # Rejected token outcomes (AI data)
        try:
            from core.rejected_tracker import get_rejected_outcome_stats
            ro = get_rejected_outcome_stats(hours=24)
            ro_total   = ro.get("total", 0) or 0
            ro_pumped  = ro.get("pumped", 0) or 0
            ro_dumped  = ro.get("dumped", 0) or 0
            ro_flat    = ro.get("flat", 0) or 0
            ro_pending = ro.get("pending", 0) or 0
            has_ro = ro_total > 0
        except Exception:
            has_ro = False

        lines = [
            "<b>🔬 FILTER ANALYSIS (24h)</b>",
            "──────────────────────────",
            "<b>Stage Breakdown</b>",
            f"Total seen:  {total}",
            f"Passed:      {passed} ({_pct(passed, total)})",
            f"  of which caution (50% size): {caution}",
            f"Stage 1 rejects: {s1} ({_pct(s1, total)})",
            f"Stage 2 rejects: {s2} ({_pct(s2, total)})",
            f"Stage 3 rejects: {s3} ({_pct(s3, total)})",
            "──────────────────────────",
            "<b>Score Averages</b>",
            f"Avg score (passed):   {avg_ps:.0f}",
            f"Avg score (rejected): {avg_rs:.0f}",
            f"Avg liq (passed):     ${avg_pl:,.0f}",
            "──────────────────────────",
            "<b>Top Reject Reasons (24h)</b>",
        ]

        if rej:
            total_rej = sum(r["count"] for r in rej)
            for i, r in enumerate(rej[:7], 1):
                pct   = _pct(r["count"], total_rej)
                label = _short_reason(r["reason"])
                lines.append(f"  {i}. {label} — {r['count']} ({pct})")
        else:
            lines.append("  No data yet")

        lines += [
            "──────────────────────────",
            "<b>Signal Channels (7d)</b>",
            f"Seen: {sv7}  Passed: {sp7} ({_pct(sp7, sv7)})",
            f"Avg risk score: {sr7:.0f}",
        ]

        if has_ro:
            lines += [
                "──────────────────────────",
                "<b>Rejected Token Outcomes (24h)</b>",
                f"Tracked: {ro_total}  Pending: {ro_pending}",
                f"Pumped:  {ro_pumped} ({_pct(ro_pumped, ro_total-ro_pending)})",
                f"Dumped:  {ro_dumped} ({_pct(ro_dumped, ro_total-ro_pending)})",
                f"Flat:    {ro_flat}   ({_pct(ro_flat, ro_total-ro_pending)})",
            ]

        await self._to(chat_id, "\n".join(lines), important=True)

    async def _reply_signals(self, chat_id):
        if not self._signal_history:
            await self._to(chat_id, "📡 No signals yet this session")
            return
        lines = ["<b>📡 SIGNAL HISTORY</b>"]
        for s in reversed(self._signal_history[-10:]):
            e   = "✅" if s["passed"] else "❌"
            mc  = f"  MC: {_mc(s.get('mc', 0))}" if s.get("mc") else ""
            scr = f"  Score: {s.get('score', 0)}"
            ch  = f"  @{s['channel']}" if s.get("channel") else ""
            if s["passed"]:
                tag = s.get("verdict", "clean").upper()
            else:
                tag = _short_reason(s.get("reason", ""))
            lines.append(
                f"\n{e} <b>{_esc(s['name'][:24])}</b>{ch}\n"
                f"   {tag}{mc}{scr}\n"
                f"   <code>{s['ca']}</code>"
            )
        await self._to(chat_id, "\n".join(lines), important=True)

    async def _reply_channels(self, chat_id):
        board = channel_tracker.get_leaderboard()
        active = runtime_config.get_channels() or config.SIGNAL_CHANNELS

        lines = ["<b>📡 CHANNEL LEADERBOARD</b>", "──────────────────────────"]
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣"]

        if board:
            for i, ch in enumerate(board[:8]):
                c_name  = ch["channel"]
                traded  = ch["traded"]
                wins    = ch["wins"]
                losses  = ch["losses"]
                pnl     = ch["pnl"]
                total   = ch["total_signals"]
                wr      = _pct(wins, traded) if traded else "—"
                e_pnl   = "🟢" if pnl >= 0 else "🔴"
                medal   = medals[i] if i < len(medals) else "  "
                active_tag = " ✅" if c_name in active else ""
                lines.append(
                    f"\n{medal} <b>@{_esc(c_name)}</b>{active_tag}\n"
                    f"   Signals: {total}  Traded: {traded}\n"
                    f"   Win rate: {wr}  {_bar(wins, losses, 8)}\n"
                    f"   P&amp;L: {e_pnl} {pnl:+.3f} SOL"
                )
        else:
            lines.append("No channel data yet.\nSignals will be tracked as they come in.")

        lines += [
            "──────────────────────────",
            f"Active: {', '.join('@'+c for c in active)}",
            "/addchannel [name] to add more",
        ]
        await self._to(chat_id, "\n".join(lines), important=True)

    async def _reply_blacklisted(self, chat_id):
        bl = runtime_config.get_blacklist()
        if not bl:
            await self._to(chat_id, "🚫 <b>BLACKLIST</b>\nNo tokens blacklisted.")
            return
        lines = [f"<b>🚫 BLACKLIST ({len(bl)} tokens)</b>"]
        for b in bl[-15:]:
            reason = f" — {_esc(b['reason'])}" if b.get("reason") else ""
            lines.append(f"<code>{b['mint'][:24]}...</code>{reason}")
        await self._to(chat_id, "\n".join(lines))

    async def _reply_settings(self, chat_id):
        rc  = runtime_config.get_all()
        sig = rc.get("SIGNAL_POSITION_SOL", config.SIGNAL_POSITION_SOL)
        pos = rc.get("POSITION_SIZE_PCT",   config.POSITION_SIZE_PCT) * 100
        dl  = rc.get("DAILY_LOSS_LIMIT_PCT", config.DAILY_LOSS_LIMIT_PCT) * 100
        mb  = rc.get("LOW_BALANCE_USD",      config.LOW_BALANCE_USD)
        chs = runtime_config.get_channels() or config.SIGNAL_CHANNELS
        ch_str = ", ".join(f"@{c}" for c in chs)

        await self._to(chat_id,
            "<b>⚙️ LIVE SETTINGS</b>\n"
            "──────────────────────────\n"
            f"Signal pos size:   {sig} SOL\n"
            f"Position size:     {pos:.0f}% of wallet\n"
            f"Daily loss limit:  {dl:.0f}%\n"
            f"Min balance:       ${mb:.0f}\n"
            f"Signal channels:   {ch_str}\n"
            "──────────────────────────\n"
            "Use /setsignal, /risk, /setloss, /setminbal to change.\n"
            "Changes survive restart.",
        )

    async def _reply_top(self, chat_id):
        trades = get_top_trades(n=5)
        if not trades:
            await self._to(chat_id,
                "🏆 <b>TOP TRADES</b>\n"
                "No closed live trades yet."
            )
            return
        lines  = ["<b>🏆 TOP 5 TRADES</b>", "──────────────────────────"]
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        for i, t in enumerate(trades):
            sym    = _esc(t["symbol"] or "?")
            pnl    = t["pnl_usd"] or 0
            pct    = t["pnl_pct"] or 0
            reason = _short_reason(t["exit_reason"] or "")
            sol_in = t["sol_spent"] or 0
            e      = "🟢" if pnl >= 0 else "🔴"
            lines.append(
                f"\n{medals[i]} <b>{sym}</b>\n"
                f"   {e} {pnl:+.3f} SOL ({pct:+.1f}%)\n"
                f"   In: {sol_in:.3f} SOL · Exit: {reason}"
            )
        await self._to(chat_id, "\n".join(lines), important=True)

    async def _reply_week(self, chat_id):
        days = get_weekly_pnl()
        if not days:
            await self._to(chat_id,
                "📅 <b>7-DAY BREAKDOWN</b>\n"
                "No closed live trades in the last 7 days."
            )
            return
        lines     = ["<b>📅 7-DAY BREAKDOWN</b>", "──────────────────────────"]
        total_pnl = 0.0
        for d in days:
            day = d["day"] or "?"
            pnl = d["pnl"] or 0.0
            w   = d["wins"] or 0
            l   = d["losses"] or 0
            e   = "🟢" if pnl >= 0 else "🔴"
            bar = _bar(w, l, width=6)
            lines.append(f"{e} <b>{day}</b>  {pnl:+.3f} SOL  {bar}  W{w}/L{l}")
            total_pnl += pnl
        lines.append("──────────────────────────")
        lines.append(f"Total: {'🟢' if total_pnl >= 0 else '🔴'} {total_pnl:+.3f} SOL")
        await self._to(chat_id, "\n".join(lines), important=True)

    async def _reply_filters(self, chat_id):
        await self._to(chat_id,
            "<b>ACTIVE SIGNAL FILTER THRESHOLDS</b>\n"
            "──────────────────────────\n"
            "<b>Hard Rejects (any of these = skip)</b>\n"
            f"Min liquidity:      $5,000\n"
            f"Max token age:      60 min\n"
            f"Max fake volume:    40%\n"
            f"Max top-10 holders: 50%\n"
            f"Max snipers now:    15%\n"
            f"Max dev holding:    10%\n"
            f"Max fake holders:   30%\n"
            f"Max risk score:     45\n"
            "──────────────────────────\n"
            "<b>Exit Logic (signal trades)</b>\n"
            f"Stop loss:          -{config.SIGNAL_STOP_LOSS_PCT*100:.0f}%\n"
            f"Trail starts at:    +{config.SIGNAL_TRAILING_START*100:.0f}%\n"
            f"Trail %:            {config.SIGNAL_TRAILING_PCT*100:.0f}%\n"
            f"Position size:      {config.SIGNAL_POSITION_SOL} SOL fixed\n"
            "──────────────────────────\n"
            "<b>Size Multipliers</b>\n"
            "Score 0-15 (clean):   100% size\n"
            "Score 16-30 (ok):      75% size\n"
            "Score 31-45 (caution): 50% size\n"
            "Multi-channel boost:  +30% if 2+ channels post same CA\n",
        )

    async def _reply_export(self, chat_id):
        await self._to(chat_id, "⏳ Exporting training data...")
        try:
            from core.trade_logger import export_training_data
            ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
            out = f"data/training/trades_{ts}.json"
            n   = export_training_data(output_path=out)
            if n == 0:
                await self._to(chat_id,
                    "📦 <b>EXPORT</b>\n"
                    "No completed trades yet.\n"
                    "Close some trades first.",
                )
            else:
                await self._to(chat_id,
                    f"📦 <b>EXPORT DONE</b>\n"
                    f"Saved {n} trades → <code>{out}</code>",
                    important=True,
                )
        except Exception as e:
            await self._to(chat_id, f"❌ Export failed: {e}")

    async def _reply_pause(self, chat_id):
        await self._pause()
        await self._to(chat_id,
            "⏸ <b>PAUSED</b>\n"
            "No new trades will open.\n"
            "Open positions still managed.\n"
            "/resume to restart",
        )

    async def _reply_resume(self, chat_id):
        await self._resume()
        await self._to(chat_id, "▶️ <b>RESUMED</b> — watching for trades")

    async def _reply_mode(self, chat_id):
        import os
        live = os.getenv("SIGNAL_TRADING", "off").strip().lower() == "on"
        await self._to(chat_id,
            f"{'LIVE MODE' if live else 'PAPER MODE'}\n"
            f"{'Real trades active. Real SOL at risk.' if live else 'Paper trading — no real SOL spent.'}"
        )

    async def _reply_setstop(self, chat_id, args):
        if not args:
            await self._to(chat_id,
                "Usage: /setstop [%]\n"
                "Example: /setstop 25\n"
                f"Current: {config.TRAILING_STOP_PCT*100:.0f}%"
            )
            return
        try:
            pct = float(args[0])
            if not 5 <= pct <= 80:
                await self._to(chat_id, "❌ Must be 5–80%")
                return
            await self._set_stop(pct / 100)
            await self._to(chat_id, f"✅ Trailing stop → <b>{pct:.0f}%</b>")
        except ValueError:
            await self._to(chat_id, "❌ Invalid number. Example: /setstop 25")

    async def _reply_sellall(self, chat_id, args):
        if not args:
            pos = await self._get_positions()
            if not pos:
                await self._to(chat_id, "No open positions.")
                return
            await self._to(chat_id,
                "Usage: /sellall [CA]\n"
                "Provide the token mint address.\n"
                "/status to see open positions."
            )
            return
        mint = args[0].strip()
        await self._to(chat_id, f"⏳ Closing <code>{mint[:16]}...</code>")
        ok = await self._sell_all(mint)
        await self._to(chat_id, "✅ Position closed." if ok else "❌ Not found — check CA")

    # ── New control commands ───────────────────────────────────────────────────

    async def _reply_blacklist(self, chat_id, args):
        if not args:
            await self._to(chat_id, "Usage: /blacklist [CA]\nExample: /blacklist AbcXyz...")
            return
        mint   = args[0].strip()
        reason = " ".join(args[1:]) if len(args) > 1 else ""
        added  = runtime_config.blacklist_add(mint, reason)
        if added:
            await self._to(chat_id,
                f"🚫 <b>BLACKLISTED</b>\n"
                f"<code>{mint[:24]}...</code>\n"
                f"{('Reason: ' + _esc(reason)) if reason else ''}\n"
                "This token will never be traded.",
                important=True,
            )
        else:
            await self._to(chat_id, "Already in blacklist.")

    async def _reply_unblacklist(self, chat_id, args):
        if not args:
            await self._to(chat_id, "Usage: /unblacklist [CA]")
            return
        mint    = args[0].strip()
        removed = runtime_config.blacklist_remove(mint)
        if removed:
            await self._to(chat_id, f"✅ Removed from blacklist:\n<code>{mint[:24]}...</code>")
        else:
            await self._to(chat_id, "Not found in blacklist.")

    async def _reply_addchannel(self, chat_id, args):
        if not args:
            await self._to(chat_id,
                "Usage: /addchannel [username]\n"
                "Example: /addchannel solearlytrending\n"
                "Note: Takes effect on next restart."
            )
            return
        name  = args[0].strip().lstrip("@").lower()
        added = runtime_config.add_channel(name)
        if added:
            channels = runtime_config.get_channels()
            await self._to(chat_id,
                f"✅ <b>CHANNEL ADDED</b>\n"
                f"@{_esc(name)} added.\n"
                f"Active: {', '.join('@'+c for c in channels)}\n"
                "Restart bot to connect to new channel.",
                important=True,
            )
        else:
            await self._to(chat_id, f"@{_esc(name)} is already in the list.")

    async def _reply_removechannel(self, chat_id, args):
        if not args:
            await self._to(chat_id, "Usage: /removechannel [username]")
            return
        name    = args[0].strip().lstrip("@").lower()
        removed = runtime_config.remove_channel(name)
        if removed:
            channels = runtime_config.get_channels()
            await self._to(chat_id,
                f"✅ <b>CHANNEL REMOVED</b>\n"
                f"@{_esc(name)} removed.\n"
                f"Remaining: {', '.join('@'+c for c in channels) or 'none'}",
            )
        else:
            await self._to(chat_id, f"@{_esc(name)} not found in list.")

    async def _reply_setsignal(self, chat_id, args):
        if not args:
            current = runtime_config.get("SIGNAL_POSITION_SOL", config.SIGNAL_POSITION_SOL)
            await self._to(chat_id,
                f"Usage: /setsignal [SOL]\n"
                f"Example: /setsignal 0.05\n"
                f"Current: {current} SOL"
            )
            return
        try:
            val = float(args[0])
            if not 0.001 <= val <= 10:
                await self._to(chat_id, "❌ Must be 0.001–10 SOL")
                return
            runtime_config.set_value("SIGNAL_POSITION_SOL", val)
            config.SIGNAL_POSITION_SOL = val
            await self._to(chat_id,
                f"✅ Signal position size → <b>{val} SOL</b>\n"
                "Applied immediately.",
                important=True,
            )
        except ValueError:
            await self._to(chat_id, "❌ Invalid number. Example: /setsignal 0.05")

    async def _reply_risk(self, chat_id, args):
        if not args:
            current = runtime_config.get("POSITION_SIZE_PCT", config.POSITION_SIZE_PCT)
            await self._to(chat_id,
                f"Usage: /risk [%]\n"
                f"Example: /risk 15\n"
                f"Current: {current*100:.0f}% of wallet per trade"
            )
            return
        try:
            pct = float(args[0])
            if not 1 <= pct <= 50:
                await self._to(chat_id, "❌ Must be 1–50%")
                return
            val = pct / 100
            runtime_config.set_value("POSITION_SIZE_PCT", val)
            config.POSITION_SIZE_PCT = val
            await self._to(chat_id,
                f"✅ Position size → <b>{pct:.0f}% of wallet</b>\n"
                "Applied immediately.",
                important=True,
            )
        except ValueError:
            await self._to(chat_id, "❌ Invalid. Example: /risk 20")

    async def _reply_setloss(self, chat_id, args):
        if not args:
            current = runtime_config.get("DAILY_LOSS_LIMIT_PCT", config.DAILY_LOSS_LIMIT_PCT)
            await self._to(chat_id,
                f"Usage: /setloss [%]\n"
                f"Example: /setloss 30\n"
                f"Current: {current*100:.0f}% daily loss limit"
            )
            return
        try:
            pct = float(args[0])
            if not 5 <= pct <= 80:
                await self._to(chat_id, "❌ Must be 5–80%")
                return
            val = pct / 100
            runtime_config.set_value("DAILY_LOSS_LIMIT_PCT", val)
            config.DAILY_LOSS_LIMIT_PCT = val
            await self._to(chat_id,
                f"✅ Daily loss limit → <b>{pct:.0f}%</b>\n"
                "Bot pauses automatically if this is hit.",
                important=True,
            )
        except ValueError:
            await self._to(chat_id, "❌ Invalid. Example: /setloss 30")

    async def _reply_setminbal(self, chat_id, args):
        if not args:
            current = runtime_config.get("LOW_BALANCE_USD", config.LOW_BALANCE_USD)
            await self._to(chat_id,
                f"Usage: /setminbal [$USD]\n"
                f"Example: /setminbal 5\n"
                f"Current: ${current:.0f} minimum balance"
            )
            return
        try:
            val = float(args[0])
            if not 0.5 <= val <= 50:
                await self._to(chat_id, "❌ Must be $0.50–$50")
                return
            runtime_config.set_value("LOW_BALANCE_USD", val)
            config.LOW_BALANCE_USD = val
            await self._to(chat_id,
                f"✅ Min balance floor → <b>${val:.2f}</b>\n"
                "Bot pauses if wallet drops below this.",
                important=True,
            )
        except ValueError:
            await self._to(chat_id, "❌ Invalid. Example: /setminbal 5")

    async def _reply_trade(self, chat_id, args):
        """
        /trade <CA> <amount_usd>
        Example: /trade 7xKXtg2CW87d97... 5
        Places a manual buy for <amount_usd> dollars on <CA>.
        Works in both PAPER and LIVE mode — in paper mode it just logs it.
        """
        if not args or len(args) < 2:
            await self._to(chat_id,
                "📌 <b>Manual Trade</b>\n\n"
                "Usage: <code>/trade CA amount</code>\n"
                "Example: <code>/trade 7xKXtg2... 5</code>\n\n"
                "• CA = token contract address\n"
                "• amount = how much in USD to spend\n\n"
                "Works in both PAPER and LIVE mode."
            )
            return

        ca = args[0].strip()
        import re
        if not re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', ca):
            await self._to(chat_id, "❌ That doesn't look like a valid Solana CA.")
            return

        try:
            amount_usd = float(args[1])
        except ValueError:
            await self._to(chat_id, "❌ Amount must be a number. Example: /trade CA 5")
            return

        if amount_usd <= 0:
            await self._to(chat_id, "❌ Amount must be greater than 0.")
            return

        if amount_usd > 500:
            await self._to(chat_id, "❌ Maximum manual trade is $500 per call.")
            return

        if not self._manual_trade:
            await self._to(chat_id, "❌ Manual trade not configured.")
            return

        await self._to(chat_id,
            f"⏳ <b>Manual trade requested</b>\n"
            f"CA: <code>{ca}</code>\n"
            f"Amount: ${amount_usd:.2f}\n"
            f"Processing..."
        )

        try:
            result = await self._manual_trade(ca=ca, amount_usd=amount_usd)
            if result.get("success"):
                mode = result.get("mode", "live")
                await self._to(chat_id,
                    f"{'📋 PAPER' if mode == 'paper' else '✅ TRADE PLACED'}\n\n"
                    f"CA: <code>{ca}</code>\n"
                    f"Amount: ${amount_usd:.2f}\n"
                    f"SOL: {result.get('sol', 0):.4f}\n"
                    f"Mode: {mode.upper()}",
                    important=True,
                )
            else:
                await self._to(chat_id,
                    f"❌ Trade failed: {result.get('error', 'unknown error')}"
                )
        except Exception as e:
            await self._to(chat_id, f"❌ Trade error: {e}")

    # ── Paper trading commands ─────────────────────────────────────────────────

    async def _reply_paper(self, chat_id, args):
        """
        /paper          — show paper trading stats
        /paper reset    — reset paper balance back to $40
        /paper export   — download all trades as CSV
        """
        if args and args[0].lower() == "on":
            import os
            os.environ["PAPER_TRADING_ENABLED"] = "true"
            await self._to(chat_id,
                "📄 <b>PAPER TRADING ON</b>\n"
                "──────────────────────────\n"
                "Paper trades will open on every signal.\n"
                "Real trades still depend on SIGNAL_TRADING setting."
            )
            return

        if args and args[0].lower() == "off":
            import os
            os.environ["PAPER_TRADING_ENABLED"] = "false"
            await self._to(chat_id,
                "⏸ <b>PAPER TRADING OFF</b>\n"
                "──────────────────────────\n"
                "Paper trades paused.\n"
                "Real trades still depend on SIGNAL_TRADING setting."
            )
            return

        if args and args[0].lower() == "export":
            if not self._paper_export:
                await self._to(chat_id, "Export not available.")
                return
            csv = await self._paper_export() if asyncio.iscoroutinefunction(self._paper_export) else self._paper_export()
            if csv.startswith("No paper"):
                await self._to(chat_id, csv)
                return
            # Send as file
            import io
            from telegram import InputFile
            ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
            bio = io.BytesIO(csv.encode())
            bio.name = f"paper_trades_{ts}.csv"
            await self._app.bot.send_document(
                chat_id=chat_id,
                document=InputFile(bio, filename=bio.name),
                caption=f"📊 Paper trades export — {ts} UTC",
            )
            return

        if args and args[0].lower() == "reset":
            if self._paper_reset:
                await self._paper_reset()
                await self._to(chat_id,
                    "📄 <b>PAPER TRADING RESET</b>\n"
                    "──────────────────────────\n"
                    f"Balance reset to ${40:.2f}\n"
                    "All paper positions cleared.\n"
                    "Starting fresh."
                )
            else:
                await self._to(chat_id, "Paper engine not available.")
            return

        if not self._paper_stats:
            await self._to(chat_id, "Paper engine not available.")
            return

        s = await self._paper_stats() if asyncio.iscoroutinefunction(self._paper_stats) else self._paper_stats()

        bal        = s.get("balance", 0)
        starting   = s.get("starting", 40)
        total_pnl     = float(s.get("total_pnl") or 0)
        total_invested= float(s.get("total_invested") or 0)
        total         = int(s.get("total_trades") or 0)
        wins          = int(s.get("wins") or 0)
        losses        = int(s.get("losses") or 0)
        win_rate      = float(s.get("win_rate") or 0)
        open_pos      = int(s.get("open_positions") or 0)
        by_ch         = s.get("by_channel") or {}
        best          = s.get("best_trade")
        worst         = s.get("worst_trade")

        # Real P&L from closed trades only (ignores auto-refills)
        pnl_emoji  = "📈" if total_pnl >= 0 else "📉"
        roi        = (total_pnl / total_invested * 100) if total_invested > 0 else 0

        msg = (
            f"📄 <b>PAPER TRADING STATS</b>\n"
            f"──────────────────────────\n"
            f"Current balance: <b>${bal:.2f}</b>\n"
            f"{pnl_emoji} Real P&amp;L (closed trades): ${total_pnl:+.2f}\n"
            f"Total invested: ${total_invested:.2f} | ROI: {roi:+.1f}%\n"
            f"Open: {open_pos} position(s)\n"
            f"──────────────────────────\n"
            f"Trades:   {total} total | {wins}W / {losses}L\n"
            f"Win rate: {win_rate:.1f}%\n"
        )

        if by_ch:
            msg += "──────────────────────────\n<b>By Channel</b>\n"
            for ch, data in by_ch.items():
                ch_wr = data['wins'] / data['trades'] * 100 if data['trades'] > 0 else 0
                msg += f"@{ch}: {data['trades']} trades | {ch_wr:.0f}% WR | ${data['pnl']:+.2f}\n"

        # AI vs non-AI comparison
        ai_t  = int(s.get("ai_trades") or 0)
        ai_wr = float(s.get("ai_win_rate") or 0)
        noai_t= int(s.get("noai_trades") or 0)
        noai_wr=float(s.get("noai_win_rate") or 0)
        if ai_t > 0 or noai_t > 0:
            msg += "──────────────────────────\n<b>AI Filter Impact</b>\n"
        if noai_t > 0:
            msg += f"Before AI: {noai_t} trades | {noai_wr:.1f}% WR\n"
        if ai_t > 0:
            diff = ai_wr - noai_wr if noai_t > 0 else 0
            arrow = "📈" if diff > 0 else "📉" if diff < 0 else "➡️"
            msg += f"After AI:  {ai_t} trades | {ai_wr:.1f}% WR {arrow}\n"

        if best:
            msg += f"──────────────────────────\n"
            msg += f"Best:  {best.get('symbol','?')} {float(best.get('final_pnl_pct') or 0):+.1f}% (peak {float(best.get('peak_multiplier') or 0):.1f}x)\n"
        if worst:
            msg += f"Worst: {worst.get('symbol','?')} {float(worst.get('final_pnl_pct') or 0):+.1f}%\n"

        msg += "──────────────────────────\n/paper reset — restart with $40\n/paper export — download CSV"

        await self._to(chat_id, msg)

    # ── Paper trade notifications ───────────────────────────────────────────────

    async def notify_paper_opened(self, mint: str, name: str, symbol: str,
                                   entry_price: float, usd_spent: float,
                                   channel: str, score: int, balance: float):
        await self._send(
            f"📋 <b>PAPER TRADE OPENED</b>\n"
            f"──────────────────────────\n"
            f"<b>{_esc(name)} (${_esc(symbol)})</b>\n"
            f"Channel: @{channel} | Score: {score}\n"
            f"Entry:   ${entry_price:.8f}\n"
            f"Spent:   ${usd_spent:.2f} (virtual)\n"
            f"Balance: ${balance:.2f} remaining\n"
            f"CA: <code>{mint}</code>\n"
            f"<a href='{DEXSCREENER}/{mint}'>DexScreener</a>",
        )

    async def notify_paper_tp(self, name: str, symbol: str, level: str,
                               usd_received: float, multiplier: float, balance: float):
        await self._send(
            f"💰 <b>PAPER TP — {level}</b>\n"
            f"──────────────────────────\n"
            f"<b>{_esc(name)} (${_esc(symbol)})</b>\n"
            f"Multiplier: {multiplier:.2f}x\n"
            f"Received:   ${usd_received:.2f} (virtual)\n"
            f"Balance:    ${balance:.2f}",
        )

    async def notify_paper_closed(self, name: str, symbol: str, reason: str,
                                   pnl_usd: float, pnl_pct: float,
                                   peak_mult: float, tp_levels: list, balance: float):
        emoji  = "✅" if pnl_usd > 0 else "❌"
        reason_map = {
            "stop_loss_phase1":      "Stop loss (-40% phase 1)",
            "trailing_stop":         "Trailing stop",
            "max_hold_72hr":         "72hr max hold",
            "24hr_check_failed":     "24hr check failed",
            "extended_hold_expired": "Extended hold expired",
        }
        reason_str = reason_map.get(reason, reason)
        tps = ", ".join(tp_levels) if tp_levels else "none"
        await self._send(
            f"{emoji} <b>PAPER TRADE CLOSED</b>\n"
            f"──────────────────────────\n"
            f"<b>{_esc(name)} (${_esc(symbol)})</b>\n"
            f"Reason:  {reason_str}\n"
            f"P&amp;L:    ${pnl_usd:+.2f} ({pnl_pct:+.1f}%)\n"
            f"Peak:    {peak_mult:.2f}x\n"
            f"TPs hit: {tps}\n"
            f"Balance: ${balance:.2f}",
            important=True,
        )

    # ── AI stats ───────────────────────────────────────────────────────────────

    async def _reply_aistats(self, chat_id):
        import os
        from pathlib import Path
        try:
            from ai.scorer import get_scorer, AI_THRESHOLD
            scorer  = get_scorer()
            loaded  = scorer._loaded
            model   = scorer._model
            threshold = float(os.getenv("AI_THRESHOLD", str(AI_THRESHOLD)))

            if not loaded or model is None:
                await self._to(chat_id,
                    "🤖 <b>AI SCORER</b>\n"
                    "──────────────────────────\n"
                    "Status: ❌ Model not loaded\n"
                    "Run python ai/train.py to train the model."
                )
                return

            # Get feature importances
            feat_names = scorer._feature_names or []
            importances= list(model.feature_importances_) if hasattr(model, 'feature_importances_') else []

            model_path = Path("ai/signal_model.pkl")
            trained_on = "unknown"
            if model_path.exists():
                import datetime
                mtime = model_path.stat().st_mtime
                trained_on = datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M UTC")

            msg = (
                f"🤖 <b>AI SCORER STATS</b>\n"
                f"──────────────────────────\n"
                f"Status:    ✅ Active\n"
                f"Threshold: {threshold:.0%} confidence to trade\n"
                f"Trained:   {trained_on}\n"
                f"──────────────────────────\n"
                f"<b>Feature Importance:</b>\n"
            )
            if feat_names and importances:
                for feat, imp in sorted(zip(feat_names, importances), key=lambda x: -x[1])[:6]:
                    bar = '█' * int(imp * 30)
                    msg += f"  {feat}: {imp:.2f} {bar}\n"

            msg += (
                f"──────────────────────────\n"
                f"To retrain: export CSV with /paper export\n"
                f"then run: python ai/train.py\n"
                f"To adjust threshold: set AI_THRESHOLD in Railway Variables"
            )
            await self._to(chat_id, msg)

        except Exception as e:
            await self._to(chat_id, f"AI stats error: {e}")

    # ── Set mode ───────────────────────────────────────────────────────────────

    async def _reply_setmode(self, chat_id, args):
        """
        /setmode paper  — switch to paper trading (no real money)
        /setmode live   — switch to live trading (real money)
        Changes take effect immediately without restart.
        """
        import os
        from core import runtime_config

        if not args:
            current = os.getenv("SIGNAL_TRADING", "off").strip().lower()
            mode = "LIVE" if current == "on" else "PAPER"
            await self._to(chat_id,
                f"Current mode: <b>{mode}</b>\n\n"
                "/setmode paper — switch to paper (safe)\n"
                "/setmode live  — switch to live (real money)"
            )
            return

        arg = args[0].strip().lower()
        if arg not in ("paper", "live", "on", "off"):
            await self._to(chat_id,
                "Usage: /setmode paper  or  /setmode live"
            )
            return

        go_live = arg in ("live", "on")
        os.environ["SIGNAL_TRADING"] = "on" if go_live else "off"
        runtime_config.set_value("SIGNAL_TRADING", "on" if go_live else "off")

        if go_live:
            await self._to(chat_id,
                "💸 <b>SWITCHED TO LIVE MODE</b>\n"
                "──────────────────────────\n"
                "Real trades will now be executed.\n"
                "Real SOL at risk.\n\n"
                "/setmode paper  — go back to paper",
                important=True,
            )
        else:
            await self._to(chat_id,
                "📄 <b>SWITCHED TO PAPER MODE</b>\n"
                "──────────────────────────\n"
                "No real trades will be placed.\n"
                "Signals still received and logged.\n\n"
                "/setmode live  — when ready to go live",
                important=True,
            )

    # ── SOL price ──────────────────────────────────────────────────────────────

    async def _reply_solprice(self, chat_id):
        import aiohttp
        sources = [
            ("Jupiter",   "https://price.jup.ag/v6/price?ids=SOL",
             lambda d: float(d["data"]["SOL"]["price"])),
            ("CoinGecko", "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
             lambda d: float(d["solana"]["usd"])),
            ("Binance",   "https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT",
             lambda d: float(d["price"])),
        ]
        price = 0.0
        source_used = "?"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=6)) as session:
                for name, url, extractor in sources:
                    try:
                        async with session.get(url) as r:
                            if r.status == 200:
                                data  = await r.json(content_type=None)
                                price = extractor(data)
                                source_used = name
                                break
                    except Exception:
                        continue
        except Exception as e:
            await self._to(chat_id, f"Price fetch failed: {e}")
            return

        if price <= 0:
            await self._to(chat_id, "Could not fetch SOL price right now.")
            return

        await self._to(chat_id,
            f"SOL price: <b>${price:,.2f}</b>\n"
            f"Source: {source_used}\n"
            f"1 SOL = ${price:,.2f} USD\n"
            f"$1 = {1/price:.6f} SOL"
        )

    # ── CA lookup ──────────────────────────────────────────────────────────────

    async def _reply_ca(self, chat_id, args):
        """
        /ca <CA>
        Fetches token info from DexScreener and RugCheck and returns a summary.
        """
        if not args:
            await self._to(chat_id,
                "Usage: /ca [contract address]\n"
                "Example: /ca 7xKXtg2CW87d97..."
            )
            return

        ca = args[0].strip()
        import re, aiohttp
        if not re.match(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$", ca):
            await self._to(chat_id, "Invalid CA — must be a Solana contract address.")
            return

        await self._to(chat_id, f"Looking up <code>{ca[:20]}...</code>")

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                # DexScreener
                dex_data = {}
                try:
                    async with session.get(
                        f"https://api.dexscreener.com/latest/dex/tokens/{ca}"
                    ) as r:
                        if r.status == 200:
                            j = await r.json()
                            pairs = j.get("pairs") or []
                            if pairs:
                                p = pairs[0]
                                dex_data = {
                                    "name":    p.get("baseToken", {}).get("name", "?"),
                                    "symbol":  p.get("baseToken", {}).get("symbol", "?"),
                                    "mc":      p.get("marketCap") or p.get("fdv") or 0,
                                    "liq":     p.get("liquidity", {}).get("usd", 0),
                                    "price":   p.get("priceUsd", "?"),
                                    "age_h":   round((datetime.now(timezone.utc).timestamp()
                                                - (p.get("pairCreatedAt", 0) / 1000)) / 3600, 1),
                                    "vol_5m":  p.get("volume", {}).get("m5", 0),
                                    "vol_1h":  p.get("volume", {}).get("h1", 0),
                                    "ch_5m":   p.get("priceChange", {}).get("m5", 0),
                                    "ch_1h":   p.get("priceChange", {}).get("h1", 0),
                                    "buys_5m": p.get("txns", {}).get("m5", {}).get("buys", 0),
                                    "sells_5m":p.get("txns", {}).get("m5", {}).get("sells", 0),
                                    "dex_url": p.get("url", ""),
                                }
                except Exception:
                    pass

                # RugCheck
                rug_data = {}
                try:
                    async with session.get(
                        f"https://api.rugcheck.xyz/v1/tokens/{ca}/report/summary"
                    ) as r:
                        if r.status == 200:
                            j = await r.json()
                            rug_data = {
                                "score":   j.get("score", "?"),
                                "verdict": j.get("score_normalised", "?"),
                                "mint_ok": not j.get("mint_authority_present", True),
                                "freeze_ok": not j.get("freeze_authority_present", True),
                                "risks":   [r["name"] for r in j.get("risks", [])[:4]],
                            }
                except Exception:
                    pass

        except Exception as e:
            await self._to(chat_id, f"Fetch error: {e}")
            return

        if not dex_data and not rug_data:
            await self._to(chat_id,
                f"No data found for this CA.\n"
                f"It may be too new or not listed yet.\n"
                f"<code>{ca}</code>"
            )
            return

        name   = dex_data.get("name", "Unknown")
        symbol = dex_data.get("symbol", "?")
        mc     = dex_data.get("mc", 0)
        liq    = dex_data.get("liq", 0)
        age_h  = dex_data.get("age_h", 0)
        price  = dex_data.get("price", "?")
        v5m    = dex_data.get("vol_5m", 0)
        v1h    = dex_data.get("vol_1h", 0)
        c5m    = dex_data.get("ch_5m", 0)
        c1h    = dex_data.get("ch_1h", 0)
        buys   = dex_data.get("buys_5m", 0)
        sells  = dex_data.get("sells_5m", 0)
        dex_url= dex_data.get("dex_url", "")

        rug_score   = rug_data.get("score", "?")
        rug_verdict = rug_data.get("verdict", "?")
        mint_ok     = rug_data.get("mint_ok", "?")
        freeze_ok   = rug_data.get("freeze_ok", "?")
        risks       = rug_data.get("risks", [])

        mc_str  = f"${mc/1e6:.2f}M" if mc >= 1e6 else f"${mc/1e3:.0f}K" if mc >= 1000 else f"${mc:.0f}"
        liq_str = f"${liq/1e3:.1f}K" if liq >= 1000 else f"${liq:.0f}"

        verdict_emoji = "🟢" if isinstance(rug_score, (int, float)) and rug_score < 200 else "🔴" if isinstance(rug_score, (int, float)) and rug_score > 500 else "🟡"

        msg = (
            f"<b>{_esc(name)} (${_esc(symbol)})</b>\n"
            f"<code>{ca}</code>\n"
            f"──────────────────────────\n"
            f"Price:   ${price}\n"
            f"MC:      {mc_str}\n"
            f"Liq:     {liq_str}\n"
            f"Age:     {age_h:.1f}h\n"
            f"──────────────────────────\n"
            f"Vol 5m:  ${v5m:,.0f}  ({'+' if c5m >= 0 else ''}{c5m:.1f}%)\n"
            f"Vol 1h:  ${v1h:,.0f}  ({'+' if c1h >= 0 else ''}{c1h:.1f}%)\n"
            f"Txns 5m: {buys} buys / {sells} sells\n"
            f"──────────────────────────\n"
            f"RugCheck: {verdict_emoji} Score {rug_score} ({rug_verdict})\n"
            f"Mint revoked:   {'✅ Yes' if mint_ok is True else '❌ NO' if mint_ok is False else '?'}\n"
            f"Freeze revoked: {'✅ Yes' if freeze_ok is True else '❌ NO' if freeze_ok is False else '?'}\n"
        )
        if risks:
            msg += f"Risks: {', '.join(risks)}\n"
        if dex_url:
            msg += f"<a href='{dex_url}'>DexScreener</a>\n"
        msg += (
            f"──────────────────────────\n"
            f"/trade {ca} 5  — buy $5 worth"
        )
        await self._to(chat_id, msg, important=True)

    async def notify_paper_refill(self, old_balance: float, new_balance: float):
        await self._send(
            f"♻️ <b>PAPER BALANCE REFILLED</b>\n"
            f"──────────────────────────\n"
            f"Was: ${old_balance:.2f}\n"
            f"Now: ${new_balance:.2f}\n"
            f"Data collection continues automatically.",
        )

    async def notify_paper_post_check(self, name: str, symbol: str, hours: int,
                                        exit_price: float, current_price: float,
                                        pct_change: float, close_reason: str,
                                        original_pnl_pct: float):
        emoji  = "📈" if pct_change > 0 else "📉"
        missed = "would have gained more" if pct_change > 0 else "glad we exited"
        await self._send(
            f"📊 <b>POST-TRADE CHECK ({hours}h after close)</b>\n"
            f"──────────────────────────\n"
            f"<b>{_esc(name)} (${_esc(symbol)})</b>\n"
            f"Closed via: {close_reason}\n"
            f"Our P&amp;L was: {original_pnl_pct:+.1f}%\n"
            f"──────────────────────────\n"
            f"Exit price:   ${exit_price:.8f}\n"
            f"Price now:    ${current_price:.8f}\n"
            f"{emoji} Since exit: {pct_change:+.1f}% — {missed}",
        )

    # ── Clear messages ────────────────────────────────────────────────────────

    async def _reply_clear(self, chat_id):
        """Delete the last 50 bot messages from the chat."""
        try:
            deleted = 0
            async for msg in self._app.bot.get_updates():
                pass  # flush
            # Send a placeholder then delete everything recent
            sent = await self._app.bot.send_message(
                chat_id=chat_id,
                text="🧹 Clearing..."
            )
            # Delete recent messages by iterating backwards from current msg id
            for msg_id in range(sent.message_id, max(sent.message_id - 100, 0), -1):
                try:
                    await self._app.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                    deleted += 1
                except Exception:
                    pass
                if deleted >= 50:
                    break
        except Exception as e:
            await self._to(chat_id, f"Clear error: {e}")

    # ── Auth ───────────────────────────────────────────────────────────────────

    async def _auth(self, update) -> bool:
        if str(update.effective_chat.id) != str(config.TELEGRAM_CHAT_ID):
            await update.message.reply_text("⛔ Unauthorised")
            return False
        try:
            task = asyncio.create_task(
                self._auto_delete(
                    str(update.effective_chat.id),
                    update.message.message_id,
                    _ROUTINE_TTL,
                )
            )
            self._cleanup_tasks.append(task)
            self._cleanup_tasks = [t for t in self._cleanup_tasks if not t.done()]
        except Exception:
            pass
        return True

    # ── Send / Edit / Delete ───────────────────────────────────────────────────

    async def send(self, msg: str, important: bool = False):
        if self._enabled:
            await self._send(msg, important=important)

    async def _send(self, text: str, important: bool = False):
        await self._to(str(config.TELEGRAM_CHAT_ID), text, important=important)

    async def _to(self, chat_id: str, text: str, important: bool = False) -> Optional[int]:
        if not self._bot or not chat_id or chat_id == "placeholder":
            return None
        if not important:
            for kw in _IMPORTANT_KEYWORDS:
                if kw in text:
                    important = True
                    break
        try:
            msg    = await self._bot.send_message(
                chat_id=chat_id, text=text[:4000], parse_mode="HTML",
            )
            msg_id = msg.message_id
            if not important:
                task = asyncio.create_task(
                    self._auto_delete(chat_id, msg_id, _ROUTINE_TTL)
                )
                self._cleanup_tasks.append(task)
                self._cleanup_tasks = [t for t in self._cleanup_tasks if not t.done()]
            return msg_id
        except Exception as e:
            logger.debug(f"Send: {e}")
            return None

    async def _auto_delete(self, chat_id: str, msg_id: int, delay: int):
        await asyncio.sleep(delay)
        if not self._bot:
            return
        try:
            await self._bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass

    async def _edit_or_send(self, msg_id: Optional[int], text: str,
                             important: bool = False) -> int:
        chat_id = str(config.TELEGRAM_CHAT_ID)
        if not self._bot or not chat_id or chat_id == "placeholder":
            return 0
        if msg_id:
            try:
                await self._bot.edit_message_text(
                    chat_id=chat_id, message_id=msg_id,
                    text=text[:4000], parse_mode="HTML",
                )
                return msg_id
            except Exception:
                pass
        new_id = await self._to(chat_id, text, important=important)
        return new_id or 0

    async def stop(self):
        if self._app and self._enabled:
            try:
                await self._app.updater.stop()
                await self._app.stop()
                await self._app.shutdown()
            except Exception as e:
                logger.debug(f"Stop: {e}")

    # ── Signal notifications ───────────────────────────────────────────────────

    async def notify_signal_rejected(self, name: str, ca: str,
                                      reason: str, score: int,
                                      mc: float = 0):
        self._signal_history.append({
            "name": name, "ca": ca, "passed": False,
            "verdict": "reject", "score": score,
            "reason": reason, "mc": mc,
        })
        if len(self._signal_history) > 20:
            self._signal_history.pop(0)

        short_rsn = _short_reason(reason)
        mc_str    = f"\nMC: {_mc(mc)}" if mc > 0 else ""

        text = (
            f"❌  <b>{_esc(name)}</b>\n"
            f"Quabsnipe 🎯: <i>Rejected</i>  {{{short_rsn}}}"
            f"{mc_str}\n"
            f"CA: <code>{ca}</code>"
        )
        self._signal_msg_id = await self._edit_or_send(
            self._signal_msg_id, text, important=False
        )

    async def notify_signal_passed(self, name: str, ca: str, score: int,
                                    verdict: str, mc: float, liq: float,
                                    age_h: float, snipers: float,
                                    bundles: float, dev: float,
                                    fake_vol: float, size_pct: float,
                                    good_flags: list, bad_flags: list,
                                    ai_prob: float = 1.0):
        self._signal_history.append({
            "name": name, "ca": ca, "passed": True,
            "verdict": verdict, "score": score,
            "reason": "", "mc": mc,
        })

        size_tag = "" if abs(size_pct - 1.0) < 0.05 else f"  📊 {size_pct*100:.0f}% size"
        age_str  = f"{age_h*60:.0f}min"
        ai_tag   = f"  🤖 {ai_prob:.0%}" if ai_prob < 1.0 else ""

        text = (
            f"✅  <b>{_esc(name)}</b>{size_tag}\n"
            f"Quabsnipe 🎯: <i>Accepted</i>  · Score: {score}{ai_tag}\n"
            f"CA: <code>{ca}</code>\n"
            f"MC: {_mc(mc)}  ·  Liq: {_mc(liq)}  ·  Age: {age_str}"
        )
        self._signal_msg_id = None
        await self._to(str(config.TELEGRAM_CHAT_ID), text, important=True)

    # ── Trade notifications ────────────────────────────────────────────────────

    async def notify_paper_trade(self, mint, source, score, verdict, trade_sol, trade_usd):
        e = "✅" if verdict == "clean" else "⚠️"
        await self._send(
            f"{e} <b>PAPER SIGNAL</b>\n"
            f"Source: {source.upper()}\n"
            f"Score: {score} ({verdict.upper()})\n"
            f"Would trade: {trade_sol:.4f} SOL\n"
            f"CA: <code>{mint}</code>\n"
            f"<a href='{DEXSCREENER}/{mint}'>DexScreener</a>",
        )

    async def notify_trade_blocked(self, name: str, ca: str, reason: str):
        await self._send(
            f"⛔ <b>TRADE BLOCKED</b>\n"
            f"Signal passed filters but trade could not be placed.\n"
            f"──────────────────────────\n"
            f"Token: {_esc(name)}\n"
            f"Reason: {reason}\n"
            f"CA: <code>{ca}</code>\n"
            f"<a href='{DEXSCREENER}/{ca}'>DexScreener</a>",
            important=True,
        )

    async def notify_trade_opened(self, symbol: str, mint: str, source: str,
                                   sol_spent: float, entry_price: float,
                                   score: int, verdict: str):
        e = "✅" if verdict == "clean" else "⚠️"
        await self._send(
            f"🚀 <b>TRADE OPENED</b>\n"
            f"{e} <b>{_esc(symbol)}</b>  [{source.upper()}]\n"
            f"Spent:  {sol_spent:.4f} SOL\n"
            f"Price:  ${entry_price:.8f}\n"
            f"Score:  {score} ({verdict.upper()})\n"
            f"CA: <code>{mint}</code>\n"
            f"<a href='{DEXSCREENER}/{mint}'>DexScreener</a>",
            important=True,
        )

    async def notify_take_profit(self, symbol: str, level: str,
                                  sold_pct: int, usd_received: float,
                                  tokens_remaining: float):
        await self._send(
            f"💰 <b>TAKE PROFIT — {level}</b>\n"
            f"Token:  <b>{_esc(symbol)}</b>\n"
            f"Sold:   {sold_pct}% → {usd_received:.3f} SOL\n"
            f"Remaining: {tokens_remaining:,.0f}",
            important=True,
        )

    async def notify_stop_loss(self, symbol: str, reason: str,
                                exit_price: float, pnl_usd: float,
                                pnl_pct: float):
        reason_map = {
            "stop_loss_phase1":      "Phase 1 stop (-40%)",
            "trailing_stop":         "Trailing stop",
            "max_hold_72hr":         "72h max hold",
            "24hr_check_failed":     "24h check failed",
            "extended_hold_expired": "Extended hold expired",
            "manual_sell":           "Manual close",
        }
        label = reason_map.get(reason, reason.replace("_", " "))
        e     = "🟢" if pnl_usd >= 0 else "🔴"
        await self._send(
            f"🛑 <b>POSITION CLOSED</b>\n"
            f"Token:  <b>{_esc(symbol)}</b>\n"
            f"Reason: {label}\n"
            f"P&amp;L:   {e} {pnl_usd:+.3f} SOL ({pnl_pct:+.1f}%)",
            important=True,
        )

    async def notify_24hr_check(self, symbol: str, signals: dict,
                                 positive: int, action: str):
        lines = [f"⏰ <b>24H CHECK — {_esc(symbol)}</b>"]
        for k, v in signals.items():
            lines.append(f"{'✅' if v else '❌'} {k.replace('_',' ')}")
        lines.append(f"\n{'✅ Extending hold' if action == 'extend' else '❌ Closing'} ({positive}/4)")
        await self._send("\n".join(lines), important=True)

    async def notify_auto_export(self, record_count: int, filepath: str):
        if record_count == 0:
            await self._send(
                "📦 <b>AUTO-EXPORT</b>\n"
                "No completed trades to export yet.",
            )
        else:
            await self._send(
                f"📦 <b>AUTO-EXPORT COMPLETE</b>\n"
                f"Records saved: <b>{record_count}</b>\n"
                f"File: <code>{filepath}</code>",
                important=True,
            )

    async def notify_daily_summary(self, pnl: dict):
        w     = pnl.get("wins", 0) or 0
        l     = pnl.get("losses", 0) or 0
        total = pnl.get("total_pnl", 0) or 0
        avg   = pnl.get("avg_pnl_pct", 0) or 0
        wr    = _pct(w, w + l)
        e     = "🟢" if total >= 0 else "🔴"
        await self._send(
            f"📊 <b>DAILY SUMMARY</b>\n"
            f"Trades:   {w+l}  W:{w} L:{l}\n"
            f"Win rate: {wr}  {_bar(w, l)}\n"
            f"P&amp;L:   {e} {total:+.3f} SOL\n"
            f"Avg:      {avg:+.1f}%",
            important=True,
        )

    # ── v10 Reliability notifications ─────────────────────────────────────────

    async def notify_restart(self, sol: float, usd: float, open_positions: int,
                              mode: str, channels: list):
        # Clean channel names — strip any accidental "SIGNAL_CHANNELS=" prefix
        clean = []
        for c in (channels or []):
            c = c.strip()
            if "=" in c:
                c = c.split("=")[-1].strip()
            if c:
                clean.append(c)

        # Alias map for display — clickable links
        alias = {
            "solearlytrending":    "Solana Early Trending",
            "soltrenchtrending":   "Trench Trending",
            "BlocApe_Signal_Calls":"BlocApe Calls",
            "trenchfree":          "TrenchFree",
        }
        ch_lines = "\n".join(
            f"  • <a href='https://t.me/{c}'>{alias.get(c, c)}</a>" for c in clean
        ) if clean else "  none"

        pos_str = f"{open_positions} recovered" if open_positions else "none"
        await self._send(
            f"🤖 <b>QUABSNIPE RESTARTED</b>\n"
            f"──────────────────────────\n"
            f"Mode:      {'📄 Paper' if mode == 'PAPER' else '💸 Live'}\n"
            f"Balance:   {sol:.4f} SOL (${usd:.2f})\n"
            f"Positions: {pos_str}\n"
            f"Channels:\n{ch_lines}\n"
            f"Time:      {_t()}\n"
            "/help — all commands",
            important=True,
        )

    async def notify_heartbeat(self, uptime: str, sol: float, usd: float,
                                open_positions: int, paused: bool):
        status = "⏸ PAUSED" if paused else "▶️ Active"
        await self._send(
            f"💓 <b>HEARTBEAT</b> — {_t()}\n"
            f"Uptime:    {uptime}\n"
            f"Balance:   {sol:.4f} SOL (${usd:.2f})\n"
            f"Positions: {open_positions} open\n"
            f"Status:    {status}",
            important=True,
        )

    async def notify_risk_paused(self, reason: str):
        await self._send(
            f"⛔ <b>BOT AUTO-PAUSED</b>\n"
            f"──────────────────────────\n"
            f"{_esc(reason)}\n\n"
            "No new trades will open.\n"
            "/resume to override manually\n"
            "(bot auto-resumes next day if daily loss limit was the cause)",
            important=True,
        )

    async def notify_risk_resumed(self):
        await self._send(
            "▶️ <b>AUTO-RESUMED</b>\n"
            "Risk guard conditions cleared.\n"
            "Trading resumed.",
            important=True,
        )

    async def notify_error(self, message: str):
        await self._send(
            f"🚨 <b>ERROR</b>\n"
            f"{_esc(message[:300])}\n"
            f"Time: {_t()}",
            important=True,
        )
