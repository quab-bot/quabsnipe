# telegram_signals/signal_bridge.py
# Routes signals from channel monitors to the trade coordinator.
# Adds: channel tracking, multi-channel dedup boost, blacklist check.

import asyncio
import aiohttp
from datetime import datetime, timezone

import config
from utils.logger import get_logger
from paper.paper_engine import PaperEngine
from ai.scorer import get_scorer
from ai.scorer import get_scorer
from utils.models import NewTokenEvent, FilterResult
from telegram_signals.signal_parser import SignalData
from telegram_signals.signal_filter import run_signal_filter
from core.analytics import log_token_analysis
from core.database import cache_token_result
from core import channel_tracker, rejected_tracker
from core.runtime_config import is_blacklisted

logger = get_logger("signal_bridge")

_HONEYPOT_AMOUNTS = ["1000000000", "100000000", "10000000", "1000000"]

# Dedup window: track when we first saw each CA and from which channels
# {mint: {"first_seen": float, "channels": set, "data": SignalData}}
_pending_dedup: dict[str, dict] = {}
_DEDUP_WINDOW_SECS = 1800  # 30 minutes — prevents same CA trading twice from different channels


class SignalBridge:
    def __init__(self, trade_coordinator, telegram_bot=None, paper_engine=None):
        self.coordinator  = trade_coordinator
        self.telegram     = telegram_bot
        self.paper_engine = paper_engine
        self._processed: set[str] = set()
        self._stats = {"received": 0, "passed": 0, "rejected": 0}

    async def on_signal(self, data: SignalData):
        self._stats["received"] += 1
        mint    = data.mint_address
        channel = data.channel or "unknown"

        if not mint:
            logger.warning("Signal missing CA — skipping")
            return

        # ── Blacklist check ────────────────────────────────────────────────────
        if is_blacklisted(mint):
            logger.info(f"🚫 BLACKLISTED: {mint[:16]}... — skipping")
            return

        # ── Multi-channel dedup boost ──────────────────────────────────────────
        now = datetime.now(timezone.utc).timestamp()
        if mint in _pending_dedup:
            _pending_dedup[mint]["channels"].add(channel)
            channel_tracker.record_signal_seen(channel, mint, passed=True)
            logger.info(
                f"📡 Multi-channel boost: {mint[:16]}... now seen by "
                f"{len(_pending_dedup[mint]['channels'])} channels: "
                f"{_pending_dedup[mint]['channels']}"
            )
            # Don't reprocess — already handled by the first channel
            return

        # ── Dedup against already-processed ───────────────────────────────────
        if mint in self._processed:
            logger.debug(f"Duplicate: {mint[:16]}... skipped")
            return

        # Register in dedup window
        _pending_dedup[mint] = {
            "first_seen": now,
            "channels":   {channel},
            "data":       data,
        }
        self._processed.add(mint)
        if len(self._processed) > 500:
            self._processed = set(list(self._processed)[-250:])

        # Prune old dedup entries
        expired = [k for k, v in _pending_dedup.items()
                   if now - v["first_seen"] > _DEDUP_WINDOW_SECS]
        for k in expired:
            _pending_dedup.pop(k, None)

        # ── AI scoring — skip low-confidence signals ──────────────────────────
        scorer = get_scorer()
        should_trade, ai_prob = scorer.should_trade(channel=channel)
        if not should_trade:
            logger.info(
                f"[AI SKIP|@{channel}] {name} | "
                f"AI confidence: {ai_prob:.0%} — below threshold"
            )
            self._stats["rejected"] += 1
            if self.telegram:
                await self.telegram.notify_signal_rejected(
                    name=name, ca=mint,
                    reason=f"AI score too low ({ai_prob:.0%})",
                    risk_score=int((1 - ai_prob) * 100),
                    mc=data.mc_usd, age_hours=data.age_hours, liq=data.liq_usd,
                )
            return

        # For simple CA signals (BlocApe-style) — enrich from DexScreener then check min liq
        if data.source == "simple_ca":
            data = await self._enrich_from_dexscreener(data)
            if data.liq_usd < 5000:
                logger.info(
                    f"[REJECT|@{channel}] {data.token_name} — "
                    f"liq ${data.liq_usd:.0f} below $5K minimum after enrichment"
                )
                if self.telegram:
                    await self.telegram.notify_signal_rejected(
                        name=data.token_name, ca=mint,
                        reason=f"liq_too_low (${data.liq_usd:.0f})",
                        risk_score=100, mc=data.mc_usd,
                        age_hours=data.age_hours, liq=data.liq_usd,
                    )
                return

        # Fetch real token name if missing
        if not data.token_name:
            data.token_name = await _fetch_token_name(mint) or ""

        logger.info(
            f"\n{'─'*55}\n"
            f"📡 SIGNAL | {data.token_name or mint[:16]} | @{channel}\n"
            f"  CA:      {mint}\n"
            f"  Age:     {data.age_hours:.1f}h | MC: ${data.mc_usd:,.0f}\n"
            f"  Liq:     ${data.liq_usd:,.0f} | Vol: ${data.vol_1h_usd:,.0f}\n"
            f"  Bundles: {data.bundles_pct_now:.1f}% | "
            f"Snipers: {data.snipers_pct_now:.1f}%\n"
            f"  Dev:     {data.dev_pct:.1f}% | "
            f"Fake vol: {data.fake_vol_pct:.1f}%\n"
            f"{'─'*55}"
        )

        # ── Filter ────────────────────────────────────────────────────────────
        result = run_signal_filter(data)

        # Log analytics
        log_token_analysis(
            mint=mint, source=f"telegram_{channel}",
            liquidity_usd=data.liq_usd,
            market_cap_usd=data.mc_usd,
            risk_score=result.risk_score,
            verdict=result.verdict,
            rejection_stage="signal_filter" if not result.passed else "pass",
            rejection_reason=result.fail_reason,
            flags=result.flags,
        )

        # Track channel signal
        channel_tracker.record_signal_seen(channel, mint, passed=result.passed)

        name = data.token_name or mint[:16]
        mc   = data.mc_usd

        if not result.passed:
            self._stats["rejected"] += 1
            cache_token_result(mint, "reject", result.risk_score,
                               result.fail_reason, f"telegram_{channel}")

            # Queue for outcome tracking (AI training data)
            rejected_tracker.schedule_rejected_token(
                mint=mint,
                source=f"telegram_{channel}",
                reject_reason=result.fail_reason or "",
                risk_score=result.risk_score,
            )

            logger.info(
                f"❌ REJECTED | {name} | "
                f"{result.fail_reason} | Score:{result.risk_score}"
            )
            if self.telegram and hasattr(self.telegram, 'notify_signal_rejected'):
                await self.telegram.notify_signal_rejected(
                    name=name, ca=mint,
                    reason=result.fail_reason,
                    score=result.risk_score,
                    mc=mc,
                )
            return

        # ── Honeypot check 1: Jupiter ──────────────────────────────────────────
        logger.info(f"🔍 Honeypot check (Jupiter): {mint[:20]}...")
        is_honeypot, hp_detail = await self._is_honeypot(mint)
        if is_honeypot:
            self._stats["rejected"] += 1
            cache_token_result(mint, "reject", 100, "honeypot_detected", f"telegram_{channel}")
            rejected_tracker.schedule_rejected_token(
                mint=mint, source=f"telegram_{channel}",
                reject_reason="honeypot_detected", risk_score=100,
            )
            logger.info(f"🍯 HONEYPOT (Jupiter): {mint[:16]} — {hp_detail}")
            if self.telegram and hasattr(self.telegram, 'notify_signal_rejected'):
                await self.telegram.notify_signal_rejected(
                    name=name, ca=mint, reason="honeypot_detected", score=100, mc=mc,
                )
            return

        # ── Honeypot check 2: DexScreener ──────────────────────────────────────
        logger.info(f"🔍 Honeypot check (DexScreener): {mint[:20]}...")
        is_dex_hp, dex_detail = await self._dexscreener_honeypot_check(mint)
        if is_dex_hp:
            self._stats["rejected"] += 1
            cache_token_result(mint, "reject", 100, f"honeypot_dex_{dex_detail}", f"telegram_{channel}")
            rejected_tracker.schedule_rejected_token(
                mint=mint, source=f"telegram_{channel}",
                reject_reason=f"honeypot_dex_{dex_detail}", risk_score=100,
            )
            logger.info(f"🍯 HONEYPOT (DexScreener): {mint[:16]} — {dex_detail}")
            if self.telegram and hasattr(self.telegram, 'notify_signal_rejected'):
                await self.telegram.notify_signal_rejected(
                    name=name, ca=mint,
                    reason=f"suspicious_pattern: {dex_detail}",
                    score=100, mc=mc,
                )
            return

        # ── PASSED ─────────────────────────────────────────────────────────────
        self._stats["passed"] += 1
        cache_token_result(mint, result.verdict, result.risk_score, "", f"telegram_{channel}")

        # Apply channel win-rate multiplier + multi-channel boost
        ch_mult        = channel_tracker.get_size_multiplier(channel)
        channel_count  = len(_pending_dedup.get(mint, {}).get("channels", {channel}))
        dedup_boost    = 0.3 if channel_count >= 2 else 0.0
        final_size     = min(result.size_multiplier * ch_mult + dedup_boost, 1.5)

        logger.info(
            f"\n{'='*55}\n"
            f"✅ SIGNAL PASSED | {name} | @{channel}\n"
            f"  Score: {result.risk_score}/45 | {result.verdict.upper()}\n"
            f"  Size:  {final_size*100:.0f}% "
            f"(filter:{result.size_multiplier:.2f} × channel:{ch_mult:.2f}"
            f"{' + dedup_boost' if dedup_boost else ''})\n"
            f"{'='*55}"
        )

        if self.telegram and hasattr(self.telegram, 'notify_signal_passed'):
            good = [b.split("(")[0] for b in result.bonus_flags]
            bad  = [f.split("(")[0] for f in result.flags]
            await self.telegram.notify_signal_passed(
                name=name, ca=mint,
                score=result.risk_score,
                verdict=result.verdict,
                mc=data.mc_usd,
                liq=data.liq_usd,
                age_h=data.age_hours,
                snipers=data.snipers_pct_now,
                bundles=data.bundles_pct_now,
                dev=data.dev_pct,
                fake_vol=data.fake_vol_pct,
                size_pct=final_size,
                good_flags=good,
                bad_flags=bad,
                ai_prob=ai_prob,
            )

        event = NewTokenEvent(
            mint_address=mint,
            pair_address=mint,
            base_mint=mint,
            quote_mint=config.WSOL_MINT,
            source=f"telegram_{channel}",
            detected_at=datetime.now(timezone.utc),
            initial_liquidity_sol=data.liq_usd / 150,
            market_cap_usd=data.mc_usd,
        )
        event.raw_data["_parsed_name"]   = data.token_name
        event.raw_data["_parsed_symbol"] = (
            data.token_symbol or
            (data.token_name.split()[0][:6].upper() if data.token_name else mint[:6])
        )
        event.raw_data["_channel"]       = channel

        filter_res = FilterResult(
            passed=True, token=event,
            filter_details={
                "risk_score":      result.risk_score,
                "verdict":         result.verdict,
                "size_multiplier": final_size,
                "flags":           result.flags,
                "source":          f"telegram_{channel}",
                "channel":         channel,
            }
        )
        # ── Paper engine — check if paper trading is enabled ──────────────────
        import os as _os
        _paper_enabled = _os.getenv("PAPER_TRADING_ENABLED", "true").strip().lower() != "false"
        if self.paper_engine and _paper_enabled:
            asyncio.create_task(
                self.paper_engine.open_trade(
                    mint=mint, name=name,
                    symbol=data.token_symbol or (name.split()[0][:6].upper() if name else mint[:6]),
                    channel=channel, score=result.risk_score,
                    ai_prob=ai_prob,
                )
            )

        # ── Live vs Paper ──────────────────────────────────────────────────────
        # Read live/paper state dynamically so /setmode takes effect immediately
        import os as _os
        _signal_live = _os.getenv("SIGNAL_TRADING", "off").strip().lower() == "on"
        if not _signal_live:
            logger.info(
                f"📋 PAPER | {name} | CA: {mint[:16]}... | "
                f"Score:{result.risk_score} — SIGNAL_TRADING=off, no real trade placed"
            )
            if self.telegram and hasattr(self.telegram, 'notify_paper_trade'):
                await self.telegram.notify_paper_trade(
                    mint=mint,
                    source=f"telegram_{channel}",
                    score=result.risk_score,
                    verdict=result.verdict,
                    trade_sol=0.0,
                    trade_usd=0.0,
                )
            return

        trade_result = await self.coordinator.on_buy_signal(filter_res)
        # If coordinator rejected the trade (low balance, max trades etc) — notify
        if trade_result and not trade_result.get("success", True):
            reason = trade_result.get("error", "Trade rejected by coordinator")
            logger.warning(f"Trade rejected after signal passed: {reason}")
            if self.telegram:
                await self.telegram.notify_trade_blocked(
                    name=name, ca=mint, reason=reason
                )

    async def _enrich_from_dexscreener(self, data: "SignalData") -> "SignalData":
        """Fetch MC, liq, age, vol from DexScreener for simple CA signals."""
        import aiohttp
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.dexscreener.com/latest/dex/tokens/{data.mint_address}",
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as resp:
                    if resp.status == 200:
                        j = await resp.json()
                        pairs = j.get("pairs") or []
                        if pairs:
                            p = pairs[0]
                            from datetime import datetime, timezone
                            created = p.get("pairCreatedAt", 0) or 0
                            age_sec = (datetime.now(timezone.utc).timestamp() - created / 1000)
                            data.mc_usd       = float(p.get("marketCap") or p.get("fdv") or 0)
                            data.liq_usd      = float((p.get("liquidity") or {}).get("usd") or 0)
                            data.vol_1h_usd   = float((p.get("volume") or {}).get("h1") or 0)
                            data.vol_5m_usd   = float((p.get("volume") or {}).get("m5") or 0)
                            data.age_hours    = round(age_sec / 3600, 2)
                            data.age_minutes  = round(age_sec / 60, 1)
                            name = (p.get("baseToken") or {}).get("name", "")
                            sym  = (p.get("baseToken") or {}).get("symbol", "")
                            if name: data.token_name   = name
                            if sym:  data.token_symbol  = sym
                            logger.info(
                                f"📊 DexScreener enrich | {data.token_name} | "
                                f"MC: ${data.mc_usd:,.0f} | Liq: ${data.liq_usd:,.0f} | "
                                f"Age: {data.age_hours:.1f}h"
                            )
        except Exception as e:
            logger.debug(f"DexScreener enrich error: {e}")
        return data

    async def _is_honeypot(self, mint: str) -> tuple[bool, str]:
        last_error      = ""
        network_failures = 0

        for attempt in range(3):
            for amount in _HONEYPOT_AMOUNTS:
                params = {
                    "inputMint":   mint,
                    "outputMint":  config.WSOL_MINT,
                    "amount":      amount,
                    "slippageBps": "5000",
                }
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(
                            "https://quote-api.jup.ag/v6/quote",
                            params=params,
                            timeout=aiohttp.ClientTimeout(total=8),
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                if data.get("routePlan"):
                                    return False, f"route_ok_amount={amount}"
                                last_error = f"no_route_for_amount_{amount}"
                            elif resp.status == 400:
                                last_error = f"http_400_amount_{amount}"
                            else:
                                last_error = f"http_{resp.status}"
                                network_failures += 1
                except Exception as e:
                    last_error = str(e)[:60]
                    network_failures += 1

            if attempt < 2:
                await asyncio.sleep(3)

        if network_failures >= 6:
            logger.warning(f"Honeypot check INCONCLUSIVE for {mint[:16]} — allowing through")
            return False, "inconclusive_network_errors"

        return True, last_error

    async def _dexscreener_honeypot_check(self, mint: str) -> tuple[bool, str]:
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=7)) as resp:
                    if resp.status != 200:
                        return False, "dex_unavailable"
                    data  = await resp.json()
                    pairs = data.get("pairs", [])
                    if not pairs:
                        return False, "no_dex_pair"

                    p        = pairs[0]
                    txns     = p.get("txns", {})
                    h1       = txns.get("h1", {})
                    buys_1h  = h1.get("buys",  0) or 0
                    sells_1h = h1.get("sells", 0) or 0
                    vol_1h   = p.get("volume", {}).get("h1", 0) or 0

                    if buys_1h >= 50 and sells_1h == 0 and vol_1h > 500:
                        return True, f"dex_zero_sells_{buys_1h}buys_${vol_1h:.0f}vol"
                    if sells_1h > 0 and buys_1h / sells_1h > 30 and vol_1h > 2_000:
                        return True, f"dex_ratio_{buys_1h}buys_{sells_1h}sells"
                    return False, f"dex_ok_{buys_1h}buys_{sells_1h}sells"
        except Exception as e:
            logger.debug(f"DexScreener honeypot check failed {mint[:16]}: {e}")
            return False, "dex_check_error"

    def stats(self) -> dict:
        return dict(self._stats)


async def _fetch_token_name(mint: str) -> str | None:
    if not config.HELIUS_API_KEY:
        return None

    url     = f"https://mainnet.helius-rpc.com/?api-key={config.HELIUS_API_KEY}"
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getAsset", "params": {"id": mint}}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload,
                              timeout=aiohttp.ClientTimeout(total=6)) as resp:
                data   = await resp.json()
                result = data.get("result", {})
                meta   = result.get("content", {}).get("metadata", {})
                name   = meta.get("name", "").strip()
                if name:
                    return name
    except Exception as e:
        logger.debug(f"Helius name fetch failed for {mint[:16]}: {e}")

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status == 200:
                    data  = await resp.json()
                    pairs = data.get("pairs", [])
                    if pairs:
                        base = pairs[0].get("baseToken", {})
                        name = base.get("name", "").strip()
                        if name:
                            return name
    except Exception as e:
        logger.debug(f"DexScreener name fetch failed for {mint[:16]}: {e}")

    return None
