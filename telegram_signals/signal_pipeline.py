# telegram_signals/signal_pipeline.py
# Filter pipeline specifically for Telegram signal tokens.
# Uses data already in the signal (no extra API calls needed for most checks).
# Then runs honeypot + RugCheck as final validation.

import aiohttp
from dataclasses import dataclass
from utils.logger import get_logger
from telegram_signals.signal_parser import SignalData
import config

logger = get_logger("signal_pipeline")


# ── Thresholds (tune based on results) ────────────────────────────────────────
FILTERS = {
    "max_age_minutes":          30,      # Token must be < 30 min old
    "min_mc_usd":            5_000,      # Min market cap $5k
    "max_mc_usd":        1_000_000,      # Max market cap $1M (avoid large caps)
    "min_liq_usd":           5_000,      # Min liquidity $5k
    "max_fake_vol_pct":          5.0,    # Max 5% fake volume
    "max_fake_holders_pct":     25.0,    # Max 25% fake holders
    "min_holders":              50,      # Min 50 real holders
    "max_bundle_pct_current":   15.0,    # Max 15% bundle holdings remaining
    "max_sniper_pct_current":   10.0,    # Max 10% sniper holdings remaining
    "max_first20_top_pct":      40.0,    # Top first-20 bucket < 40%
    "max_dev_holdings_pct":      5.0,    # Dev holds < 5%
    "max_dev_sold_pct":         50.0,    # Dev sold < 50% (some selling OK)
    "require_security_ok":      True,    # Must have ✅ security
}


@dataclass
class SignalFilterResult:
    passed: bool
    ca: str
    signal: SignalData
    fail_reason: str = ""
    fail_field: str = ""
    risk_score: int = 0
    verdict: str = ""        # "clean" | "caution" | "reject"
    size_multiplier: float = 1.0


async def run_signal_pipeline(signal: SignalData) -> SignalFilterResult:
    """
    Run all signal filters in order.
    Returns SignalFilterResult with passed=True only if all checks pass.
    """
    ca = signal.ca

    if not ca:
        return SignalFilterResult(passed=False, ca="", signal=signal,
            fail_reason="No CA extracted from bot response",
            fail_field="ca")

    logger.info(
        f"Signal pipeline: {ca[:20]}... | "
        f"MC:${signal.mc_usd:,.0f} | Liq:${signal.liq_usd:,.0f} | "
        f"Age:{signal.age_minutes}m | Holders:{signal.holders}"
    )

    risk_score = 0

    # ── 1. Security check ──────────────────────────────────────────────────────
    if FILTERS["require_security_ok"] and not signal.security_ok:
        return _reject(signal, "security_check_failed", "Security ✅ not present", risk_score)

    # ── 2. Age ─────────────────────────────────────────────────────────────────
    if signal.age_minutes > 0 and signal.age_minutes > FILTERS["max_age_minutes"]:
        return _reject(signal, "too_old",
            f"Age {signal.age_minutes}m > {FILTERS['max_age_minutes']}m", risk_score)

    # ── 3. Market cap ──────────────────────────────────────────────────────────
    if signal.mc_usd > 0:
        if signal.mc_usd < FILTERS["min_mc_usd"]:
            return _reject(signal, "mc_too_low",
                f"MC ${signal.mc_usd:,.0f} < ${FILTERS['min_mc_usd']:,}", risk_score)
        if signal.mc_usd > FILTERS["max_mc_usd"]:
            return _reject(signal, "mc_too_high",
                f"MC ${signal.mc_usd:,.0f} > ${FILTERS['max_mc_usd']:,}", risk_score)

    # ── 4. Liquidity ───────────────────────────────────────────────────────────
    if signal.liq_usd > 0 and signal.liq_usd < FILTERS["min_liq_usd"]:
        return _reject(signal, "liq_too_low",
            f"Liq ${signal.liq_usd:,.0f} < ${FILTERS['min_liq_usd']:,}", risk_score)

    # ── 5. Fake volume ─────────────────────────────────────────────────────────
    if signal.fake_vol_pct > FILTERS["max_fake_vol_pct"]:
        return _reject(signal, "fake_vol_too_high",
            f"Fake vol {signal.fake_vol_pct:.1f}% > {FILTERS['max_fake_vol_pct']}%", risk_score)

    # ── 6. Fake holders ────────────────────────────────────────────────────────
    if signal.fake_holders_pct > FILTERS["max_fake_holders_pct"]:
        return _reject(signal, "fake_holders_too_high",
            f"Fake holders {signal.fake_holders_pct:.1f}% > {FILTERS['max_fake_holders_pct']}%",
            risk_score)

    # ── 7. Holder count ────────────────────────────────────────────────────────
    if signal.holders > 0 and signal.holders < FILTERS["min_holders"]:
        return _reject(signal, "holders_too_low",
            f"Holders {signal.holders} < {FILTERS['min_holders']}", risk_score)

    # ── 8. Bundle concentration ────────────────────────────────────────────────
    if signal.bundle_pct_current > FILTERS["max_bundle_pct_current"]:
        return _reject(signal, "bundle_concentration_high",
            f"Bundle current {signal.bundle_pct_current:.1f}% > {FILTERS['max_bundle_pct_current']}%",
            risk_score)

    # ── 9. Sniper concentration ────────────────────────────────────────────────
    if signal.sniper_pct_current > FILTERS["max_sniper_pct_current"]:
        return _reject(signal, "sniper_concentration_high",
            f"Sniper current {signal.sniper_pct_current:.1f}% > {FILTERS['max_sniper_pct_current']}%",
            risk_score)

    # ── 10. First 20 concentration ─────────────────────────────────────────────
    if signal.first20_pct_1 > FILTERS["max_first20_top_pct"]:
        return _reject(signal, "first20_concentration_high",
            f"First20 top bucket {signal.first20_pct_1:.1f}% > {FILTERS['max_first20_top_pct']}%",
            risk_score)

    # ── 11. Dev holdings ───────────────────────────────────────────────────────
    if signal.dev_holdings_pct > FILTERS["max_dev_holdings_pct"]:
        return _reject(signal, "dev_holds_too_much",
            f"Dev holds {signal.dev_holdings_pct:.1f}% > {FILTERS['max_dev_holdings_pct']}%",
            risk_score)

    # ── 12. Dev sold too much ──────────────────────────────────────────────────
    if signal.dev_sold_pct > FILTERS["max_dev_sold_pct"]:
        return _reject(signal, "dev_sold_too_much",
            f"Dev sold {signal.dev_sold_pct:.1f}% > {FILTERS['max_dev_sold_pct']}%", risk_score)

    # ── 13. Soft risk scoring (caution zone) ───────────────────────────────────
    # These don't reject but increase the risk score
    if signal.age_minutes > 15:             risk_score += 10
    if signal.mc_usd < 15_000:             risk_score += 10
    if signal.liq_usd < 8_000:             risk_score += 15
    if signal.fake_vol_pct > 2.0:          risk_score += 10
    if signal.bundle_pct_current > 5.0:    risk_score += 10
    if signal.sniper_pct_current > 3.0:    risk_score += 10
    if signal.dev_sold_pct > 20.0:         risk_score += 15
    if signal.holders < 150:               risk_score += 10
    if not signal.security_ok:             risk_score += 20

    # ── 14. Honeypot check ─────────────────────────────────────────────────────
    is_honeypot = await _check_honeypot(ca)
    if is_honeypot:
        return _reject(signal, "honeypot", "Sell simulation failed — honeypot", risk_score)

    # ── 15. RugCheck ──────────────────────────────────────────────────────────
    rc_score = await _check_rugcheck(ca)
    if rc_score is not None:
        if rc_score > 200:
            return _reject(signal, f"rugcheck_score_{rc_score}",
                f"RugCheck score {rc_score} too high", risk_score)
        elif rc_score > 50:
            risk_score += 20

    # ── Verdict ────────────────────────────────────────────────────────────────
    if risk_score > 60:
        verdict = "caution"
        size_mul = 0.50
    elif risk_score > 30:
        verdict = "caution"
        size_mul = 0.75
    else:
        verdict = "clean"
        size_mul = 1.0

    logger.info(
        f"✅ SIGNAL PASSED | {ca[:20]}... | "
        f"Score:{risk_score} | Verdict:{verdict.upper()} | Size:{size_mul*100:.0f}%"
    )
    return SignalFilterResult(
        passed=True, ca=ca, signal=signal,
        risk_score=risk_score, verdict=verdict,
        size_multiplier=size_mul,
    )


def _reject(signal: SignalData, field: str, reason: str, score: int) -> SignalFilterResult:
    logger.info(f"❌ SIGNAL REJECTED | {signal.ca[:20] if signal.ca else '?'}... | {reason}")
    return SignalFilterResult(
        passed=False, ca=signal.ca, signal=signal,
        fail_reason=reason, fail_field=field, risk_score=score,
    )


async def _check_honeypot(mint: str) -> bool:
    """Return True if token appears to be a honeypot."""
    params = {
        "inputMint": mint, "outputMint": config.WSOL_MINT,
        "amount": "1000000", "slippageBps": "5000",
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://quote-api.jup.ag/v6/quote",
                             params=params,
                             timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status != 200: return True
                data = await resp.json()
                return not bool(data.get("routePlan"))
    except Exception:
        return True  # Fail safe — treat as honeypot on error


async def _check_rugcheck(mint: str) -> int | None:
    """Return RugCheck risk score or None if not indexed."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{config.RUGCHECK_API_URL}/tokens/{mint}/report/summary",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200: return None
                data = await resp.json()
                return data.get("score", 0)
    except Exception:
        return None
