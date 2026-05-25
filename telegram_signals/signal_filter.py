# telegram_signals/signal_filter.py
# Filter pipeline for @solearlytrending signals
# Philosophy: channel already pre-screens — our job is to find the BEST movers
# Target: 80% win rate by filtering out the worst within their already-filtered set

from dataclasses import dataclass
from utils.logger import get_logger
from telegram_signals.signal_parser import SignalData

logger = get_logger("signal_filter")


@dataclass
class SignalFilterResult:
    passed:          bool
    verdict:         str
    risk_score:      int
    fail_reason:     str
    size_multiplier: float
    flags:           list
    bonus_flags:     list


# ── Hard reject — instant fail ─────────────────────────────────────────────────
HARD = {
    "min_liq":        5_000,   # Must have real liquidity
    "max_age_h":         1.0,  # Max 60 min old (raised from 30 min — channel posts can lag)
    "min_mc":         3_000,   # Minimum MC
    "max_mc":     2_000_000,   # Already mooned
    "max_fake_vol":    25.0,   # > 25% fake volume
    "max_snipers":      8.0,   # Snipers still holding > 8%
    "max_dev_pct":      5.0,   # Dev holding > 5%
    "max_fake_hold":   40.0,   # > 40% fake holders
}

# ── Risk scores ────────────────────────────────────────────────────────────────
S = {
    "bundles_high":          50,   # > 25% bundles now
    "bundles_medium":        25,   # 15–25%
    "bundles_low":           12,   # 10–15%
    "first20_very_high":     45,   # Top bucket > 55%
    "first20_high":          22,   # 40–55%
    "fake_holders_high":     35,   # 25–40%
    "age_old":               18,   # 45–60 min
    "age_medium":             8,   # 20–45 min
    "mc_high":               35,   # > $500K
    "dev_sold_heavy":        18,   # > 60% sold
    "low_holders":           20,   # < 100
    "low_5m_vol":            15,   # Low momentum
}

# ── Bonuses (reduce score) ────────────────────────────────────────────────────
B = {
    "snipers_zero":         -18,   # 0% snipers remaining
    "zero_fake_vol":        -12,   # $0 fake volume
    "dev_zero":             -12,   # 0% dev holding
    "security_ok":           -5,
    "new_trending":         -10,   # Fresh trending tag
    "strong_5m":            -12,   # Active now
    "healthy_liq":           -8,   # Liq > $15K
    "low_bundles_now":       -8,   # Bundles < 5% now
}

CLEAN   = 20   # Full size
CAUTION = 45   # 50% size
# > 45 = reject


def run_signal_filter(data: SignalData) -> SignalFilterResult:
    score   = 0
    flags   = []
    bonuses = []

    def add(k, pts=None):
        nonlocal score
        p = pts if pts is not None else S.get(k, 0)
        score += p
        flags.append(f"{k}(+{p})")

    def bon(k):
        nonlocal score
        p = B.get(k, 0)
        score += p
        bonuses.append(f"{k}({p})")

    # ── Must have CA ───────────────────────────────────────────────────────────
    if not data.mint_address:
        return _r(False, 100, flags, bonuses, "no_ca_found")

    # ── Liquidity ──────────────────────────────────────────────────────────────
    if data.liq_usd < HARD["min_liq"]:
        return _r(False, 100, flags, bonuses,
                  f"liq_too_low_${data.liq_usd:,.0f}")

    if data.liq_usd >= 15_000:
        bon("healthy_liq")

    # ── MC ─────────────────────────────────────────────────────────────────────
    if data.mc_usd > 0:
        if data.mc_usd < HARD["min_mc"]:
            return _r(False, 100, flags, bonuses, "mc_too_low")
        if data.mc_usd > 500_000:
            add("mc_high")

    # ── Age (in hours) ─────────────────────────────────────────────────────────
    # Channel posts can arrive 20–45 min after token launch — allow up to 60 min
    if data.age_hours > HARD["max_age_h"]:
        return _r(False, 100, flags, bonuses,
                  f"token_too_old_{data.age_hours*60:.0f}min")
    elif data.age_hours > 0.75:    # > 45 min
        add("age_old")
    elif data.age_hours > 0.33:    # > 20 min
        add("age_medium")

    if data.is_new_trending:
        bon("new_trending")

    # ── Fake volume ────────────────────────────────────────────────────────────
    if data.fake_vol_pct > HARD["max_fake_vol"]:
        return _r(False, 100, flags, bonuses,
                  f"fake_vol_{data.fake_vol_pct:.1f}%")
    elif data.fake_vol_pct == 0:
        bon("zero_fake_vol")

    # ── Volume momentum ────────────────────────────────────────────────────────
    if data.vol_5m_usd > 500:
        bon("strong_5m")
    elif data.vol_1h_usd > 10_000 and data.vol_5m_usd < 100:
        add("low_5m_vol")

    # ── Holders ────────────────────────────────────────────────────────────────
    if 0 < data.holders < 100:
        add("low_holders")

    # ── Bundles ────────────────────────────────────────────────────────────────
    if data.bundles_pct_now > 25:
        add("bundles_high")
    elif data.bundles_pct_now > 15:
        add("bundles_medium")
    elif data.bundles_pct_now > 10:
        add("bundles_low")
    elif 0 < data.bundles_pct_now <= 5:
        bon("low_bundles_now")

    # ── Snipers ────────────────────────────────────────────────────────────────
    if data.snipers_pct_now > HARD["max_snipers"]:
        return _r(False, 100, flags, bonuses,
                  f"snipers_holding_{data.snipers_pct_now:.1f}%")
    elif data.snipers_pct_now == 0:
        bon("snipers_zero")

    # ── First 20 ───────────────────────────────────────────────────────────────
    if data.first20_pct_1 > 55:
        add("first20_very_high")
    elif data.first20_pct_1 > 40:
        add("first20_high")

    # ── Dev ────────────────────────────────────────────────────────────────────
    if data.dev_pct > HARD["max_dev_pct"]:
        return _r(False, 100, flags, bonuses, f"dev_holding_{data.dev_pct:.1f}%")
    elif data.dev_pct == 0:
        bon("dev_zero")
    if data.dev_sold_pct > 60:
        add("dev_sold_heavy")

    # ── Fake holders ───────────────────────────────────────────────────────────
    if data.fake_holders_pct > HARD["max_fake_hold"]:
        return _r(False, 100, flags, bonuses,
                  f"fake_holders_{data.fake_holders_pct:.1f}%")
    elif data.fake_holders_pct > 25:
        add("fake_holders_high")

    # ── Security ───────────────────────────────────────────────────────────────
    if data.security_ok:
        bon("security_ok")

    score = max(0, score)

    if score >= 100 or score > CAUTION:
        reason = flags[-1].split("(")[0] if flags else f"score_{score}"
        return _r(False, score, flags, bonuses, reason)

    verdict = "caution" if score > CLEAN else "clean"
    mul     = 0.5 if verdict == "caution" else 1.0

    logger.info(
        f"Signal {verdict.upper()} | Score:{score} | "
        f"Size:{mul*100:.0f}% | "
        f"Flags:[{', '.join(f.split('(')[0] for f in flags)}] "
        f"Bonuses:[{', '.join(b.split('(')[0] for b in bonuses)}]"
    )
    return SignalFilterResult(
        passed=True, verdict=verdict, risk_score=score,
        fail_reason="", size_multiplier=mul,
        flags=flags, bonus_flags=bonuses
    )


def _r(passed, score, flags, bonuses, reason):
    verdict = "reject" if not passed else ("caution" if score > CLEAN else "clean")
    mul     = 0.0 if not passed else (0.5 if verdict == "caution" else 1.0)
    if not passed:
        logger.info(f"Signal REJECTED | Score:{score} | Reason:{reason}")
    return SignalFilterResult(
        passed=passed, verdict=verdict, risk_score=score,
        fail_reason=reason, size_multiplier=mul,
        flags=flags, bonus_flags=bonuses
    )
