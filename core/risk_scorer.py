# core/risk_scorer.py — Risk scoring engine (tuned from live data)
# 0–25  = CLEAN  (full size)
# 26–55 = CAUTION (50% size)
# 56+   = REJECT

from dataclasses import dataclass, field
from utils.logger import get_logger

logger = get_logger("risk_scorer")

SCORE_CLEAN       = 25
SCORE_CAUTION     = 55
SCORE_AUTO_REJECT = 100
CAUTION_SIZE_PCT  = 0.50

SCORES = {
    # ── Liquidity ──
    "liquidity_auto_reject":        100,   # < $500
    "liquidity_high_risk":           20,   # $500–$2k (was 35)
    "liquidity_medium_risk":          8,   # $2k–$5k (was 15)
    # $5k+ = no penalty

    # ── RugCheck ──
    "no_rugcheck_data":              15,   # Not indexed yet
    "rugcheck_score_high":           20,   # Score 30–100 (was 30)
    "rugcheck_score_very_high":      50,   # Score > 100 (was 55)

    # ── Hard rug signals ──
    "mint_authority_enabled":        45,
    "freeze_authority_enabled":      45,
    "lp_not_locked":                 40,
    "bundled_launch":                35,
    "sniper_wallets":                20,
    "insider_wallets":               25,
    "top10_holders_high":            15,   # > 30% (was 25)
    "top10_holders_very_high":       40,   # > 50%

    # ── Name signals ──
    "name_auto_reject":             100,
    "name_risk_keyword":              8,   # was 10

    # ── Dev wallet ──
    # TUNED: young/low-tx wallet alone = CAUTION not REJECT
    "dev_wallet_too_young":          15,   # < 7 days (was 20)
    "dev_wallet_low_tx":             12,   # < 10 txs (was 15)
    "dev_funded_by_rugger":          55,
    "dev_round_funding":              8,
    "dev_previous_rugs":             65,

    # ── Market structure ──
    "wash_trading_detected":         30,
    "liq_to_mc_ratio_low":           12,

    # ── Stage 3 ──
    "honeypot_simulation_fail":     100,
    "known_bad_early_buyer":         50,
    "website_not_resolving":         12,
    "domain_too_new":                20,
    "twitter_not_resolving":          8,
    "no_metadata":                   18,
    "social_created_after_launch":   12,
}


@dataclass
class RiskScore:
    total: int = 0
    flags: list = field(default_factory=list)
    auto_rejected: bool = False
    auto_reject_reason: str = ""

    # Analytics
    deployer_age_days:          float = 0
    deployer_tx_count:          int   = 0
    holder_concentration_pct:   float = 0
    wallet_cluster_signals:     int   = 0
    lp_locked:                  bool  = False
    mint_authority_revoked:     bool  = True
    freeze_authority_revoked:   bool  = True

    def add(self, flag: str, override_points: int | None = None):
        points = override_points if override_points is not None else SCORES.get(flag, 0)
        self.total += points
        self.flags.append(f"{flag}(+{points})")

        if flag == "lp_not_locked":            self.lp_locked = False
        if flag == "mint_authority_enabled":   self.mint_authority_revoked = False
        if flag == "freeze_authority_enabled": self.freeze_authority_revoked = False

        if points >= SCORE_AUTO_REJECT or self.total >= SCORE_AUTO_REJECT:
            self.auto_rejected      = True
            self.auto_reject_reason = flag

    @property
    def verdict(self) -> str:
        if self.auto_rejected or self.total > SCORE_CAUTION:
            return "reject"
        if self.total > SCORE_CLEAN:
            return "caution"
        return "clean"

    @property
    def position_size_multiplier(self) -> float:
        if self.verdict == "reject":  return 0.0
        if self.verdict == "caution": return CAUTION_SIZE_PCT
        return 1.0

    def summary(self) -> str:
        return (
            f"Score: {self.total} | Verdict: {self.verdict.upper()} | "
            f"Flags: {', '.join(self.flags) if self.flags else 'none'}"
        )
