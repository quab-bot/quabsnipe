# filters/pipeline.py — Staged filter pipeline with risk scoring + analytics
# Stage 1: cheap/fast → Stage 2: medium → Stage 3: expensive
# Key fix: liq_usd now uses the LIVE value set by queue_manager after 75s delay
# Key fix: tokens with no RugCheck data get a default risk penalty, not a free pass

import asyncio
import aiohttp
import time
from datetime import datetime, timezone

import config
from core.risk_scorer import RiskScore
from core.database import (
    get_cached_token, cache_token_result,
    get_cached_rugcheck, cache_rugcheck,
    get_cached_wallet, cache_wallet,
    get_cached_domain, cache_domain,
)
from core.analytics import log_token_analysis
from utils.logger import get_logger
from utils.models import NewTokenEvent, FilterResult
from utils import wallet_db

logger = get_logger("filter_pipeline")

_sol_price_cache = {"price": 0.0, "updated": 0.0}

# Max age of a token we're willing to trade (minutes)
# After 75s observation delay, we allow up to 20 minutes total age
MAX_TOKEN_AGE_MINUTES = 20


async def run_pipeline(event: NewTokenEvent) -> FilterResult:
    mint = event.mint_address

    # ── Cache check ────────────────────────────────────────────────────────────
    cached = get_cached_token(mint, max_age_minutes=30)
    if cached:
        logger.debug(f"Cache hit {mint[:16]}... → {cached['result']}")
        passed = cached["result"] in ("pass", "caution")
        return FilterResult(passed=passed, token=event, failed_at="cache",
            fail_reason=cached.get("fail_reason") or "cached_reject",
            filter_details={"cached": True, "risk_score": cached["risk_score"]})

    score   = RiskScore()
    sol_p   = await _sol_price()

    # ── Use live liquidity from queue_manager (set after 75s delay) ────────────
    # event.initial_liquidity_sol was updated with DexScreener value / sol_price
    # so multiplying back gives us the real USD value
    liq_usd = event.initial_liquidity_sol * sol_p
    mc_usd  = event.market_cap_usd

    logger.debug(f"Pipeline: {mint[:16]}... | Liq: ${liq_usd:,.0f} | MC: ${mc_usd:,.0f}")

    # ── Age check — only analyze tokens born recently ──────────────────────────
    if event.source == "raydium":
        age_ok, age_reason = await _check_token_age_dexscreener(mint)
        if not age_ok:
            logger.debug(f"[AGE] {mint[:16]}... | {age_reason}")
            # Log explicitly as reject — do NOT use score.verdict (it's 0/clean by default)
            log_token_analysis(
                mint=mint, source=event.source,
                liquidity_usd=liq_usd, market_cap_usd=mc_usd,
                risk_score=0, verdict="reject",
                rejection_stage="stage1", rejection_reason=age_reason,
                flags=[age_reason],
            )
            cache_token_result(mint, "reject", 0, age_reason, event.source)
            return FilterResult(passed=False, token=event, failed_at="stage1",
                fail_reason=age_reason,
                filter_details={"risk_score": 0, "liquidity_usd": liq_usd})

    # ── STAGE 1: Cheap fast checks ─────────────────────────────────────────────
    await _stage1(event, score, liq_usd)

    if score.auto_rejected or score.verdict == "reject":
        reason = _best_reason(score)
        logger.info(f"[S1 REJECT] {mint[:16]}... | {score.summary()}")
        _log(event, score, "stage1", reason, liq_usd, mc_usd)
        cache_token_result(mint, "reject", score.total, reason, event.source)
        return FilterResult(passed=False, token=event, failed_at="stage1",
            fail_reason=reason,
            filter_details={"risk_score": score.total, "flags": score.flags,
                            "liquidity_usd": liq_usd})

    # ── STAGE 2: Medium cost checks ────────────────────────────────────────────
    await _stage2(event, score, liq_usd, mc_usd)

    if score.auto_rejected or score.verdict == "reject":
        reason = _best_reason(score)
        logger.info(f"[S2 REJECT] {mint[:16]}... | {score.summary()}")
        _log(event, score, "stage2", reason, liq_usd, mc_usd)
        cache_token_result(mint, "reject", score.total, reason, event.source)
        return FilterResult(passed=False, token=event, failed_at="stage2",
            fail_reason=reason,
            filter_details={"risk_score": score.total, "flags": score.flags,
                            "liquidity_usd": liq_usd})

    # ── STAGE 3: Expensive checks ──────────────────────────────────────────────
    await _stage3(event, score)

    verdict = score.verdict
    reason  = _best_reason(score) if verdict == "reject" else ""

    if verdict == "reject":
        # Log with explicit "stage3" so analytics counts it correctly
        _log(event, score, "stage3", reason, liq_usd, mc_usd)
        cache_token_result(mint, "reject", score.total, reason, event.source)
        logger.info(f"[S3 REJECT] {mint[:16]}... | {score.summary()}")
        return FilterResult(passed=False, token=event, failed_at="stage3",
            fail_reason=reason,
            filter_details={"risk_score": score.total, "flags": score.flags,
                            "liquidity_usd": liq_usd})

    # Passed all stages
    label = "caution" if verdict == "caution" else "pass"
    _log(event, score, label, reason, liq_usd, mc_usd)
    cache_token_result(mint, label, score.total, reason, event.source)

    logger.info(
        f"[✅ {verdict.upper()}] {mint[:16]}... | {score.summary()} | "
        f"Liq:${liq_usd:,.0f} | Size:{score.position_size_multiplier*100:.0f}%"
    )
    return FilterResult(passed=True, token=event,
        filter_details={"risk_score": score.total, "verdict": verdict,
                        "size_multiplier": score.position_size_multiplier,
                        "flags": score.flags, "liquidity_usd": liq_usd,
                        "market_cap_usd": mc_usd})


# ── Age check ──────────────────────────────────────────────────────────────────

async def _check_token_age_dexscreener(mint: str) -> tuple[bool, str]:
    """
    Only proceed if the pair was created within MAX_TOKEN_AGE_MINUTES.
    Uses DexScreener pairCreatedAt but validates the timestamp is sane.
    Falls back to allowing through if data is unavailable or timestamp looks wrong.
    """
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status != 200:
                    return True, ""  # Can't verify → allow
                data  = await resp.json()
                pairs = data.get("pairs", [])
                if not pairs:
                    return True, ""  # Not indexed yet → allow (very new token)

                created_ms = pairs[0].get("pairCreatedAt", 0)
                if not created_ms:
                    return True, ""

                now_ms   = time.time() * 1000
                age_ms   = now_ms - created_ms
                age_min  = age_ms / 60000

                # Sanity check — if age is negative or > 1 year, timestamp is bad
                if age_min < 0 or age_min > 525_600:
                    logger.debug(f"Bad pairCreatedAt timestamp for {mint[:16]} "
                                 f"(age={age_min:.0f}min) — allowing through")
                    return True, ""

                if age_min > MAX_TOKEN_AGE_MINUTES:
                    return False, f"token_too_old({age_min:.0f}min_>{MAX_TOKEN_AGE_MINUTES}min)"
                return True, ""
    except Exception:
        return True, ""  # On error → allow through


# ── Stage 1: Cheap fast ────────────────────────────────────────────────────────

async def _stage1(event: NewTokenEvent, score: RiskScore, liq_usd: float):
    # Liquidity tiers
    if liq_usd < 500:
        score.add("liquidity_auto_reject"); return
    elif liq_usd < 2_000:
        score.add("liquidity_high_risk")
    elif liq_usd < 5_000:
        score.add("liquidity_medium_risk")

    # Name check
    name   = (event.raw_data.get("_parsed_name","") or "").upper()
    symbol = (event.raw_data.get("_parsed_symbol","") or "").upper()
    _check_name(name, symbol, score)
    if score.auto_rejected: return

    # RugCheck — IMPORTANT: if not indexed, add a penalty (not a free pass)
    await _check_rugcheck(event.mint_address, score)


def _check_name(name: str, symbol: str, score: RiskScore):
    AUTO = {"OFFICIAL","REAL","2.0","V2","V3","LEGIT","FAKE","SCAM"}
    RISK = {"SAFE","ELON","TRUMP","CEO","PRESIDENT","MOON","PEPE2","DOGE2"}
    combined = name + " " + symbol
    for kw in AUTO:
        if kw in combined: score.add("name_auto_reject"); return
    for kw in RISK:
        if kw in combined: score.add("name_risk_keyword")


async def _check_rugcheck(mint: str, score: RiskScore):
    # Try cache first
    cached = get_cached_rugcheck(mint, max_age_minutes=30)
    if cached:
        _apply_rugcheck(cached["score"], cached["risks"], score)
        return

    # Retry up to 3 times with exponential backoff (RugCheck rate-limits aggressively)
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{config.RUGCHECK_API_URL}/tokens/{mint}/report/summary",
                    timeout=aiohttp.ClientTimeout(total=6)
                ) as resp:
                    if resp.status == 404:
                        # Not indexed yet — new token, add moderate risk penalty
                        score.add("no_rugcheck_data", override_points=20)
                        return
                    if resp.status == 429:
                        # Rate limited — wait and retry
                        wait = 2 ** attempt
                        logger.debug(f"RugCheck rate-limited, waiting {wait}s ({mint[:16]})")
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        score.add("no_rugcheck_data", override_points=15)
                        return
                    data  = await resp.json()
                    rc    = data.get("score", 0)
                    risks = data.get("risks", [])
                    cache_rugcheck(mint, rc, risks)
                    _apply_rugcheck(rc, risks, score)
                    return   # Success
        except asyncio.TimeoutError:
            logger.debug(f"RugCheck timeout attempt {attempt+1} for {mint[:16]}")
            if attempt < 2:
                await asyncio.sleep(1 + attempt)
        except Exception as e:
            logger.debug(f"RugCheck error {mint[:16]}: {e}")
            break

    # All retries exhausted — penalise lightly (network issues ≠ bad token)
    score.add("no_rugcheck_data", override_points=10)


def _apply_rugcheck(rc: int, risks: list, score: RiskScore):
    if rc > 100:  score.add("rugcheck_score_very_high")
    elif rc > 30: score.add("rugcheck_score_high")
    s = " ".join(r.get("name","").lower() for r in risks)
    if "mint" in s and "enabled" in s:  score.add("mint_authority_enabled")
    if "freeze" in s:                   score.add("freeze_authority_enabled")
    if "lp" in s and "unlocked" in s:   score.add("lp_not_locked")
    if "bundle" in s:                   score.add("bundled_launch")
    if "sniper" in s:                   score.add("sniper_wallets")
    if "insider" in s:                  score.add("insider_wallets")
    if "concentration" in s:            score.add("top10_holders_high")


# ── Stage 2: Medium cost ───────────────────────────────────────────────────────

async def _stage2(event: NewTokenEvent, score: RiskScore, liq_usd: float, mc_usd: float):
    deployer = _deployer(event)
    if deployer:
        await _check_dev_wallet(deployer, score)
        if score.auto_rejected: return

    # Liquidity / MC ratio check
    if mc_usd > 0 and liq_usd > 0:
        ratio = liq_usd / mc_usd
        if ratio < config.MIN_LIQUIDITY_TO_MCAP_PCT:
            score.add("liq_to_mc_ratio_low")


async def _check_dev_wallet(address: str, score: RiskScore):
    if wallet_db.is_known_bad(address):
        score.add("dev_funded_by_rugger"); return

    cached = get_cached_wallet(address, max_age_hours=24)
    if cached:
        _apply_wallet(cached, score)
        score.deployer_age_days = cached.get("age_days", 0)
        score.deployer_tx_count = cached.get("tx_count", 0)
        return

    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(config.HELIUS_RPC_URL,
                json={"jsonrpc":"2.0","id":1,
                      "method":"getSignaturesForAddress",
                      "params":[address,{"limit":100,"commitment":"confirmed"}]},
                timeout=aiohttp.ClientTimeout(total=6)) as resp:
                sigs = (await resp.json()).get("result", [])

        tx_count = len(sigs)
        age_days = 0.0
        if sigs:
            ts = sigs[-1].get("blockTime", 0)
            if ts:
                age_days = (datetime.now(timezone.utc).timestamp() - ts) / 86400

        cache_wallet(address, age_days, tx_count)
        score.deployer_age_days = age_days
        score.deployer_tx_count = tx_count
        _apply_wallet({"age_days": age_days, "tx_count": tx_count, "is_bad": 0}, score)

    except Exception as e:
        logger.debug(f"Dev wallet error {address[:16]}: {e}")


def _apply_wallet(w: dict, score: RiskScore):
    if w.get("is_bad"):
        score.add("dev_funded_by_rugger"); return
    if w.get("age_days", 999) < config.DEV_WALLET_MIN_AGE_DAYS:
        score.add("dev_wallet_too_young")
    if w.get("tx_count", 999) < config.DEV_WALLET_MIN_TX_COUNT:
        score.add("dev_wallet_low_tx")


# ── Stage 3: Expensive ────────────────────────────────────────────────────────

async def _stage3(event: NewTokenEvent, score: RiskScore):
    await _check_honeypot(event.mint_address, score)
    if score.auto_rejected: return
    await _check_metadata(event, score)


async def _check_honeypot(mint: str, score: RiskScore):
    params = {"inputMint": mint, "outputMint": config.WSOL_MINT,
              "amount": "1000000", "slippageBps": "5000"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://quote-api.jup.ag/v6/quote", params=params,
                             timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status != 200:
                    score.add("honeypot_simulation_fail"); return
                data = await resp.json()
                if not data.get("routePlan"):
                    score.add("honeypot_simulation_fail")
    except Exception:
        score.add("honeypot_simulation_fail")


async def _check_metadata(event: NewTokenEvent, score: RiskScore):
    import re
    mint = event.mint_address
    uri  = await _token_uri(mint)
    if not uri: score.add("no_metadata"); return
    meta = await _fetch_meta(uri)
    if not meta: score.add("no_metadata"); return

    website = meta.get("website") or meta.get("external_url") or ""
    twitter = meta.get("twitter") or (meta.get("extensions") or {}).get("twitter") or ""

    if website:
        domain = re.sub(r"https?://(www\.)?","",website).split("/")[0]
        cached = get_cached_domain(domain)
        if cached:
            if not cached["resolves"]:         score.add("website_not_resolving")
            if (cached.get("age_hours") or 9999) < 48: score.add("domain_too_new")
        else:
            ok, age_h = await _resolve_website(website, domain)
            cache_domain(domain, ok, age_h)
            if not ok: score.add("website_not_resolving")
            if age_h is not None and age_h < 48: score.add("domain_too_new")

    if twitter:
        if not twitter.startswith("http"):
            twitter = f"https://twitter.com/{twitter.lstrip('@')}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(twitter, timeout=aiohttp.ClientTimeout(total=5),
                                 allow_redirects=True) as resp:
                    if resp.status == 404: score.add("twitter_not_resolving")
        except Exception:
            score.add("twitter_not_resolving")


async def _resolve_website(url: str, domain: str) -> tuple[bool, float | None]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=5),
                             allow_redirects=True) as r:
                ok = r.status < 400
        age_h = await asyncio.get_event_loop().run_in_executor(None, _whois_age, domain)
        return ok, age_h
    except Exception:
        return False, None


def _whois_age(domain: str) -> float | None:
    try:
        import whois
        w = whois.whois(domain)
        c = w.creation_date
        if isinstance(c, list): c = c[0]
        if c:
            if c.tzinfo is None: c = c.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - c).total_seconds() / 3600
    except Exception: pass
    return None


async def _token_uri(mint: str) -> str:
    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={config.HELIUS_API_KEY}"
        async with aiohttp.ClientSession() as s:
            async with s.post(url,
                json={"jsonrpc":"2.0","id":1,"method":"getAsset","params":{"id":mint}},
                timeout=aiohttp.ClientTimeout(total=5)) as resp:
                return (await resp.json()).get("result",{}).get("content",{}).get("json_uri","")
    except Exception: return ""


async def _fetch_meta(uri: str) -> dict | None:
    if not uri or not uri.startswith("http"): return None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(uri, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
    except Exception: return None


def _deployer(event: NewTokenEvent) -> str | None:
    try:
        keys = event.raw_data.get("transaction",{}).get("message",{}).get("accountKeys",[])
        if not keys: return None
        f = keys[0]
        return f.get("pubkey") if isinstance(f, dict) else str(f)
    except Exception: return None


def _best_reason(score: RiskScore) -> str:
    if score.auto_reject_reason:
        return score.auto_reject_reason
    if not score.flags:
        return f"high_risk_score_{score.total}"
    def pts(f):
        try: return int(f.split("+")[1].rstrip(")"))
        except: return 0
    return sorted(score.flags, key=pts, reverse=True)[0].split("(")[0]


def _log(event: NewTokenEvent, score: RiskScore,
         stage: str, reason: str, liq_usd: float, mc_usd: float):
    """Write token analysis record to analytics DB."""
    # Determine true verdict — never trust score.verdict for explicit reject stages
    if stage in ("stage1", "stage2", "stage3"):
        verdict = "reject"
    elif stage in ("pass", "caution"):
        verdict = stage
    else:
        verdict = score.verdict

    try:
        log_token_analysis(
            mint=event.mint_address, source=event.source,
            liquidity_usd=liq_usd, market_cap_usd=mc_usd,
            risk_score=score.total, verdict=verdict,
            rejection_stage=stage if verdict == "reject" else stage,
            rejection_reason=reason,
            flags=score.flags,
            deployer_age_days=score.deployer_age_days,
            deployer_tx_count=score.deployer_tx_count,
            holder_concentration_pct=score.holder_concentration_pct,
            wallet_cluster_signals=score.wallet_cluster_signals,
            lp_locked=score.lp_locked,
            mint_authority_revoked=score.mint_authority_revoked,
            freeze_authority_revoked=score.freeze_authority_revoked,
        )
    except Exception as e:
        logger.debug(f"Analytics log error: {e}")


async def _sol_price() -> float:
    if time.time() - _sol_price_cache["updated"] < 60 and _sol_price_cache["price"] > 0:
        return _sol_price_cache["price"]
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://price.jup.ag/v6/price?ids=SOL",
                             timeout=aiohttp.ClientTimeout(total=4)) as resp:
                p = float((await resp.json())["data"]["SOL"]["price"])
                _sol_price_cache.update({"price": p, "updated": time.time()})
                return p
    except Exception:
        return _sol_price_cache["price"] or 150.0
