# filters/rugcheck.py — Filter 1: RugCheck.xyz API
# Rejects tokens with a rug score above 30 or failed safety checks

import aiohttp
import config
from utils.logger import get_logger
from utils.models import NewTokenEvent

logger = get_logger("filter_rugcheck")


async def run_rugcheck(event: NewTokenEvent) -> tuple[bool, str, dict]:
    """
    Returns (passed: bool, reason: str, extra_data: dict)
    """
    mint = event.mint_address
    url  = f"{config.RUGCHECK_API_URL}/tokens/{mint}/report/summary"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 404:
                    # Token too new — RugCheck hasn't indexed it yet, skip for now
                    # We'll be conservative and reject until it's indexed
                    return False, "RugCheck: token not yet indexed (too new)", {}

                if resp.status != 200:
                    return False, f"RugCheck API error: HTTP {resp.status}", {}

                data = await resp.json()

    except aiohttp.ClientError as e:
        return False, f"RugCheck network error: {e}", {}

    score = data.get("score", 999)  # Default high if missing
    risks = data.get("risks", [])

    extra = {
        "rugcheck_score": score,
        "risks": risks,
    }

    # Hard score threshold
    if score > config.RUGCHECK_MAX_SCORE:
        return False, f"RugCheck score too high: {score} (max {config.RUGCHECK_MAX_SCORE})", extra

    # Specific risk checks
    risk_ids = [r.get("name", "").lower() for r in risks]

    # Mint authority not revoked
    if any("mint" in r and "enabled" in r for r in risk_ids):
        return False, "RugCheck: mint authority still enabled", extra

    # Freeze authority not revoked
    if any("freeze" in r for r in risk_ids):
        return False, "RugCheck: freeze authority still enabled", extra

    # LP not locked
    if any("lp" in r and "unlocked" in r for r in risk_ids):
        return False, "RugCheck: LP unlocked", extra

    # Top holder concentration
    if any("concentration" in r or "top holder" in r for r in risk_ids):
        return False, "RugCheck: suspicious top holder concentration", extra

    # Insider wallets
    if any("insider" in r for r in risk_ids):
        return False, "RugCheck: insider wallets detected", extra

    # Bundled launch
    if any("bundle" in r for r in risk_ids):
        return False, "RugCheck: bundled launch detected", extra

    # Sniper wallets
    if any("sniper" in r for r in risk_ids):
        return False, "RugCheck: sniper wallets detected", extra

    return True, "RugCheck passed", extra
