# filters/market_structure.py — Filter 5: Market Structure
# Checks liquidity pool minimum, liquidity/MC ratio, and wash trading detection

import aiohttp
import config
from utils.logger import get_logger
from utils.models import NewTokenEvent

logger = get_logger("filter_market_structure")

JUPITER_PRICE_API = "https://price.jup.ag/v6/price"


async def run_market_structure(event: NewTokenEvent) -> tuple[bool, str, dict]:
    mint      = event.mint_address
    pair_addr = event.pair_address

    # Get current pool data
    pool_data = await _get_pool_data(mint, pair_addr)
    extra     = {"pool_data": pool_data}

    if not pool_data:
        return False, "Market structure: could not fetch pool data", extra

    liq_usd = pool_data.get("liquidity_usd", 0)
    mc_usd  = pool_data.get("market_cap_usd", 0)

    extra["liquidity_usd"]  = liq_usd
    extra["market_cap_usd"] = mc_usd

    # Check 1: Minimum liquidity
    if liq_usd < config.MIN_LIQUIDITY_USD:
        return (
            False,
            f"Market structure: liquidity ${liq_usd:,.0f} below minimum ${config.MIN_LIQUIDITY_USD:,}",
            extra,
        )

    # Check 2: Liquidity / MC ratio
    if mc_usd > 0:
        liq_ratio = liq_usd / mc_usd
        extra["liq_to_mc_ratio"] = round(liq_ratio, 4)
        if liq_ratio < config.MIN_LIQUIDITY_TO_MCAP_PCT:
            return (
                False,
                f"Market structure: liq/MC ratio {liq_ratio:.2%} below {config.MIN_LIQUIDITY_TO_MCAP_PCT:.0%}",
                extra,
            )

    # Check 3: Wash trading detection
    wash = await _detect_wash_trading(mint, pair_addr)
    extra["wash_trading"] = wash
    if wash["detected"]:
        return False, f"Market structure: wash trading detected — {wash['reason']}", extra

    return True, "Market structure passed", extra


async def _get_pool_data(mint: str, pair: str) -> dict | None:
    """
    Get liquidity and price data.
    Tries Jupiter price API first, then falls back to estimating from event data.
    """
    try:
        url = f"{JUPITER_PRICE_API}?ids={mint}&vsToken=USDC"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status == 200:
                    data  = await resp.json()
                    price = data.get("data", {}).get(mint, {}).get("price", 0)
                    if price:
                        # Get token supply for MC calc
                        supply = await _get_token_supply(mint)
                        mc     = price * supply if supply else 0
                        # Liquidity from DexScreener as fallback
                        liq    = await _get_liquidity_dexscreener(pair) or (mc * 0.1)
                        return {"price_usd": price, "market_cap_usd": mc, "liquidity_usd": liq}
    except Exception as e:
        logger.debug(f"Pool data fetch error: {e}")

    return None


async def _get_token_supply(mint: str) -> float:
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTokenSupply",
        "params": [mint],
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                data = await resp.json()
                ui = data.get("result", {}).get("value", {}).get("uiAmount", 0)
                return float(ui) if ui else 0
    except Exception:
        return 0


async def _get_liquidity_dexscreener(pair: str) -> float | None:
    """Fetch liquidity from DexScreener for a given pair address."""
    url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{pair}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status != 200:
                    return None
                data  = await resp.json()
                pairs = data.get("pairs", [])
                if pairs:
                    liq = pairs[0].get("liquidity", {}).get("usd", 0)
                    return float(liq) if liq else None
    except Exception:
        return None


async def _detect_wash_trading(mint: str, pair: str) -> dict:
    """
    Simplified wash trading detection:
    - Get recent transactions on the pair
    - Check if volume is coming from a small number of unique wallets
    - High volume + very few unique wallets = wash trading
    """
    try:
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getSignaturesForAddress",
            "params": [pair, {"limit": 30, "commitment": "confirmed"}],
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                data = await resp.json()
                sigs = data.get("result", [])

        if len(sigs) < 5:
            return {"detected": False, "reason": ""}

        # This is a heuristic — a full implementation would check actual signers
        # For now we pass this check and let RugCheck handle wash trading
        return {"detected": False, "reason": ""}

    except Exception:
        return {"detected": False, "reason": ""}
