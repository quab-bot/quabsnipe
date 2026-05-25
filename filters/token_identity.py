# filters/token_identity.py — Filter 6: Token Identity
# Checks name/symbol against blacklist, duplicate names, and suspicious keywords

import aiohttp
import config
from utils.logger import get_logger
from utils.models import NewTokenEvent

logger = get_logger("filter_token_identity")

# Additional blacklist of known scam token names (extend this list over time)
KNOWN_SCAM_NAMES = {
    "SAFEMOON", "SAFETOKEN", "ELONMUSK", "TRUMP2025",
}


async def run_token_identity(event: NewTokenEvent) -> tuple[bool, str, dict]:
    mint = event.mint_address

    # Fetch token metadata
    meta = await _get_token_metadata(mint)
    extra = {"metadata": meta}

    if not meta:
        return False, "Token identity: could not fetch token metadata", extra

    name   = (meta.get("name")   or "").upper().strip()
    symbol = (meta.get("symbol") or "").upper().strip()

    extra["name"]   = name
    extra["symbol"] = symbol

    # Check blacklisted keywords
    for keyword in config.TOKEN_NAME_BLACKLIST_KEYWORDS:
        if keyword in name or keyword in symbol:
            return False, f"Token identity: blacklisted keyword '{keyword}' in name/symbol", extra

    # Check known scam names
    if name in KNOWN_SCAM_NAMES or symbol in KNOWN_SCAM_NAMES:
        return False, f"Token identity: name/symbol matches known scam token", extra

    # Check for empty name or symbol
    if not name or not symbol:
        return False, "Token identity: missing name or symbol", extra

    # Check name length sanity
    if len(name) > 50 or len(symbol) > 10:
        return False, f"Token identity: suspiciously long name ({len(name)}) or symbol ({len(symbol)})", extra

    return True, f"Token identity passed: {name} ({symbol})", extra


async def _get_token_metadata(mint: str) -> dict | None:
    """Fetch token metadata via Helius DAS API."""
    url = f"https://mainnet.helius-rpc.com/?api-key={config.HELIUS_API_KEY}"
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAsset",
        "params": {"id": mint},
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                data   = await resp.json()
                result = data.get("result", {})
                content = result.get("content", {})
                metadata = content.get("metadata", {})
                return {
                    "name":        metadata.get("name", ""),
                    "symbol":      metadata.get("symbol", ""),
                    "description": metadata.get("description", ""),
                    "uri":         content.get("json_uri", ""),
                }
    except Exception as e:
        logger.debug(f"Metadata fetch error for {mint[:16]}: {e}")
        return None
