# filters/first_buyer.py — Filter 4: First Buyer Check
# Scan wallets that bought in the first 10 seconds and cross-reference against bad wallet DB

import aiohttp
import asyncio
from datetime import datetime, timezone

import config
from utils.logger import get_logger
from utils.models import NewTokenEvent
from utils import wallet_db

logger = get_logger("filter_first_buyer")


async def run_first_buyer(event: NewTokenEvent) -> tuple[bool, str, dict]:
    mint      = event.mint_address
    pair_addr = event.pair_address

    early_buyers = await _get_early_buyers(mint, pair_addr, event.detected_at)
    extra = {"early_buyer_count": len(early_buyers), "early_buyers": early_buyers[:5]}

    for buyer in early_buyers:
        if wallet_db.is_known_bad(buyer):
            cat = wallet_db.get_category(buyer)
            return False, f"First buyer {buyer[:12]}... is a known {cat}", extra

    return True, f"First buyer check passed ({len(early_buyers)} early wallets scanned)", extra


async def _get_early_buyers(mint: str, pair: str, detected_at: datetime) -> list[str]:
    """
    Find wallets that purchased the token within the first 10 seconds of launch.
    We use Helius getSignaturesForAddress on the pair/pool, then filter by blockTime.
    """
    detected_ts = detected_at.replace(tzinfo=timezone.utc).timestamp()
    cutoff_ts   = detected_ts + config.FIRST_BUYER_SCAN_SECONDS

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [pair, {"limit": 50, "commitment": "confirmed"}],
    }

    buyers = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                sigs = data.get("result", [])

        # Only look at transactions within the first 10 seconds
        early_sigs = [
            s["signature"] for s in sigs
            if s.get("blockTime") and s["blockTime"] <= cutoff_ts
        ]

        # Fetch each tx to get the wallet address
        fetch_tasks = [_get_tx_signer(sig) for sig in early_sigs[:20]]
        results     = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, str) and r:
                buyers.append(r)

    except Exception as e:
        logger.debug(f"Early buyer scan error for {mint[:16]}: {e}")

    return list(set(buyers))


async def _get_tx_signer(signature: str) -> str:
    """Get the signer (index 0) of a transaction."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                data = await resp.json()
                tx   = data.get("result", {})
                keys = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
                if keys:
                    first = keys[0]
                    return first.get("pubkey", "") if isinstance(first, dict) else str(first)
    except Exception:
        pass
    return ""
