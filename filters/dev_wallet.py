# filters/dev_wallet.py — Filter 3: Dev Wallet Analysis

import asyncio
from datetime import datetime, timezone
import aiohttp

import config
from utils.logger import get_logger
from utils.models import NewTokenEvent
from utils import wallet_db

logger = get_logger("filter_dev_wallet")

# Suspicious round-number funding amounts (in SOL)
SUSPICIOUS_ROUND_SOL = {0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0}
ROUND_SOL_TOLERANCE  = 0.001  # Within 0.001 SOL = "round number"


async def run_dev_wallet(event: NewTokenEvent) -> tuple[bool, str, dict]:
    """
    Check the wallet that deployed the token against:
    1. Wallet age > 7 days
    2. ≥ 10 prior transactions
    3. No previously rugged tokens
    4. Not funded from known rug addresses
    5. Funding amount not a suspicious round number
    """
    mint = event.mint_address

    # Get the deployer address from the transaction
    deployer = _extract_deployer(event)
    if not deployer:
        return False, "Dev wallet: could not identify deployer address", {}

    extra = {"deployer": deployer}

    # Check against known bad wallets
    if wallet_db.is_known_bad(deployer):
        cat = wallet_db.get_category(deployer)
        return False, f"Dev wallet: deployer is a known {cat}", extra

    # Fetch wallet transaction history
    tx_history = await _get_wallet_transactions(deployer)
    if tx_history is None:
        return False, "Dev wallet: failed to fetch wallet history", extra

    # Check transaction count
    tx_count = len(tx_history)
    extra["tx_count"] = tx_count
    if tx_count < config.DEV_WALLET_MIN_TX_COUNT:
        return False, f"Dev wallet: only {tx_count} prior txs (min {config.DEV_WALLET_MIN_TX_COUNT})", extra

    # Check wallet age from oldest transaction
    wallet_age_days = await _get_wallet_age_days(deployer, tx_history)
    extra["wallet_age_days"] = round(wallet_age_days, 1)
    if wallet_age_days < config.DEV_WALLET_MIN_AGE_DAYS:
        return False, f"Dev wallet: wallet only {wallet_age_days:.1f} days old (min {config.DEV_WALLET_MIN_AGE_DAYS})", extra

    # Check for previous rugged tokens
    prev_rugged = await _check_previous_rugs(deployer, mint)
    extra["prev_rugged"] = prev_rugged
    if prev_rugged:
        # Add this wallet to bad wallet DB for future reference
        wallet_db.add_bad_wallet(deployer, "ruggers")
        return False, f"Dev wallet: deployer has {len(prev_rugged)} previous rug(s)", extra

    # Check funding source
    funding_check = await _check_funding_source(deployer)
    extra["funding_suspicious"] = funding_check["suspicious"]
    if funding_check["suspicious"]:
        return False, f"Dev wallet: {funding_check['reason']}", extra

    return True, "Dev wallet passed", extra


def _extract_deployer(event: NewTokenEvent) -> str | None:
    """Extract the deployer/creator wallet from the raw transaction data."""
    try:
        tx      = event.raw_data.get("transaction", {})
        message = tx.get("message", {})
        keys    = message.get("accountKeys", [])
        if not keys:
            return None
        first = keys[0]
        if isinstance(first, dict):
            return first.get("pubkey")
        return str(first)
    except Exception:
        return None


async def _get_wallet_transactions(address: str) -> list | None:
    """Fetch the last 100 transaction signatures for a wallet."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [
            address,
            {"limit": 100, "commitment": "confirmed"},
        ],
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                return data.get("result", [])
    except Exception as e:
        logger.debug(f"getSignaturesForAddress error for {address[:16]}: {e}")
        return None


async def _get_wallet_age_days(address: str, tx_history: list) -> float:
    """Calculate wallet age from the earliest transaction timestamp."""
    if not tx_history:
        return 0.0
    # Signatures are returned newest-first; last entry = oldest tx
    oldest = tx_history[-1]
    block_time = oldest.get("blockTime")
    if not block_time:
        return 0.0
    created_dt = datetime.fromtimestamp(block_time, tz=timezone.utc)
    now        = datetime.now(tz=timezone.utc)
    return (now - created_dt).total_seconds() / 86400


async def _check_previous_rugs(deployer: str, current_mint: str) -> list[str]:
    """
    Look for tokens previously deployed by this wallet that ended in rugs.
    We check RugCheck for each token the wallet created.
    This is a best-effort check limited to the first 5 previous tokens.
    """
    rugged = []
    try:
        # Get wallet's token creation history via Helius
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [deployer, {"limit": 50, "commitment": "confirmed"}],
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                data = await resp.json()
                sigs = data.get("result", [])

        # For each sig, check if it created a token — simplified check
        # A full implementation would parse each tx; here we use RugCheck's
        # "tokens created by" endpoint if available
        url = f"{config.RUGCHECK_API_URL}/tokens/creator/{deployer}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    created_tokens = await resp.json()
                    for token in created_tokens[:10]:  # Check up to 10 past tokens
                        t_mint = token.get("mint", "")
                        if t_mint == current_mint:
                            continue
                        score = token.get("score", 0)
                        if score > 70:  # High score = likely rugged
                            rugged.append(t_mint)

    except Exception as e:
        logger.debug(f"Previous rug check error for {deployer[:16]}: {e}")

    return rugged


async def _check_funding_source(address: str) -> dict:
    """
    Check how this wallet was funded.
    Red flags: funded from known rug addresses, or suspicious round numbers.
    """
    try:
        # Get oldest transactions to find where initial SOL came from
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [address, {"limit": 10, "commitment": "confirmed"}],
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                data = await resp.json()
                sigs = data.get("result", [])

        if not sigs:
            return {"suspicious": False, "reason": ""}

        # Check the first/oldest tx for funding
        oldest_sig = sigs[-1].get("signature", "")
        if not oldest_sig:
            return {"suspicious": False, "reason": ""}

        # Fetch that transaction to see the funding source
        payload2 = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [oldest_sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload2,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                data = await resp.json()
                tx = data.get("result", {})

        if not tx:
            return {"suspicious": False, "reason": ""}

        meta  = tx.get("meta", {})
        msg   = tx.get("transaction", {}).get("message", {})
        keys  = msg.get("accountKeys", [])
        pre   = meta.get("preBalances", [])
        post  = meta.get("postBalances", [])

        if len(keys) >= 2 and len(pre) >= 2 and len(post) >= 2:
            # Index 0 = sender, index 1 = receiver (funded wallet)
            sender_key = keys[0]
            if isinstance(sender_key, dict):
                sender_key = sender_key.get("pubkey", "")

            # Check if sender is a known bad wallet
            if wallet_db.is_known_bad(str(sender_key)):
                return {
                    "suspicious": True,
                    "reason": f"funded from known bad wallet {str(sender_key)[:16]}...",
                }

            # Check for suspicious round number funding
            sol_received = (post[1] - pre[1]) / 1e9
            for round_amount in SUSPICIOUS_ROUND_SOL:
                if abs(sol_received - round_amount) < ROUND_SOL_TOLERANCE:
                    return {
                        "suspicious": True,
                        "reason": f"funded with suspicious round amount: {sol_received:.3f} SOL",
                    }

        return {"suspicious": False, "reason": ""}

    except Exception as e:
        logger.debug(f"Funding source check error for {address[:16]}: {e}")
        return {"suspicious": False, "reason": ""}  # Don't reject on error
