# trader/wallet_manager.py
# Handles wallet keypair loading, SOL balance reading, and profit transfers

import base58
import aiohttp
from solders.keypair import Keypair  # type: ignore
from solders.pubkey import Pubkey    # type: ignore

import config
from utils.logger import get_logger

logger = get_logger("wallet_manager")

_sniper_keypair: Keypair | None = None


def get_sniper_keypair() -> Keypair:
    """Load and cache the sniper wallet keypair from config."""
    global _sniper_keypair
    if _sniper_keypair is None:
        raw = config.SNIPER_WALLET_PRIVATE_KEY.strip()
        try:
            # Try base58 first (most common export format)
            decoded = base58.b58decode(raw)
            _sniper_keypair = Keypair.from_bytes(decoded)
        except Exception:
            try:
                # Try raw bytes array [1,2,3,...] format
                import json
                arr = json.loads(raw)
                _sniper_keypair = Keypair.from_bytes(bytes(arr))
            except Exception as e:
                raise ValueError(
                    f"Could not load sniper wallet private key. "
                    f"Export it as base58 from Phantom/Solflare. Error: {e}"
                )
        logger.info(f"Sniper wallet loaded: {str(_sniper_keypair.pubkey())[:16]}...")
    return _sniper_keypair


def get_sniper_pubkey() -> str:
    return str(get_sniper_keypair().pubkey())


async def get_sol_balance(address: str | None = None) -> float:
    """Return SOL balance of an address (defaults to sniper wallet)."""
    if address is None:
        address = get_sniper_pubkey()
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getBalance",
        "params": [address, {"commitment": "confirmed"}],
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                data = await resp.json()
                lamports = data.get("result", {}).get("value", 0)
                return lamports / 1e9
    except Exception as e:
        logger.error(f"Failed to fetch SOL balance: {e}")
        return 0.0


async def get_token_balance(mint: str, owner: str | None = None) -> float:
    """Return UI token balance for a given mint in the sniper wallet."""
    if owner is None:
        owner = get_sniper_pubkey()
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [
            owner,
            {"mint": mint},
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                data = await resp.json()
                accounts = data.get("result", {}).get("value", [])
                if not accounts:
                    return 0.0
                ui_amount = (
                    accounts[0]
                    .get("account", {})
                    .get("data", {})
                    .get("parsed", {})
                    .get("info", {})
                    .get("tokenAmount", {})
                    .get("uiAmount", 0.0)
                )
                return float(ui_amount or 0.0)
    except Exception as e:
        logger.error(f"Failed to fetch token balance for {mint[:16]}: {e}")
        return 0.0


async def calculate_trade_size_sol(sol_price_usd: float) -> float:
    """
    Calculate how much SOL to spend on this trade.
    = 20% of sniper wallet balance, clamped to MIN/MAX USD limits.
    """
    sol_balance = await get_sol_balance()

    # Reserve 0.05 SOL for fees (priority fees + rent)
    tradeable_sol = max(0.0, sol_balance - 0.05)

    trade_sol = tradeable_sol * config.SNIPER_POSITION
    trade_usd = trade_sol * sol_price_usd

    # Clamp to USD limits
    if trade_usd < config.MIN_TRADE_USD:
        logger.info(
            f"Trade size ${trade_usd:.2f} below minimum ${config.MIN_TRADE_USD} — skipping"
        )
        return 0.0

    if trade_usd > config.MAX_TRADE_USD:
        trade_sol = config.MAX_TRADE_USD / sol_price_usd

    logger.info(
        f"Trade size: {trade_sol:.4f} SOL (${trade_sol * sol_price_usd:.2f}) "
        f"| Wallet: {sol_balance:.4f} SOL"
    )
    return trade_sol


async def calculate_signal_trade_size_sol(sol_price_usd: float) -> float:
    """
    Calculate SOL to spend on a signal trade.
    Uses SIGNAL_POSITION_USD (fixed dollar amount) converted to SOL live.
    Default $3. Set SIGNAL_POSITION_USD in .env to change.
    """
    if sol_price_usd <= 0:
        return 0.0

    target_usd = config.SIGNAL_POSITION_USD
    trade_sol  = target_usd / sol_price_usd

    sol_balance   = await get_sol_balance()
    tradeable_sol = max(0.0, sol_balance - 0.05)  # keep 0.05 for fees

    if trade_sol > tradeable_sol:
        logger.warning(
            f"Signal trade ${target_usd:.2f} ({trade_sol:.4f} SOL) "
            f"exceeds available {tradeable_sol:.4f} SOL — skipping"
        )
        return 0.0  # coordinator will catch this as "Trade size too small"

    logger.info(
        f"Signal trade size: {trade_sol:.4f} SOL (${target_usd:.2f} at ${sol_price_usd:.0f}/SOL) "
        f"| Wallet: {sol_balance:.4f} SOL"
    )
    return trade_sol
