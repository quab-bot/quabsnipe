# filters/honeypot.py — Filter 2: Honeypot Check
# Simulates a sell transaction. If simulation fails → reject.

import base64
import aiohttp
import config
from utils.logger import get_logger
from utils.models import NewTokenEvent

logger = get_logger("filter_honeypot")

# Jupiter Quote API to build a sell swap transaction for simulation
JUPITER_QUOTE_API = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP_API  = "https://quote-api.jup.ag/v6/swap"


async def run_honeypot(event: NewTokenEvent) -> tuple[bool, str, dict]:
    """
    Simulate selling 1 token via Helius simulateTransaction.
    If the simulation fails, the token is a honeypot.
    """
    mint = event.mint_address

    try:
        # Step 1: Get a quote for selling a tiny amount (1 token = 1_000_000 base units)
        quote = await _get_sell_quote(mint)
        if not quote:
            # Jupiter can't route it — likely no sell path = honeypot
            return False, "Honeypot: no sell route found on Jupiter", {}

        # Step 2: Build the swap transaction (unsigned)
        swap_tx_b64 = await _build_swap_tx(quote)
        if not swap_tx_b64:
            return False, "Honeypot: failed to build sell transaction", {}

        # Step 3: Simulate via Helius RPC
        sim_result = await _simulate_transaction(swap_tx_b64)
        if sim_result is None:
            return False, "Honeypot: simulation RPC call failed", {}

        if sim_result.get("err"):
            err_msg = str(sim_result["err"])
            return False, f"Honeypot: sell simulation failed — {err_msg[:100]}", {
                "simulation_error": err_msg
            }

        return True, "Honeypot check passed (sell simulated OK)", {
            "simulated_logs": sim_result.get("logs", [])[:3]
        }

    except Exception as e:
        logger.warning(f"Honeypot check exception for {mint[:16]}: {e}")
        # On unexpected errors, fail safe
        return False, f"Honeypot: exception during check — {e}", {}


async def _get_sell_quote(mint: str) -> dict | None:
    """Get Jupiter quote for selling 1_000_000 base units of token → WSOL."""
    params = {
        "inputMint":  mint,
        "outputMint": config.WSOL_MINT,
        "amount":     "1000000",   # 1 token with 6 decimals
        "slippageBps": "5000",     # 50% slippage — we don't care about price, just routability
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                JUPITER_QUOTE_API,
                params=params,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return data if data.get("routePlan") else None
    except Exception as e:
        logger.debug(f"Jupiter quote error: {e}")
        return None


async def _build_swap_tx(quote: dict) -> str | None:
    """Build an unsigned swap transaction from a Jupiter quote."""
    # Use a dummy public key for simulation (doesn't need to be funded)
    dummy_pubkey = "11111111111111111111111111111111"
    payload = {
        "quoteResponse":          quote,
        "userPublicKey":          dummy_pubkey,
        "wrapAndUnwrapSol":       True,
        "skipUserAccountsRpcCalls": True,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                JUPITER_SWAP_API,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return data.get("swapTransaction")
    except Exception as e:
        logger.debug(f"Jupiter swap build error: {e}")
        return None


async def _simulate_transaction(tx_b64: str) -> dict | None:
    """Simulate a base64 transaction via Helius simulateTransaction RPC."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "simulateTransaction",
        "params": [
            tx_b64,
            {
                "encoding":              "base64",
                "commitment":            "confirmed",
                "replaceRecentBlockhash": True,
                "sigVerify":             False,
            },
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
                return data.get("result", {}).get("value")
    except Exception as e:
        logger.debug(f"simulateTransaction error: {e}")
        return None
