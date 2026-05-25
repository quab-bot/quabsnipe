# trader/executor.py
# Executes buy and sell swaps via Jupiter Aggregator API
# Uses the sniper wallet keypair to sign and submit transactions

import asyncio
import base64
import aiohttp

from solders.keypair import Keypair          # type: ignore
from solders.transaction import VersionedTransaction  # type: ignore

import config
from utils.logger import get_logger
from trader.wallet_manager import get_sniper_keypair

logger = get_logger("executor")

JUPITER_QUOTE_API = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP_API  = "https://quote-api.jup.ag/v6/swap"

# Priority fee in microlamports (dynamic — we set a baseline and adjust)
BASE_PRIORITY_FEE_MICROLAMPORTS = 500_000   # ~$0.05–0.15 depending on SOL price
HIGH_PRIORITY_FEE_MICROLAMPORTS = 1_000_000  # Used during high congestion


class SwapResult:
    def __init__(
        self,
        success: bool,
        signature: str = "",
        in_amount: float = 0.0,
        out_amount: float = 0.0,
        price_per_token: float = 0.0,
        error: str = "",
    ):
        self.success         = success
        self.signature       = signature
        self.in_amount       = in_amount    # SOL spent (for buys) or tokens spent (for sells)
        self.out_amount      = out_amount   # Tokens received (for buys) or SOL received (for sells)
        self.price_per_token = price_per_token
        self.error           = error

    def __repr__(self):
        if self.success:
            return f"SwapResult(✅ sig={self.signature[:16]}... in={self.in_amount:.6f} out={self.out_amount:.6f})"
        return f"SwapResult(❌ error={self.error})"


async def buy_token(
    mint: str,
    sol_amount: float,
    slippage_bps: int = 2000,  # 20% slippage for new tokens
    retry: int = 2,
) -> SwapResult:
    """
    Buy a token by swapping SOL → token via Jupiter.
    sol_amount: how much SOL to spend (e.g. 0.1)
    """
    lamports = int(sol_amount * 1e9)
    logger.info(f"BUY {mint[:16]}... | {sol_amount:.4f} SOL | slippage {slippage_bps/100:.0f}%")

    for attempt in range(retry + 1):
        try:
            quote = await _get_quote(
                input_mint=config.WSOL_MINT,
                output_mint=mint,
                amount=lamports,
                slippage_bps=slippage_bps,
            )
            if not quote:
                logger.warning(f"Buy quote failed attempt {attempt + 1}")
                continue

            result = await _execute_swap(quote, is_buy=True)
            if result.success:
                logger.info(f"✅ BUY SUCCESS | {mint[:16]}... | sig: {result.signature[:20]}...")
                return result

            logger.warning(f"Buy swap failed attempt {attempt + 1}: {result.error}")
            if attempt < retry:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Buy attempt {attempt + 1} exception: {e}")
            if attempt < retry:
                await asyncio.sleep(1)

    return SwapResult(success=False, error=f"Buy failed after {retry + 1} attempts")


async def sell_token(
    mint: str,
    token_amount: float,
    token_decimals: int = 6,
    slippage_bps: int = 3000,  # 30% slippage for sells (we always want out)
    retry: int = 2,
) -> SwapResult:
    """
    Sell tokens by swapping token → SOL via Jupiter.
    token_amount: UI amount (e.g. 1000.0 tokens)
    """
    raw_amount = int(token_amount * (10 ** token_decimals))
    logger.info(f"SELL {mint[:16]}... | {token_amount:.2f} tokens | slippage {slippage_bps/100:.0f}%")

    for attempt in range(retry + 1):
        try:
            quote = await _get_quote(
                input_mint=mint,
                output_mint=config.WSOL_MINT,
                amount=raw_amount,
                slippage_bps=slippage_bps,
            )
            if not quote:
                logger.warning(f"Sell quote failed attempt {attempt + 1}")
                continue

            result = await _execute_swap(quote, is_buy=False)
            if result.success:
                logger.info(f"✅ SELL SUCCESS | {mint[:16]}... | sig: {result.signature[:20]}...")
                return result

            logger.warning(f"Sell swap failed attempt {attempt + 1}: {result.error}")
            if attempt < retry:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Sell attempt {attempt + 1} exception: {e}")
            if attempt < retry:
                await asyncio.sleep(1)

    return SwapResult(success=False, error=f"Sell failed after {retry + 1} attempts")


# ── Internal helpers ───────────────────────────────────────────────────────────

async def _get_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
) -> dict | None:
    params = {
        "inputMint":   input_mint,
        "outputMint":  output_mint,
        "amount":      str(amount),
        "slippageBps": str(slippage_bps),
        "onlyDirectRoutes": "false",
        "asLegacyTransaction": "false",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                JUPITER_QUOTE_API,
                params=params,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    logger.debug(f"Jupiter quote HTTP {resp.status}")
                    return None
                data = await resp.json()
                if not data.get("routePlan"):
                    return None
                return data
    except Exception as e:
        logger.debug(f"Jupiter quote error: {e}")
        return None


async def _execute_swap(quote: dict, is_buy: bool) -> SwapResult:
    """Build, sign, and submit the swap transaction."""
    keypair = get_sniper_keypair()
    pubkey  = str(keypair.pubkey())

    swap_payload = {
        "quoteResponse":        quote,
        "userPublicKey":        pubkey,
        "wrapAndUnwrapSol":     True,
        "dynamicComputeUnitLimit": True,
        "prioritizationFeeLamports": {
            "priorityLevelWithMaxLamports": {
                "maxLamports": HIGH_PRIORITY_FEE_MICROLAMPORTS,
                "priorityLevel": "high",
            }
        },
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                JUPITER_SWAP_API,
                json=swap_payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    return SwapResult(success=False, error=f"Jupiter swap API HTTP {resp.status}: {body[:100]}")
                swap_data = await resp.json()

        tx_b64 = swap_data.get("swapTransaction")
        if not tx_b64:
            return SwapResult(success=False, error="No swapTransaction in Jupiter response")

        # Deserialize, sign, reserialize
        tx_bytes = base64.b64decode(tx_b64)
        tx       = VersionedTransaction.from_bytes(tx_bytes)
        signed   = VersionedTransaction(tx.message, [keypair])
        signed_b64 = base64.b64encode(bytes(signed)).decode("utf-8")

        # Submit
        sig = await _send_transaction(signed_b64)
        if not sig:
            return SwapResult(success=False, error="Transaction submission failed")

        # Wait for confirmation
        confirmed = await _confirm_transaction(sig)
        if not confirmed:
            return SwapResult(success=False, error=f"Transaction not confirmed: {sig[:20]}")

        # Parse amounts from quote
        in_amount  = int(quote.get("inAmount",  0))
        out_amount = int(quote.get("outAmount", 0))

        if is_buy:
            # SOL in, tokens out
            sol_spent  = in_amount / 1e9
            tokens_out = out_amount / (10 ** _get_decimals(quote))
            price      = (sol_spent / tokens_out) if tokens_out > 0 else 0
            return SwapResult(
                success=True, signature=sig,
                in_amount=sol_spent, out_amount=tokens_out,
                price_per_token=price,
            )
        else:
            # Tokens in, SOL out
            tokens_spent = in_amount / (10 ** _get_decimals(quote))
            sol_out      = out_amount / 1e9
            price        = (sol_out / tokens_spent) if tokens_spent > 0 else 0
            return SwapResult(
                success=True, signature=sig,
                in_amount=tokens_spent, out_amount=sol_out,
                price_per_token=price,
            )

    except Exception as e:
        logger.error(f"Swap execution error: {e}")
        return SwapResult(success=False, error=str(e))


async def _send_transaction(signed_b64: str) -> str | None:
    """Submit a signed base64 transaction and return the signature."""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "sendTransaction",
        "params": [
            signed_b64,
            {
                "encoding":             "base64",
                "skipPreflight":        False,
                "preflightCommitment":  "confirmed",
                "maxRetries":           3,
            },
        ],
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
                if "error" in data:
                    logger.error(f"sendTransaction error: {data['error']}")
                    return None
                return data.get("result")
    except Exception as e:
        logger.error(f"sendTransaction network error: {e}")
        return None


async def _confirm_transaction(signature: str, max_retries: int = 20) -> bool:
    """Poll for transaction confirmation with exponential backoff."""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getSignatureStatuses",
        "params": [[signature], {"searchTransactionHistory": True}],
    }
    for i in range(max_retries):
        await asyncio.sleep(1.5 if i < 5 else 3.0)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    config.HELIUS_RPC_URL,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    data    = await resp.json()
                    results = data.get("result", {}).get("value", [None])
                    status  = results[0] if results else None

                    if status is None:
                        continue

                    if status.get("err"):
                        logger.error(f"Transaction failed on-chain: {status['err']}")
                        return False

                    conf = status.get("confirmationStatus", "")
                    if conf in ("confirmed", "finalized"):
                        logger.debug(f"Transaction confirmed: {signature[:20]}... ({conf})")
                        return True

        except Exception as e:
            logger.debug(f"Confirm poll error: {e}")

    logger.warning(f"Transaction not confirmed after {max_retries} polls: {signature[:20]}...")
    return False


def _get_decimals(quote: dict) -> int:
    """Extract token decimals from Jupiter route plan, default 6."""
    try:
        route = quote.get("routePlan", [{}])
        if route:
            swap_info = route[-1].get("swapInfo", {})
            return int(swap_info.get("outputDecimals", 6))
    except Exception:
        pass
    return 6


async def get_token_price_sol(mint: str) -> float | None:
    """Get current token price in SOL via Jupiter."""
    quote = await _get_quote(
        input_mint=mint,
        output_mint=config.WSOL_MINT,
        amount=1_000_000,   # 1 token (6 decimals)
        slippage_bps=5000,
    )
    if not quote:
        return None
    try:
        out_lamports = int(quote.get("outAmount", 0))
        return out_lamports / 1e9  # SOL per token
    except Exception:
        return None
