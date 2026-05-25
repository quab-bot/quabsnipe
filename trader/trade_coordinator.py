# trader/trade_coordinator.py
# Bridges the filter pipeline → buy execution → position tracking
# This is what main.py calls when all 7 filters pass
#
# FIX: SOL price cache now expires after 60 seconds (was never-expiring).
# Stale price caused incorrect trade sizing when SOL moved significantly.

import asyncio
import time
from datetime import datetime, timezone

import aiohttp

import config
from utils.logger import get_logger
from utils.models import FilterResult, Position
from trader.executor import buy_token, get_token_price_sol
from trader.wallet_manager import calculate_trade_size_sol, calculate_signal_trade_size_sol, get_sol_balance
from trader.position_manager import PositionManager

logger = get_logger("trade_coordinator")

SOL_PRICE_TTL = 60  # seconds before refreshing SOL price


class TradeCoordinator:
    """
    Receives a passing FilterResult, sizes the trade, executes the buy,
    and registers the position with PositionManager.
    """

    def __init__(self, position_manager: PositionManager):
        self.pm               = position_manager
        self._sol_price       = 0.0
        self._sol_price_ts    = 0.0   # FIX: track when price was last fetched
        self._executing: set[str] = set()  # Prevent double-buying same mint

    async def on_buy_signal(self, result: FilterResult):
        """Called by main.py when all 7 filters pass."""
        event = result.token
        mint  = event.mint_address

        # Guard: don't double-buy
        if mint in self._executing:
            logger.debug(f"Already executing buy for {mint[:16]}, skipping duplicate signal")
            return
        if self.pm.get_position(mint):
            logger.debug(f"Already have position in {mint[:16]}, skipping")
            return
        if self.pm.position_count() >= config.MAX_OPEN_TRADES:
            logger.warning(
                f"Max open trades ({config.MAX_OPEN_TRADES}) reached — skipping {mint[:16]}"
            )
            return {"success": False, "error": f"Max open trades ({config.MAX_OPEN_TRADES}) already open"}

        self._executing.add(mint)
        try:
            return await self._execute_buy(result)
        finally:
            self._executing.discard(mint)

    async def _execute_buy(self, result: FilterResult):
        event = result.token
        mint  = event.mint_address

        # Get current SOL price (fresh, TTL-based cache)
        sol_price = await self._get_sol_price()
        if sol_price <= 0:
            logger.error("Cannot execute buy: SOL price unavailable")
            return {"success": False, "error": "SOL price unavailable"}

        # Calculate trade size
        # Use fixed USD amount for signal trades, % wallet for sniper trades
        is_signal = event.source.startswith("telegram")
        if is_signal:
            sol_to_spend = await calculate_signal_trade_size_sol(sol_price)
        else:
            sol_to_spend = await calculate_trade_size_sol(sol_price)
        if sol_to_spend <= 0:
            sol_bal   = await get_sol_balance()
            avail_usd = max(0.0, sol_bal - 0.05) * sol_price
            logger.warning(f"Trade size too small — skipping {mint[:16]}")
            return {
                "success": False,
                "error":   f"Insufficient balance — ${avail_usd:.2f} available, need ${config.SIGNAL_POSITION_USD:.0f}. Top up your wallet."
            }

        # Fetch token metadata for display
        name, symbol = await self._get_token_meta(mint, event)
        usd_to_spend = sol_to_spend * sol_price
        mc_usd       = event.market_cap_usd

        logger.info(
            f"\n{'═'*55}\n"
            f"🚀 EXECUTING BUY\n"
            f"   Token:  {name} ({symbol})\n"
            f"   Mint:   {mint}\n"
            f"   Spend:  {sol_to_spend:.4f} SOL (${usd_to_spend:.2f})\n"
            f"   MC:     ${mc_usd:,.0f}\n"
            f"   Source: {event.source.upper()}\n"
            f"{'═'*55}"
        )

        # Execute the buy
        swap = await buy_token(mint, sol_to_spend)

        if not swap.success:
            logger.error(f"Buy FAILED for {symbol}: {swap.error}")
            return

        # Build position
        entry_price_usd = swap.price_per_token * sol_price if swap.price_per_token > 0 else (
            (sol_to_spend * sol_price) / swap.out_amount if swap.out_amount > 0 else 0
        )

        pos = Position(
            mint_address=mint,
            token_name=name,
            token_symbol=symbol,
            entry_price_usd=entry_price_usd,
            entry_time=datetime.now(timezone.utc),
            sol_amount=swap.in_amount,
            usd_amount=swap.in_amount * sol_price,
            tokens_held=swap.out_amount,
            tokens_initial=swap.out_amount,
            pair_address=event.pair_address,
            source=event.source,
        )

        self.pm.add_position(pos)

        logger.info(
            f"✅ BUY CONFIRMED\n"
            f"   {symbol}: {swap.out_amount:,.2f} tokens\n"
            f"   Entry price: ${entry_price_usd:.8f}\n"
            f"   Tx: {swap.signature[:30]}...\n"
            f"   Stop loss (Phase 1): ${pos.fixed_stop_price:.8f} (-40%)\n"
            f"   Phase 2 (trail) triggers at +15% gain or 5 min"
        )

    async def _get_sol_price(self) -> float:
        """
        Get SOL/USD price with TTL cache (60s).
        Tries multiple sources in order — first success wins.
        """
        now = time.monotonic()
        if self._sol_price > 0 and (now - self._sol_price_ts) < SOL_PRICE_TTL:
            return self._sol_price

        sources = [
            # Source 1: Jupiter v6
            lambda s: (
                "https://price.jup.ag/v6/price?ids=SOL",
                lambda d: float(d["data"]["SOL"]["price"])
            ),
            # Source 2: CoinGecko (free, no key)
            lambda s: (
                "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
                lambda d: float(d["solana"]["usd"])
            ),
            # Source 3: Binance
            lambda s: (
                "https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT",
                lambda d: float(d["price"])
            ),
        ]

        async with aiohttp.ClientSession() as session:
            for src in sources:
                url, extractor = src(None)
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        data  = await resp.json(content_type=None)
                        price = extractor(data)
                        if price > 0:
                            self._sol_price    = price
                            self._sol_price_ts = now
                            return price
                except Exception as e:
                    logger.debug(f"SOL price source failed ({url[:40]}): {e}")
                    continue

        logger.error("SOL price fetch failed: all sources exhausted")
        return self._sol_price  # stale or 0

    async def get_sol_price(self) -> float:
        """Public accessor for PositionManager."""
        return await self._get_sol_price()

    async def _get_token_meta(self, mint: str, event) -> tuple[str, str]:
        """Try to get token name and symbol from Helius DAS or raw_data."""
        # Check if pumpfun listener already parsed it
        name   = event.raw_data.get("_parsed_name", "")
        symbol = event.raw_data.get("_parsed_symbol", "")

        if name and symbol:
            return name, symbol

        # Fetch from Helius DAS
        try:
            url = f"https://mainnet.helius-rpc.com/?api-key={config.HELIUS_API_KEY}"
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getAsset",
                "params": {"id": mint},
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data     = await resp.json()
                    meta     = data.get("result", {}).get("content", {}).get("metadata", {})
                    name     = meta.get("name", "")[:30] or f"Token_{mint[:8]}"
                    symbol   = meta.get("symbol", "")[:10] or mint[:8]
                    return name, symbol
        except Exception:
            return f"Token_{mint[:8]}", mint[:8]
