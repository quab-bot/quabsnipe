# detector/token_detector.py
# Coordinates both Raydium and Pump.fun listeners simultaneously.
# Deduplicates events, enriches them with price/MC data,
# then feeds them into the filter pipeline queue.

import asyncio
from datetime import datetime
from typing import Callable, Awaitable

import aiohttp

import config
from utils.logger import get_logger
from utils.models import NewTokenEvent
from detector.raydium_listener import RaydiumListener
from detector.pumpfun_listener import PumpFunListener

logger = get_logger("token_detector")


class TokenDetector:
    """
    Runs Raydium + Pump.fun listeners in parallel.
    Enriches each NewTokenEvent with SOL price and market cap,
    then calls on_new_token(event) for each unique new token found.
    """

    def __init__(self, on_new_token: Callable[[NewTokenEvent], Awaitable[None]]):
        self.on_new_token   = on_new_token
        self._seen_mints: set[str] = set()
        self._sol_price_usd: float = 0.0
        self._sol_price_updated: datetime | None = None

        self._raydium  = RaydiumListener(self._handle_event)
        self._pumpfun  = PumpFunListener(self._handle_event)

    async def start(self):
        """Start both listeners and the SOL price refresher concurrently."""
        logger.info("TokenDetector starting — monitoring Raydium + Pump.fun")
        await asyncio.gather(
            self._raydium.start(),
            self._pumpfun.start(),
            self._sol_price_loop(),
        )

    async def stop(self):
        await self._raydium.stop()
        await self._pumpfun.stop()
        logger.info("TokenDetector stopped.")

    # ── Internal handlers ──────────────────────────────────────────────────────

    async def _handle_event(self, event: NewTokenEvent):
        """
        Called by each listener when a new token is found.
        Deduplicates by mint address, enriches, then forwards.
        """
        if event.mint_address in self._seen_mints:
            logger.debug(f"Duplicate mint ignored: {event.mint_address[:16]}...")
            return

        self._seen_mints.add(event.mint_address)
        # Prevent unbounded growth
        if len(self._seen_mints) > 50_000:
            self._seen_mints = set(list(self._seen_mints)[-25_000:])

        # Enrich with USD values
        await self._enrich_event(event)

        logger.info(
            f"[{event.source.upper()}] ✦ New token: {event.mint_address[:16]}... "
            f"| Liq: ${event.initial_liquidity_sol * self._sol_price_usd:,.0f} "
            f"| MC: ${event.market_cap_usd:,.0f} "
            f"| Sending to filter pipeline..."
        )

        # Forward to filter pipeline (non-blocking)
        asyncio.create_task(self.on_new_token(event))

    async def _enrich_event(self, event: NewTokenEvent):
        """Add USD price and market cap to the event."""
        sol_price = await self._get_sol_price()
        event.initial_price_usd     = event.initial_liquidity_sol * sol_price / 1e6  # rough
        event.initial_liquidity_sol = event.initial_liquidity_sol  # already set
        # Convert SOL liquidity to USD
        liq_usd = event.initial_liquidity_sol * sol_price

        # Estimate market cap from Helius token supply if possible
        mc = await self._estimate_market_cap(event.mint_address, liq_usd)
        event.market_cap_usd = mc

    async def _estimate_market_cap(self, mint: str, liq_usd: float) -> float:
        """
        Fetch token supply and estimate market cap.
        MC = price_per_token * total_supply
        We estimate price from liquidity: price ≈ liq_usd / circulating_supply
        """
        try:
            supply = await self._get_token_supply(mint)
            if supply and supply > 0:
                # Very rough estimate — real MC needs price from pool
                # Filter 5 will do the precise check
                price_per_token = liq_usd / supply if supply > 0 else 0
                return price_per_token * supply  # = liq_usd (placeholder until real price)
            return liq_usd * 20  # Assume 5% of MC is liquidity as default
        except Exception:
            return liq_usd * 20

    async def _get_token_supply(self, mint: str) -> float | None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
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
                    result = data.get("result", {}).get("value", {})
                    ui_amount = result.get("uiAmount")
                    return float(ui_amount) if ui_amount else None
        except Exception as e:
            logger.debug(f"getTokenSupply failed for {mint[:16]}: {e}")
            return None

    # ── SOL Price ──────────────────────────────────────────────────────────────

    async def _sol_price_loop(self):
        """Refresh SOL price every 60 seconds."""
        while True:
            try:
                price = await self._fetch_sol_price()
                if price:
                    self._sol_price_usd = price
                    logger.debug(f"SOL price updated: ${price:,.2f}")
            except Exception as e:
                logger.debug(f"SOL price fetch error: {e}")
            await asyncio.sleep(60)

    async def _get_sol_price(self) -> float:
        if self._sol_price_usd > 0:
            return self._sol_price_usd
        price = await self._fetch_sol_price()
        self._sol_price_usd = price or 150.0
        return self._sol_price_usd

    async def _fetch_sol_price(self) -> float | None:
        """Fetch SOL/USD price from Jupiter price API (free, no key needed)."""
        url = "https://price.jup.ag/v6/price?ids=SOL"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json()
                    return float(data["data"]["SOL"]["price"])
        except Exception as e:
            logger.debug(f"Jupiter price API error: {e}")
            return None
