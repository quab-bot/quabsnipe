# core/queue_manager.py
# Token queue with observation delay system.
#
# OBSERVATION DELAY ARCHITECTURE:
#   Token detected at birth (near-zero liquidity)
#         ↓
#   Placed in "pending" pool with timestamp
#         ↓
#   Wait OBSERVATION_DELAY seconds (liquidity formation window)
#         ↓
#   Re-fetch live liquidity from DexScreener/Jupiter
#         ↓
#   If liquidity now meets threshold → run full filter pipeline
#   If still too low → discard silently
#
# This avoids analyzing tokens before liquidity has formed,
# which was causing 98% of rejections to be premature.

import asyncio
import aiohttp
from datetime import datetime, timezone
from typing import Callable, Awaitable

import config
from utils.logger import get_logger
from utils.models import NewTokenEvent

logger = get_logger("queue_manager")

# ── Tunable parameters ─────────────────────────────────────────────────────────
OBSERVATION_DELAY  = 75      # Seconds to wait before analyzing (liquidity formation)
MAX_WORKERS        = 3       # Max simultaneous filter pipeline runs
SCAN_TIMEOUT       = 20.0    # Seconds before a pipeline run is force-killed
MAX_PENDING        = 200     # Max tokens sitting in observation pool
MAX_QUEUE_SIZE     = 30      # Max tokens queued for analysis

# Minimum liquidity after delay to proceed to pipeline
# This is checked live, not from the initial detection data
MIN_LIQ_TO_ANALYZE_USD = 500.0


class TokenQueue:
    """
    Two-stage queue:
    1. Pending pool  — tokens wait OBSERVATION_DELAY seconds
    2. Analysis queue — tokens with sufficient live liquidity run the pipeline
    """

    def __init__(self, process_fn: Callable[[NewTokenEvent], Awaitable[None]]):
        self._process_fn   = process_fn
        self._running      = False

        # Stage 1: pending pool (mint → event, awaiting delay)
        self._pending: dict[str, NewTokenEvent] = {}

        # Stage 2: ready for pipeline
        self._analysis_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

        self._stats = {
            "enqueued":         0,
            "dropped_full":     0,
            "dropped_low_liq":  0,
            "promoted":         0,
            "processed":        0,
            "timed_out":        0,
        }

        self._sol_price: float = 150.0

    # ── Public API ─────────────────────────────────────────────────────────────

    async def enqueue(self, event: NewTokenEvent):
        """Accept a new token into the observation pool."""
        if len(self._pending) >= MAX_PENDING:
            self._stats["dropped_full"] += 1
            logger.debug(f"Pending pool full — dropping {event.mint_address[:16]}...")
            return

        if event.mint_address in self._pending:
            return  # Already waiting

        self._pending[event.mint_address] = event
        self._stats["enqueued"] += 1
        logger.debug(
            f"Pending [{len(self._pending)}] {event.mint_address[:16]}... "
            f"→ analyze in {OBSERVATION_DELAY}s"
        )

        # Schedule the delayed promotion
        asyncio.create_task(self._delayed_promote(event))

    def queue_size(self) -> int:
        return self._analysis_queue.qsize()

    def pending_size(self) -> int:
        return len(self._pending)

    def stats(self) -> dict:
        return dict(self._stats)

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self):
        self._running = True
        logger.info(
            f"Token queue started — "
            f"Observation delay: {OBSERVATION_DELAY}s | "
            f"{MAX_WORKERS} workers | "
            f"Min liq after delay: ${MIN_LIQ_TO_ANALYZE_USD:,.0f}"
        )
        # Start SOL price refresher + workers
        await asyncio.gather(
            self._sol_price_loop(),
            *[self._worker(i) for i in range(MAX_WORKERS)],
        )

    async def stop(self):
        self._running = False
        self._pending.clear()

    # ── Observation delay ──────────────────────────────────────────────────────

    async def _delayed_promote(self, event: NewTokenEvent):
        """
        Wait OBSERVATION_DELAY seconds, then check live liquidity.
        If liquidity is sufficient → promote to analysis queue.
        Otherwise → discard.
        """
        await asyncio.sleep(OBSERVATION_DELAY)

        if not self._running:
            return

        # Remove from pending pool
        self._pending.pop(event.mint_address, None)

        # Fetch live liquidity (not the birth snapshot)
        live_liq_usd = await self._get_live_liquidity(event)

        if live_liq_usd < MIN_LIQ_TO_ANALYZE_USD:
            self._stats["dropped_low_liq"] += 1
            logger.debug(
                f"Post-delay liq too low: ${live_liq_usd:,.0f} | "
                f"{event.mint_address[:16]}... — discarded"
            )
            return

        # Update event with fresh liquidity data
        event.initial_liquidity_sol = live_liq_usd / self._sol_price

        if self._analysis_queue.full():
            self._stats["dropped_full"] += 1
            logger.debug(f"Analysis queue full — dropping {event.mint_address[:16]}...")
            return

        self._stats["promoted"] += 1
        await self._analysis_queue.put(event)
        logger.info(
            f"⬆️  PROMOTED {event.mint_address[:16]}... | "
            f"Live liq: ${live_liq_usd:,.0f} | "
            f"Source: {event.source.upper()} | "
            f"→ Running pipeline"
        )

    async def _get_live_liquidity(self, event: NewTokenEvent) -> float:
        """
        Fetch current liquidity from DexScreener.
        Falls back to Jupiter price estimate if DexScreener fails.
        """
        mint = event.mint_address

        # Try DexScreener first (most accurate)
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status == 200:
                        data  = await resp.json()
                        pairs = data.get("pairs", [])
                        if pairs:
                            # Take the highest liquidity pair
                            liq = max(
                                (p.get("liquidity", {}).get("usd", 0) or 0)
                                for p in pairs
                            )
                            if liq > 0:
                                logger.debug(f"DexScreener liq: ${liq:,.0f} | {mint[:16]}...")
                                return float(liq)
        except Exception as e:
            logger.debug(f"DexScreener error for {mint[:16]}: {e}")

        # Fallback: use birth liquidity (SOL × price)
        fallback = event.initial_liquidity_sol * self._sol_price
        logger.debug(f"Fallback liq: ${fallback:,.0f} | {mint[:16]}...")
        return fallback

    # ── Worker pool ────────────────────────────────────────────────────────────

    async def _worker(self, worker_id: int):
        logger.debug(f"Worker {worker_id} started")
        while self._running:
            try:
                event = await asyncio.wait_for(
                    self._analysis_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue

            try:
                await asyncio.wait_for(
                    self._process_fn(event),
                    timeout=SCAN_TIMEOUT,
                )
                self._stats["processed"] += 1
            except asyncio.TimeoutError:
                self._stats["timed_out"] += 1
                logger.warning(
                    f"[Worker {worker_id}] Pipeline timeout ({SCAN_TIMEOUT}s) "
                    f"for {event.mint_address[:16]}... — skipped"
                )
            except Exception as e:
                logger.error(f"[Worker {worker_id}] Error: {e}")
            finally:
                self._analysis_queue.task_done()

        logger.debug(f"Worker {worker_id} stopped")

    # ── SOL price ──────────────────────────────────────────────────────────────

    async def _sol_price_loop(self):
        while self._running:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        "https://price.jup.ag/v6/price?ids=SOL",
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        data = await resp.json()
                        self._sol_price = float(data["data"]["SOL"]["price"])
            except Exception:
                pass
            await asyncio.sleep(60)
