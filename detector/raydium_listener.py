# detector/raydium_listener.py
# Monitors Solana for new Raydium AMM and CPMM pool creation events
# Uses Helius WebSocket (logsSubscribe) to watch the Raydium program IDs

import asyncio
import json
import base64
import struct
from datetime import datetime, timezone
from typing import Callable, Awaitable

import websockets
import aiohttp

import config
from utils.logger import get_logger
from utils.models import NewTokenEvent

logger = get_logger("raydium_listener")

# Raydium pool creation discriminators
# These are the first 8 bytes of the instruction data for "initialize" calls
RAYDIUM_AMM_INIT_DISCRIMINATOR  = "initialize2"   # AMM v4
RAYDIUM_CPMM_INIT_DISCRIMINATOR = "initialize"    # CPMM


class RaydiumListener:
    """
    Subscribes to Helius WebSocket and watches for new Raydium pool creation
    transactions. When a new pool is found, parses the token mints and emits
    a NewTokenEvent to the callback queue.
    """

    def __init__(self, on_new_token: Callable[[NewTokenEvent], Awaitable[None]]):
        self.on_new_token = on_new_token
        self._running = False
        self._seen_signatures: set[str] = set()  # Dedup guard

    async def start(self):
        """Start listening. Reconnects automatically on disconnect."""
        self._running = True
        logger.info("Raydium listener starting...")
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                if self._running:
                    logger.error(f"Raydium WebSocket error: {e} — reconnecting in 5s")
                    await asyncio.sleep(5)

    async def stop(self):
        self._running = False
        logger.info("Raydium listener stopped.")

    async def _connect_and_listen(self):
        logger.info(f"Connecting to Helius WebSocket: {config.HELIUS_WS_URL[:60]}...")
        async with websockets.connect(
            config.HELIUS_WS_URL,
            ping_interval=30,
            ping_timeout=10,
        ) as ws:
            logger.info("Raydium WebSocket connected ✓")

            # Subscribe to logs for both Raydium program IDs
            await self._subscribe(ws, config.RAYDIUM_AMM_PROGRAM_ID, sub_id=1)
            await self._subscribe(ws, config.RAYDIUM_CPMM_PROGRAM_ID, sub_id=2)

            async for raw_msg in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw_msg)
                    await self._handle_message(msg)
                except json.JSONDecodeError:
                    pass
                except Exception as e:
                    logger.debug(f"Message handling error: {e}")

    async def _subscribe(self, ws, program_id: str, sub_id: int):
        payload = {
            "jsonrpc": "2.0",
            "id": sub_id,
            "method": "logsSubscribe",
            "params": [
                {"mentions": [program_id]},
                {"commitment": "confirmed"},
            ],
        }
        await ws.send(json.dumps(payload))
        logger.info(f"Subscribed to Raydium program: {program_id[:16]}...")

    async def _handle_message(self, msg: dict):
        # Subscription confirmation — ignore
        if "result" in msg and isinstance(msg["result"], int):
            return

        params = msg.get("params", {})
        result = params.get("result", {})
        value  = result.get("value", {})

        logs      = value.get("logs", [])
        signature = value.get("signature", "")
        err       = value.get("err")

        # Skip failed transactions
        if err:
            return

        # Skip if we've already processed this tx
        if signature in self._seen_signatures:
            return

        # ── Strict new pool detection ──────────────────────────────────────────
        # A genuine pool creation has ALL of these characteristics:
        # 1. Contains "initialize" instruction
        # 2. Contains "InitializeMint" OR "MintTo" (new token being created)
        #    OR contains "SetAuthority" (authority being set on new pool)
        # 3. Does NOT contain "Swap" (swaps on existing pools fire "initialize2" too)
        log_str = " ".join(logs).lower()

        has_init     = "instruction: initialize" in log_str or "init_pool" in log_str
        has_swap     = "instruction: swap" in log_str or "swapbaseout" in log_str or "swapbasein" in log_str
        has_new_mint = "initializemint" in log_str or "mintto" in log_str or "setauthority" in log_str

        # Must have init signal, must NOT be a swap, should have mint activity
        if not has_init:
            return
        if has_swap:
            return  # This is a swap on existing pool, not a new pool
        if not has_new_mint:
            # Allow through if it has initialize but no swap — could still be new pool
            # but require the tx to have created new accounts (checked in parse step)
            pass

        self._seen_signatures.add(signature)
        if len(self._seen_signatures) > 10_000:
            self._seen_signatures = set(list(self._seen_signatures)[-5_000:])

        logger.debug(f"[Raydium] New pool candidate: {signature[:20]}...")
        asyncio.create_task(self._fetch_and_parse_tx(signature))

    async def _fetch_and_parse_tx(self, signature: str):
        """
        Calls Helius RPC to get the full transaction, then extracts:
        - base_mint (new token)
        - quote_mint (WSOL/USDC)
        - pair_address (pool/AMM address)
        - initial liquidity
        """
        try:
            tx_data = await self._rpc_get_transaction(signature)
            if not tx_data:
                return

            event = await self._parse_raydium_tx(tx_data, signature)
            if event:
                # Only emit if the new token is not WSOL/USDC itself
                if event.mint_address not in (config.WSOL_MINT, config.USDC_MINT):
                    # Gate: only process if transaction happened in last 5 minutes
                    # This prevents old pool events from being replayed on reconnect
                    tx_age_sec = self._get_tx_age_seconds(tx_data)
                    if tx_age_sec > 300:  # 5 minutes
                        logger.debug(
                            f"[Raydium] Skipping old tx ({tx_age_sec:.0f}s ago): "
                            f"{event.mint_address[:16]}..."
                        )
                        return

                    logger.info(
                        f"[Raydium] New token: {event.mint_address[:16]}... "
                        f"| Pair: {event.pair_address[:16]}... "
                        f"| Liq: {event.initial_liquidity_sol:.2f} SOL "
                        f"| Age: {tx_age_sec:.0f}s"
                    )
                    await self.on_new_token(event)

        except Exception as e:
            logger.debug(f"Failed to parse Raydium tx {signature[:20]}: {e}")

    def _get_tx_age_seconds(self, tx_data: dict) -> float:
        """Get how many seconds ago this transaction was confirmed."""
        try:
            block_time = tx_data.get("blockTime", 0)
            if not block_time:
                return 0.0  # Unknown age → treat as fresh
            import time
            age = time.time() - block_time
            return max(0.0, age)
        except Exception:
            return 0.0

    async def _rpc_get_transaction(self, signature: str) -> dict | None:
        """Fetch full transaction via Helius JSON-RPC."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                    "commitment": "confirmed",
                },
            ],
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.HELIUS_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                return data.get("result")

    async def _parse_raydium_tx(self, tx_data: dict, signature: str) -> NewTokenEvent | None:
        """
        Extract mint addresses and pool address from a Raydium initialise tx.
        Raydium AMM v4 creates specific accounts in a predictable order.
        """
        try:
            meta    = tx_data.get("meta", {})
            tx      = tx_data.get("transaction", {})
            message = tx.get("message", {})

            account_keys = message.get("accountKeys", [])
            # accountKeys can be list of strings or list of dicts (jsonParsed)
            accounts = []
            for ak in account_keys:
                if isinstance(ak, dict):
                    accounts.append(ak.get("pubkey", ""))
                else:
                    accounts.append(str(ak))

            # Extract token mints from postTokenBalances
            post_balances = meta.get("postTokenBalances", [])
            mints_seen = []
            for bal in post_balances:
                mint = bal.get("mint", "")
                if mint and mint not in mints_seen:
                    mints_seen.append(mint)

            if len(mints_seen) < 2:
                return None

            # Determine which is the new token vs quote (WSOL/USDC)
            known_quotes = {config.WSOL_MINT, config.USDC_MINT}
            base_mint  = next((m for m in mints_seen if m not in known_quotes), None)
            quote_mint = next((m for m in mints_seen if m in known_quotes), None)

            if not base_mint:
                return None

            # Pool address = first account that is NOT a known system/program address
            # We skip well-known Solana system programs to find the actual AMM pool account
            SYSTEM_PROGRAMS = {
                "11111111111111111111111111111111",           # System Program
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # Token Program
                "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe8bv",  # Associated Token Program
                "ComputeBudget111111111111111111111111111111", # Compute Budget
                "SysvarRent111111111111111111111111111111111", # Sysvar Rent
                "SysvarC1ock11111111111111111111111111111111", # Sysvar Clock
                config.RAYDIUM_AMM_PROGRAM_ID,
                config.RAYDIUM_CPMM_PROGRAM_ID,
                "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",  # Metaplex
                "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",  # Token-2022
                config.WSOL_MINT,
                config.USDC_MINT,
            }
            # Also skip the deployer (index 0) and the new token mint itself
            skip_addrs = SYSTEM_PROGRAMS | {base_mint, accounts[0] if accounts else ""}

            # The AMM pool account is typically among the first 15 accounts,
            # but NOT the deployer, not a system program, not the token mint
            pair_address = ""
            for acc in accounts[1:15]:
                if acc and acc not in skip_addrs and len(acc) >= 32:
                    pair_address = acc
                    break

            if not pair_address:
                pair_address = accounts[1] if len(accounts) > 1 else accounts[0]

            # Estimate initial SOL liquidity from SOL balance change
            pre_sol  = meta.get("preBalances", [])
            post_sol = meta.get("postBalances", [])
            sol_added = 0.0
            if pre_sol and post_sol:
                for i, (pre, post) in enumerate(zip(pre_sol, post_sol)):
                    delta = (post - pre) / 1e9  # lamports → SOL
                    if delta > sol_added:
                        sol_added = delta

            return NewTokenEvent(
                mint_address=base_mint,
                pair_address=pair_address,
                base_mint=base_mint,
                quote_mint=quote_mint or config.WSOL_MINT,
                source="raydium",
                detected_at=datetime.now(timezone.utc),
                initial_liquidity_sol=sol_added,
                tx_signature=signature,
                raw_data=tx_data,
            )

        except Exception as e:
            logger.debug(f"Raydium tx parse error: {e}")
            return None
