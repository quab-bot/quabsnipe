# detector/pumpfun_listener.py
# Monitors Pump.fun for new token creation events
# Pump.fun emits a "create" instruction when a new bonding curve token launches

import asyncio
import json
from datetime import datetime, timezone
from typing import Callable, Awaitable

import websockets
import aiohttp

import config
from utils.logger import get_logger
from utils.models import NewTokenEvent

logger = get_logger("pumpfun_listener")

# Pump.fun bonding curve program
PUMPFUN_PROGRAM_ID = config.PUMPFUN_PROGRAM_ID

# Pump.fun "create" instruction identifier (first 8 bytes of sha256("global:create"))
PUMPFUN_CREATE_DISCRIMINATOR = "Program log: Instruction: Create"


class PumpFunListener:
    """
    Subscribes to Helius WebSocket and watches for new Pump.fun token launches.
    Pump.fun tokens are created with a bonding curve — we intercept the Create
    instruction and emit a NewTokenEvent immediately.
    """

    def __init__(self, on_new_token: Callable[[NewTokenEvent], Awaitable[None]]):
        self.on_new_token = on_new_token
        self._running = False
        self._seen_signatures: set[str] = set()

    async def start(self):
        self._running = True
        logger.info("Pump.fun listener starting...")
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                if self._running:
                    logger.error(f"Pump.fun WebSocket error: {e} — reconnecting in 5s")
                    await asyncio.sleep(5)

    async def stop(self):
        self._running = False
        logger.info("Pump.fun listener stopped.")

    async def _connect_and_listen(self):
        logger.info(f"Connecting to Helius WebSocket for Pump.fun...")
        async with websockets.connect(
            config.HELIUS_WS_URL,
            ping_interval=30,
            ping_timeout=10,
        ) as ws:
            logger.info("Pump.fun WebSocket connected ✓")
            await self._subscribe(ws)

            async for raw_msg in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw_msg)
                    await self._handle_message(msg)
                except json.JSONDecodeError:
                    pass
                except Exception as e:
                    logger.debug(f"Pump.fun message error: {e}")

    async def _subscribe(self, ws):
        payload = {
            "jsonrpc": "2.0",
            "id": 10,
            "method": "logsSubscribe",
            "params": [
                {"mentions": [PUMPFUN_PROGRAM_ID]},
                {"commitment": "confirmed"},
            ],
        }
        await ws.send(json.dumps(payload))
        logger.info(f"Subscribed to Pump.fun program: {PUMPFUN_PROGRAM_ID[:16]}...")

    async def _handle_message(self, msg: dict):
        if "result" in msg and isinstance(msg["result"], int):
            return

        params = msg.get("params", {})
        result = params.get("result", {})
        value  = result.get("value", {})

        logs      = value.get("logs", [])
        signature = value.get("signature", "")
        err       = value.get("err")

        if err or not signature:
            return

        if signature in self._seen_signatures:
            return

        # Look for the Create instruction in logs
        is_create = any(
            "Instruction: Create" in log or "create" in log.lower()
            for log in logs
            if "Program log" in log
        )
        if not is_create:
            return

        # Also make sure this isn't just a trade on an existing token
        # "InitializeMint" or "MintTo" in logs = brand new token
        has_mint_init = any("InitializeMint" in log or "MintTo" in log for log in logs)

        # Even without mint init, "Create" from pump.fun IS a new token
        self._seen_signatures.add(signature)
        if len(self._seen_signatures) > 10_000:
            self._seen_signatures = set(list(self._seen_signatures)[-5_000:])

        logger.info(f"[Pump.fun] New token create tx: {signature[:20]}...")
        asyncio.create_task(self._fetch_and_parse_tx(signature, logs))

    async def _fetch_and_parse_tx(self, signature: str, logs: list[str]):
        try:
            tx_data = await self._rpc_get_transaction(signature)
            if not tx_data:
                return

            event = await self._parse_pumpfun_tx(tx_data, signature, logs)
            if event:
                logger.info(
                    f"[Pump.fun] New token: {event.mint_address[:16]}... "
                    f"| Pair: {event.pair_address[:16]}..."
                )
                await self.on_new_token(event)

        except Exception as e:
            logger.debug(f"Failed to parse Pump.fun tx {signature[:20]}: {e}")

    async def _rpc_get_transaction(self, signature: str) -> dict | None:
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

    async def _parse_pumpfun_tx(
        self, tx_data: dict, signature: str, logs: list[str]
    ) -> NewTokenEvent | None:
        try:
            meta    = tx_data.get("meta", {})
            tx      = tx_data.get("transaction", {})
            message = tx.get("message", {})

            account_keys = message.get("accountKeys", [])
            accounts = []
            for ak in account_keys:
                if isinstance(ak, dict):
                    accounts.append(ak.get("pubkey", ""))
                else:
                    accounts.append(str(ak))

            # Pump.fun token structure:
            # accounts[0] = signer (deployer)
            # accounts[1] = mint address (new token)
            # accounts[2] = bonding curve (the "pair" for our purposes)
            # accounts[3] = associated bonding curve token account
            # accounts[4] = global state
            # accounts[5] = mpl token metadata program

            if len(accounts) < 3:
                return None

            # The mint is reliably at index 1 for Pump.fun create instructions
            # Validate by checking postTokenBalances
            post_balances = meta.get("postTokenBalances", [])
            known_quotes  = {config.WSOL_MINT, config.USDC_MINT}

            mints_seen = [
                bal.get("mint", "") for bal in post_balances
                if bal.get("mint") and bal.get("mint") not in known_quotes
            ]

            # Prefer the mint from token balances; fall back to accounts[1]
            base_mint    = mints_seen[0] if mints_seen else accounts[1]
            pair_address = accounts[2] if len(accounts) > 2 else accounts[1]  # bonding curve

            # Extract deployer SOL spent (initial virtual liquidity)
            pre_sol  = meta.get("preBalances", [0])
            post_sol = meta.get("postBalances", [0])
            sol_added = max(0.0, (pre_sol[0] - post_sol[0]) / 1e9) if pre_sol and post_sol else 0.0

            # Try to extract token name/symbol from logs
            name, symbol = self._extract_name_symbol_from_logs(logs)

            event = NewTokenEvent(
                mint_address=base_mint,
                pair_address=pair_address,
                base_mint=base_mint,
                quote_mint=config.WSOL_MINT,
                source="pumpfun",
                detected_at=datetime.now(timezone.utc),
                initial_liquidity_sol=sol_added,
                tx_signature=signature,
                raw_data=tx_data,
            )

            # Attach name/symbol to raw_data for downstream use
            event.raw_data["_parsed_name"]   = name
            event.raw_data["_parsed_symbol"] = symbol

            return event

        except Exception as e:
            logger.debug(f"Pump.fun tx parse error: {e}")
            return None

    def _extract_name_symbol_from_logs(self, logs: list[str]) -> tuple[str, str]:
        """
        Pump.fun logs sometimes include token name/symbol in the program data logs.
        This is a best-effort extraction; the filter pipeline will call the RPC
        for authoritative metadata.
        """
        name   = ""
        symbol = ""
        for log in logs:
            if "name:" in log.lower():
                try:
                    name = log.split("name:")[-1].strip().split(",")[0].strip()[:50]
                except Exception:
                    pass
            if "symbol:" in log.lower():
                try:
                    symbol = log.split("symbol:")[-1].strip().split(",")[0].strip()[:20]
                except Exception:
                    pass
        return name, symbol
