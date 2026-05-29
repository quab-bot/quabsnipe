# telegram_signals/channel_monitor.py
# Multi-channel support — one ChannelMonitor instance per channel.
# Pass channel_name at init; it tags every SignalData before calling on_signal.

import asyncio
import random
from datetime import datetime, timezone
from typing import Callable, Awaitable

from telethon import TelegramClient, events

from utils.logger import get_logger
from telegram_signals.signal_parser import (
    SignalData, parse_channel_signal, parse_simple_ca_signal, parse_simple_ca_signal,
    enrich_signal_from_bot, extract_ca_from_bot_response,
)

logger = get_logger("channel_monitor")

SOUL_SNIPER_BOT      = "Soul_Sniper_Bot"
POLL_INTERVAL        = 30
BOT_RESPONSE_TIMEOUT = 12
MIN_CLICK_DELAY      = 2.0
MAX_CLICK_DELAY      = 4.0


class ChannelMonitor:
    def __init__(
        self,
        client: TelegramClient,
        channel_username: str,
        on_signal: Callable[[SignalData], Awaitable[None]],
    ):
        self.client           = client
        self.channel_username = channel_username.lstrip("@").strip()
        self.on_signal        = on_signal
        self._running         = False
        self._channel_id      = None
        self._start_time      = datetime.now(timezone.utc)
        self._seen_msg_ids:  set[int] = set()
        self._processing:    set[int] = set()
        self._last_bot_text: str | None = None
        self._last_bot_time: datetime | None = None
        self._stats = {
            "detected": 0, "ca_from_url": 0,
            "ca_from_bot": 0, "skipped_old": 0, "errors": 0
        }

    async def start(self):
        self._running    = True
        self._start_time = datetime.now(timezone.utc)
        logger.info(f"Channel monitor → @{self.channel_username}")

        try:
            entity           = await self.client.get_entity(self.channel_username)
            self._channel_id = entity.id
            logger.info(f"✓ Channel: '{entity.title}' id={entity.id}")
        except Exception as e:
            logger.error(f"Cannot resolve @{self.channel_username}: {e}")
            return

        self.client.add_event_handler(
            self._on_channel_message,
            events.NewMessage(chats=self._channel_id)
        )
        self.client.add_event_handler(
            self._on_any_incoming,
            events.NewMessage(incoming=True)
        )
        logger.info(f"✓ Event handlers registered for @{self.channel_username}")

        try:
            msgs = await self.client.get_messages(self._channel_id, limit=10)
            for m in msgs:
                self._seen_msg_ids.add(m.id)
            logger.info(f"✓ Seeded {len(msgs)} existing message IDs")
        except Exception as e:
            logger.debug(f"Seed error: {e}")

        await asyncio.gather(self._poll_loop(), self._keepalive())

    async def stop(self):
        self._running = False

    async def _keepalive(self):
        while self._running:
            await asyncio.sleep(1)

    async def _poll_loop(self):
        logger.info(f"✓ Polling every {POLL_INTERVAL}s for @{self.channel_username}")
        while self._running:
            await asyncio.sleep(POLL_INTERVAL)
            try:
                await self._poll_channel()
            except Exception as e:
                logger.debug(f"Poll error (@{self.channel_username}): {e}")

    async def _poll_channel(self):
        if not self._channel_id:
            return
        msgs = await self.client.get_messages(self._channel_id, limit=5)
        for msg in reversed(msgs):
            if msg.id in self._seen_msg_ids:
                continue
            await self._handle_new_message(msg, source="POLL")

    async def _on_channel_message(self, event):
        msg = event.message
        if msg.id in self._seen_msg_ids:
            return
        await self._handle_new_message(msg, source="EVENT")

    async def _handle_new_message(self, msg, source: str):
        self._seen_msg_ids.add(msg.id)

        if msg.date:
            msg_time = msg.date
            if msg_time.tzinfo is None:
                msg_time = msg_time.replace(tzinfo=timezone.utc)
            if msg_time < self._start_time:
                self._stats["skipped_old"] += 1
                return

        text = self._get_text(msg)
        if not text:
            return

        # Accept message if it has a buy button OR if the text already contains a CA
        # (some channels embed the CA directly in the message without a bot button)
        from telegram_signals.signal_parser import extract_ca_from_url
        has_button = self._has_buy_button(msg.buttons)
        has_ca_in_text = bool(extract_ca_from_url(text))

        if not has_button and not has_ca_in_text:
            logger.debug(
                f"[SKIPPED|@{self.channel_username}] msg_id={msg.id} — "
                f"no buy button and no CA found in text"
            )
            return

        logger.info(
            f"[{source}|@{self.channel_username}] Signal msg_id={msg.id} | "
            f"{text[:60].replace(chr(10), ' ')}..."
        )
        self._stats["detected"] += 1

        # Choose parser based on channel — simple channels just post CA + optional links
        _SIMPLE_CA_CHANNELS = {"blocape_signal_calls", "bloc_ape_calls", "blocapecalls", "trenchfree"}
        if self.channel_username.lower().replace("-", "_") in _SIMPLE_CA_CHANNELS:
            signal_data = parse_simple_ca_signal(text)
        else:
            signal_data = parse_channel_signal(text)

        if signal_data:
            signal_data.channel = self.channel_username
            asyncio.create_task(self._process(msg, signal_data))
        else:
            logger.debug(
                f"[SKIPPED|@{self.channel_username}] msg_id={msg.id} — "
                f"not recognized as a signal"
            )

    def _get_text(self, msg) -> str:
        raw      = getattr(msg, 'raw_text',  None) or ""
        txt      = getattr(msg, 'text',      None) or ""
        msg_attr = getattr(msg, 'message',   None) or ""
        body     = max([raw, txt, msg_attr], key=len)

        # Also append any button URLs — Soul Sniper embeds CA in button URL
        # e.g. t.me/Soul_Sniper_Bot?start=ref_channel_CA
        extra_urls = []
        try:
            buttons = getattr(msg, 'buttons', None) or []
            for row in buttons:
                if not isinstance(row, list):
                    row = [row]
                for btn in row:
                    url = getattr(btn, 'url', None)
                    if url and ('Soul_Sniper' in url or 'start=' in url):
                        extra_urls.append(url)
        except Exception:
            pass

        if extra_urls:
            return body + "\n" + "\n".join(extra_urls)
        return body

    async def _on_any_incoming(self, event):
        msg  = event.message
        text = self._get_text(msg)
        if not text or len(text) < 20:
            return
        from telegram_signals.signal_parser import extract_ca_from_bot_response
        ca = extract_ca_from_bot_response(text)
        if not ca:
            return
        try:
            sender = await msg.get_sender()
            uname  = (getattr(sender, "username", "") or "").lower()
            if "soul" not in uname and "sniper" not in uname:
                return
        except Exception:
            pass
        self._last_bot_text = text
        self._last_bot_time = datetime.now(timezone.utc)

    async def _process(self, msg, signal_data: SignalData):
        if msg.id in self._processing:
            return
        self._processing.add(msg.id)
        try:
            if signal_data.mint_address:
                self._stats["ca_from_url"] += 1
                logger.info(
                    f"⚡ CA from URL: {signal_data.mint_address[:20]}... "
                    f"[@{self.channel_username}]"
                )
                await self.on_signal(signal_data)
                asyncio.create_task(self._background_enrich(msg, signal_data))
                return

            buy_btn = self._find_buy_button(msg.buttons)
            if not buy_btn:
                return

            delay = random.uniform(MIN_CLICK_DELAY, MAX_CLICK_DELAY)
            await asyncio.sleep(delay)

            click_time         = datetime.now(timezone.utc)
            self._last_bot_text = None
            await msg.click(data=buy_btn.data)

            bot_text = await self._wait_bot(click_time)
            if not bot_text:
                logger.warning(f"No bot response for msg {msg.id} (@{self.channel_username})")
                return

            signal_data = enrich_signal_from_bot(signal_data, bot_text)
            self._stats["ca_from_bot"] += 1

            if signal_data.mint_address:
                signal_data.channel = self.channel_username
                await self.on_signal(signal_data)

        except Exception as e:
            logger.error(f"Process error (@{self.channel_username}): {e}")
            self._stats["errors"] += 1
        finally:
            self._processing.discard(msg.id)

    async def _background_enrich(self, msg, signal_data: SignalData):
        try:
            buy_btn = self._find_buy_button(msg.buttons)
            if not buy_btn:
                return
            await asyncio.sleep(random.uniform(3, 6))
            click_time         = datetime.now(timezone.utc)
            self._last_bot_text = None
            await msg.click(data=buy_btn.data)
            await self._wait_bot(click_time)
        except Exception as e:
            logger.debug(f"Background enrich error: {e}")

    async def _wait_bot(self, since: datetime) -> str | None:
        for i in range(BOT_RESPONSE_TIMEOUT):
            await asyncio.sleep(1)
            if (self._last_bot_text and self._last_bot_time
                    and self._last_bot_time > since):
                return self._last_bot_text
            if i >= 3:
                try:
                    msgs = await self.client.get_messages(SOUL_SNIPER_BOT, limit=2)
                    for m in msgs:
                        if not m.date:
                            continue
                        mdt = m.date.replace(tzinfo=timezone.utc) if m.date.tzinfo is None else m.date
                        if mdt > since:
                            text = self._get_text(m)
                            from telegram_signals.signal_parser import extract_ca_from_bot_response
                            if extract_ca_from_bot_response(text):
                                return text
                except Exception:
                    pass
        return None

    def _has_buy_button(self, buttons) -> bool:
        return self._find_buy_button(buttons) is not None

    def _find_buy_button(self, buttons):
        if not buttons:
            return None
        for row in buttons:
            if not isinstance(row, list):
                row = [row]
            for btn in row:
                if hasattr(btn, "text") and "Buy" in btn.text and "SOL" not in btn.text:
                    return btn
        return None

    def stats(self) -> dict:
        return {**self._stats, "channel": self.channel_username}