# telegram_signals/session_client.py
# Telethon user session wrapper for reading Telegram channels
# FIX: This file was missing from v7 — caused ImportError on startup when
# SIGNAL_ENABLED=true. Created as a proper Telethon session manager.

from telethon import TelegramClient
from utils.logger import get_logger

logger = get_logger("session_client")


class SessionClient:
    """
    Wraps a Telethon TelegramClient for reading channel messages.
    Requires TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE from .env
    """

    def __init__(self, api_id: int, api_hash: str, phone: str):
        self._api_id   = api_id
        self._api_hash = api_hash
        self._phone    = phone
        self._client   = TelegramClient(
            "sniper_session",  # session file name
            api_id,
            api_hash,
        )

    async def start(self):
        """Connect and authenticate the Telegram client."""
        logger.info("Starting Telegram session client...")
        await self._client.start(phone=self._phone)
        me = await self._client.get_me()
        logger.info(f"✅ Telegram session active — logged in as @{me.username or me.id}")

    def get_client(self) -> TelegramClient:
        """Return the raw Telethon client (used by ChannelMonitor)."""
        return self._client

    async def stop(self):
        """Disconnect cleanly."""
        if self._client.is_connected():
            await self._client.disconnect()
            logger.info("Telegram session disconnected")
