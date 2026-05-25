# core/runtime_config.py
# Persist runtime setting changes made via Telegram commands.
# Changes survive bot restarts and override .env defaults.
#
# Usage:
#   from core.runtime_config import get, set_value, get_all
#   set_value("SIGNAL_POSITION_SOL", 0.05)
#   val = get("SIGNAL_POSITION_SOL", default=0.03)

import json
import threading
from pathlib import Path

from utils.logger import get_logger

logger     = get_logger("runtime_config")
_PATH      = Path("data/runtime_config.json")
_lock      = threading.Lock()
_cache: dict = {}


def _load():
    global _cache
    try:
        if _PATH.exists():
            with open(_PATH) as f:
                _cache = json.load(f)
    except Exception as e:
        logger.debug(f"runtime_config load error: {e}")
        _cache = {}


def _save():
    try:
        _PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_PATH, "w") as f:
            json.dump(_cache, f, indent=2)
    except Exception as e:
        logger.debug(f"runtime_config save error: {e}")


# Load on import
_load()


def get(key: str, default=None):
    with _lock:
        return _cache.get(key, default)


def set_value(key: str, value):
    with _lock:
        _cache[key] = value
        _save()
    logger.info(f"Runtime config updated: {key} = {value}")


def delete(key: str):
    with _lock:
        _cache.pop(key, None)
        _save()


def get_all() -> dict:
    with _lock:
        return dict(_cache)


def get_channels() -> list:
    """Returns the live list of signal channels."""
    import config
    with _lock:
        return list(_cache.get("SIGNAL_CHANNELS", config.SIGNAL_CHANNELS))


def add_channel(username: str) -> bool:
    username = username.lstrip("@").strip().lower()
    channels = get_channels()
    if username in channels:
        return False
    channels.append(username)
    set_value("SIGNAL_CHANNELS", channels)
    return True


def remove_channel(username: str) -> bool:
    username  = username.lstrip("@").strip().lower()
    channels  = get_channels()
    if username not in channels:
        return False
    channels.remove(username)
    set_value("SIGNAL_CHANNELS", channels)
    return True


def get_blacklist() -> list:
    with _lock:
        return list(_cache.get("BLACKLIST", []))


def blacklist_add(mint: str, reason: str = "") -> bool:
    bl = get_blacklist()
    if mint in [b["mint"] for b in bl]:
        return False
    from datetime import datetime, timezone
    bl.append({"mint": mint, "reason": reason,
                "added_at": datetime.now(timezone.utc).isoformat()})
    set_value("BLACKLIST", bl)
    return True


def blacklist_remove(mint: str) -> bool:
    bl = get_blacklist()
    new_bl = [b for b in bl if b["mint"] != mint]
    if len(new_bl) == len(bl):
        return False
    set_value("BLACKLIST", new_bl)
    return True


def is_blacklisted(mint: str) -> bool:
    return any(b["mint"] == mint for b in get_blacklist())
