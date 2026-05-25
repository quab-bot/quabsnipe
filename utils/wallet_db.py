# utils/wallet_db.py — Local database of known bad wallets
# Persisted as a JSON file so it survives restarts and grows over time.

import json
import threading
from pathlib import Path
from utils.logger import get_logger

logger = get_logger("wallet_db")

DB_PATH = Path("data/bad_wallets.json")
DB_PATH.parent.mkdir(exist_ok=True)

_lock = threading.Lock()


def _load() -> dict:
    if not DB_PATH.exists():
        return {"ruggers": [], "bundlers": [], "snipers": [], "rug_funders": []}
    try:
        with open(DB_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load wallet DB: {e}")
        return {"ruggers": [], "bundlers": [], "snipers": [], "rug_funders": []}


def _save(db: dict):
    with open(DB_PATH, "w") as f:
        json.dump(db, f, indent=2)


def is_known_bad(address: str) -> bool:
    """Return True if wallet is in any bad wallet list."""
    with _lock:
        db = _load()
        for category in db.values():
            if address in category:
                return True
    return False


def get_category(address: str) -> str | None:
    """Return which bad category a wallet belongs to, or None."""
    with _lock:
        db = _load()
        for cat, wallets in db.items():
            if address in wallets:
                return cat
    return None


def add_bad_wallet(address: str, category: str = "ruggers"):
    """Add a wallet to the bad wallet database."""
    valid = {"ruggers", "bundlers", "snipers", "rug_funders"}
    if category not in valid:
        raise ValueError(f"Category must be one of {valid}")
    with _lock:
        db = _load()
        if address not in db[category]:
            db[category].append(address)
            _save(db)
            logger.info(f"Added {address[:8]}... to bad wallet DB [{category}]")


def add_many_bad_wallets(addresses: list[str], category: str = "ruggers"):
    """Bulk add wallets to the bad wallet database."""
    with _lock:
        db = _load()
        added = 0
        for addr in addresses:
            if addr not in db.get(category, []):
                db.setdefault(category, []).append(addr)
                added += 1
        _save(db)
        logger.info(f"Bulk added {added} wallets to [{category}]")


def stats() -> dict:
    """Return count of bad wallets per category."""
    db = _load()
    return {cat: len(wallets) for cat, wallets in db.items()}
