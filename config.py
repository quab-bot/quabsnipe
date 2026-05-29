# config.py — Central configuration loader
# All settings come from .env — never hardcode secrets here

import os
from dotenv import load_dotenv

load_dotenv()

# ── RPC / API ──────────────────────────────────────────────────────────────────
HELIUS_API_KEY      = os.getenv("HELIUS_API_KEY", "")
HELIUS_RPC_URL      = os.getenv("HELIUS_RPC_URL", f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}")
HELIUS_WS_URL       = os.getenv("HELIUS_WS_URL",  f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}")
RUGCHECK_API_URL    = os.getenv("RUGCHECK_API_URL", "https://api.rugcheck.xyz/v1")

# ── Wallets ────────────────────────────────────────────────────────────────────
SNIPER_WALLET_PRIVATE_KEY = os.getenv("SNIPER_WALLET_PRIVATE_KEY", "")
PROFIT_WALLET_ADDRESS     = os.getenv("PROFIT_WALLET_ADDRESS", "")
COLD_WALLET_ADDRESS       = os.getenv("COLD_WALLET_ADDRESS", "")

# ── Telegram ───────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Position Sizing ────────────────────────────────────────────────────────────
SNIPER_POSITION = float(os.getenv("SNIPER_POSITION", "0.10"))  # % of wallet per sniper trade (0.10 = 10%)
POSITION_SIZE_PCT = SNIPER_POSITION  # internal alias — code uses POSITION_SIZE_PCT
MIN_TRADE_USD     = float(os.getenv("MIN_TRADE_USD", "2"))
MAX_TRADE_USD     = float(os.getenv("MAX_TRADE_USD", "500"))

# ── Exit Logic ─────────────────────────────────────────────────────────────────
# IMPROVED: Phase 2 (trailing) triggers earlier at +15% gain instead of +30%
# Reasoning: many meme coins pump 20% then dump — catching the trail sooner
# means more trades enter trailing protection before they reverse.
STOP_LOSS_PHASE1_PCT      = 0.25   # 25% below entry — tightened from 40%
TRAILING_STOP_PCT         = 0.30   # 30% below highest price (default, overridden by dynamic)
PHASE1_DURATION_SECONDS   = 120    # 2 minutes — faster trail activation
PHASE2_TRIGGER_GAIN_PCT   = 0.15   # IMPROVED: was 0.30 — trail kicks in at +15% not +30%

# IMPROVED: Income lock sells 50% at 2x instead of 70%
# Reasoning: 70% sell at 2x leaves only 30% riding. On a 50x coin, leaving 50%
# riding from 2x means 25x on half your position — much better expected value.
INCOME_LOCK_SELL_PCT      = 0.50   # IMPROVED: was 0.70 — sell 50% at 2x, ride 50%
INCOME_LOCK_MULTIPLIER    = 2.0    # 2x triggers income lock

TAKE_PROFIT_SELL_PCT      = 0.20   # Sell 20% at each TP level
# IMPROVED: Added 3x level to fill the gap between 2x income lock and 5x
# Previously: 2x → 5x gap with no partial takes
# Now:        2x → 3x → 5x → 10x → 15x → 20x → every 5x after
TAKE_PROFIT_LEVELS        = [3, 5, 10, 15, 20]  # IMPROVED: added 3x
TP_STEP_AFTER_20X         = 5      # Every 5x after 20x
MAX_HOLD_HOURS            = 72
EXTEND_HOLD_HOURS         = 24
DAILY_CHECK_HOURS         = 24

# ── Filter Thresholds ──────────────────────────────────────────────────────────
# Calibrated from live paper trading data — May 2026
RUGCHECK_MAX_SCORE        = 50    # Relaxed from 30 — score of 30 was too strict
MIN_LIQUIDITY_USD         = 500   # Updated — $500 floor after 75s delay
MIN_LIQUIDITY_TO_MCAP_PCT = 0.03  # Relaxed from 5% — meme coins often launch thin
DEV_WALLET_MIN_AGE_DAYS   = 3     # Relaxed from 7 days — many legit devs are newer
DEV_WALLET_MIN_TX_COUNT   = 5     # Relaxed from 10 — 5 txs is enough signal
FIRST_BUYER_SCAN_SECONDS  = 10    # Scan wallets that bought in first 10 sec

# ── Fees ───────────────────────────────────────────────────────────────────────
PRIORITY_FEE_USD = 0.15
DEX_FEE_PCT      = 0.0025  # 0.25%

# ── Token Name Blacklist ───────────────────────────────────────────────────────
TOKEN_NAME_BLACKLIST_KEYWORDS = [
    "OFFICIAL", "REAL", "2.0", "V2", "V3", "LEGIT",
    "SAFE", "SCAM", "TEST", "FAKE",
]

# ── Known Program IDs ──────────────────────────────────────────────────────────
RAYDIUM_AMM_PROGRAM_ID  = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_CPMM_PROGRAM_ID = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
PUMPFUN_PROGRAM_ID      = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
WSOL_MINT               = "So11111111111111111111111111111111111111112"
USDC_MINT               = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# ── Runtime ────────────────────────────────────────────────────────────────────
LOG_LEVEL       = "INFO"
BOT_PAUSED      = False   # Toggled via /pause Telegram command
MAX_OPEN_TRADES = 5       # Max simultaneous open positions

# ── Risk guards ─────────────────────────────────────────────────────────────────
DAILY_LOSS_LIMIT_PCT = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.30"))  # 30% daily loss → auto-pause
LOW_BALANCE_USD      = float(os.getenv("LOW_BALANCE_USD", "5.0"))         # Pause below $5

# ── Bot reliability ─────────────────────────────────────────────────────────────
HEARTBEAT_INTERVAL_HOURS = int(os.getenv("HEARTBEAT_INTERVAL_HOURS", "6"))  # Alive ping every 6h

def validate():
    """Call at startup to ensure critical config is present."""
    required = {
        "HELIUS_API_KEY": HELIUS_API_KEY,
        "SNIPER_WALLET_PRIVATE_KEY": SNIPER_WALLET_PRIVATE_KEY,
        "PROFIT_WALLET_ADDRESS": PROFIT_WALLET_ADDRESS,
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            f"Copy .env.example to .env and fill in your values."
        )

# ── Telegram Signal Automation ─────────────────────────────────────────────────
TELEGRAM_API_ID    = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH  = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE     = os.getenv("TELEGRAM_PHONE", "")

# Signal channel settings
SIGNAL_CHANNEL     = "solearlytrending"
SIGNAL_CHANNEL_2   = "soltrenchtrending"
SIGNAL_CHANNEL_3   = "BlocApe_Signal_Calls"
SIGNAL_CHANNEL_4   = "trenchfree"
SIGNAL_BOT         = "Soul_Sniper_Bot"
# ON = LIVE (real signal trades), OFF = PAPER (logs only) — default is OFF (safe)
_signal_raw    = os.getenv("SIGNAL_TRADING", "off").strip().lower()
SIGNAL_LIVE    = (_signal_raw == "on")   # True = execute real trades, False = paper only
SIGNAL_TRADING = SIGNAL_LIVE             # alias
SIGNAL_ENABLED = True                    # channel listener always runs — controlled by SIGNAL_TRADING for live/paper

# ── Signal channels list (comma-separated, overridable via /addchannel) ─────────
def _parse_channels(raw: str) -> list:
    """Parse channel list — strips accidental KEY=value prefix from Railway vars."""
    channels = []
    for c in raw.split(","):
        c = c.strip()
        if "=" in c:
            c = c.split("=")[-1].strip()
        if c:
            channels.append(c)
    return channels

SIGNAL_CHANNELS = _parse_channels(
    os.getenv("SIGNAL_CHANNELS", f"{SIGNAL_CHANNEL},{SIGNAL_CHANNEL_2},{SIGNAL_CHANNEL_3},{SIGNAL_CHANNEL_4}")
)

# ── Apply runtime overrides (from /sethelius, /setsignal etc saved to JSON) ──────
def _apply_runtime_overrides():
    """Load data/runtime_config.json and override in-memory config values."""
    import json
    from pathlib import Path
    p = Path("data/runtime_config.json")
    if not p.exists():
        return
    try:
        overrides = json.loads(p.read_text())
        g = globals()
        _numeric = {
            "SIGNAL_POSITION_USD":   float,
            "SIGNAL_POSITION_SOL":   float,
            "SNIPER_POSITION":       float,
            "POSITION_SIZE_PCT":     float,
            "DAILY_LOSS_LIMIT_PCT":  float,
            "LOW_BALANCE_USD":       float,
        }
        for key, cast in _numeric.items():
            if key in overrides:
                g[key] = cast(overrides[key])
        # Helius key (requires restart to re-init connections)
        if "HELIUS_API_KEY" in overrides and overrides["HELIUS_API_KEY"]:
            g["HELIUS_API_KEY"] = overrides["HELIUS_API_KEY"]
    except Exception:
        pass  # Never crash on a config override error

_apply_runtime_overrides()

# ── Signal-specific trading config ────────────────────────────────────────────
# These are different from sniper bot settings — signal tokens are older/proven

# Position sizing for signal trades
SIGNAL_POSITION_USD = float(os.getenv("SIGNAL_POSITION_USD", "3.0"))  # Fixed USD per signal trade (default $3)
SIGNAL_POSITION_SOL = SIGNAL_POSITION_USD / 86.0  # Converted to SOL at ~$86/SOL; recalculated live at trade time
SIGNAL_POSITION_MIN_SOL  = 0.01    # Minimum
SIGNAL_POSITION_MAX_SOL  = 0.10    # Maximum

# Exit logic for signal trades
SIGNAL_STOP_LOSS_PCT     = 0.25    # 25% stop loss (tighter — signal tokens move fast)

# IMPROVED: Start trailing at +30% gain instead of +50%
# Reasoning: waiting for +50% before trailing activates means any reversal before
# that level only has a 25% hard stop. Starting at +30% locks in gains sooner.
SIGNAL_TRAILING_START    = 0.30    # IMPROVED: was 0.50 — trail activates at +30%
SIGNAL_TRAILING_PCT      = 0.20    # Trail 20% below highest (tighter than sniper)

# Take profit ladder for signals
# IMPROVED: Added 1.5x level for faster partial exit on weaker signals
SIGNAL_TP_LEVELS = [
    (1.5,  0.20),   # IMPROVED: 1.5x → sell 20% early (quick partial, was not present)
    (2.0,  0.25),   # 2x  → sell 25% of remainder (was 30% of full)
    (3.0,  0.25),   # 3x  → sell 25% of remainder
    (5.0,  0.25),   # 5x  → sell 25% of remainder
    (10.0, 0.50),   # 10x → sell 50% of remainder
]
# Remainder rides with trailing stop