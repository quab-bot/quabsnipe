# utils/models.py — Shared data models (dataclasses) used across all modules

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class NewTokenEvent:
    """Emitted by the detector when a new token pair is discovered."""
    mint_address: str           # The new token's mint address
    pair_address: str           # LP pair / pool address
    base_mint: str              # Usually the new token
    quote_mint: str             # Usually WSOL or USDC
    source: str                 # "raydium" | "pumpfun"
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    initial_liquidity_sol: float = 0.0
    initial_price_usd: float = 0.0
    market_cap_usd: float = 0.0
    tx_signature: str = ""      # The transaction that created the pair
    raw_data: dict = field(default_factory=dict)  # Full raw response for debugging


@dataclass
class FilterResult:
    """Result from running the 7-filter pipeline."""
    passed: bool
    token: NewTokenEvent
    failed_at: Optional[str] = None   # e.g. "filter_1_rugcheck"
    fail_reason: Optional[str] = None
    rugcheck_score: Optional[int] = None
    filter_details: dict = field(default_factory=dict)


@dataclass
class Position:
    """An open trade position."""
    mint_address: str
    token_name: str
    token_symbol: str
    entry_price_usd: float
    entry_time: datetime
    sol_amount: float           # SOL spent
    usd_amount: float           # USD value spent
    tokens_held: float          # Current token balance
    tokens_initial: float       # Tokens at entry (before any TPs)
    pair_address: str
    source: str                 # "raydium" | "pumpfun"

    # Exit tracking
    phase: int = 1              # 1 = fixed SL, 2 = trailing stop active
    highest_price_usd: float = 0.0
    trailing_stop_price: float = 0.0
    fixed_stop_price: float = 0.0
    tp_levels_hit: list = field(default_factory=list)
    total_profit_usd: float = 0.0
    extended_at: Optional[datetime] = None

    def __post_init__(self):
        if self.highest_price_usd == 0.0:
            self.highest_price_usd = self.entry_price_usd
        if self.fixed_stop_price == 0.0:
            self.fixed_stop_price = self.entry_price_usd * 0.60  # 40% below entry
