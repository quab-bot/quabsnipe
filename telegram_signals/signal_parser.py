# telegram_signals/signal_parser.py
# Handles all Telethon message formats — text, message, raw_text

import re
from dataclasses import dataclass, field
from utils.logger import get_logger

logger = get_logger("signal_parser")


@dataclass
class SignalData:
    mint_address:        str   = ""
    token_name:          str   = ""
    token_symbol:        str   = ""
    channel:             str   = ""   # Which Telegram channel sent this signal
    age_minutes:         float = 0
    age_hours:           float = 0
    security_ok:         bool  = False
    is_new_trending:     bool  = False
    mc_usd:              float = 0
    mc_fdv_usd:          float = 0
    liq_usd:             float = 0
    vol_1h_usd:          float = 0
    vol_5m_usd:          float = 0
    fake_vol_usd:        float = 0
    fake_vol_pct:        float = 0
    holders:             int   = 0
    fake_holders:        int   = 0
    fake_holders_pct:    float = 0
    bundles_count:       int   = 0
    bundles_pct_initial: float = 0
    bundles_pct_now:     float = 0
    snipers_count:       int   = 0
    snipers_pct_initial: float = 0
    snipers_pct_now:     float = 0
    first20_pct_1:       float = 0
    first20_pct_2:       float = 0
    first20_pct_3:       float = 0
    dev_sol:             float = 0
    dev_pct:             float = 0
    dev_bundled_pct:     float = 0
    dev_sold_pct:        float = 0
    dev_airdrop_pct:     float = 0
    raw_text:            str   = ""
    source:              str   = "channel"


def _clean(text: str) -> str:
    """
    Normalize text for parsing — remove markdown links, zero-width chars,
    and other formatting that Telethon may or may not strip.
    Always returns plain parseable text.
    """
    if not text:
        return ""
    # Remove markdown links [text](url) → keep text
    text = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', text)
    # Remove bare URLs
    text = re.sub(r'https?://\S+', '', text)
    # Remove zero-width and invisible chars
    text = re.sub(r'[\u200b\u200c\u200d\u2060\ufeff\u00ad]', '', text)
    # Remove HTML entities
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    # Normalize whitespace but keep newlines
    lines = []
    for line in text.split('\n'):
        lines.append(' '.join(line.split()))
    return '\n'.join(lines)


def extract_ca_from_url(text: str) -> str | None:
    """Extract CA from Soul Sniper URL — fastest, most reliable method."""
    # Primary: Soul Sniper bot URL — take the LAST underscore-segment after start=
    # Handles all formats: start=CA, start=buy_CA, start=ref_channel_CA, etc.
    m = re.search(r'[Ss]oul_[Ss]niper_[Bb]ot[?/]start[=:]([^\s&]+)', text)
    if m:
        raw = m.group(1)
        ca  = raw.split('_')[-1]  # last segment is always the CA
        if re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', ca):
            return ca

    # Secondary: token URL in dexscreener/geckoterminal/solscan
    m = re.search(r'/tokens?/([1-9A-HJ-NP-Za-km-z]{32,44})', text)
    if m: return m.group(1)

    # Tertiary: pump address anywhere
    m = re.search(r'[1-9A-HJ-NP-Za-km-z]{32,44}pump', text)
    if m: return m.group(0)

    # Last resort: standalone base58 string on its own line (some channels post raw CA)
    for line in text.strip().split('\n')[:10]:
        line = line.strip()
        if re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', line):
            return line

    return None


def extract_ca_from_bot_response(text: str) -> str | None:
    """Extract CA from Soul Sniper bot response."""
    cleaned = _clean(text)
    for line in cleaned.strip().split('\n')[:8]:
        line = line.strip()
        if re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', line):
            return line
    m = re.search(r'[1-9A-HJ-NP-Za-km-z]{32,44}pump', cleaned)
    if m: return m.group(0)
    m = re.search(r'\b[1-9A-HJ-NP-Za-km-z]{43,44}\b', cleaned)
    if m: return m.group(0)
    return None


def parse_channel_signal(text: str) -> 'SignalData | None':
    """
    Parse signal from @solearlytrending.
    Works on raw Telethon text WITH or WITHOUT markdown stripped.
    """
    if not text:
        return None

    # Always extract CA from raw text first (before cleaning strips URLs)
    ca = extract_ca_from_url(text)

    # Clean text for field parsing
    t = _clean(text)

    if not any(k in t for k in ["MC:", "Liq:", "Hodls", "Bundles", "Snipers",
                                   "Age:", "Security"]):
        return None

    d = SignalData(raw_text=t)
    d.mint_address = ca or ""

    # Token name — multiple formats:
    # "🔥 Nietzschean New Trending"
    # "[**Clarity Act**](url) New Trending"  → after cleaning: "Clarity Act New Trending"
    # "🔥 ‎Kiba New Trending"
    nm = re.search(
        r'(?:🔥|📈|⚡|🌙)\s*[‎\s]*([^\n(]+?)(?:\s+New\s+Trending|\s+Trending|\s*\()',
        t
    )
    if nm:
        name = nm.group(1).strip()
        # Remove trailing punctuation/emojis
        name = re.sub(r'[\s\U0001F300-\U0001F9FF]+$', '', name).strip()
        d.token_name = name[:40]
    else:
        # Try first line if it has the token name
        first = t.strip().split('\n')[0]
        nm2 = re.search(r'[A-Za-z][A-Za-z0-9\s]{1,30}(?:\s+New\s+Trending|\s+Trending)', first)
        if nm2:
            d.token_name = nm2.group(0).replace("New Trending","").replace("Trending","").strip()[:40]

    d.is_new_trending = "New Trending" in t
    d.security_ok     = bool(re.search(r'Security:\s*✅', t))

    # Age — "Age: 50m" / "Age: 1h" / "Age: 1h 30m" / "Age: 5h"
    ah = re.search(r'Age:\s*(\d+)\s*h', t)
    am = re.search(r'Age:\s*(?:\d+\s*h\s*)?(\d+)\s*m', t)
    if ah: d.age_hours += float(ah.group(1))
    if am: d.age_hours += float(am.group(1)) / 60
    d.age_minutes = d.age_hours * 60

    # MC — "MC: $20K" or "MC: $20K • $40.9K"
    mc = re.search(r'MC:\s*\$?([\d,]+\.?\d*\s*[KkMmBb]?)', t)
    if mc: d.mc_usd = _amt(mc.group(1))
    fdv = re.search(r'(?:🔝|•\s*🔝|•)\s*\$?([\d,]+\.?\d*\s*[KkMmBb]?)', t)
    if fdv: d.mc_fdv_usd = _amt(fdv.group(1))

    # Liq — handles ALL formats:
    # "Liq: $10.9K" / "💧 Liq: $10.9K" / "vLiq: $12K" (pinned message)
    lq = re.search(r'(?:v?Liq|Liquidity):\s*\$?([\d,]+\.?\d*\s*[KkMmBb]?)', t, re.I)
    if lq:
        d.liq_usd = _amt(lq.group(1))
    else:
        # Last resort — look for dollar amount on the Liq line
        for line in t.split('\n'):
            if 'Liq' in line:
                m2 = re.search(r'\$?([\d,]+\.?\d*\s*[KkMmBb]?)', line)
                if m2:
                    val = _amt(m2.group(1))
                    if val > 0:
                        d.liq_usd = val
                        break

    # Vol — "Vol: 1h: $74.8K" / "Vol: $50.5K" / "Vol: 1h: $19.8K"
    v1 = re.search(r'Vol:.*?1h[:\s]*\$?([\d,]+\.?\d*\s*[KkMmBb]?)', t, re.I)
    if not v1:
        v1 = re.search(r'Vol:\s*\$?([\d,]+\.?\d*\s*[KkMmBb]?)', t)
    if v1: d.vol_1h_usd = _amt(v1.group(1))

    v5 = re.search(r'\$?([\d,]+\.?\d*\s*[KkMmBb]?)\s*\[5m\]', t)
    if v5: d.vol_5m_usd = _amt(v5.group(1))

    # Fake vol — "Fake: $384 [0.5%]" / "Fake: $0"
    fv = re.search(r'Fake[:\s]+\$?([\d,]+\.?\d*\s*[KkMmBb]?)(?:\s*\[([0-9.]+)%\])?', t)
    if fv:
        d.fake_vol_usd = _amt(fv.group(1))
        d.fake_vol_pct = float(fv.group(2)) if fv.group(2) else 0.0

    # Holders — "Hodls: 156" / "Hodls: 153 • CTO"
    h = re.search(r'Hodls?[:\s]*([\d,]+)', t)
    if h: d.holders = int(h.group(1).replace(',', ''))

    # Fake holders — "Fake: 39 [25.2%]" (bot response)
    fh = re.search(r'Fake:\s*(\d+)\s*\[([0-9.]+)%\]', t)
    if fh:
        d.fake_holders     = int(fh.group(1))
        d.fake_holders_pct = float(fh.group(2))

    # Bundles — "16 • 44% → 0%"
    b = re.search(r'Bundles?[:\s]*(\d+)\s*[•·]\s*([0-9.]+)%\s*[→>]\s*([0-9.]+)%', t)
    if b:
        d.bundles_count       = int(b.group(1))
        d.bundles_pct_initial = float(b.group(2))
        d.bundles_pct_now     = float(b.group(3))

    # Snipers — "29 • 29% → 0%"
    s = re.search(r'Snipers?[:\s]*(\d+)\s*[•·]\s*([0-9.]+)%\s*[→>]\s*([0-9.]+)%', t)
    if s:
        d.snipers_count       = int(s.group(1))
        d.snipers_pct_initial = float(s.group(2))
        d.snipers_pct_now     = float(s.group(3))

    # First 20 — "32% | 17% | 2%"
    f = re.search(r'First\s*20[:\s]*([0-9.]+)%\s*\|[^|]*?([0-9.]+)%\s*\|[^|]*?([0-9.]+)%', t)
    if f:
        d.first20_pct_1 = float(f.group(1))
        d.first20_pct_2 = float(f.group(2))
        d.first20_pct_3 = float(f.group(3))

    # Dev — "Dev: 31 SOL | 0%" or "Dev: 0 SOL | 0%"
    dv = re.search(r'Dev[:\s]+([0-9.]+)\s*SOL\s*\|\s*([0-9.]+)%', t)
    if dv:
        d.dev_sol = float(dv.group(1))
        d.dev_pct = float(dv.group(2))

    db = re.search(r'Bundled:\s*([0-9.]+)%', t)
    ds = re.search(r'Sold:\s*([0-9.]+)%', t)
    da = re.search(r'Airdrops?:\s*([0-9.]+)%', t)
    if db: d.dev_bundled_pct = float(db.group(1))
    if ds: d.dev_sold_pct    = float(ds.group(1))
    if da: d.dev_airdrop_pct = float(da.group(1))

    return d


def parse_simple_ca_signal(text: str) -> 'SignalData | None':
    """
    Lightweight parser for channels like BlocApe that post just a CA,
    optionally with a name and social links. No MC/Liq data in the message.
    We fetch everything from DexScreener ourselves after getting the CA.
    Skips ETH addresses automatically.
    """
    if not text:
        return None

    ETH = re.compile(r'0x[a-fA-F0-9]{40}')
    B58 = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')

    ca = None

    # 1. DexScreener Solana URL
    m = re.search(r'dexscreener\.com/solana/([1-9A-HJ-NP-Za-km-z]{32,44})', text)
    if m:
        ca = m.group(1)

    # 2. Bare CA on its own line (check last 8 lines — CA is usually at the bottom)
    if not ca:
        lines = [l.strip() for l in text.strip().split('\n') if l.strip()]
        for line in reversed(lines[-8:]):
            if ETH.match(line):
                continue
            if B58.fullmatch(line):
                ca = line
                break

    # 3. pump.fun CA anywhere in text
    if not ca:
        m = re.search(r'[1-9A-HJ-NP-Za-km-z]{32,44}pump', text)
        if m:
            ca = m.group(0)

    # 4. Any long base58 string
    if not ca:
        m = re.search(r'\b[1-9A-HJ-NP-Za-km-z]{43,44}\b', text)
        if m:
            ca = m.group(0)

    if not ca or ca.startswith('0x'):
        return None

    # Extract token name — first short non-URL non-CA line
    name = ""
    lines = [l.strip() for l in text.strip().split('\n') if l.strip()]
    for line in lines[:5]:
        if line.startswith('http'):
            continue
        if B58.fullmatch(line):
            continue
        if ETH.match(line):
            continue
        if 2 < len(line) < 60:
            name = line
            break

    d = SignalData(raw_text=text)
    d.mint_address = ca
    d.token_name   = name or ca[:8]
    d.token_symbol = name.split()[0].upper()[:8] if name else ca[:6].upper()
    d.source       = "simple_ca"
    return d


def enrich_signal_from_bot(data: SignalData, bot_text: str) -> SignalData:
    """Enrich with bot response data (more current numbers)."""
    data.source = "bot_response"
    if not data.mint_address:
        data.mint_address = extract_ca_from_bot_response(bot_text) or ""
    bot = parse_channel_signal(bot_text)
    if bot:
        if bot.mc_usd > 0:          data.mc_usd          = bot.mc_usd
        if bot.liq_usd > 0:         data.liq_usd         = bot.liq_usd
        if bot.vol_5m_usd > 0:      data.vol_5m_usd      = bot.vol_5m_usd
        if bot.fake_holders_pct > 0:
            data.fake_holders     = bot.fake_holders
            data.fake_holders_pct = bot.fake_holders_pct
    return data


def _amt(s: str) -> float:
    """Convert '$10.9K', '74.8K', '$20K' etc to float."""
    if not s: return 0.0
    s = re.sub(r'[,$\s]', '', str(s)).strip()
    mul = 1
    if s.upper().endswith('K'): mul = 1_000;           s = s[:-1]
    elif s.upper().endswith('M'): mul = 1_000_000;     s = s[:-1]
    elif s.upper().endswith('B'): mul = 1_000_000_000; s = s[:-1]
    try:
        return float(s) * mul
    except:
        return 0.0