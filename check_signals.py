# check_signals.py — run in second terminal to confirm signal monitor is working
# python check_signals.py

import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB = Path("data/sniper.db")
if not DB.exists():
    print("No database yet — run: python setup_db.py")
    exit()

conn = sqlite3.connect(str(DB))
conn.execute("PRAGMA journal_mode=WAL")
conn.row_factory = sqlite3.Row

print("\n" + "="*55)
print(f"  SIGNAL MONITOR STATUS — {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
print("="*55)

# All sources active
print("\n SOURCES DETECTED")
rows = conn.execute("""
    SELECT source, COUNT(*) as cnt, MAX(detected_at) as last
    FROM token_analytics
    GROUP BY source
    ORDER BY cnt DESC
""").fetchall()
if rows:
    for r in rows:
        print(f"  {(r['source'] or 'unknown'):<22} count={r['cnt']:5d}  last={str(r['last'] or '')[:16]}")
else:
    print("  No data yet — bot still starting up")

# Telegram signal tokens specifically
print("\n TELEGRAM SIGNALS (last 10)")
sigs = conn.execute("""
    SELECT mint, detected_at, liquidity_usd, market_cap_usd, verdict, rejection_reason
    FROM token_analytics
    WHERE source = 'telegram_signal'
    ORDER BY detected_at DESC
    LIMIT 10
""").fetchall()
if sigs:
    for s in sigs:
        status = s['verdict'].upper() if s['verdict'] else '?'
        reason = s['rejection_reason'] or ''
        print(
            f"  [{status:<7}] {s['mint'][:20]}...  "
            f"Liq:${s['liquidity_usd'] or 0:>8,.0f}  "
            f"MC:${s['market_cap_usd'] or 0:>10,.0f}  "
            f"{reason[:30]}"
        )
else:
    print("  None yet — waiting for @solearlytrending to post a signal")
    print("  The channel posts roughly every few minutes during active hours")

# Recent activity last 5 min
recent = conn.execute("""
    SELECT COUNT(*) FROM token_analytics
    WHERE detected_at >= datetime('now', '-5 minutes')
""").fetchone()[0]
total = conn.execute("SELECT COUNT(*) FROM token_analytics").fetchone()[0]
print(f"\n ACTIVITY: {recent} tokens analyzed in last 5min | {total} total")

# Check if session file exists (signal monitor auth)
session = Path("data/telegram_session.session")
if session.exists():
    print(f" SESSION:  Telegram session file found ✓")
else:
    print(f" SESSION:  No session file — signal monitor may not be authenticated")

print("="*55 + "\n")
conn.close()
