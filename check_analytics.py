# check_analytics.py — run anytime in a second terminal
# python check_analytics.py

import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB = Path("data/sniper.db")

if not DB.exists():
    print("No database — run: python setup_db.py  then  python main.py")
    exit()

conn = sqlite3.connect(str(DB))
conn.row_factory = sqlite3.Row

print("\n" + "="*65)
print(f"  SNIPER ANALYTICS — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
print("="*65)

row = conn.execute("""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN verdict='reject' THEN 1 ELSE 0 END) as rejected,
        SUM(CASE WHEN verdict IN ('clean','caution') THEN 1 ELSE 0 END) as passed,
        SUM(CASE WHEN verdict='caution' THEN 1 ELSE 0 END) as caution,
        SUM(CASE WHEN rejection_stage='stage1' THEN 1 ELSE 0 END) as s1,
        SUM(CASE WHEN rejection_stage='stage2' THEN 1 ELSE 0 END) as s2,
        SUM(CASE WHEN rejection_stage IN ('stage3','reject') THEN 1 ELSE 0 END) as s3,
        ROUND(AVG(CASE WHEN liquidity_usd>0 AND liquidity_usd<10000000
                       THEN liquidity_usd END),0) as avg_liq,
        ROUND(MIN(CASE WHEN liquidity_usd>0 THEN liquidity_usd END),0) as min_liq,
        ROUND(MAX(CASE WHEN liquidity_usd<10000000 THEN liquidity_usd END),0) as max_liq,
        ROUND(AVG(CASE WHEN risk_score>0 THEN risk_score END),1) as avg_score
    FROM token_analytics
    WHERE rejection_stage NOT IN ('cache') OR rejection_stage IS NULL
""").fetchone()

total  = row["total"] or 0
passed = row["passed"] or 0
rate   = (passed/total*100) if total > 0 else 0

print(f"\n TOTAL UNIQUE TOKENS ANALYZED: {total}")
print(f"   Passed:   {passed} ({rate:.1f}%)" + (f"  (caution mode: {row['caution']})" if row['caution'] else ""))
print(f"   Rejected: {row['rejected']}")
print(f"\n REJECTION BY STAGE")
print(f"   Stage 1 (cheap):    {row['s1']:5d}  liquidity / name / RugCheck")
print(f"   Stage 2 (medium):   {row['s2']:5d}  dev wallet / holders / MC ratio")
print(f"   Stage 3 (expensive):{row['s3']:5d}  honeypot / metadata")
print(f"\n LIQUIDITY AT ANALYSIS (post-75s delay)")
print(f"   Min: ${row['min_liq'] or 0:>12,.0f}")
print(f"   Avg: ${row['avg_liq'] or 0:>12,.0f}")
print(f"   Max: ${row['max_liq'] or 0:>12,.0f}")
print(f"   Avg risk score: {row['avg_score'] or 0:.1f}")

print(f"\n LIQUIDITY DISTRIBUTION")
buckets = [
    ("< $500",       "liquidity_usd > 0 AND liquidity_usd < 500"),
    ("$500-$2k",     "liquidity_usd >= 500 AND liquidity_usd < 2000"),
    ("$2k-$5k",      "liquidity_usd >= 2000 AND liquidity_usd < 5000"),
    ("$5k-$10k",     "liquidity_usd >= 5000 AND liquidity_usd < 10000"),
    ("$10k-$100k",   "liquidity_usd >= 10000 AND liquidity_usd < 100000"),
    ("> $100k",      "liquidity_usd >= 100000 AND liquidity_usd < 10000000"),
]
for label, cond in buckets:
    cnt = conn.execute(f"SELECT COUNT(*) FROM token_analytics WHERE {cond}").fetchone()[0]
    bar = "=" * min(int(cnt/max(total,1)*40), 40)
    print(f"   {label:<14}  {cnt:5d}  {bar}")

print(f"\n TOP REJECTION REASONS")
rows = conn.execute("""
    SELECT rejection_reason, COUNT(*) as cnt
    FROM token_analytics
    WHERE verdict='reject'
      AND rejection_reason IS NOT NULL AND rejection_reason != ''
      AND rejection_reason NOT IN ('cached_reject','cache')
    GROUP BY rejection_reason ORDER BY cnt DESC LIMIT 15
""").fetchall()
for r in rows:
    bar = "=" * min(int(r["cnt"]/max(total,1)*50), 50)
    print(f"   {r['cnt']:5d}x  {r['rejection_reason'][:42]:<42}  {bar}")

too_old = conn.execute(
    "SELECT COUNT(*) FROM token_analytics WHERE rejection_reason LIKE 'token_too_old%'"
).fetchone()[0]
if too_old:
    print(f"\n   (Age filter: {too_old} Raydium tokens rejected as too old >30min)")

print(f"\n SOURCE BREAKDOWN")
sources = conn.execute("""
    SELECT source,
           COUNT(*) as cnt,
           ROUND(AVG(CASE WHEN liquidity_usd<10000000 THEN liquidity_usd END),0) as avg_liq,
           SUM(CASE WHEN verdict IN ('clean','caution') THEN 1 ELSE 0 END) as passed,
           SUM(CASE WHEN rejection_stage='stage1' THEN 1 ELSE 0 END) as s1,
           SUM(CASE WHEN rejection_stage='stage2' THEN 1 ELSE 0 END) as s2
    FROM token_analytics
    WHERE rejection_stage NOT IN ('cache') OR rejection_stage IS NULL
    GROUP BY source
""").fetchall()
for s in sources:
    print(f"   {(s['source'] or '?').upper():<10}  "
          f"Total:{s['cnt']:5d}  S1:{s['s1']:4d}  S2:{s['s2']:4d}  "
          f"Pass:{s['passed']:3d}  AvgLiq:${s['avg_liq'] or 0:>9,.0f}")

print(f"\n PASSED TOKENS")
pr = conn.execute("""
    SELECT mint, source, liquidity_usd, market_cap_usd,
           risk_score, verdict, detected_at
    FROM token_analytics
    WHERE verdict IN ('clean','caution')
    ORDER BY detected_at DESC LIMIT 10
""").fetchall()
if pr:
    for p in pr:
        print(f"   [{p['verdict'].upper():<7}] {p['mint'][:20]}...  "
              f"Liq:${p['liquidity_usd']:>8,.0f}  "
              f"Score:{p['risk_score']:3d}  "
              f"{(p['source'] or '').upper():<8} {(p['detected_at'] or '')[:16]}")
else:
    print("   None yet — normal during calibration phase")
    print("   The 75s delay means first signals appear ~2min after launch")

recent = conn.execute(
    "SELECT COUNT(*) FROM token_analytics WHERE detected_at >= datetime('now','-10 minutes')"
).fetchone()[0]
oldest = conn.execute("SELECT MIN(detected_at) FROM token_analytics").fetchone()[0]
print(f"\n   Active: {recent} tokens analyzed in last 10min")
if oldest:
    print(f"   Tracking since: {oldest[:16]} UTC")
print("\n" + "="*65 + "\n")
conn.close()
