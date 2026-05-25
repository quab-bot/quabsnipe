#!/usr/bin/env python3
# scripts/export_training_data.py
# Export all completed trades as a JSON + CSV dataset for ML training
#
# Usage:
#   python scripts/export_training_data.py
#   python scripts/export_training_data.py --format csv
#   python scripts/export_training_data.py --format json

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/sniper.db")


def export_json(rows: list, out: Path):
    with open(out, "w") as f:
        json.dump(rows, f, indent=2)
    print(f"✓ Exported {len(rows)} records → {out}")


def export_csv(rows: list, out: Path):
    if not rows:
        print("No records to export.")
        return
    import csv
    keys = list(rows[0].keys())
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    print(f"✓ Exported {len(rows)} records → {out}")


def main():
    parser = argparse.ArgumentParser(description="Export Quabsnipe training data")
    parser.add_argument("--format", choices=["json", "csv", "both"], default="both")
    parser.add_argument("--min-trades", type=int, default=0,
                        help="Only export if at least N completed trades exist")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print("❌ No database found. Run python setup_db.py first.")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # ── Trade outcomes (main ML training table) ────────────────────────────────
    trade_rows = conn.execute("""
        SELECT
            mint, symbol, source,
            entry_time, entry_price_usd, entry_mc_usd, entry_liq_usd,
            entry_age_min, entry_risk_score, entry_verdict,
            entry_snipers_pct, entry_bundles_pct, entry_dev_pct,
            entry_fake_vol_pct, entry_holders, entry_fake_holders_pct,
            entry_security_ok, entry_sol_spent, entry_usd_spent,
            exit_time, exit_reason, hold_hours,
            peak_multiplier, pnl_usd, pnl_pct,
            tp_levels_hit_json, outcome_label
        FROM trade_log
        WHERE exit_time IS NOT NULL
          AND outcome_label NOT IN ('ongoing', '')
        ORDER BY entry_time
    """).fetchall()

    trades = []
    for r in trade_rows:
        d = dict(r)
        d["tp_levels_hit"] = json.loads(d.pop("tp_levels_hit_json") or "[]")
        trades.append(d)

    # ── Full funnel: every token seen (rejected + bought) ─────────────────────
    seen_rows = conn.execute("""
        SELECT mint, source, seen_at, mc_usd, liq_usd, age_min,
               risk_score, verdict, reject_reason, flags_json,
               was_bought, outcome_label
        FROM token_seen_log
        ORDER BY seen_at
    """).fetchall()

    seen = []
    for r in seen_rows:
        d = dict(r)
        d["flags"] = json.loads(d.pop("flags_json") or "[]")
        seen.append(d)

    conn.close()

    if args.min_trades > 0 and len(trades) < args.min_trades:
        print(f"Only {len(trades)} completed trades — need {args.min_trades} to export.")
        sys.exit(0)

    out_dir = Path("data/training")
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")

    if args.format in ("json", "both"):
        export_json(trades, out_dir / f"trades_{ts}.json")
        export_json(seen,   out_dir / f"funnel_{ts}.json")

    if args.format in ("csv", "both"):
        export_csv(trades, out_dir / f"trades_{ts}.csv")
        export_csv(seen,   out_dir / f"funnel_{ts}.csv")

    # Print label distribution
    from collections import Counter
    labels = Counter(t["outcome_label"] for t in trades)
    print("\n📊 Outcome distribution:")
    for label, count in sorted(labels.items(), key=lambda x: -x[1]):
        pct = count / len(trades) * 100 if trades else 0
        print(f"   {label:12s} {count:4d}  ({pct:.1f}%)")

    print(f"\n🧠 Full funnel: {len(seen)} tokens seen (including rejected)")
    print(f"   Win rate so far: {sum(1 for t in trades if t['pnl_usd'] > 0)}/{len(trades)}")


if __name__ == "__main__":
    main()
