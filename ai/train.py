# ai/train.py
# Retrain the XGBoost signal scorer on latest paper trade data.
# Run from the project root: python ai/train.py
#
# Automatically exports paper trades from the DB, trains the model,
# evaluates it, and saves to ai/signal_model.pkl

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from datetime import datetime
import xgboost as xgb
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import classification_report, roc_auc_score
import joblib
from pathlib import Path

# Load from DB or CSV
def load_trades():
    # Try DB first
    try:
        from core.database import paper_get_all_trades
        rows = paper_get_all_trades()
        closed = [r for r in rows if r['status'] == 'closed' and r['final_pnl_usd'] is not None]
        print(f"Loaded {len(closed)} closed trades from DB")
        return closed, 'db'
    except Exception as e:
        print(f"DB load failed ({e}), trying CSV...")

    # Fallback to CSV
    csv_files = list(Path('data').glob('paper_trades_*.csv')) + list(Path('.').glob('paper_trades_*.csv'))
    if csv_files:
        csv_file = sorted(csv_files)[-1]
        df = pd.read_csv(csv_file, engine='python', on_bad_lines='skip')
        closed = df[df['status'] == 'closed'].to_dict('records')
        print(f"Loaded {len(closed)} closed trades from {csv_file}")
        return closed, 'csv'

    raise FileNotFoundError("No trade data found. Export CSV with /paper export first.")


CHANNEL_MAP = {
    "soltrenchtrending":    0,
    "solearlytrending":     1,
    "trenchfree":           2,
    "BlocApe_Signal_Calls": 3,
    "blocape_signal_calls": 3,
}
GOOD_HOURS = {3, 4, 21}
BAD_HOURS  = {5, 8, 9, 11, 12, 18, 23}


def build_features(row):
    try:
        entry_time = row.get('entry_time') or row.get('entry_time', '')
        dt = datetime.fromisoformat(str(entry_time).replace('Z', ''))
        hour = dt.hour
        dow  = dt.weekday()
        ch   = str(row.get('channel', ''))
        import math
        return {
            'hour':         hour,
            'dow':          dow,
            'is_weekend':   1 if dow >= 5 else 0,
            'is_good_hour': 1 if hour in GOOD_HOURS else 0,
            'is_bad_hour':  1 if hour in BAD_HOURS else 0,
            'channel_id':   CHANNEL_MAP.get(ch, 4),
            'hour_sin':     math.sin(2 * math.pi * hour / 24),
            'hour_cos':     math.cos(2 * math.pi * hour / 24),
            'dow_sin':      math.sin(2 * math.pi * dow / 7),
            'dow_cos':      math.cos(2 * math.pi * dow / 7),
        }
    except Exception as e:
        return None


def main():
    print("=" * 55)
    print("  QUABSNIPE AI — Signal Scorer Training")
    print("=" * 55)

    trades, source = load_trades()

    features, labels = [], []
    for row in trades:
        f = build_features(row)
        if f is None:
            continue
        pnl = float(row.get('final_pnl_usd') or 0)
        features.append(f)
        labels.append(1 if pnl > 0 else 0)

    X = pd.DataFrame(features)
    y = np.array(labels)

    print(f"\nDataset: {len(y)} trades | Win rate: {y.mean()*100:.1f}%")
    print(f"Features: {list(X.columns)}")

    if len(y) < 50:
        print("\nNot enough data — need at least 50 closed trades.")
        sys.exit(1)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        scale_pos_weight=(y == 0).sum() / (y == 1).sum(),
        random_state=42,
        eval_metric='logloss',
        verbosity=0,
    )

    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    y_pred      = model.predict(X_test)
    y_pred_prob = model.predict_proba(X_test)[:, 1]

    print("\n=== MODEL PERFORMANCE ===")
    print(classification_report(y_test, y_pred, target_names=['loss', 'win']))
    print(f"AUC-ROC: {roc_auc_score(y_test, y_pred_prob):.3f}")

    cv_scores = cross_val_score(model, X, y, cv=5, scoring='roc_auc')
    print(f"5-fold CV AUC: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")

    print("\n=== FEATURE IMPORTANCE ===")
    for feat, imp in sorted(zip(X.columns, model.feature_importances_), key=lambda x: -x[1]):
        print(f"  {feat:20s}: {imp:.3f} {'█' * int(imp * 80)}")

    print("\n=== CONFIDENCE THRESHOLD SIMULATION ===")
    all_probs = model.predict_proba(X)[:, 1]
    best_threshold = 0.50
    best_wr = 0
    for threshold in [0.40, 0.45, 0.50, 0.52, 0.55, 0.60, 0.65]:
        mask   = all_probs >= threshold
        subset = y[mask]
        if len(subset) < 10:
            continue
        wr = subset.mean() * 100
        pct_taken = mask.sum() / len(y) * 100
        print(f"  {threshold:.0%} threshold: {mask.sum():4d} trades ({pct_taken:.0f}% of signals) | WR: {wr:.1f}%")
        if wr > best_wr:
            best_wr = wr
            best_threshold = threshold

    print(f"\n→ Recommended threshold: {best_threshold:.0%} (WR: {best_wr:.1f}%)")
    print(f"  Set AI_THRESHOLD={best_threshold:.2f} in Railway Variables")

    # Save
    model_path    = Path(__file__).parent / 'signal_model.pkl'
    features_path = Path(__file__).parent / 'feature_names.pkl'
    joblib.dump(model, model_path)
    joblib.dump(list(X.columns), features_path)
    print(f"\n✅ Model saved → {model_path}")
    print(f"✅ Features saved → {features_path}")
    print("\nDeploy the updated model files and Railway will use them automatically.")


if __name__ == '__main__':
    main()
