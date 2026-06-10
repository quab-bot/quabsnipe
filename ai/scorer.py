# ai/scorer.py
# XGBoost signal scorer — retrained June 8 on 1168 trades across all datasets.
# Only trades during proven profitable hours. Hard whitelist for soltrenchtrending.

import os
import math
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("ai_scorer")

MODEL_PATH    = Path(__file__).parent / "signal_model.pkl"
FEATURES_PATH = Path(__file__).parent / "feature_names.pkl"

# Minimum confidence to trade
AI_THRESHOLD = float(os.getenv("AI_THRESHOLD", "0.52"))

# Channel ID map
CHANNEL_MAP = {
    "soltrenchtrending":    0,
    "solearlytrending":     1,
    "trenchfree":           2,
    "blocape_signal_calls": 3,
    "BlocApe_Signal_Calls": 3,
}

# STRICT whitelist for soltrenchtrending — only trade these hours (UTC)
# Based on 1168 trades: 02,10,12,13 UTC = 65% WR, +$12.58
# Everything else is net negative
STRICT_TRADE_HOURS = {2, 10, 12, 13}

# Per-channel good hours (whitelist)
CHANNEL_GOOD_HOURS = {
    "solearlytrending": {1, 2, 6},
}

# Per-channel bad hours (blacklist — skip regardless of AI score)
CHANNEL_BAD_HOURS = {
    "solearlytrending":  {8, 9, 13, 15, 17, 20, 21, 22},
    "soltrenchtrending": {0, 4, 9, 14, 16, 17, 18, 19, 22, 23},
}


class SignalScorer:
    def __init__(self):
        self._model         = None
        self._feature_names = None
        self._loaded        = False
        self._load()

    def _load(self):
        try:
            import joblib
            if MODEL_PATH.exists() and FEATURES_PATH.exists():
                self._model         = joblib.load(MODEL_PATH)
                self._feature_names = joblib.load(FEATURES_PATH)
                self._loaded        = True
                logger.info(f"AI scorer loaded — threshold: {AI_THRESHOLD:.0%} | features: {len(self._feature_names)}")
            else:
                logger.warning("AI model not found — all signals will trade (no filtering)")
        except Exception as e:
            logger.error(f"AI scorer load error: {e}")

    def _build_features(self, channel: str, dt: datetime) -> dict:
        hour  = dt.hour
        dow   = dt.weekday()
        ch_id = CHANNEL_MAP.get(channel, CHANNEL_MAP.get(channel.lower(), 4))
        return {
            "hour":           hour,
            "dow":            dow,
            "is_weekend":     1 if dow >= 5 else 0,
            "channel_id":     ch_id,
            "hour_sin":       math.sin(2 * math.pi * hour / 24),
            "hour_cos":       math.cos(2 * math.pi * hour / 24),
            "dow_sin":        math.sin(2 * math.pi * dow / 7),
            "dow_cos":        math.cos(2 * math.pi * dow / 7),
            "is_soltrench":   1 if ch_id == 0 else 0,
            "is_strict_good": 1 if hour in {2, 10, 12, 13} else 0,
            "is_good_hour":   1 if hour in {1, 2, 5, 6, 10, 12, 13} else 0,
            "is_bad_hour":    1 if hour in {0, 4, 9, 14, 16, 17, 18, 19, 22, 23} else 0,
            "hour_in_asia":   1 if 0 <= hour <= 8 else 0,
            "hour_in_london": 1 if 8 <= hour <= 16 else 0,
            "hour_in_us":     1 if 16 <= hour <= 23 else 0,
            "is_best_hour":   1 if hour in {2, 12} else 0,
        }

    def score(self, channel: str, dt: datetime = None) -> float:
        """Returns win probability 0.0-1.0. Returns 1.0 if model not loaded."""
        if not self._loaded or self._model is None:
            return 1.0
        if dt is None:
            dt = datetime.now(timezone.utc)
        try:
            import pandas as pd
            features = self._build_features(channel, dt)
            X = pd.DataFrame([features])[self._feature_names]
            return float(self._model.predict_proba(X)[0][1])
        except Exception as e:
            logger.error(f"AI scoring error: {e}")
            return 1.0

    def should_trade(self, channel: str, dt: datetime = None) -> tuple[bool, float]:
        """Returns (should_trade, probability)."""
        if dt is None:
            dt = datetime.now(timezone.utc)

        hour = dt.hour
        ch   = channel.lower().replace("-", "_")

        # soltrenchtrending: STRICT whitelist — only proven hours
        if "soltrench" in ch:
            if hour not in STRICT_TRADE_HOURS:
                return False, 0.0

        # Bad hour blacklist
        bad = CHANNEL_BAD_HOURS.get(ch, CHANNEL_BAD_HOURS.get(channel, set()))
        if bad and hour in bad:
            return False, 0.0

        # Good hour whitelist
        good = CHANNEL_GOOD_HOURS.get(ch, CHANNEL_GOOD_HOURS.get(channel))
        if good is not None and hour not in good:
            return False, 0.0

        prob = self.score(channel, dt)
        return prob >= AI_THRESHOLD, prob

    def reload(self):
        self._loaded = False
        self._load()


_scorer = None

def get_scorer() -> SignalScorer:
    global _scorer
    if _scorer is None:
        _scorer = SignalScorer()
    return _scorer
