# ai/scorer.py
# XGBoost signal scorer — predicts win probability for every incoming signal.
# Trained on paper trade history. Returns 0.0-1.0 probability.
# Bot only trades signals above the configured threshold.
#
# Features used (all available at signal time, no lookahead):
#   - Hour of day (UTC)
#   - Day of week
#   - Channel
#   - Time-of-day buckets from historical analysis
#
# Retrain by running: python ai/train.py

import os
import math
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("ai_scorer")

MODEL_PATH    = Path(__file__).parent / "signal_model.pkl"
FEATURES_PATH = Path(__file__).parent / "feature_names.pkl"

# Minimum confidence to trade — 0.50 means take all, 0.60 means be selective
AI_THRESHOLD = float(os.getenv("AI_THRESHOLD", "0.52"))

# Channel ID map — must match training data
CHANNEL_MAP = {
    "soltrenchtrending":    0,
    "solearlytrending":     1,
    "trenchfree":           2,
    "blocape_signal_calls": 3,
    "BlocApe_Signal_Calls": 3,
}

# Good/bad hours from historical analysis (general)
GOOD_HOURS = {3, 4, 21}
BAD_HOURS  = {5, 8, 9, 11, 12, 18, 23}

# Per-channel hour restrictions — updated June 4 from data analysis
CHANNEL_GOOD_HOURS = {
    "solearlytrending":     {1, 2, 4, 6, 7},
    "soltrenchtrending":    None,   # no good-hour restriction — use bad-hour block instead
    "trenchfree":           None,
    "BlocApe_Signal_Calls": None,
    "blocape_signal_calls": None,
}

# Per-channel bad hours — hard skip regardless of AI score
CHANNEL_BAD_HOURS = {
    "solearlytrending":  {8, 9, 13, 15, 17, 20, 21, 22},
    "soltrenchtrending": {8, 13, 15, 20, 21},   # worst hours from June 4 data
}


class SignalScorer:
    def __init__(self):
        self._model        = None
        self._feature_names= None
        self._loaded       = False
        self._load()

    def _load(self):
        try:
            import joblib
            if MODEL_PATH.exists() and FEATURES_PATH.exists():
                self._model         = joblib.load(MODEL_PATH)
                self._feature_names = joblib.load(FEATURES_PATH)
                self._loaded        = True
                logger.info(f"AI scorer loaded — threshold: {AI_THRESHOLD:.0%}")
            else:
                logger.warning("AI model not found — scoring disabled, all signals will trade")
        except Exception as e:
            logger.error(f"AI scorer load error: {e}")

    def _build_features(self, channel: str, dt: datetime) -> dict:
        hour         = dt.hour
        dow          = dt.weekday()
        ch_id        = CHANNEL_MAP.get(channel, CHANNEL_MAP.get(channel.lower(), 4))
        is_soltrench = 1 if ch_id == 0 else 0
        _good_h      = {0, 3, 6, 7, 11, 12, 16, 22}
        _bad_h       = {8, 13, 15, 20, 21}
        return {
            "hour":           hour,
            "dow":            dow,
            "is_weekend":     1 if dow >= 5 else 0,
            "channel_id":     ch_id,
            "hour_sin":       math.sin(2 * math.pi * hour / 24),
            "hour_cos":       math.cos(2 * math.pi * hour / 24),
            "dow_sin":        math.sin(2 * math.pi * dow / 7),
            "dow_cos":        math.cos(2 * math.pi * dow / 7),
            "is_soltrench":   is_soltrench,
            "is_good_hour":   1 if (is_soltrench and hour in _good_h) else 0,
            "is_bad_hour":    1 if (is_soltrench and hour in _bad_h) else 0,
            "is_best_hour":   1 if hour in {3, 12, 16} else 0,
            "hour_in_asia":   1 if 0 <= hour <= 8 else 0,
            "hour_in_london": 1 if 8 <= hour <= 16 else 0,
            "hour_in_us":     1 if 16 <= hour <= 23 else 0,
        }

    def score(self, channel: str, dt: datetime = None) -> float:
        """
        Returns win probability 0.0-1.0.
        If model not loaded, returns 1.0 (always trade).
        """
        if not self._loaded or self._model is None:
            return 1.0

        if dt is None:
            dt = datetime.now(timezone.utc)

        try:
            import pandas as pd
            features = self._build_features(channel, dt)
            X = pd.DataFrame([features])[self._feature_names]
            prob = float(self._model.predict_proba(X)[0][1])
            return prob
        except Exception as e:
            logger.error(f"AI scoring error: {e}")
            return 1.0

    def should_trade(self, channel: str, dt: datetime = None) -> tuple[bool, float]:
        """
        Returns (should_trade, probability).
        Uses AI_THRESHOLD env var to decide.
        Also applies per-channel hour restrictions.
        """
        if dt is None:
            dt = datetime.now(timezone.utc)

        hour = dt.hour
        ch   = channel.lower().replace("-", "_")

        # Check channel-specific bad hours first — hard skip
        bad = CHANNEL_BAD_HOURS.get(ch, CHANNEL_BAD_HOURS.get(channel, set()))
        if bad and hour in bad:
            return False, 0.0

        # Check channel-specific good hours — only trade in these
        good = CHANNEL_GOOD_HOURS.get(ch, CHANNEL_GOOD_HOURS.get(channel))
        if good is not None and hour not in good:
            return False, 0.0

        prob = self.score(channel, dt)
        return prob >= AI_THRESHOLD, prob

    def reload(self):
        """Reload model from disk — call after retraining."""
        self._loaded = False
        self._load()


# Singleton
_scorer = None

def get_scorer() -> SignalScorer:
    global _scorer
    if _scorer is None:
        _scorer = SignalScorer()
    return _scorer
