"""Indicators shared by strategies. All of them are causal: the value at bar t only
uses bars up to and including t."""

from __future__ import annotations

import numpy as np
import pandas as pd


def atr(bars: pd.DataFrame, length: int = 14) -> np.ndarray:
    """Wilder's Average True Range; NaN for the first length-1 bars."""
    prev_close = bars["close"].shift()
    true_range = pd.concat([
        bars["high"] - bars["low"],
        (bars["high"] - prev_close).abs(),
        (bars["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    return true_range.ewm(alpha=1 / length, adjust=False, min_periods=length).mean().to_numpy()


def empty_signals(n: int) -> dict[str, np.ndarray]:
    return {"signal": np.zeros(n, dtype=np.int8), "stop": np.full(n, np.nan), "target": np.full(n, np.nan)}


def to_frame(sig: dict[str, np.ndarray]) -> pd.DataFrame:
    return pd.DataFrame(sig)
