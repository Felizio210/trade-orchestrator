"""Channel breakout: trade a close beyond the highest high / lowest low of the last `length` bars.

    long:   close > highest high of the previous `length` bars, and close > slow EMA
    short:  close < lowest low of the previous `length` bars, and close < slow EMA
    stop:   stop_atr x ATR from the close
    target: rr x risk
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from backtest.strategies.common import atr, empty_signals, to_frame

DEFAULTS = {
    "length": 48,  # bars of the channel (two days of 1h bars)
    "slow": 200,  # trend filter
    "atr_len": 14,
    "stop_atr": 1.5,  # chosen on Nov 2024-Oct 2025 XAUUSD 1h, then checked on the year after
    "rr": 2.0,
}


def signals(bars: pd.DataFrame, *, length, slow, atr_len, stop_atr, rr) -> pd.DataFrame:
    c = bars["close"].to_numpy(dtype=float)
    hi = bars["high"].rolling(length).max().shift().to_numpy()  # previous `length` bars, not this one
    lo = bars["low"].rolling(length).min().shift().to_numpy()
    es = bars["close"].ewm(span=slow, adjust=False, min_periods=slow).mean().to_numpy()
    a = atr(bars, atr_len)
    out = empty_signals(len(bars))
    for i in range(len(bars)):
        if np.isnan(a[i]) or np.isnan(hi[i]) or np.isnan(es[i]):
            continue
        if c[i] > hi[i] and c[i] > es[i]:
            stop = c[i] - stop_atr * a[i]
            out["signal"][i], out["stop"][i], out["target"][i] = 1, stop, c[i] + rr * (c[i] - stop)
        elif c[i] < lo[i] and c[i] < es[i]:
            stop = c[i] + stop_atr * a[i]
            out["signal"][i], out["stop"][i], out["target"][i] = -1, stop, c[i] - rr * (stop - c[i])
    return to_frame(out)
