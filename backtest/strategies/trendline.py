"""Trendlines through swing points.

    swing high/low: the highest/lowest bar of pivot_len bars on each side, so it is only
                    known pivot_len bars after it happened
    up line:        through the last two swing lows, the second higher than the first
    down line:      through the last two swing highs, the second lower than the first

mode=bounce: a fresh touch of the up line (low within touch_atr x ATR, close above it)
             -> long, stop stop_atr x ATR under the line; mirror for the down line -> short
mode=break:  a close below the up line -> short; a close above the down line -> long,
             stop stop_atr x ATR from the close
Target = rr x risk. A close through a line ends it; the next swing point builds a new one.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from backtest.strategies.common import atr, empty_signals, to_frame

DEFAULTS = {
    "mode": "bounce",  # bounce | break
    "pivot_len": 5,  # bars on each side of a swing point
    "min_gap": 10,  # bars between the two swing points of a line
    "atr_len": 14,
    "touch_atr": 0.2,  # how close counts as a touch (bounce mode)
    "stop_atr": 1.0,
    "rr": 2.0,
}


class _Line:
    def __init__(self, i1: int, p1: float, i2: int, p2: float):
        self.i2, self.p2 = i2, p2
        self.slope = (p2 - p1) / (i2 - i1)

    def at(self, i: int) -> float:
        return self.p2 + self.slope * (i - self.i2)


def _line(points: list[tuple[int, float]], rising: bool, min_gap: int) -> _Line | None:
    if len(points) < 2:
        return None
    (i1, p1), (i2, p2) = points[-2:]
    if i2 - i1 < min_gap or (p2 <= p1 if rising else p2 >= p1):
        return None
    return _Line(i1, p1, i2, p2)


def signals(bars: pd.DataFrame, *, mode, pivot_len, min_gap, atr_len, touch_atr, stop_atr, rr) -> pd.DataFrame:
    if mode not in ("bounce", "break"):
        raise ValueError(f"trendline mode must be bounce or break, got {mode!r}")
    h, l, c = (bars[k].to_numpy(dtype=float) for k in ("high", "low", "close"))
    a = atr(bars, atr_len)
    out = empty_signals(len(bars))
    p = pivot_len
    swing_lows: list[tuple[int, float]] = []
    swing_highs: list[tuple[int, float]] = []
    up: _Line | None = None
    down: _Line | None = None

    def emit(i: int, side: int, stop: float) -> None:
        out["signal"][i], out["stop"][i] = side, stop
        out["target"][i] = c[i] + side * rr * abs(c[i] - stop)

    for i in range(len(bars)):
        # 1. Swing points confirmed by this bar
        j = i - p
        if j - p >= 0:
            window = slice(j - p, i + 1)
            if np.argmin(l[window]) == p:  # first occurrence, so equal lows don't double count
                swing_lows.append((j, l[j]))
                up = _line(swing_lows, rising=True, min_gap=min_gap)
            if np.argmax(h[window]) == p:
                swing_highs.append((j, h[j]))
                down = _line(swing_highs, rising=False, min_gap=min_gap)
        if np.isnan(a[i]):
            continue

        # 2. Lines against this bar
        if up is not None:
            line = up.at(i)
            if c[i] < line:
                up = None
                if mode == "break":
                    emit(i, -1, c[i] + stop_atr * a[i])
            elif mode == "bounce" and l[i] <= line + touch_atr * a[i] \
                    and (i == 0 or l[i - 1] > up.at(i - 1) + touch_atr * a[i - 1]):
                emit(i, 1, line - stop_atr * a[i])
        if down is not None and out["signal"][i] == 0:
            line = down.at(i)
            if c[i] > line:
                down = None
                if mode == "break":
                    emit(i, 1, c[i] - stop_atr * a[i])
            elif mode == "bounce" and h[i] >= line - touch_atr * a[i] \
                    and (i == 0 or h[i - 1] < down.at(i - 1) - touch_atr * a[i - 1]):
                emit(i, -1, line + stop_atr * a[i])
        elif down is not None and c[i] > down.at(i):
            down = None  # broken on a bar that already signalled from the up line

    return to_frame(out)
