"""ICT Silver Bullet: inside a one-hour New York window, a liquidity sweep, then a fair
value gap in the other direction, traded on the first retrace into the gap.

    window:   the New York hours in `windows` (default 03-04, 10-11, 14-15 local, DST-aware)
    sweep:    price trades below the low (above the high) of the `liq_bars` bars before the
              window opened
    FVG:      after a sell-side sweep, a bullish gap: high[i-2] < low[i], with an up candle
              at i-1 whose body is at least disp_body_atr x ATR (bearish: the mirror image)
    entry:    the first later bar in the window that trades back into the gap and closes
              inside or above it signals long; one that closes below the gap kills it
    stop:     past the window's extreme (the sweep), plus stop_buffer_atr x ATR
    target:   rr x risk

One trade per window. Bars use their open time, so a bar is in a window when it opens in it.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from backtest.strategies.common import atr, empty_signals, to_frame

DEFAULTS = {
    "windows": "3,10,14",  # New York start hours of the one-hour windows
    "liq_bars": 12,  # bars before the window whose high/low is the liquidity to sweep
    "atr_len": 14,
    "disp_body_atr": 1.0,  # the displacement candle's body is at least this x ATR
    "min_gap_atr": 0.1,  # the gap is at least this x ATR
    "stop_buffer_atr": 0.1,  # stop this far past the window's extreme
    "rr": 2.0,  # target = rr x risk
}


def signals(bars: pd.DataFrame, *, windows, liq_bars, atr_len, disp_body_atr, min_gap_atr, stop_buffer_atr,
            rr) -> pd.DataFrame:
    o, h, l, c = (bars[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close"))
    a = atr(bars, atr_len)
    hours = {int(x) for x in str(windows).split(",") if x.strip()}
    ny = pd.DatetimeIndex(bars["ts"]).tz_convert("America/New_York")
    keys = [(d, hr) if hr in hours else None for d, hr in zip(ny.date, ny.hour)]
    out = empty_signals(len(bars))

    key, active = None, False
    for i in range(len(bars)):
        if keys[i] != key:  # a new window (or none)
            key = keys[i]
            if key is None or i < liq_bars:
                active = False
                continue
            active, start = True, i
            ref_hi, ref_lo = h[i - liq_bars:i].max(), l[i - liq_bars:i].min()
            swept_lo = swept_hi = None  # bar of the first sweep
            bull: list[tuple[float, float]] = []  # (top, bottom) of live gaps
            bear: list[tuple[float, float]] = []
        if not active or np.isnan(a[i]):
            continue
        win_lo, win_hi = l[start:i + 1].min(), h[start:i + 1].max()

        # 1. Retrace into a gap made on an earlier bar
        for top, bottom in list(bull):
            if l[i] <= top:
                bull.remove((top, bottom))
                stop = win_lo - stop_buffer_atr * a[i]
                if c[i] >= bottom and stop < c[i]:
                    out["signal"][i], out["stop"][i], out["target"][i] = 1, stop, c[i] + rr * (c[i] - stop)
                    break
        if out["signal"][i] == 0:
            for bottom, top in list(bear):
                if h[i] >= bottom:
                    bear.remove((bottom, top))
                    stop = win_hi + stop_buffer_atr * a[i]
                    if c[i] <= top and stop > c[i]:
                        out["signal"][i], out["stop"][i], out["target"][i] = -1, stop, c[i] - rr * (stop - c[i])
                        break
        if out["signal"][i] != 0:
            active = False  # one trade per window
            continue

        # 2. Sweeps, then gaps that close on this bar (tradable from the next one)
        if swept_lo is None and l[i] < ref_lo:
            swept_lo = i
        if swept_hi is None and h[i] > ref_hi:
            swept_hi = i
        if i - 1 < start or np.isnan(a[i - 1]):
            continue
        displacement = abs(c[i - 1] - o[i - 1]) >= disp_body_atr * a[i - 1]
        if swept_lo is not None and swept_lo <= i - 1 and displacement and c[i - 1] > o[i - 1] \
                and l[i] - h[i - 2] >= min_gap_atr * a[i]:
            bull.insert(0, (l[i], h[i - 2]))
        if swept_hi is not None and swept_hi <= i - 1 and displacement and c[i - 1] < o[i - 1] \
                and l[i - 2] - h[i] >= min_gap_atr * a[i]:
            bear.insert(0, (h[i], l[i - 2]))

    return to_frame(out)
