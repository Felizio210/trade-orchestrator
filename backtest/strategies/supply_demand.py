"""Supply and demand: trade the first retest of a fresh base -> impulse zone.

    demand zone: 1..base_max small-bodied candles, then a big up candle that closes above them
                 proximal = highest base body, distal = lowest base low
    supply zone: the same with a big down candle
                 proximal = lowest base body, distal = highest base high

A zone is live from the bar after its impulse. The first bar that reaches the proximal
line signals long (demand) / short (supply), stop just past the distal line, target rr x
risk; the zone is then used up. A close beyond the distal line deletes the zone.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from backtest.strategies.common import atr, empty_signals, to_frame

DEFAULTS = {
    "atr_len": 14,
    "base_max": 3,  # most candles in a base
    "base_body_atr": 0.5,  # a base candle's body is at most this x ATR
    "impulse_body_atr": 1.5,  # the impulse candle's body is at least this x ATR
    "stop_buffer_atr": 0.1,  # stop this far past the distal line
    "rr": 2.0,  # target = rr x risk
    "max_zones": 5,  # live zones kept per side, newest first
    "zone_max_age": 500,  # bars a zone stays live
}


def signals(bars: pd.DataFrame, *, atr_len, base_max, base_body_atr, impulse_body_atr, stop_buffer_atr,
            rr, max_zones, zone_max_age) -> pd.DataFrame:
    o, h, l, c = (bars[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close"))
    a = atr(bars, atr_len)
    body = np.abs(c - o)
    body_top, body_bottom = np.maximum(o, c), np.minimum(o, c)
    out = empty_signals(len(bars))
    # zone = [proximal, distal, created_at]
    demand: list[list[float]] = []
    supply: list[list[float]] = []

    for i in range(len(bars)):
        if np.isnan(a[i]):
            continue

        # 1. Existing zones against this bar (zones made on this bar only count from the next one)
        demand[:] = [z for z in demand if i - z[2] <= zone_max_age and c[i] >= z[1]]
        supply[:] = [z for z in supply if i - z[2] <= zone_max_age and c[i] <= z[1]]
        for z in demand:  # newest first
            if l[i] <= z[0]:
                stop = z[1] - stop_buffer_atr * a[i]
                out["signal"][i], out["stop"][i], out["target"][i] = 1, stop, c[i] + rr * (c[i] - stop)
                demand.remove(z)
                break
        if out["signal"][i] == 0:
            for z in supply:
                if h[i] >= z[0]:
                    stop = z[1] + stop_buffer_atr * a[i]
                    out["signal"][i], out["stop"][i], out["target"][i] = -1, stop, c[i] - rr * (stop - c[i])
                    supply.remove(z)
                    break

        # 2. Is this bar an impulse leaving a base?
        if body[i] < impulse_body_atr * a[i]:
            continue
        k = 0
        while k < base_max and i - k - 1 >= 0 and not np.isnan(a[i - k - 1]) \
                and body[i - k - 1] <= base_body_atr * a[i - k - 1]:
            k += 1
        if k == 0:
            continue
        base = slice(i - k, i)
        if c[i] > o[i] and c[i] > body_top[base].max():
            demand.insert(0, [body_top[base].max(), l[base].min(), i])
            del demand[max_zones:]
        elif c[i] < o[i] and c[i] < body_bottom[base].min():
            supply.insert(0, [body_bottom[base].min(), h[base].max(), i])
            del supply[max_zones:]

    return to_frame(out)
