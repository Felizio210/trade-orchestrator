"""Tick -> OHLCV bar aggregation."""

from __future__ import annotations

import pandas as pd

BAR_COLUMNS = ["ts", "open", "high", "low", "close", "volume"]


def ticks_to_bars(ticks: pd.DataFrame, timeframe: str = "1min", side: str = "bid") -> pd.DataFrame:
    """Resample ticks into OHLCV bars. Bars with no ticks are dropped (not forward-filled).

    `ts` is the bar open time (UTC). `volume` is bid_volume + ask_volume as reported by
    Dukascopy (millions of units for FX).
    """
    if ticks.empty:
        return pd.DataFrame(columns=BAR_COLUMNS)
    df = ticks.set_index("ts")
    price = (df["bid"] + df["ask"]) / 2 if side == "mid" else df[side]
    grouped = price.resample(timeframe, label="left", closed="left")
    bars = grouped.ohlc()
    bars["volume"] = (df["bid_volume"] + df["ask_volume"]).resample(timeframe).sum()
    bars["volume"] = bars["volume"].round(6)
    bars = bars[grouped.count() > 0]
    return bars.reset_index()[BAR_COLUMNS]


def candles_to_bars(bid: pd.DataFrame, ask: pd.DataFrame, side: str = "bid") -> pd.DataFrame:
    """Combine Dukascopy BID and ASK 1-minute candles into the same bar shape as ticks_to_bars.

    OHLC and volume (bid + ask) match tick-built bars exactly. side="mid" averages the two sides' OHLC, also an approximation for high/low.
    Minutes with no volume on either side (market closed) are dropped, as in ticks_to_bars.
    """
    if bid.empty or ask.empty:
        return pd.DataFrame(columns=BAR_COLUMNS)
    m = bid.merge(ask, on="ts", suffixes=("_bid", "_ask"), validate="one_to_one")
    ohlc = ["open", "high", "low", "close"]
    if side == "mid":
        prices = {c: (m[f"{c}_bid"] + m[f"{c}_ask"]) / 2 for c in ohlc}
    else:
        prices = {c: m[f"{c}_{side}"] for c in ohlc}
    bars = pd.DataFrame({"ts": m["ts"], **prices})
    bars["volume"] = (m["volume_bid"] + m["volume_ask"]).round(6)
    bars = bars[bars["volume"] > 0]
    return bars.reset_index(drop=True)[BAR_COLUMNS]


def resample_bars(bars: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Aggregate finer OHLCV bars into coarser ones (open first, high max, low min, close last, volume sum)."""
    if bars.empty or timeframe == "1min":
        return bars
    r = bars.set_index("ts").resample(timeframe, label="left", closed="left")
    out = r.agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
    out["volume"] = out["volume"].round(6)
    return out[r["open"].count() > 0].reset_index()[BAR_COLUMNS]
