"""Live trading against its backtest: does an approved strategy trade the way it was approved?

For each strategy/timeframe/symbol in approvals, the trades of the backtest it was judged on are
lined up with the positions trading actually opened over the same span:

  * the span starts at the latest of: TRADING_DRIFT_DAYS ago, when trading started tracking the
    timeframe/symbol, when the strategy was last approved; it ends where the backtest's data ends
  * a live position matches a backtest trade with the same side and entry bar (live fills are
    floored to the bar they happened in: a realtime broker fills a minute or so into the bar the
    backtest enters at the open of)
  * an unmatched backtest trade is "explained" when live trading had its signal and skipped or
    rejected it (risk limits, kill switch, stale bar, price past the stop), otherwise "unexplained"
  * entry slippage is how much worse the live entry was than the backtest's, in R (the backtest
    trade's distance to its stop): positive = worse

Written to trading.drift, one row per key and day.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from backtest.config import BacktestSettings
from backtest.tables import table_id as bt_table
from cloud import Cloud
from trading import store
from trading.config import TradingSettings
from trading.executor import signal_id

log = logging.getLogger(__name__)


def _since(events: pd.DataFrame, kind: str, ref: str) -> pd.Timestamp | None:
    rows = events[(events["kind"] == kind) & (events["ref"] == ref)]
    return rows["ts"].max() if len(rows) else None


def compare(settings: TradingSettings, backtest: BacktestSettings, cloud: Cloud,
            *, now: datetime | None = None) -> list[dict]:
    now = pd.Timestamp(now or datetime.now(timezone.utc))
    ds = settings.dataset
    approvals = store.read(cloud, ds, "approvals")
    if approvals.empty:
        return []
    events = store.read(cloud, ds, "events", "kind IN ('tracking', 'approved')")
    runs = cloud.bq.query(f"SELECT run_id, end_date FROM `{bt_table(backtest.dataset, 'runs')}`")
    run_end = dict(zip(runs["run_id"], pd.to_datetime(runs["end_date"]).dt.tz_localize("UTC")))
    rows = []
    for a in approvals.itertuples():
        key = f"{a.strategy}:{a.timeframe}:{a.symbol}"
        tracking = _since(events, "tracking", f"{a.timeframe}:{a.symbol}")
        approved = _since(events, "approved", key)
        if tracking is None or approved is None or a.run_id not in run_end:
            continue  # never traded live
        start = max(now - timedelta(days=settings.drift_days), tracking, approved)
        end = min(now, run_end[a.run_id] + timedelta(days=1))
        if start >= end:
            continue
        span = pd.Timedelta(a.timeframe)
        live = store.read(cloud, ds, "positions",
                          "strategy = @s AND timeframe = @t AND symbol = @y AND entry_ts >= @a AND entry_ts < @b",
                          {"s": a.strategy, "t": a.timeframe, "y": a.symbol, "a": start, "b": end})
        bt = cloud.bq.query(
            f"SELECT side, entry_ts, entry_price, stop, r_multiple FROM `{bt_table(backtest.dataset, 'trades')}` "
            "WHERE run_id = @r AND symbol = @y AND entry_ts >= @a AND entry_ts < @b",
            {"r": a.run_id, "y": a.symbol, "a": start, "b": end})
        bt["entry_ts"] = pd.to_datetime(bt["entry_ts"], utc=True)
        live["bar"] = live["entry_ts"].dt.floor(span) if len(live) else pd.Series(dtype="datetime64[ns, UTC]")
        merged = bt.merge(live, left_on=["entry_ts", "side"], right_on=["bar", "side"], how="outer",
                          suffixes=("_bt", "_live"), indicator=True)
        both = merged[merged["_merge"] == "both"]
        missed = merged[merged["_merge"] == "left_only"]
        signals = set(store.read(cloud, ds, "signals", "strategy = @s AND timeframe = @t AND symbol = @y",
                                 {"s": a.strategy, "t": a.timeframe, "y": a.symbol})["signal_id"])
        explained = sum(signal_id(a.strategy, a.timeframe, a.symbol, ts - span) in signals for ts in missed["entry_ts_bt"])
        direction = both["side"].map({"long": 1, "short": -1})
        risk = (both["entry_price_bt"] - both["stop_bt"]).abs()
        slippage = ((both["entry_price_live"] - both["entry_price_bt"]) * direction / risk).mean() if len(both) else None
        closed = both[both["r_multiple_live"].notna()]
        rows.append({
            "date": now.date(), "strategy": a.strategy, "timeframe": a.timeframe, "symbol": a.symbol,
            "run_id": a.run_id, "window_start": start, "window_end": end, "live_trades": len(live),
            "backtest_trades": len(bt), "matched": len(both), "missed_explained": int(explained),
            "missed_unexplained": int(len(missed) - explained), "extra": int((merged["_merge"] == "right_only").sum()),
            "entry_slippage_r": store.clean(slippage),
            "live_avg_r": store.clean(closed["r_multiple_live"].mean()) if len(closed) else None,
            "backtest_avg_r": store.clean(closed["r_multiple_bt"].mean()) if len(closed) else None,
            "computed_at": now,
        })
        log.info("drift %s: %s", key, rows[-1])
    store.write(cloud, ds, "drift", rows)
    return rows
