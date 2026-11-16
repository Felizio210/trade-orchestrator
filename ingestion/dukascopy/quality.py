"""Data quality checks on one symbol/day of bars, run after each load. Results go to
{BQ_DATASET}.quality, one row per (date, symbol, timeframe, rule):

    ohlc         fail  a price <= 0, high < low, or open/close outside [low, high]
    duplicates   fail  the same ts twice
    in_day       fail  a bar outside its date
    hours        warn  1h bars: count differs from the hours the market was open (market_hours.py)
    previous_day warn  the trading day before has no bars (a gap between days)
    provisional  warn  the complete day changed bars that the intraday load had already written
                       (different close, or a provisional bar that's no longer there)

Only "fail" fails the load; warnings are recorded and show up in the daily summary.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd

from cloud import Cloud, Field
from ingestion.dukascopy.market_hours import open_hours

QUALITY_TABLE = "quality"
QUALITY_SCHEMA = [
    Field("date", "DATE", "REQUIRED"),
    Field("symbol", "STRING", "REQUIRED"),
    Field("timeframe", "STRING", "REQUIRED"),
    Field("rule", "STRING", "REQUIRED"),
    Field("status", "STRING", "REQUIRED"),  # ok | warn | fail
    Field("detail", "STRING"),
    Field("checked_at", "TIMESTAMP", "REQUIRED"),
]
QUALITY_KEY = ["date", "symbol", "timeframe", "rule"]


def quality_table(bq_dataset: str) -> str:
    return f"{bq_dataset}.{QUALITY_TABLE}"


def previous_trading_day(day: date) -> date:
    prev = day - timedelta(days=1)
    while open_hours(prev) == 0 and prev > day - timedelta(days=10):
        prev -= timedelta(days=1)
    return prev


def _read(cloud: Cloud, table: str, symbol: str, day: date) -> pd.DataFrame:
    df = cloud.bq.query(f"SELECT ts, open, high, low, close FROM `{table}` WHERE symbol = @s AND date = @d",
                        {"s": symbol, "d": day})
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def provisional_snapshot(cloud: Cloud, table: str, symbol: str, day: date) -> pd.DataFrame:
    """The rows already there before a load replaces them (the intraday load's provisional bars)."""
    return _read(cloud, table, symbol, day)


def check_bars(cloud: Cloud, table: str, timeframe: str, symbol: str, day: date,
               provisional: pd.DataFrame | None = None, now: datetime | None = None) -> list[dict]:
    now = now or datetime.now(timezone.utc)
    bars = _read(cloud, table, symbol, day)
    rows = []

    def rule(name: str, status: str, detail: str | None = None) -> None:
        rows.append({"date": day, "symbol": symbol, "timeframe": timeframe, "rule": name, "status": status,
                     "detail": detail, "checked_at": now})

    bad = bars[(bars[["open", "high", "low", "close"]] <= 0).any(axis=1) | (bars["high"] < bars["low"])
               | (bars["open"] > bars["high"]) | (bars["open"] < bars["low"])
               | (bars["close"] > bars["high"]) | (bars["close"] < bars["low"])]
    rule("ohlc", "fail" if len(bad) else "ok", f"{len(bad)} bad bars, first {bad['ts'].iloc[0]}" if len(bad) else None)
    dupes = int(bars["ts"].duplicated().sum())
    rule("duplicates", "fail" if dupes else "ok", f"{dupes} repeated ts" if dupes else None)
    outside = int((bars["ts"].dt.date != day).sum())
    rule("in_day", "fail" if outside else "ok", f"{outside} bars outside {day}" if outside else None)
    if pd.Timedelta(timeframe) == pd.Timedelta("1h"):
        expected = open_hours(day)
        rule("hours", "ok" if len(bars) == expected else "warn",
             None if len(bars) == expected else f"{len(bars)} bars, market open {expected}h")
    prev = previous_trading_day(day)
    has_prev = not cloud.bq.query(f"SELECT ts FROM `{table}` WHERE symbol = @s AND date = @d LIMIT 1",
                                  {"s": symbol, "d": prev}).empty
    rule("previous_day", "ok" if has_prev else "warn", None if has_prev else f"no bars on {prev}")
    if provisional is not None and len(provisional):
        joined = provisional.merge(bars, on="ts", how="left", suffixes=("_old", ""))
        gone = int(joined["close"].isna().sum())
        changed = int(((joined["close"] - joined["close_old"]).abs() > 1e-9 * joined["close_old"].abs()).sum())
        rule("provisional", "warn" if gone or changed else "ok",
             f"of {len(provisional)} provisional bars, {changed} changed close, {gone} gone" if gone or changed else None)
    return rows


def write(cloud: Cloud, bq_dataset: str, rows: list[dict]) -> None:
    if rows:
        # Created on first use too, so an install set up before this table existed keeps loading
        cloud.bq.ensure_dataset(bq_dataset)
        cloud.bq.ensure_table(quality_table(bq_dataset), QUALITY_SCHEMA, "date", ["symbol"])
        cloud.bq.upsert_from_dataframe(pd.DataFrame(rows), quality_table(bq_dataset), QUALITY_KEY)
