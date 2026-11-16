"""Task functions for the Airflow DAGs. The DAG files only call these.

Everything heavy (pandas, the cloud clients, the pipeline) is imported inside the
functions, so the scheduler parses the DAGs without loading any of it.
Airflow puts this folder on sys.path, so the DAGs just `import dlh_tasks`.
"""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

PROJECT_DIR = Path(os.environ.get("DLH_PROJECT_DIR", Path(__file__).resolve().parents[2]))


def _pipeline():
    if str(PROJECT_DIR) not in sys.path:
        sys.path.insert(0, str(PROJECT_DIR))
    from cloud import Cloud
    from ingestion.dukascopy import pipeline
    from ingestion.dukascopy.config import Settings

    return pipeline, Settings.from_env(), Cloud.from_env()


def _ticks(settings):
    """The scheduled DAGs always build from tick files, whatever DUKASCOPY_BAR_SOURCE says:
    candle files only exist per day, and the hourly runs have already cached the tick hours,
    so finalize and repair reuse them instead of downloading the day a second time.
    DUKASCOPY_BAR_SOURCE=candles still applies to the CLI (e.g. long manual backfills)."""
    from dataclasses import replace

    return replace(settings, bar_source="ticks")


def is_trading_day(ds: str, **_) -> bool:
    """False on Saturdays (UTC): nothing trades, and the empty hours are never cached."""
    return date.fromisoformat(ds).weekday() != 5


def setup(**_) -> list[str]:
    pipeline, settings, cloud = _pipeline()
    return pipeline.setup(settings, cloud)


FINALIZE_WAIT_HOURS = 3  # how long finalize waits for yesterday's last hour before accepting it as empty


def finalize(data_interval_end, **_) -> dict:
    """Publish the last complete UTC day (yesterday) to the lake and Pub/Sub, once.

    Runs at the start of every dukascopy_hourly run. At 00:10 yesterday has just closed and
    gets published from the tick hours already on disk (only missing hours are fetched);
    the `load` task then replaces yesterday's provisional rows with the complete day. Later
    runs find it in the lake and only do a few lookups. Saturdays are skipped.

    Incomplete days are not published and the task fails, without retries: the next hourly
    run tries again, and dukascopy_repair covers anything older.
    """
    from datetime import datetime, time, timedelta, timezone

    pipeline, settings, cloud = _pipeline()
    settings = _ticks(settings)
    from ingestion.dukascopy.client import DukascopyClient
    from ingestion.dukascopy.downloader import download_day, problem_summary, problems

    settings.validate()
    day = data_interval_end.replace(minute=0, second=0, microsecond=0).date() - timedelta(days=1)
    if day.weekday() == 5:
        return {"date": day.isoformat(), "skipped": "saturday"}
    datasets = pipeline._dataset_names(settings)
    client = DukascopyClient(settings)
    published, existing, incomplete = [], 0, []
    for symbol in (s.upper() for s in settings.symbols):
        if all(cloud.storage.exists(pipeline.lake_uri(settings, d, symbol, day)) for d in datasets):
            existing += 1
            continue
        manifest = download_day(symbol, day, settings, client=client)
        if problems(manifest):
            incomplete.append(f"{symbol}: {problem_summary(manifest)}")
            continue
        # Dukascopy publishes an hour a few minutes after it ends, and an hour that isn't there
        # yet downloads as empty. Once the day is in the lake nothing fetches it again, so in the
        # first hours after midnight an empty last hour (market open) means "wait", not "publish".
        last = manifest["hours"][-1]
        last_hour = datetime.combine(day, time(23), tzinfo=timezone.utc)
        if (last["status"] == "empty" and _market_open(last_hour)
                and data_interval_end - last_hour < timedelta(hours=FINALIZE_WAIT_HOURS + 1)):
            incomplete.append(f"{symbol}: 23h not published yet")
            continue
        pipeline.publish_day(symbol, day, settings, cloud)
        published.append(symbol)
    if incomplete:
        raise RuntimeError(f"{day} incomplete, next hourly run retries: " + "; ".join(incomplete))
    return {"date": day.isoformat(), "published": published, "existing": existing}


def load(**_) -> dict:
    """Drain Pub/Sub into BigQuery. Failed messages stay un-acked and are redelivered."""
    pipeline, settings, cloud = _pipeline()
    result = pipeline.load_pending(settings, cloud)
    if result["failed"]:
        raise RuntimeError(f"{len(result['failed'])} messages failed to load: {result['failed'][:3]}")
    return {"loaded": len(result["loaded"])}


def repair(ds: str, **_) -> dict:
    """Backfill any day missing from the last DUKASCOPY_REPAIR_DAYS days up to `ds`.

    Covers days finalize never published: the scheduler was down (catchup=False skips
    downtime) or Dukascopy stayed incomplete. Days already in the lake are skipped, so a
    quiet day costs a few lookups. Days still incomplete fail the task at the end so
    Airflow retries them.

    After downtime longer than the window, the window is stretched back to the newest day
    already in the lake (at most DUKASCOPY_REPAIR_MAX_DAYS, default 60), so the days in
    between are filled too instead of staying a hole that backtests would run over.
    """
    from datetime import timedelta

    pipeline, settings, cloud = _pipeline()
    settings = _ticks(settings)
    end = min(date.fromisoformat(ds), pipeline.last_complete_day())  # a manual run's ds is today
    start = end - timedelta(days=int(os.environ.get("DUKASCOPY_REPAIR_DAYS", "7")) - 1)
    start = pipeline.repair_start(settings.symbols, start, settings, cloud,
                                  max_days=int(os.environ.get("DUKASCOPY_REPAIR_MAX_DAYS", "60")))
    result = pipeline.backfill_range(settings.symbols, start, end, settings, cloud)
    summary = {"start": start.isoformat(), "end": end.isoformat(), "existing": result["existing"],
               "published": [f"{p['symbol']} {p['date']}" for p in result["published"]],
               "loaded": len(result["loaded"])}
    problems = [f"{s['symbol']} {s['date']}: {s['summary']}" for s in result["skipped"]]
    if result["failed"]:
        problems.append(f"{len(result['failed'])} messages failed to load")
    if problems:
        raise RuntimeError(f"repair incomplete, will retry: {'; '.join(problems)} ({summary})")
    return summary


def _market_open(hour) -> bool:
    """Rough FX/metals hours, which follow New York time: closed Friday 17:00 to Sunday 18:00,
    and the daily 17:00 rollover hour (gold has no ticks then). In UTC that is 21:00/22:00 in
    summer and 22:00/23:00 in winter, so the hour is converted rather than compared in UTC.
    The sessions of Christmas, New Year's Day and Good Friday (18:00 the evening before to
    17:00) don't trade at all. Shortened sessions around other US holidays are not modelled.
    Only decides whether an empty hour is worth a retry."""
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    from dateutil.easter import easter

    ny = hour.astimezone(ZoneInfo("America/New_York"))
    weekday = ny.weekday()
    if weekday == 5 or (weekday == 4 and ny.hour >= 17) or (weekday == 6 and ny.hour < 18) or ny.hour == 17:
        return False
    session = (ny + timedelta(hours=6)).date()  # a session starts at 18:00 the evening before
    return not ((session.month, session.day) in ((12, 25), (1, 1))
                or session == easter(session.year) - timedelta(days=2))


def freshness(data_interval_end, **_) -> dict:
    """Fail when a symbol's newest bar is too old, so a stalled feed doesn't go unnoticed.

    For every bar timeframe up to 1h, counts the market-open hours that have closed since
    each symbol's newest bar closed. More than DUKASCOPY_MAX_LAG_HOURS (default 2) fails the
    task. Closed-market hours don't count, so weekends never look stale.
    """
    from datetime import timedelta

    import pandas as pd

    pipeline, settings, cloud = _pipeline()
    from ingestion.dukascopy.tables import bq_table_id, dataset_name

    max_lag = int(os.environ.get("DUKASCOPY_MAX_LAG_HOURS", "2"))
    now_hour = pd.Timestamp(data_interval_end).tz_convert("UTC").floor("h")
    symbols = [s.upper() for s in settings.symbols]
    lags, stale = {}, []
    for tf in settings.bar_timeframes:
        step = pd.Timedelta(pd.tseries.frequencies.to_offset(tf))
        if step > timedelta(hours=1):
            continue
        table = bq_table_id(settings.bq_dataset, dataset_name("bars", tf))
        rows = cloud.bq.query(f"SELECT symbol, MAX(ts) AS ts FROM {table} GROUP BY symbol")
        newest = dict(zip(rows["symbol"], pd.to_datetime(rows["ts"], utc=True)))  # strings locally
        for symbol in symbols:
            if symbol not in newest:
                stale.append(f"{symbol} {tf}: no bars")
                continue
            hour = (newest[symbol] + step).ceil("h")  # first hour that should have a newer bar
            missing = 0
            while hour < now_hour:
                missing += _market_open(hour)
                hour += pd.Timedelta(hours=1)
            lags[f"{symbol} {tf}"] = f"{newest[symbol]:%Y-%m-%d %H:%M} ({missing}h behind)"
            if missing > max_lag:
                stale.append(f"{symbol} {tf}: newest bar {newest[symbol]:%Y-%m-%d %H:%M}, "
                             f"{missing} market hours behind")
    if stale:
        raise RuntimeError(f"stale market data (max {max_lag}h): " + "; ".join(stale))
    return lags


def intraday(data_interval_end, **_) -> dict:
    """Load the closed hours of the current UTC day for every symbol, as provisional rows.

    Fails (so Airflow retries) when an hour failed to download, or when the hour that just
    closed is still empty while the market should be open: Dukascopy publishes each hour a
    few minutes after it ends. Next day's finalize replaces these rows with the complete day.
    """
    from datetime import timedelta

    pipeline, settings, cloud = _pipeline()
    settings = _ticks(settings)
    from ingestion.dukascopy.client import DukascopyClient

    client = DukascopyClient(settings)
    results = [pipeline.load_intraday(s, settings, cloud, now=data_interval_end, client=client)
               for s in settings.symbols]
    last_hour = data_interval_end.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    waiting = [r["symbol"] for r in results if r["last_hour"] == "empty" and _market_open(last_hour)]
    if waiting:
        raise RuntimeError(f"{last_hour:%Y-%m-%d %H}h not published yet for {waiting}, will retry")
    return {r["symbol"]: f"{r['status']} {r['rows']} rows" for r in results}


# --- backtest ---

MARKET_BARS = "dlh://market/bars"  # Dataset URI: dukascopy_repair emits it, backtest_daily runs on it
HOURLY_BARS = "dlh://market/bars/hourly"  # dukascopy_hourly emits it once today's bars are loaded (trading_live runs every 5 minutes)
BACKTEST_RESULTS = "dlh://backtest/metrics"  # backtest_daily emits it, trading_gate runs on it


def backtest_jobs() -> list[tuple[str, str]]:
    """(strategy, timeframe) pairs from BACKTEST_DAILY_RUNS, e.g. "supply_demand:1h,trendline:1h".

    Read when the DAG is parsed (one task per pair), so it only uses os.environ.
    """
    raw = os.environ.get("BACKTEST_DAILY_RUNS") or "supply_demand:1h"
    jobs = []
    for item in (part.strip() for part in raw.split(",") if part.strip()):
        strategy, sep, timeframe = item.partition(":")
        jobs.append((strategy.strip(), timeframe.strip() if sep else "1h"))
    return jobs


def _backtest():
    if str(PROJECT_DIR) not in sys.path:
        sys.path.insert(0, str(PROJECT_DIR))
    from backtest import runner
    from backtest.config import BacktestSettings
    from cloud import Cloud
    from ingestion.dukascopy.config import Settings

    return runner, BacktestSettings.from_env(), Settings.from_env(), Cloud.from_env()


def backtest_setup(**_) -> list[str]:
    runner, settings, _market, cloud = _backtest()
    return runner.setup(settings, cloud)


def backtest(strategy: str, timeframe: str, ds: str, **_) -> dict:
    """Backtest the last BACKTEST_DAILY_LOOKBACK_DAYS days up to `ds` on DUKASCOPY_SYMBOLS.

    The run_id is fixed per strategy, timeframe and day, so a retry or a re-run of the
    same day replaces its results instead of adding another run.
    """
    from datetime import timedelta

    runner, settings, market, cloud = _backtest()
    end = date.fromisoformat(ds)
    start = end - timedelta(days=int(os.environ.get("BACKTEST_DAILY_LOOKBACK_DAYS", "730")))
    result = runner.run(strategy, market.symbols, timeframe, start, end, settings, market, cloud,
                        run_id=f"daily-{strategy}-{timeframe}-{ds}")
    return {"run_id": result["run_id"],
            "metrics": {m["symbol"]: {k: m[k] for k in ("n_trades", "total_return", "max_drawdown", "sharpe")}
                        for m in result["metrics"]}}


# --- trading ---


def _trading():
    if str(PROJECT_DIR) not in sys.path:
        sys.path.insert(0, str(PROJECT_DIR))
    from backtest.config import BacktestSettings
    from cloud import Cloud
    from ingestion.dukascopy.config import Settings
    from trading.config import TradingSettings

    return TradingSettings.from_env(), BacktestSettings.from_env(), Settings.from_env(), Cloud.from_env()


def trading_setup(**_) -> list[str]:
    settings, _backtest, _market, cloud = _trading()  # puts the project on sys.path first
    from trading.gate import setup as trading_tables

    return trading_tables(settings, cloud)


def trading_gate(**_) -> dict:
    """Approve or reject each strategy/timeframe/symbol from its latest daily backtest."""
    settings, backtest, _market, cloud = _trading()  # puts the project on sys.path first
    from trading.gate import evaluate

    return {f"{r['strategy']}:{r['timeframe']}:{r['symbol']}": r["status"] + (f" ({r['reasons']})" if r["reasons"] else "")
            for r in evaluate(settings, backtest, cloud)}


def trading_step(**_) -> dict:
    """Trade every bar that closed since the last step. Safe to retry: ids are deterministic."""
    settings, backtest, market, cloud = _trading()  # puts the project on sys.path first
    from trading.executor import step

    return step(settings, backtest, market, cloud)
