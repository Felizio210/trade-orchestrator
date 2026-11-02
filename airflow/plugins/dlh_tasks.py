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


def is_trading_day(ds: str, **_) -> bool:
    """False on Saturdays (UTC): nothing trades, and the empty hours are never cached."""
    return date.fromisoformat(ds).weekday() != 5


def setup(**_) -> list[str]:
    pipeline, settings, cloud = _pipeline()
    return pipeline.setup(settings, cloud)


def download_publish(ds: str, **_) -> dict:
    """Download one UTC day for every symbol and publish it. Safe to retry.

    Symbols already in the lake are skipped, so a retry only redoes what failed.
    Incomplete days are not published; the task fails at the end so Airflow retries.
    """
    pipeline, settings, cloud = _pipeline()
    from ingestion.dukascopy.client import DukascopyClient
    from ingestion.dukascopy.downloader import download_day, problem_summary, problems

    settings.validate()
    day = date.fromisoformat(ds)
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
        pipeline.publish_day(symbol, day, settings, cloud)
        published.append(symbol)
    if incomplete:
        raise RuntimeError(f"{ds} incomplete, will retry: " + "; ".join(incomplete))
    return {"published": published, "existing": existing}


def load(**_) -> dict:
    """Drain Pub/Sub into BigQuery. Failed messages stay un-acked and are redelivered."""
    pipeline, settings, cloud = _pipeline()
    result = pipeline.load_pending(settings, cloud)
    if result["failed"]:
        raise RuntimeError(f"{len(result['failed'])} messages failed to load: {result['failed'][:3]}")
    return {"loaded": len(result["loaded"])}


def repair(ds: str, **_) -> dict:
    """Backfill any day missing from the last DUKASCOPY_REPAIR_DAYS days up to `ds`.

    Covers days dukascopy_daily never ran (catchup=False skips downtime) and days whose
    run failed. Days already in the lake are skipped, so a quiet day costs a few lookups.
    Days still incomplete fail the task at the end so Airflow retries them.
    """
    from datetime import timedelta

    pipeline, settings, cloud = _pipeline()
    end = date.fromisoformat(ds)
    start = end - timedelta(days=int(os.environ.get("DUKASCOPY_REPAIR_DAYS", "7")) - 1)
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
    """Rough FX/metals hours: closed Friday 21:00 to Sunday 22:00 UTC, and the daily 21:00
    rollover hour (gold has no ticks then). Only decides whether an empty hour is worth a retry."""
    weekday = hour.weekday()
    return not (weekday == 5 or (weekday == 4 and hour.hour >= 21) or (weekday == 6 and hour.hour < 22)
                or hour.hour == 21)


def hourly(data_interval_end, **_) -> dict:
    """Load the closed hours of the current UTC day for every symbol, ahead of the daily run.

    Fails (so Airflow retries) when an hour failed to download, or when the hour that just
    closed is still empty while the market should be open: Dukascopy publishes each hour a
    few minutes after it ends.

    Always reads ticks, whatever DUKASCOPY_BAR_SOURCE says: candle files only exist per day.
    The daily runs keep their own source, and replace these provisional rows next morning.
    """
    from dataclasses import replace
    from datetime import timedelta

    pipeline, settings, cloud = _pipeline()
    settings = replace(settings, bar_source="ticks")
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
HOURLY_BARS = "dlh://market/bars/hourly"  # dukascopy_hourly emits it, trading_live runs on it
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
