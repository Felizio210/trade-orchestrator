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
    from ingestion.dukascopy.downloader import download_day, problems

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
            incomplete.append(f"{symbol}: {len(problems(manifest))} failed/corrupt files")
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


# --- backtest ---

MARKET_BARS = "dlh://market/bars"  # Dataset URI: dukascopy_daily's load emits it, backtest_daily runs on it


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
