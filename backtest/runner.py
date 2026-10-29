"""One backtest run: load bars -> signals -> simulate -> metrics -> write results.

Every table is written with replace-by-run_id, so a run can be re-run under the same
run_id without duplicating rows. The runs row is written first with status=running and
updated to done/failed at the end, so a crashed run is visible in backtest.runs.
"""

from __future__ import annotations

import json
import logging
import subprocess
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from backtest import strategies
from backtest.config import BacktestSettings
from backtest.data import load_bars
from backtest.engine import Costs, simulate
from backtest.metrics import bar_labels, combine_equity, compute, drawdown
from backtest.tables import TABLES, table_id
from cloud import Cloud
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.instruments import point_factor

log = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parents[1]


def setup(settings: BacktestSettings, cloud: Cloud) -> list[str]:
    """Create the backtest dataset and tables if they don't exist yet."""
    cloud.bq.ensure_dataset(settings.dataset)
    created = []
    for name, (schema, partition, cluster) in TABLES.items():
        cloud.bq.ensure_table(table_id(settings.dataset, name), schema, partition, cluster)
        created.append(f"table {table_id(settings.dataset, name)}")
    return created


def _git_sha() -> str | None:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=PROJECT_DIR, capture_output=True,
                              text=True, check=True).stdout.strip() or None
    except (OSError, subprocess.CalledProcessError):
        return None


def new_run_id(strategy: str) -> str:
    return f"{strategy}-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def run(
    strategy: str,
    symbols: list[str],
    timeframe: str,
    start: date,
    end: date,
    settings: BacktestSettings,
    market: Settings,
    cloud: Cloud,
    *,
    params: dict[str, str] | None = None,
    run_id: str | None = None,
) -> dict:
    """Backtest `strategy` on each symbol separately (each gets initial_capital) and store
    the results. Returns {"run_id", "metrics": [...]}; raises after recording status=failed."""
    settings.validate()
    resolved = strategies.resolve_params(strategy, params)
    symbols = [s.upper() for s in symbols]
    run_id = run_id or new_run_id(strategy)
    ds = settings.dataset
    run_row = {
        "run_id": run_id, "strategy": strategy, "params_json": json.dumps(resolved, sort_keys=True),
        "symbols": ",".join(symbols), "timeframe": timeframe, "start_date": start, "end_date": end,
        "initial_capital": settings.initial_capital, "risk_per_trade": settings.risk_per_trade,
        "spread_json": json.dumps({s: settings.spread_for(s) for s in symbols}),
        "commission_rate": settings.commission_rate, "git_sha": _git_sha(), "data_cutoff": None,
        "status": "running", "error": None, "created_at": datetime.now(timezone.utc), "finished_at": None,
    }

    def write_run(**changes) -> None:
        run_row.update(changes)
        cloud.bq.replace_from_dataframe(pd.DataFrame([run_row]), table_id(ds, "runs"), {"run_id": run_id})

    write_run()
    try:
        all_trades, all_equity, metric_rows, curves, cutoffs = [], [], [], {}, []
        for symbol in symbols:
            bars = load_bars(cloud, market, symbol, timeframe, start, end)
            if bars.empty:
                log.warning("%s: no %s bars between %s and %s", symbol, timeframe, start, end)
                continue
            cutoffs.append(bars["ingested_at"].max())
            costs = Costs(spread=settings.spread_for(symbol) / point_factor(symbol, market.point_overrides),
                          commission_rate=settings.commission_rate, price_side=bars["price_side"].iloc[0])
            sig = strategies.run(strategy, bars, resolved)
            trades, equity = simulate(bars, sig, costs, settings.initial_capital, settings.risk_per_trade)

            trades["symbol"] = symbol
            trades["date"] = pd.to_datetime(trades["entry_ts"], utc=True).dt.date
            equity["symbol"] = symbol
            equity["date"] = pd.to_datetime(equity["ts"], utc=True).dt.date
            equity["drawdown"] = drawdown(equity["equity"])
            equity[["side", "lots", "note"]] = bar_labels(equity, trades, settings.contract_size_for(symbol))
            all_trades.append(trades)
            all_equity.append(equity)
            curves[symbol] = equity
            metric_rows.append({"symbol": symbol, **compute(trades, equity, settings.initial_capital)})
            log.info("%s %s: %d trades, return %.2f%%", run_id, symbol, len(trades),
                     metric_rows[-1]["total_return"] * 100)

        if len(curves) > 1:
            combined = combine_equity(curves, settings.initial_capital)
            capital = settings.initial_capital * len(curves)
            metric_rows.append({"symbol": "ALL",
                                **compute(pd.concat(all_trades), combined, capital)})

        for name, frames in (("trades", all_trades), ("equity", all_equity)):
            df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
                columns=[f.name for f in TABLES[name][0]])
            df["run_id"] = run_id
            cloud.bq.replace_from_dataframe(df, table_id(ds, name), {"run_id": run_id})
        metrics = pd.DataFrame(metric_rows, columns=[f.name for f in TABLES["metrics"][0] if f.name != "run_id"])
        metrics.insert(0, "run_id", run_id)
        cloud.bq.replace_from_dataframe(metrics, table_id(ds, "metrics"), {"run_id": run_id})
    except Exception as exc:
        write_run(status="failed", error=f"{type(exc).__name__}: {exc}"[:1000],
                  finished_at=datetime.now(timezone.utc))
        raise

    write_run(status="done", data_cutoff=max(cutoffs) if cutoffs else None, finished_at=datetime.now(timezone.utc))
    return {"run_id": run_id, "metrics": metric_rows}
