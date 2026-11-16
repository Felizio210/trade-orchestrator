"""One backtest run: load bars -> signals -> simulate -> metrics -> write results.

Every table is written with replace-by-run_id, so a run can be re-run under the same
run_id without duplicating rows. The runs row is written first with status=running and
updated to done/failed at the end, so a crashed run is visible in backtest.runs (and marked
failed by the next run once it is older than BACKTEST_RUN_TIMEOUT_HOURS).
"""

from __future__ import annotations

import json
import logging
import subprocess
import uuid
from datetime import date, datetime, timedelta, timezone
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


def _fold_bounds(first: pd.Timestamp, last: pd.Timestamp, train_days: int, test_days: int) -> list[tuple]:
    """(train_start, test_start, test_end) per fold; test periods tile the span after the first
    training window, and the last one runs to the end of the data."""
    train, test = pd.Timedelta(days=train_days), pd.Timedelta(days=test_days)
    folds, test_start = [], first + train
    end = last + pd.Timedelta(microseconds=1)
    while test_start < end:
        test_end = test_start + test
        if end - test_end < test / 2:  # a short tail joins the last fold
            test_end = end
        folds.append((test_start - train, test_start, test_end))
        test_start = test_end
    return folds


def _choose(combos: list[dict], signals: list[dict[str, pd.DataFrame]], bars: dict[str, pd.DataFrame],
            costs: dict[str, Costs], start: pd.Timestamp, end: pd.Timestamp, settings: BacktestSettings) -> int:
    """Index of the parameter set with the best mean Sharpe across symbols on [start, end), among
    those with at least wf_min_trades trades; 0 (the base parameters) when none qualifies."""
    best, best_score = 0, None
    for k in range(len(combos)):
        scores, trades = [], 0
        for symbol, b in bars.items():
            window = (b["ts"] >= start) & (b["ts"] < end)
            if window.sum() < 2:
                continue
            tr, eq = simulate(b[window].reset_index(drop=True), signals[k][symbol][window.to_numpy()].reset_index(drop=True),
                              costs[symbol], settings.initial_capital, settings.risk_per_trade)
            trades += len(tr)
            sharpe = compute(tr, eq, settings.initial_capital)["sharpe"]
            scores.append(sharpe if sharpe is not None else 0.0)
        if not scores or trades < settings.wf_min_trades:
            continue
        score = sum(scores) / len(scores)
        if best_score is None or score > best_score:
            best, best_score = k, score
    return best


def walk_forward(strategy: str, base: dict, bars: dict[str, pd.DataFrame], costs: dict[str, Costs],
                 settings: BacktestSettings) -> tuple[dict[str, pd.DataFrame], dict, list[dict]]:
    """Out-of-sample signals per symbol (zero before the first test period), the parameters chosen
    on the newest training window (what trading would use from now on), and the folds.

    Parameters are chosen jointly for all symbols, so one run has one parameter set. Strategies
    are causal, so each parameter set's signals are computed once over all bars and sliced."""
    combos = strategies.grid(strategy, base)
    signals = [{s: strategies.run(strategy, b, params) for s, b in bars.items()} for params in combos]
    first = min(b["ts"].iloc[0] for b in bars.values())
    last = max(b["ts"].iloc[-1] for b in bars.values())
    folds = []
    out = {s: pd.DataFrame({"signal": 0, "stop": float("nan"), "target": float("nan")}, index=b.index)
           .astype({"signal": "int8"}) for s, b in bars.items()}
    for train_start, test_start, test_end in _fold_bounds(first, last, settings.wf_train_days, settings.wf_test_days):
        k = _choose(combos, signals, bars, costs, train_start, test_start, settings)
        folds.append({"train_start": str(train_start), "test_start": str(test_start), "test_end": str(test_end),
                      "params": combos[k]})
        for s, b in bars.items():
            rows = ((b["ts"] >= test_start) & (b["ts"] < test_end)).to_numpy()
            out[s].loc[rows] = signals[k][s].loc[rows, ["signal", "stop", "target"]].to_numpy()
    if not folds:
        raise ValueError(f"walk-forward needs more than {settings.wf_train_days} days of bars; "
                         "shorten BACKTEST_WF_TRAIN_DAYS or backtest a longer span")
    final = combos[_choose(combos, signals, bars, costs, last - pd.Timedelta(days=settings.wf_train_days),
                           last + pd.Timedelta(microseconds=1), settings)]
    return out, final, folds


def recent_metrics(trades: pd.DataFrame, equity: pd.DataFrame, days: int) -> dict:
    """compute() over the last `days` of a run, starting from the equity it had then."""
    ts = pd.to_datetime(equity["ts"], utc=True)
    cutoff = ts.iloc[-1] - pd.Timedelta(days=days)
    inside = ts > cutoff
    before = equity.loc[~inside, "equity"]
    start_equity = float(before.iloc[-1]) if len(before) else float(equity["equity"].iloc[0])
    recent_trades = trades[pd.to_datetime(trades["entry_ts"], utc=True) > cutoff] if len(trades) else trades
    return compute(recent_trades, equity[inside].reset_index(drop=True), start_equity)


def fail_stuck_runs(settings: BacktestSettings, cloud: Cloud, now: datetime | None = None) -> list[str]:
    """Mark runs still "running" longer than BACKTEST_RUN_TIMEOUT_HOURS as failed (the process was killed)."""
    now = now or datetime.now(timezone.utc)
    runs = table_id(settings.dataset, "runs")
    stuck = cloud.bq.query(f"SELECT * FROM `{runs}` WHERE status = 'running' AND created_at < @cutoff",
                           {"cutoff": now - timedelta(hours=settings.run_timeout_hours)})
    for row in stuck.to_dict("records"):
        row.update(status="failed", finished_at=now,
                   error=f"still running after {settings.run_timeout_hours:g}h: the process was killed")
        cloud.bq.replace_from_dataframe(pd.DataFrame([row]), runs, {"run_id": row["run_id"]})
        log.warning("marked stuck run %s as failed", row["run_id"])
    return list(stuck["run_id"])


def prune_equity(settings: BacktestSettings, cloud: Cloud, now: datetime | None = None) -> dict[str, int]:
    """Thin old equity curves to one row per symbol and day (the day's last bar).

    Kept bar by bar: the newest done daily run of each strategy/timeframe (what the gate judged and
    what you'd inspect) and every run younger than BACKTEST_EQUITY_FULL_DAYS. Trades and metrics are
    never touched. Returns {run_id: rows removed}."""
    now = now or datetime.now(timezone.utc)
    runs_t, equity_t = table_id(settings.dataset, "runs"), table_id(settings.dataset, "equity")
    runs = cloud.bq.query(f"SELECT run_id, strategy, timeframe, status, created_at FROM `{runs_t}`")
    if runs.empty:
        return {}
    runs["created_at"] = pd.to_datetime(runs["created_at"], utc=True)
    daily = runs[runs["run_id"].str.startswith("daily-") & (runs["status"] == "done")]
    keep = set(daily.sort_values("created_at").groupby(["strategy", "timeframe"])["run_id"].last())
    keep |= set(runs.loc[runs["created_at"] > now - timedelta(days=settings.equity_full_days), "run_id"])
    sizes = cloud.bq.query(f"SELECT run_id, symbol, COUNT(*) AS n, COUNT(DISTINCT date) AS days "
                           f"FROM `{equity_t}` GROUP BY run_id, symbol")
    thick = sizes[(sizes["n"] > sizes["days"]) & ~sizes["run_id"].isin(keep)]
    removed = {}
    for run_id in sorted(set(thick["run_id"])):
        eq = cloud.bq.query(f"SELECT * FROM `{equity_t}` WHERE run_id = @r", {"r": run_id})
        eq["ts"] = pd.to_datetime(eq["ts"], utc=True)
        thin = eq.sort_values("ts").groupby(["symbol", "date"], as_index=False).last()
        cloud.bq.replace_from_dataframe(thin, equity_t, {"run_id": run_id})
        removed[run_id] = len(eq) - len(thin)
        log.info("equity of %s thinned to daily: %d rows removed", run_id, removed[run_id])
    if removed:
        cloud.bq.compact(settings.dataset)
    return removed


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
    walk_forward_mode: bool | None = None,
) -> dict:
    """Backtest `strategy` on each symbol separately (each gets initial_capital) and store
    the results. Returns {"run_id", "metrics": [...], "recent": [...], "params"}; raises after
    recording status=failed. A timeframe the strategy can't work on is refused before anything
    is recorded.

    walk_forward_mode (default BACKTEST_WALK_FORWARD): parameters are chosen from the strategy's
    GRID on rolling training windows and only the test periods are simulated and stored (metrics
    scope "oos"); params_json is what the newest training window chose. Otherwise the whole span
    runs with fixed parameters (scope "full"). Both also store the last BACKTEST_RECENT_DAYS
    (scope "recent")."""
    settings.validate()
    strategies.check_timeframe(strategy, timeframe)
    resolved = strategies.resolve_params(strategy, params)
    wf = settings.walk_forward if walk_forward_mode is None else walk_forward_mode
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
        "mode": "walk_forward" if wf else "fixed", "folds_json": None,
    }

    def write_run(**changes) -> None:
        run_row.update(changes)
        cloud.bq.replace_from_dataframe(pd.DataFrame([run_row]), table_id(ds, "runs"), {"run_id": run_id})

    fail_stuck_runs(settings, cloud)
    write_run()
    scope = "oos" if wf else "full"
    try:
        all_trades, all_equity, metric_rows, recent_rows, curves, cutoffs = [], [], [], [], {}, []
        bars_by, costs_by = {}, {}
        for symbol in symbols:
            bars = load_bars(cloud, market, symbol, timeframe, start, end)
            if bars.empty:
                log.warning("%s: no %s bars between %s and %s", symbol, timeframe, start, end)
                continue
            cutoffs.append(bars["ingested_at"].max())
            bars_by[symbol] = bars
            costs_by[symbol] = Costs(spread=settings.spread_for(symbol) / point_factor(symbol, market.point_overrides),
                                     commission_rate=settings.commission_rate, price_side=bars["price_side"].iloc[0])
        if wf and bars_by:
            sigs, chosen, folds = walk_forward(strategy, resolved, bars_by, costs_by, settings)
            run_row.update(params_json=json.dumps(chosen, sort_keys=True), folds_json=json.dumps(folds))
            oos_start = pd.Timestamp(folds[0]["test_start"])
        else:
            sigs = {s: strategies.run(strategy, b, resolved) for s, b in bars_by.items()}
            oos_start = None

        for symbol, bars in bars_by.items():
            sig = sigs[symbol]
            if oos_start is not None:
                keep = (bars["ts"] >= oos_start).to_numpy()
                bars, sig = bars[keep].reset_index(drop=True), sig[keep].reset_index(drop=True)
                if bars.empty:
                    continue
            trades, equity = simulate(bars, sig, costs_by[symbol], settings.initial_capital, settings.risk_per_trade)

            trades["symbol"] = symbol
            trades["date"] = pd.to_datetime(trades["entry_ts"], utc=True).dt.date
            equity["symbol"] = symbol
            equity["date"] = pd.to_datetime(equity["ts"], utc=True).dt.date
            equity["drawdown"] = drawdown(equity["equity"])
            equity[["side", "lots", "note"]] = bar_labels(equity, trades, settings.contract_size_for(symbol))
            all_trades.append(trades)
            all_equity.append(equity)
            curves[symbol] = equity
            metric_rows.append({"symbol": symbol, "scope": scope,
                                **compute(trades, equity, settings.initial_capital)})
            recent_rows.append({"symbol": symbol, "scope": "recent",
                                **recent_metrics(trades, equity, settings.recent_days)})
            log.info("%s %s: %d trades, return %.2f%%", run_id, symbol, len(trades),
                     metric_rows[-1]["total_return"] * 100)

        if len(curves) > 1:
            combined = combine_equity(curves, settings.initial_capital)
            capital = settings.initial_capital * len(curves)
            metric_rows.append({"symbol": "ALL", "scope": scope,
                                **compute(pd.concat(all_trades), combined, capital)})

        for name, frames in (("trades", all_trades), ("equity", all_equity)):
            df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
                columns=[f.name for f in TABLES[name][0]])
            df["run_id"] = run_id
            cloud.bq.replace_from_dataframe(df, table_id(ds, name), {"run_id": run_id})
        metrics = pd.DataFrame(metric_rows + recent_rows,
                               columns=[f.name for f in TABLES["metrics"][0] if f.name != "run_id"])
        metrics.insert(0, "run_id", run_id)
        cloud.bq.replace_from_dataframe(metrics, table_id(ds, "metrics"), {"run_id": run_id})
    except Exception as exc:
        write_run(status="failed", error=f"{type(exc).__name__}: {exc}"[:1000],
                  finished_at=datetime.now(timezone.utc))
        raise

    write_run(status="done", data_cutoff=max(cutoffs) if cutoffs else None, finished_at=datetime.now(timezone.utc))
    return {"run_id": run_id, "metrics": metric_rows, "recent": recent_rows, "params": json.loads(run_row["params_json"]),
            "mode": run_row["mode"]}
