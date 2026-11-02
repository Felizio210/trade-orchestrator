"""The gate: which strategies may trade, judged on their latest daily backtest.

For each (strategy, timeframe, symbol), the newest done run whose run_id starts with
TRADING_RUN_PREFIX ("daily-") is checked against the TRADING_MIN_* / TRADING_MAX_*
thresholds. A pass is stored as approved together with that run's exact parameters, so
trading never uses parameters that weren't backtested. Every failed check is written to
`reasons`. A strategy that was approved and now fails is revoked: it opens no new
positions, and its open ones run to their stop or target.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import pandas as pd

from backtest.config import BacktestSettings
from backtest.tables import table_id as bt_table
from cloud import Cloud
from trading import store
from trading.config import TradingSettings
from trading.tables import TABLES, table_id

log = logging.getLogger(__name__)

METRICS = ["n_trades", "total_return", "sharpe", "profit_factor", "max_drawdown", "win_rate", "avg_r"]


def setup(settings: TradingSettings, cloud: Cloud) -> list[str]:
    """Create the trading dataset and tables if they don't exist yet."""
    cloud.bq.ensure_dataset(settings.dataset)
    created = []
    for name, (schema, partition, cluster, _key) in TABLES.items():
        cloud.bq.ensure_table(table_id(settings.dataset, name), schema, partition, cluster)
        created.append(f"table {table_id(settings.dataset, name)}")
    return created


def _missing(value) -> bool:
    return value is None or value != value  # None or NaN


def check(m: dict, settings: TradingSettings) -> list[str]:
    """Failed checks for one metrics row; empty means it passes."""
    reasons = []

    def at_least(name: str, limit: float | None, value, fmt: str = "{:.2f}") -> None:
        if limit is None:
            return
        if _missing(value):
            reasons.append(f"{name} missing")
        elif value < limit:
            reasons.append(f"{name} {fmt.format(value)} < {fmt.format(limit)}")

    at_least("n_trades", settings.min_trades, m.get("n_trades"), "{:.0f}")
    at_least("sharpe", settings.min_sharpe, m.get("sharpe"))
    pf = m.get("profit_factor")
    if _missing(pf) and not _missing(m.get("n_trades")) and m["n_trades"] > 0:
        pf = float("inf")  # metrics leaves it empty when no trade lost: undefined, not bad
    at_least("profit_factor", settings.min_profit_factor, pf)
    at_least("avg_r", settings.min_avg_r, m.get("avg_r"))
    at_least("win_rate", settings.min_win_rate, m.get("win_rate"), "{:.1%}")
    at_least("total_return", settings.min_return, m.get("total_return"), "{:.1%}")
    if settings.max_drawdown is not None:
        dd = m.get("max_drawdown")
        if _missing(dd):
            reasons.append("max_drawdown missing")
        elif -dd > settings.max_drawdown:
            reasons.append(f"max_drawdown {dd:.1%} worse than -{settings.max_drawdown:.1%}")
    return reasons


def latest_metrics(settings: TradingSettings, backtest: BacktestSettings, cloud: Cloud) -> pd.DataFrame:
    """Per-symbol metrics of the newest done daily run of each (strategy, timeframe, symbol)."""
    if backtest.dataset not in cloud.bq.list_datasets() or not {"runs", "metrics"} <= set(
            cloud.bq.list_tables(backtest.dataset)):
        log.warning("No %s.runs / .metrics yet: run `python -m backtest setup` and a backtest first", backtest.dataset)
        return pd.DataFrame()
    runs, metrics = bt_table(backtest.dataset, "runs"), bt_table(backtest.dataset, "metrics")
    df = cloud.bq.query(
        f"SELECT r.run_id, r.strategy, r.timeframe, r.params_json, r.created_at, m.symbol, "
        f"{', '.join('m.' + c for c in METRICS)} FROM `{runs}` r JOIN `{metrics}` m ON m.run_id = r.run_id "
        "WHERE r.status = 'done' AND m.symbol <> 'ALL' AND r.run_id LIKE @prefix",
        {"prefix": settings.run_prefix + "%"},
    )
    if df.empty:
        return df
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    return (df.sort_values("created_at").groupby(["strategy", "timeframe", "symbol"], as_index=False).last()
            .reset_index(drop=True))


def evaluate(settings: TradingSettings, backtest: BacktestSettings, cloud: Cloud,
             *, now: datetime | None = None) -> list[dict]:
    """Judge every strategy/timeframe/symbol and store the decisions. Returns the approvals rows."""
    now = now or datetime.now(timezone.utc)
    latest = latest_metrics(settings, backtest, cloud)
    previous = store.read(cloud, settings.dataset, "approvals")
    was_approved = {(r.strategy, r.timeframe, r.symbol) for r in previous.itertuples() if r.status == "approved"}

    rows, events = [], []
    for m in latest.to_dict("records"):
        m = {k: store.clean(v) for k, v in m.items()}
        reasons = check(m, settings)
        key = (m["strategy"], m["timeframe"], m["symbol"])
        status = "rejected" if reasons else "approved"
        rows.append({
            "strategy": key[0], "timeframe": key[1], "symbol": key[2], "status": status, "run_id": m["run_id"],
            "params_json": m["params_json"], "reasons": "; ".join(reasons) or None,
            **{c: m[c] for c in METRICS}, "decided_at": now,
        })
        # Only changes are logged; a rejection's reasons are already in approvals
        kind = ("approved" if status == "approved" and key not in was_approved
                else "revoked" if status == "rejected" and key in was_approved else None)
        if kind:
            ref = ":".join(key)
            events.append({"event_id": f"{kind}:{ref}:{m['run_id']}", "ts": now,
                           "date": now.date(), "kind": kind, "ref": ref,
                           "message": f"{ref} {kind} on {m['run_id']}"
                                      + (f": {'; '.join(reasons)}" if reasons else f" ({json.loads(m['params_json'])})")})
        log.info("%s %s%s", ":".join(key), status, f" ({'; '.join(reasons)})" if reasons else "")

    store.write(cloud, settings.dataset, "approvals", rows)
    store.write(cloud, settings.dataset, "events", events)
    return rows
