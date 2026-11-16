"""One answer to "is it healthy?": data freshness, lake gaps, Airflow runs, approvals, broker,
kill switch, data quality, live-vs-backtest drift and the account. Read-only.

    python -m ops status        # print it
    ops_daily DAG (09:45 UTC)   # sends it as the daily summary

Each check adds lines to its section; anything that needs attention also goes to `problems`, and
the summary starts with OK or ATTENTION.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


@dataclass
class Health:
    now: datetime
    sections: dict[str, list[str]] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)

    def add(self, section: str, line: str, problem: bool = False) -> None:
        self.sections.setdefault(section, []).append(("! " if problem else "  ") + line)
        if problem:
            self.problems.append(f"{section}: {line}")

    @property
    def ok(self) -> bool:
        return not self.problems

    def render(self) -> str:
        head = "OK" if self.ok else f"ATTENTION ({len(self.problems)})"
        out = [f"DLH health {self.now:%Y-%m-%d %H:%M} UTC: {head}"]
        for name, lines in self.sections.items():
            out += ["", name.upper(), *lines]
        return "\n".join(out)


def _guard(health: Health, section: str):
    """Decorator-ish helper: a failing check becomes a problem line, never an exception."""
    def wrap(fn):
        try:
            fn()
        except Exception as exc:
            health.add(section, f"check failed: {type(exc).__name__}: {exc}"[:300], problem=True)
    return wrap


def _airflow_db() -> str | None:
    home = os.environ.get("AIRFLOW_HOME") or str(Path(__file__).resolve().parents[1] / ".airflow")
    path = Path(home) / "airflow.db"
    return str(path) if path.exists() and not os.environ.get("DLH_AIRFLOW_DB_URL") else None


def _current_dags() -> set[str]:
    import re

    folder = Path(__file__).resolve().parents[1] / "airflow" / "dags"
    return {m for f in folder.glob("*.py") for m in re.findall(r'dag_id="([^"]+)"', f.read_text())}


def dag_status(now: datetime) -> tuple[list[dict], list[dict]] | None:
    """(last run per DAG, task instances failed in the last 24h) from Airflow's metadata database:
    through Airflow when this runs inside it, else straight from the SQLite file. None if neither."""
    since = now - timedelta(hours=24)
    runs_sql = ("SELECT dag_id, state, start_date FROM dag_run d WHERE start_date = "
                "(SELECT MAX(start_date) FROM dag_run WHERE dag_id = d.dag_id) ORDER BY dag_id")
    failed_sql = ("SELECT dag_id, task_id, run_id, end_date FROM task_instance WHERE state = 'failed' "
                  "AND end_date >= :since ORDER BY end_date DESC")
    try:
        from airflow.settings import Session
        from sqlalchemy import text

        with closing(Session()) as session:
            runs = [dict(r._mapping) for r in session.execute(text(runs_sql))]
            failed = [dict(r._mapping) for r in session.execute(text(failed_sql), {"since": since})]
        return runs, failed
    except ImportError:
        pass
    db = _airflow_db()
    if db is None:
        return None
    with closing(sqlite3.connect(f"file:{db}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        runs = [dict(r) for r in conn.execute(runs_sql)]
        failed = [dict(r) for r in conn.execute(failed_sql, {"since": since.isoformat(sep=" ")})]
    return runs, failed


def collect(now: datetime | None = None, *, cloud=None, market=None, trading=None, backtest=None,
            broker=None, airflow: bool = True) -> Health:
    from backtest.config import BacktestSettings
    from cloud import Cloud
    from ingestion.dukascopy.config import Settings
    from ingestion.dukascopy.market_hours import is_open
    from ingestion.dukascopy.pipeline import in_lake, last_complete_day
    from ingestion.dukascopy.quality import quality_table
    from ingestion.dukascopy.market_hours import open_hours
    from ingestion.dukascopy.tables import bq_table_id, dataset_name
    from trading import brokers, store
    from trading.config import TradingSettings

    now = pd.Timestamp(now or datetime.now(timezone.utc)).tz_convert("UTC")
    cloud = cloud or Cloud.from_env()
    market = market or Settings.from_env()
    trading = trading or TradingSettings.from_env()
    backtest = backtest or BacktestSettings.from_env()
    h = Health(now.to_pydatetime())
    max_lag = int(os.environ.get("DUKASCOPY_MAX_LAG_HOURS", "2"))
    datasets = cloud.bq.list_datasets()

    @_guard(h, "market data")
    def _():
        for tf in market.bar_timeframes:
            table = bq_table_id(market.bq_dataset, dataset_name("bars", tf))
            if table.split(".")[0] not in datasets:
                h.add("market data", f"{tf}: no table (run setup)", problem=True)
                continue
            rows = cloud.bq.query(f"SELECT symbol, MAX(ts) AS ts FROM `{table}` GROUP BY symbol")
            newest = dict(zip(rows["symbol"], pd.to_datetime(rows["ts"], utc=True)))
            for symbol in (s.upper() for s in market.symbols):
                if symbol not in newest:
                    h.add("market data", f"{symbol} {tf}: no bars", problem=True)
                    continue
                hour = (newest[symbol] + pd.Timedelta(tf)).ceil("h")
                behind = 0
                while hour < now.floor("h"):
                    behind += is_open(hour.to_pydatetime())
                    hour += pd.Timedelta(hours=1)
                h.add("market data", f"{symbol} {tf}: newest bar {newest[symbol]:%Y-%m-%d %H:%M}, "
                      f"{behind} market hours behind", problem=behind > max_lag)

    @_guard(h, "lake")
    def _():
        days = int(os.environ.get("DLH_HEALTH_LAKE_DAYS", "14"))
        end = last_complete_day(now.to_pydatetime())
        missing = []
        for i in range(days):
            day = end - timedelta(days=i)
            if open_hours(day) == 0:
                continue
            missing += [f"{s} {day}" for s in market.symbols if not in_lake(market, cloud, s, day)]
        h.add("lake", f"last {days} days: " + (f"{len(missing)} missing: {', '.join(sorted(missing))}"
                                                if missing else "complete"), problem=bool(missing))

    if airflow:
        @_guard(h, "airflow")
        def _():
            status = dag_status(now.to_pydatetime())
            if status is None:
                h.add("airflow", "metadata database not found (not checked)")
                return
            runs, failed = status
            current = _current_dags()  # DAGs deleted since (e.g. dukascopy_daily) stay in the database
            runs = [r for r in runs if r["dag_id"] in current]
            failed = [f for f in failed if f["dag_id"] in current]
            for r in runs:
                start = pd.Timestamp(r["start_date"]).tz_localize("UTC") if pd.Timestamp(r["start_date"]).tzinfo is None \
                    else pd.Timestamp(r["start_date"])
                h.add("airflow", f"{r['dag_id']}: last run {r['state']} at {start:%Y-%m-%d %H:%M}",
                      problem=r["state"] == "failed")
            if runs:
                newest = max(pd.Timestamp(r["start_date"]) for r in runs)
                newest = newest.tz_localize("UTC") if newest.tzinfo is None else newest
                if now - newest > pd.Timedelta(hours=2):
                    h.add("airflow", f"nothing has run since {newest:%Y-%m-%d %H:%M}: is the scheduler up?", problem=True)
            if failed:
                tasks = sorted({f"{f['dag_id']}.{f['task_id']}" for f in failed})
                h.add("airflow", f"{len(failed)} task failures in 24h: {', '.join(tasks)}", problem=True)

    def have(table: str) -> bool:
        ds, name = table.split(".")
        return ds in datasets and name in cloud.bq.list_tables(ds)

    @_guard(h, "strategies")
    def _():
        if not have(f"{trading.dataset}.approvals"):
            h.add("strategies", "no approvals table yet")
            return
        approvals = store.read(cloud, trading.dataset, "approvals")
        approved = approvals[approvals["status"] == "approved"]
        for r in approvals.sort_values(["status", "strategy"]).itertuples():
            age = (now - r.decided_at).total_seconds() / 86400
            stale = r.status == "approved" and age > trading.approval_max_age_days
            h.add("strategies", f"{r.status:<8} {r.strategy}:{r.timeframe}:{r.symbol} on {r.run_id} "
                  f"(decided {age:.1f} days ago)", problem=stale)
        if approved.empty:
            h.add("strategies", "nothing is approved: nothing trades")

    @_guard(h, "trading")
    def _():
        if not have(f"{trading.dataset}.controls"):
            return
        controls = store.read(cloud, trading.dataset, "controls").set_index("key")["value"].dropna()
        kill = controls.get("kill_switch")
        h.add("trading", f"kill switch: {'ON: ' + kill if kill else 'off'}", problem=bool(kill))
        if trading.broker != "paper":
            b = broker or brokers.get(trading, backtest, market)
            problem = b.health()
            h.add("trading", f"broker {trading.broker}: {'DOWN: ' + problem if problem else 'up'}", problem=bool(problem))
        else:
            h.add("trading", "broker: paper")
        account = store.read(cloud, trading.dataset, "account")
        if len(account):
            a = account.sort_values("ts").iloc[-1]
            h.add("trading", f"account {a['ts']:%Y-%m-%d %H:%M}: balance {a['balance']:.2f}, equity {a['equity']:.2f}, "
                  f"{a['open_positions']} open, open risk {a['open_risk']:.2f}")
        external = store.read(cloud, trading.dataset, "events", "kind = 'unknown_position' AND ts >= @s",
                              {"s": now - timedelta(days=1)})
        if len(external):
            h.add("trading", f"{len(external)} positions at the broker this system didn't open: "
                  + ", ".join(external["ref"]), problem=True)

    @_guard(h, "data quality")
    def _():
        table = quality_table(market.bq_dataset)
        if not have(table):
            return
        q = cloud.bq.query(f"SELECT date, symbol, timeframe, rule, status, detail FROM `{table}` "
                           "WHERE date >= @d AND status <> 'ok' ORDER BY date DESC",
                           {"d": (now - timedelta(days=7)).date()})
        if q.empty:
            n = cloud.bq.query(f"SELECT COUNT(*) AS n FROM `{table}` WHERE date >= @d",
                               {"d": (now - timedelta(days=7)).date()})["n"][0]
            h.add("data quality", f"last 7 days: all {n} checks ok" if n else "no checks recorded in the last 7 days")
        for r in q.itertuples():
            h.add("data quality", f"{r.status} {r.date} {r.symbol} {r.timeframe} {r.rule}: {r.detail}",
                  problem=r.status == "fail")

    @_guard(h, "drift")
    def _():
        if not have(f"{trading.dataset}.drift"):
            return
        d = store.read(cloud, trading.dataset, "drift")
        if d.empty:
            return
        for r in d[d["date"] == d["date"].max()].itertuples():
            slip = "-" if r.entry_slippage_r != r.entry_slippage_r or r.entry_slippage_r is None else f"{r.entry_slippage_r:+.3f}R"
            h.add("drift", f"{r.strategy}:{r.timeframe}:{r.symbol}: matched {r.matched}/{r.backtest_trades} backtest "
                  f"trades, missed {r.missed_explained} skipped + {r.missed_unexplained} unexplained, extra {r.extra}, "
                  f"entry slippage {slip}", problem=bool(r.missed_unexplained or r.extra))

    return h
