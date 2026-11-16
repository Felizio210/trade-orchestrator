"""Trading: warehouse keyed writes, the gate, the paper broker against the backtest engine,
step idempotency, risk limits, and the intraday load. Local emulation only."""

import json
import lzma
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from backtest import strategies
from backtest.config import BacktestSettings
from backtest.engine import Costs, simulate
from backtest.runner import setup as backtest_setup
from cloud import Cloud, CloudSettings, Field
from ingestion.dukascopy.client import TICK_DTYPE, DukascopyClient
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.instruments import point_factor
from ingestion.dukascopy.pipeline import load_intraday, setup as market_setup
from ingestion.dukascopy.tables import bq_table_id, shape
from tests.test_backtest import random_bars
from trading import risk, store
from trading.__main__ import main
from trading.config import TradingSettings
from trading.executor import signal_id, step
from trading.gate import check, evaluate, setup
from trading.tables import key_for

STRATEGY = "supply_demand"
NO_LIMITS = TradingSettings(max_open_positions=None, max_open_risk=None, max_daily_loss=None, warmup_days=365)


# --- warehouse keyed writes ---

@pytest.fixture
def bq(tmp_path):
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    cloud.bq.ensure_dataset("s")
    cloud.bq.ensure_table("s.t", [Field("id", "STRING", "REQUIRED"), Field("v", "INT64")])
    return cloud.bq


def test_insert_if_absent_only_inserts_new_keys(bq):
    assert bq.insert_if_absent(pd.DataFrame({"id": ["a", "b"], "v": [1, 2]}), "s.t", ["id"]) == 2
    assert bq.insert_if_absent(pd.DataFrame({"id": ["b", "c"], "v": [9, 3]}), "s.t", ["id"]) == 1
    rows = bq.query("SELECT id, v FROM s.t ORDER BY id")
    assert rows.values.tolist() == [["a", 1], ["b", 2], ["c", 3]]  # b kept its first value


def test_upsert_replaces_by_key(bq):
    bq.upsert_from_dataframe(pd.DataFrame({"id": ["a", "b"], "v": [1, 2]}), "s.t", ["id"])
    bq.upsert_from_dataframe(pd.DataFrame({"id": ["b"], "v": [5]}), "s.t", ["id"])
    assert bq.query("SELECT id, v FROM s.t ORDER BY id").values.tolist() == [["a", 1], ["b", 5]]
    with pytest.raises(ValueError, match="Duplicate"):
        bq.upsert_from_dataframe(pd.DataFrame({"id": ["x", "x"], "v": [1, 2]}), "s.t", ["id"])
    with pytest.raises(ValueError, match="key columns"):
        bq.insert_if_absent(pd.DataFrame({"id": ["x"]}), "s.t", ["nope"])


# --- gate ---

GOOD = {"n_trades": 120, "sharpe": 1.5, "profit_factor": 1.6, "max_drawdown": -0.08, "avg_r": 0.3,
        "win_rate": 0.45, "total_return": 0.2, "recent_return": 0.03}


def test_check_lists_every_failed_standard():
    settings = TradingSettings()
    assert check(GOOD, settings) == []
    bad = {**GOOD, "n_trades": 10, "sharpe": 0.4, "max_drawdown": -0.25, "avg_r": None}
    assert check(bad, settings) == [
        "n_trades 10 < 50", "sharpe 0.40 < 1.00", "avg_r missing", "max_drawdown -25.0% worse than -15.0%"]
    # No losing trade: profit factor is undefined, which passes
    assert check({**GOOD, "profit_factor": None}, settings) == []
    # An empty threshold is not checked
    assert check({**GOOD, "sharpe": None}, replace(settings, min_sharpe=None)) == []


@pytest.fixture
def env(tmp_path):
    """1h bars for EURUSD in the warehouse, with backtest and trading tables set up."""
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    market = Settings(local_data_dir=str(tmp_path / "data"), datasets=["bars"], bar_timeframes=["1h"])
    market_setup(market, cloud)
    bars = random_bars(n=24 * 60)
    bars["date"] = bars["ts"].dt.date
    for day, df in bars.groupby("date"):
        shaped = shape(df.drop(columns="date"), "bars", "EURUSD", day, pd.Timestamp.now(tz="UTC"),
                       timeframe="1h", price_side="bid")
        cloud.bq.replace_from_dataframe(shaped, bq_table_id("market", "bars_1h"), {"symbol": "EURUSD", "date": day})
    backtest = BacktestSettings()
    backtest_setup(backtest, cloud)
    setup(TradingSettings(), cloud)
    return cloud, market, backtest, bars.drop(columns="date")


def write_metrics(cloud, run_id, created_at, scope=None, recent_return=0.03, **metrics):
    """A done daily run with its metrics (scope None = written before scopes existed = full),
    plus its recent-window row unless recent_return is None."""
    cloud.bq.replace_from_dataframe(pd.DataFrame([{
        "run_id": run_id, "strategy": STRATEGY, "params_json": json.dumps({"rr": 2.0}), "symbols": "EURUSD",
        "timeframe": "1h", "start_date": date(2024, 1, 1), "end_date": date(2026, 1, 1), "initial_capital": 1e4,
        "risk_per_trade": 0.01, "spread_json": "{}", "commission_rate": 0.0, "status": "done",
        "created_at": created_at}]), "backtest.runs", {"run_id": run_id})
    good = {k: v for k, v in GOOD.items() if k != "recent_return"}
    rows = [{"run_id": run_id, "symbol": "EURUSD", "exposure": 0.1, "scope": scope, **{**good, **metrics}}]
    if recent_return is not None:
        rows.append({"run_id": run_id, "symbol": "EURUSD", "scope": "recent", **good, "n_trades": 12,
                     "total_return": recent_return})
    cloud.bq.replace_from_dataframe(pd.DataFrame(rows), "backtest.metrics", {"run_id": run_id})


def test_gate_approves_revokes_and_ignores_adhoc_runs(env):
    cloud, _market, backtest, _bars = env
    settings, t0 = TradingSettings(), datetime(2026, 10, 1, tzinfo=timezone.utc)
    write_metrics(cloud, "daily-a", t0)
    write_metrics(cloud, "adhoc-tuned", t0 + timedelta(hours=1), sharpe=0.1)  # not a daily run: ignored
    [row] = evaluate(settings, backtest, cloud, now=t0)
    assert (row["status"], row["run_id"], row["params_json"]) == ("approved", "daily-a", '{"rr": 2.0}')

    write_metrics(cloud, "daily-b", t0 + timedelta(days=1), sharpe=0.5)
    evaluate(settings, backtest, cloud, now=t0 + timedelta(days=1))
    approvals = store.read(cloud, "trading", "approvals")
    assert approvals[["status", "run_id", "reasons"]].values.tolist() == [["rejected", "daily-b", "sharpe 0.50 < 1.00"]]
    events = store.read(cloud, "trading", "events").sort_values("ts")
    assert events["kind"].tolist() == ["approved", "revoked"]


def test_gate_judges_only_the_newest_run_and_rejects_it_when_stale(env):
    cloud, _market, backtest, _bars = env
    settings, t0 = TradingSettings(min_sharpe=None), datetime(2026, 10, 1, tzinfo=timezone.utc)
    write_metrics(cloud, "daily-a", t0, avg_r=0.5)
    # The newest run has no avg_r: it must not be filled in from the older run
    write_metrics(cloud, "daily-b", t0 + timedelta(days=1), avg_r=None)
    [row] = evaluate(settings, backtest, cloud, now=t0 + timedelta(days=1))
    assert (row["run_id"], row["status"], row["reasons"]) == ("daily-b", "rejected", "avg_r missing")

    # No longer backtested (e.g. dropped from BACKTEST_DAILY_RUNS): its last run can't keep approving it
    write_metrics(cloud, "daily-c", t0 + timedelta(days=2))
    [row] = evaluate(settings, backtest, cloud, now=t0 + timedelta(days=2))
    assert row["status"] == "approved"
    [row] = evaluate(settings, backtest, cloud, now=t0 + timedelta(days=12))
    assert (row["status"], row["reasons"]) == ("rejected", "latest backtest is 10 days old (max 3)")


def test_gate_uses_out_of_sample_metrics_and_the_recent_window(env):
    cloud, _market, backtest, _bars = env
    t0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
    # A walk-forward run: the oos row is judged (sharpe 1.2), not the in-sample full row (sharpe 3.0)
    write_metrics(cloud, "daily-wf", t0, scope="oos", sharpe=1.2)
    extra = pd.DataFrame([{"run_id": "daily-wf", "symbol": "EURUSD", "scope": "full", **{
        k: v for k, v in GOOD.items() if k != "recent_return"}, "sharpe": 3.0}])
    rows = pd.concat([cloud.bq.query("SELECT * FROM backtest.metrics WHERE run_id = 'daily-wf'"), extra])
    cloud.bq.replace_from_dataframe(rows, "backtest.metrics", {"run_id": "daily-wf"})
    [row] = evaluate(TradingSettings(require_out_of_sample=True), backtest, cloud, now=t0)
    assert (row["status"], row["metrics_window"], row["sharpe"], row["recent_trades"]) == ("approved", "oos", 1.2, 12)

    # Still fine over two years, but losing over the last 90 days: revoked
    write_metrics(cloud, "daily-later", t0 + timedelta(days=1), scope="oos", recent_return=-0.04)
    [row] = evaluate(TradingSettings(), backtest, cloud, now=t0 + timedelta(days=1))
    assert (row["status"], row["reasons"]) == ("rejected", "recent_return -4.0% < 0.0%")

    # A fixed-parameter (in-sample) run can't approve when out-of-sample is required
    write_metrics(cloud, "daily-fixed", t0 + timedelta(days=2))
    [row] = evaluate(TradingSettings(require_out_of_sample=True), backtest, cloud, now=t0 + timedelta(days=2))
    assert (row["metrics_window"], row["reasons"]) == ("full", "not a walk-forward (out-of-sample) backtest")


def test_gate_rejects_timeframes_faster_than_the_trading_schedule(env):
    cloud, _market, backtest, _bars = env
    t0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
    write_metrics(cloud, "daily-1min", t0)  # good metrics, but on 1min bars
    runs = cloud.bq.query("SELECT * FROM backtest.runs WHERE run_id = 'daily-1min'").assign(timeframe="1min")
    cloud.bq.replace_from_dataframe(runs, "backtest.runs", {"run_id": "daily-1min"})
    [row] = evaluate(TradingSettings(), backtest, cloud, now=t0)
    assert (row["timeframe"], row["status"]) == ("1min", "rejected")
    assert row["reasons"] == "1min is faster than trading_live's 5min schedule"


# --- paper trading ---

def approve(cloud, decided_at, params=None, symbol="EURUSD"):
    store.write(cloud, "trading", "approvals", [{
        "strategy": STRATEGY, "timeframe": "1h", "symbol": symbol, "status": "approved", "run_id": "daily-x",
        "params_json": json.dumps(strategies.resolve_params(STRATEGY, params)), "decided_at": decided_at}])


def set_cursor(cloud, ts):
    store.write(cloud, "trading", "cursors",
                [{"timeframe": "1h", "symbol": "EURUSD", "last_bar_ts": ts, "updated_at": ts}])


def after(bars, i):
    """A moment just after bar i closed."""
    return bars["ts"].iloc[i] + pd.Timedelta("1h") + pd.Timedelta("1min")


def test_paper_trades_match_the_backtest_engine(env):
    cloud, market, backtest, bars = env
    k, end = 200, len(bars) - 1
    approve(cloud, after(bars, end))
    set_cursor(cloud, bars["ts"].iloc[k])

    # Two steps over the same bars, split part way, as hourly runs would do
    step(NO_LIMITS, backtest, market, cloud, now=after(bars, 900))
    result = step(NO_LIMITS, backtest, market, cloud, now=after(bars, end))

    sig = strategies.run(STRATEGY, bars, strategies.resolve_params(STRATEGY))
    sig.loc[:k, "signal"] = 0  # trading only acts on bars after the cursor
    costs = Costs(spread=backtest.spread_for("EURUSD") / point_factor("EURUSD"), price_side="bid")
    expected, _ = simulate(bars, sig, costs, NO_LIMITS.paper_capital, backtest.risk_per_trade)
    expected = expected[expected["exit_reason"] != "end"]  # the engine closes what's open on the last bar

    got = store.read(cloud, "trading", "positions", "status = 'closed'").sort_values("entry_ts")
    assert len(got) == len(expected) > 5
    for col in ("entry_ts", "exit_ts", "exit_reason"):
        assert got[col].tolist() == expected[col].tolist()
    for col in ("entry_price", "exit_price", "qty", "pnl"):
        np.testing.assert_allclose(got[col], expected[col].astype(float), rtol=1e-9)
    assert result["balance"] == pytest.approx(NO_LIMITS.paper_capital + expected["pnl"].sum())
    orders = store.read(cloud, "trading", "orders")
    assert (orders["status"] == "filled").sum() == len(got) + result["open_positions"]
    assert (store.read(cloud, "trading", "events")["kind"] == "closed").sum() == len(got)


def test_step_is_idempotent(env):
    cloud, market, backtest, bars = env
    approve(cloud, after(bars, len(bars) - 1))
    set_cursor(cloud, bars["ts"].iloc[300])
    now = after(bars, len(bars) - 1)
    first = step(NO_LIMITS, backtest, market, cloud, now=now)
    assert first["ordered"] > 0

    def snapshot():
        return {t: store.read(cloud, "trading", t).drop(columns=["updated_at", "created_at"], errors="ignore")
                .sort_values(key_for(t), ignore_index=True)
                for t in ("signals", "orders", "positions")}

    # Same moment again: no new bars, nothing changes
    before = snapshot()
    again = step(NO_LIMITS, backtest, market, cloud, now=now)
    assert again["bars"] == again["signals"] == 0
    # A retry that lost its cursor write replays the same bars: same ids, no new orders
    set_cursor(cloud, bars["ts"].iloc[300])
    replay = step(NO_LIMITS, backtest, market, cloud, now=now)
    assert replay["ordered"] == 0 and replay["duplicate"] == first["signals"]
    after_ = snapshot()
    for t in before:
        assert len(after_[t]) == len(before[t])
    assert sorted(after_["orders"]["order_id"]) == sorted(before["orders"]["order_id"])


def test_first_step_only_starts_tracking(env):
    cloud, market, backtest, bars = env
    approve(cloud, after(bars, 1000))
    result = step(NO_LIMITS, backtest, market, cloud, now=after(bars, 1000))
    assert result["bars"] == 0
    cursors = store.read(cloud, "trading", "cursors")
    assert cursors["last_bar_ts"].tolist() == [bars["ts"].iloc[1000]]  # newest closed bar, nothing later


def test_stale_and_revoked_approvals_stop_new_orders(env):
    cloud, market, backtest, bars = env
    now = after(bars, len(bars) - 1)
    set_cursor(cloud, bars["ts"].iloc[300])
    approve(cloud, now - timedelta(days=10))  # older than TRADING_APPROVAL_MAX_AGE_DAYS
    result = step(NO_LIMITS, backtest, market, cloud, now=now)
    assert result["signals"] == 0 and store.read(cloud, "trading", "orders").empty

    # A pending order whose strategy lost its approval is cancelled
    approve(cloud, now)
    set_cursor(cloud, bars["ts"].iloc[300])
    step(NO_LIMITS, backtest, market, cloud, now=now)
    pending = store.read(cloud, "trading", "orders", "status = 'pending'")
    store.write(cloud, "trading", "approvals", [{**store.read(cloud, "trading", "approvals").iloc[0].to_dict(),
                                                  "status": "rejected"}])
    result = step(NO_LIMITS, backtest, market, cloud, now=now)
    assert result["cancelled"] == len(pending)
    assert store.read(cloud, "trading", "orders", "status = 'pending'").empty


def test_kill_switch_blocks_new_orders_and_resume_clears_it(env, monkeypatch, capsys):
    cloud, market, backtest, bars = env
    monkeypatch.setenv("DLH_ENV", "local")
    monkeypatch.setenv("LOCAL_GCP_DIR", cloud.settings.local_dir)
    now = after(bars, len(bars) - 1)
    approve(cloud, now)
    set_cursor(cloud, bars["ts"].iloc[300])
    main(["kill", "--reason", "testing"])
    result = step(NO_LIMITS, backtest, market, cloud, now=now)
    assert result["ordered"] == 0 and result["skipped"] == result["signals"] > 0
    assert set(store.read(cloud, "trading", "signals")["reason"]) == {"kill switch on"}
    main(["resume"])
    main(["status"])
    assert "KILL SWITCH: off" in capsys.readouterr().out


def test_daily_loss_limit_turns_on_the_kill_switch(env):
    cloud, market, backtest, bars = env
    now = after(bars, len(bars) - 1)
    approve(cloud, now)
    set_cursor(cloud, bars["ts"].iloc[300])
    # Any losing day trips it: each stop loses 1% of the balance
    settings = replace(NO_LIMITS, max_daily_loss=0.001)
    result = step(settings, backtest, market, cloud, now=now)
    assert result["kill_switch"].startswith("daily loss")
    controls = store.read(cloud, "trading", "controls")
    assert controls["value"].iloc[0] == result["kill_switch"]
    kinds = store.read(cloud, "trading", "events")["kind"]
    assert (kinds == "kill_switch").sum() == 1


def test_risk_limits():
    settings = TradingSettings()
    ok = {"kill_switch": False, "busy": False, "stale": False, "balance": 10_000.0, "open_count": 0,
          "open_risk": 0.0, "new_risk": 100.0}
    assert risk.check(settings, **ok) is None
    assert risk.check(settings, **{**ok, "busy": True}) == "position already open"
    assert risk.check(settings, **{**ok, "open_count": 5}) == "max open positions (5)"
    assert risk.check(settings, **{**ok, "open_risk": 450.0}) == "max open risk (5.5% of balance)"
    assert risk.check(settings, **{**ok, "stale": True}).startswith("stale")


def test_signal_id_is_deterministic():
    ts = pd.Timestamp("2026-10-30 13:00", tz="UTC")
    assert signal_id("trendline", "1h", "XAUUSD", ts) == "trendline:1h:XAUUSD:20261030T1300"


# --- intraday load ---

def make_bi5(rows):
    return lzma.compress(np.array(rows, dtype=TICK_DTYPE).tobytes(), format=lzma.FORMAT_ALONE)


def test_intraday_loads_only_closed_bars_of_today(tmp_path, monkeypatch):
    market = Settings(local_data_dir=str(tmp_path / "data"), datasets=["bars"], bar_timeframes=["15min", "1h"])
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    market_setup(market, cloud)
    client = DukascopyClient(market)
    fetched = []

    def fetch(symbol, hour):
        fetched.append(hour.hour)
        # ticks at :00 and :40 of every hour
        return make_bi5([(0, 117012, 117010, 1.0, 1.0), (2_400_000, 117022, 117020, 1.0, 1.0)])

    monkeypatch.setattr(client, "fetch_hour_raw", fetch)
    now = datetime(2026, 10, 29, 3, 10, tzinfo=timezone.utc)  # a Thursday, 03:00 just closed
    result = load_intraday("EURUSD", market, cloud, now=now, client=client)
    assert (result["status"], result["date"], result["last_hour"]) == ("loaded", "2026-10-29", "ok")
    assert sorted(fetched) == [0, 1, 2]  # hours that haven't closed aren't requested
    bars = cloud.bq.query("SELECT ts FROM market_1h.dukascopy_bars ORDER BY ts")["ts"].tolist()
    assert [t[11:16] for t in bars] == ["00:00", "01:00", "02:00"]

    # Just after midnight the last hour of yesterday still gets loaded
    fetched.clear()
    result = load_intraday("EURUSD", market, cloud, now=datetime(2026, 10, 30, 0, 10, tzinfo=timezone.utc),
                           client=client)
    assert result["date"] == "2026-10-29" and sorted(fetched) == list(range(3, 24))  # 0-2h come from disk
    assert len(cloud.bq.query("SELECT ts FROM market_1h.dukascopy_bars")) == 24


def test_intraday_needs_tick_source(tmp_path):
    market = Settings(local_data_dir=str(tmp_path / "data"), datasets=["bars"], bar_source="candles")
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    with pytest.raises(ValueError, match="ticks"):
        load_intraday("EURUSD", market, cloud, now=datetime(2026, 10, 29, 3, 10, tzinfo=timezone.utc))


# --- live against the backtest ---

def test_drift_lines_paper_trades_up_with_the_backtest(env):
    from backtest.runner import run as backtest_run
    from trading.drift import compare

    cloud, market, backtest, bars = env
    k, end = 200, len(bars) - 1
    start_day, end_day = bars["ts"].iloc[0].date(), bars["ts"].iloc[-1].date()
    backtest_run(STRATEGY, ["EURUSD"], "1h", start_day, end_day, backtest, market, cloud, run_id="daily-x",
                 walk_forward_mode=False)
    approve(cloud, after(bars, end))
    set_cursor(cloud, bars["ts"].iloc[k])
    t = bars["ts"].iloc[k]
    store.write(cloud, "trading", "events", [
        {"event_id": "tracking:1h:EURUSD", "ts": t, "date": t.date(), "kind": "tracking", "ref": "1h:EURUSD",
         "message": "x"},
        {"event_id": "approved:x", "ts": t, "date": t.date(), "kind": "approved", "ref": f"{STRATEGY}:1h:EURUSD",
         "message": "x"}])
    step(NO_LIMITS, backtest, market, cloud, now=after(bars, end))

    [row] = compare(replace(NO_LIMITS, drift_days=3650), backtest, cloud, now=after(bars, end))
    assert row["live_trades"] > 5 and row["missed_unexplained"] == 0
    # The backtest may hold a position across the cursor that live trading never had: at most one
    # trade differs at the start, everything after lines up exactly
    assert row["matched"] >= row["live_trades"] - 1 and row["extra"] <= 1
    assert row["entry_slippage_r"] == pytest.approx(0.0, abs=1e-9)
    assert row["live_avg_r"] == pytest.approx(row["backtest_avg_r"], rel=1e-6)
    assert len(store.read(cloud, "trading", "drift")) == 1
