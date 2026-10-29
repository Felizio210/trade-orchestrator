from datetime import date

import numpy as np
import pandas as pd
import pytest

from backtest import strategies
from backtest.__main__ import main
from backtest.config import BacktestSettings
from backtest.engine import Costs, simulate
from backtest.runner import run, setup
from cloud import Cloud, CloudSettings
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.pipeline import setup as market_setup
from ingestion.dukascopy.tables import bq_table_id, shape


def make_bars(rows, start="2026-09-01", freq="1h"):
    """rows: [(open, high, low, close), ...]"""
    df = pd.DataFrame(rows, columns=["open", "high", "low", "close"], dtype=float)
    df.insert(0, "ts", pd.date_range(start, periods=len(df), freq=freq, tz="UTC"))
    df["volume"] = 1.0
    return df


def random_bars(n=1500, seed=7):
    rng = np.random.default_rng(seed)
    close = 1.1 + np.cumsum(rng.normal(0, 0.001, n) * rng.choice([1, 1, 1, 4], n))
    open_ = np.r_[close[0], close[:-1]]
    spread = np.abs(rng.normal(0, 0.0007, n))
    return make_bars(np.c_[open_, np.maximum(open_, close) + spread, np.minimum(open_, close) - spread, close])


def signal_frame(n, at=None):
    """One signal at bar `at`: (index, side, stop, target)."""
    sig = pd.DataFrame({"signal": np.zeros(n, dtype=np.int8), "stop": np.nan, "target": np.nan})
    if at:
        i, side, stop, target = at
        sig.loc[i, ["signal", "stop", "target"]] = side, stop, target
    return sig


NO_COSTS = Costs(spread=0.0)


# --- engine ---

def test_long_fills_next_open_and_hits_target():
    bars = make_bars([(10, 10, 10, 10), (11, 11, 11, 11), (11, 13, 11, 12), (12, 12, 12, 12)])
    trades, equity = simulate(bars, signal_frame(4, (0, 1, 9, 12)), NO_COSTS, 1000, 0.1)
    t = trades.iloc[0]
    assert (t.entry_price, t.exit_price, t.exit_reason) == (11, 12, "target")
    assert t.qty == pytest.approx(100 / 2)  # risk 10% of 1000 over a 2.0 stop distance
    assert t.pnl == pytest.approx(50) and t.r_multiple == pytest.approx(0.5)
    assert list(equity["equity"]) == [1000, 1000, 1050, 1050]
    assert list(equity["position"]) == [0, 50, 0, 0]


def test_stop_wins_when_bar_hits_both():
    bars = make_bars([(10, 10, 10, 10), (10, 10, 10, 10), (10, 13, 8, 10)])
    trades, _ = simulate(bars, signal_frame(3, (0, 1, 9, 12)), NO_COSTS, 1000, 0.1)
    assert trades.iloc[0].exit_reason == "stop" and trades.iloc[0].exit_price == 9
    assert trades.iloc[0].r_multiple == pytest.approx(-1)


def test_gap_through_stop_fills_at_open():
    bars = make_bars([(10, 10, 10, 10), (10, 10, 10, 10), (7, 8, 6, 7)])
    trades, _ = simulate(bars, signal_frame(3, (0, 1, 9, 12)), NO_COSTS, 1000, 0.1)
    assert trades.iloc[0].exit_price == 7 and trades.iloc[0].r_multiple == pytest.approx(-3)


def test_short_sells_bid_and_buys_back_ask():
    # Bid bars, spread 0.5: a bid low of 7.6 is an ask low of 8.1, short of an 8.0 target
    bars = make_bars([(10, 10, 10, 10), (10, 10, 10, 10), (10, 10, 7.6, 9), (9, 9, 7.4, 8)])
    trades, _ = simulate(bars, signal_frame(4, (0, -1, 11, 8)), Costs(spread=0.5), 1000, 0.1)
    t = trades.iloc[0]
    assert t.entry_price == 10  # sold the bid
    assert (t.exit_ts, t.exit_price, t.exit_reason) == (bars["ts"][3], 8, "target")


def test_long_buys_ask_on_mid_bars():
    bars = make_bars([(10, 10, 10, 10), (10, 10, 10, 10), (10, 10, 10, 10)])
    trades, _ = simulate(bars, signal_frame(3, (0, 1, 9, np.nan)), Costs(spread=0.2, price_side="mid"), 1000, 0.1)
    assert trades.iloc[0].entry_price == pytest.approx(10.1)
    assert trades.iloc[0].exit_price == pytest.approx(9.9)  # closed at the end on the bid


def test_open_position_closed_on_last_bar():
    bars = make_bars([(10, 10, 10, 10), (10, 10, 10, 10), (10, 10.5, 9.5, 10.2)])
    trades, equity = simulate(bars, signal_frame(3, (0, 1, 9, np.nan)), NO_COSTS, 1000, 0.1)
    assert trades.iloc[0].exit_reason == "end" and trades.iloc[0].exit_price == 10.2
    assert equity["position"].iloc[-1] == 0 and equity["equity"].iloc[-1] == pytest.approx(1000 + 100 * 0.2)


# --- strategies ---

@pytest.mark.parametrize("name,overrides", [
    ("supply_demand", {}),
    ("trendline", {"mode": "bounce"}),
    ("trendline", {"mode": "break"}),
])
def test_strategies_never_look_ahead(name, overrides):
    bars = random_bars()
    params = strategies.resolve_params(name, overrides)
    full = strategies.run(name, bars, params)
    assert (full["signal"] != 0).sum() > 5, "the test data should produce signals"
    for k in (300, 777, 1200):
        prefix = strategies.run(name, bars.iloc[:k].reset_index(drop=True), params)
        pd.testing.assert_frame_equal(prefix, full.iloc[:k].reset_index(drop=True))


@pytest.mark.parametrize("name", ["supply_demand", "trendline"])
def test_signals_have_stop_on_the_losing_side(name):
    bars = random_bars()
    sig = strategies.run(name, bars, strategies.resolve_params(name))
    s = sig[sig["signal"] != 0]
    close = bars.loc[s.index, "close"]
    assert ((s["stop"] - close) * s["signal"] < 0).all()
    assert ((s["target"] - close) * s["signal"] > 0).all()


def test_supply_demand_trades_demand_zone_retest():
    flat = [(10, 10.2, 9.8, 10)] * 20  # ATR ~0.4
    base = [(10, 10.1, 9.9, 10.05), (10.05, 10.1, 9.9, 10.0)]
    impulse = [(10, 11.2, 10, 11.1)]
    away = [(11.1, 11.4, 11, 11.3)] * 3
    retest = [(11.3, 11.3, 10.0, 10.3)]  # reaches the zone top 10.05, closes above the low 9.9
    bars = make_bars(flat + base + impulse + away + [retest[0]])
    sig = strategies.run("supply_demand", bars, strategies.resolve_params("supply_demand"))
    hits = sig.index[sig["signal"] != 0].tolist()
    assert hits == [len(bars) - 1] and sig["signal"].iloc[-1] == 1
    assert sig["stop"].iloc[-1] < 9.9


def test_trendline_bounce_buys_touch_of_rising_line():
    # Swing lows 10 (bar 2) and 11 (bar 7), known at bar 9: the line is 11 + 0.2 x (i - 7).
    # Bar 12 dips to 12.05 against a line at 12.0 and closes above it -> long.
    lows = [12, 11, 10, 11, 12, 12.5, 12, 11, 11.5, 12.5, 13, 13.5, 12.05, 13]
    bars = make_bars([(low + 0.5, low + 1, low, low + 0.6) for low in lows])
    params = strategies.resolve_params("trendline", {"pivot_len": "2", "min_gap": "3", "atr_len": "3"})
    sig = strategies.run("trendline", bars, params)
    assert sig.index[sig["signal"] != 0].tolist() == [12] and sig["signal"][12] == 1
    assert sig["stop"][12] < 12.0 < bars["close"][12]


def test_unknown_param_is_rejected():
    with pytest.raises(ValueError, match="no parameter"):
        strategies.resolve_params("trendline", {"nope": "1"})


# --- end to end on the local warehouse ---

@pytest.fixture
def env(tmp_path):
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    market = Settings(local_data_dir=str(tmp_path / "data"), datasets=["bars"], bar_timeframes=["1h"])
    market_setup(market, cloud)
    bars = random_bars(n=24 * 60)
    bars["date"] = bars["ts"].dt.date
    for symbol in ("EURUSD", "GBPUSD"):
        for day, df in bars.groupby("date"):
            shaped = shape(df.drop(columns="date"), "bars", symbol, day, pd.Timestamp.now(tz="UTC"),
                           timeframe="1h", price_side="bid")
            cloud.bq.replace_from_dataframe(shaped, bq_table_id("market", "bars_1h"),
                                            {"symbol": symbol, "date": day})
    settings = BacktestSettings()
    setup(settings, cloud)
    return cloud, market, settings


def test_run_stores_results_and_rerun_replaces_them(env):
    cloud, market, settings = env
    args = ("supply_demand", ["EURUSD", "GBPUSD"], "1h", date(2026, 9, 1), date(2026, 10, 30), settings, market, cloud)
    result = run(*args)
    rid = result["run_id"]
    assert [m["symbol"] for m in result["metrics"]] == ["EURUSD", "GBPUSD", "ALL"]

    def counts():
        return {t: int(cloud.bq.query(f"SELECT COUNT(*) AS n FROM backtest.{t} WHERE run_id = @r",
                                      {"r": rid})["n"][0]) for t in ("runs", "trades", "equity", "metrics")}

    first = counts()
    assert first["runs"] == 1 and first["metrics"] == 3 and first["equity"] == 2 * 24 * 60
    assert first["trades"] == sum(m["n_trades"] for m in result["metrics"][:2]) > 0
    runs = cloud.bq.query("SELECT status, data_cutoff, params_json FROM backtest.runs WHERE run_id = @r", {"r": rid})
    assert runs["status"][0] == "done" and runs["data_cutoff"][0] is not None

    run(*args, run_id=rid)
    assert counts() == first

    # Trades join back to the bars they were run on
    joined = cloud.bq.query(
        "SELECT COUNT(*) AS n FROM backtest.trades t JOIN market_1h.dukascopy_bars b "
        "ON b.symbol = t.symbol AND b.ts = t.entry_ts WHERE t.run_id = @r", {"r": rid})
    assert joined["n"][0] == first["trades"]


def test_failed_run_is_recorded(env):
    cloud, market, settings = env
    with pytest.raises(ValueError):
        run("trendline", ["EURUSD"], "1h", date(2026, 9, 1), date(2026, 9, 30), settings, market, cloud,
            params={"mode": "sideways"}, run_id="bad-run")
    row = cloud.bq.query("SELECT status, error FROM backtest.runs WHERE run_id = 'bad-run'")
    assert row["status"][0] == "failed" and "sideways" in row["error"][0]


def test_cli_run_and_list(env, monkeypatch, capsys):
    cloud, market, settings = env
    monkeypatch.setenv("DLH_ENV", "local")
    monkeypatch.setenv("LOCAL_GCP_DIR", cloud.settings.local_dir)
    monkeypatch.setenv("DUKASCOPY_BAR_TIMEFRAMES", "1h")
    main(["run", "--strategy", "trendline", "--symbols", "EURUSD", "--start", "2026-09-01",
          "--end", "2026-10-30", "--param", "mode=break"])
    main(["list"])
    out = capsys.readouterr().out
    assert "run_id trendline-" in out and "done" in out
