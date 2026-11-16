"""The Dukascopy broker against a fake dlh-bridge: sizing, labels, fills, exits, the account
balance, and a trading step that only acts on the newest bar. No network."""

import json
from dataclasses import replace

import pandas as pd
import pytest

from backtest import strategies
from backtest.runner import setup as backtest_setup
from backtest.config import BacktestSettings
from cloud import Cloud, CloudSettings
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.pipeline import setup as market_setup
from ingestion.dukascopy.tables import bq_table_id, shape
from trading import executor, risk, store
from trading.config import TradingSettings
from trading.gate import setup as trading_setup
from trading.brokers.dukascopy import BridgeError, DukascopyBroker, exit_reason, label
from trading.executor import signal_id, step
from trading.store import Order
from tests.test_backtest import SILVER_BULLET_5MIN, random_bars
from tests.test_trading import NO_LIMITS, STRATEGY, after, approve, env, set_cursor  # noqa: F401 (fixture)

T0 = pd.Timestamp("2026-11-03 10:00", tz="UTC")


class FakeBridge(DukascopyBroker):
    """Answers like bridge/ does: market orders fill at once at the bid/ask, labels are unique."""

    def __init__(self, bid=100.0, ask=100.2, balance=50_000.0, equity=None, live=None):
        super().__init__("http://bridge.test")
        self.live = live  # {side: DataFrame of bars} served by /bars
        self.bar_requests: list[dict] = []
        self.bid, self.ask, self.account_balance = bid, ask, balance
        self.account_equity = balance if equity is None else equity
        self.orders: dict[str, dict] = {}
        self.sent: list[dict] = []
        self.external: list[dict] = []  # positions opened outside this system

    def _call(self, method, path, body=None, **query):
        if path == "/health":
            return {"connected": True, "account_id": "demo"}
        if path == "/positions":
            return {"positions": [{"label": o["label"], "symbol": "XAU/USD", "side": o["side"], "qty": o["qty"],
                                   "open_price": o["open_price"], "stop": o["stop"]}
                                  for o in [*self.orders.values(), *self.external] if o["state"] == "FILLED"]}
        if path == "/account":
            return {"balance": self.account_balance, "equity": self.account_equity, "currency": "USD"}
        if path == "/bars":
            self.bar_requests.append(query)
            df = (self.live or {}).get(query["side"], pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"]))
            df = df[df["ts"] >= pd.Timestamp(query["from"], unit="ms", tz="UTC")]
            return {"symbol": query["symbol"], "bars": [
                {"time": int(r.ts.timestamp() * 1000), "open": r.open, "high": r.high, "low": r.low,
                 "close": r.close, "volume": r.volume} for r in df.itertuples()]}
        if path == "/tick":
            return {"bid": self.bid, "ask": self.ask, "time": int(T0.timestamp() * 1000)}
        if method == "POST" and path == "/orders":
            self.sent.append(body)
            if body["label"] in self.orders:
                return {**self.orders[body["label"]], "duplicate": True}
            qty = float(int(body["qty"]))  # XAUUSD: 1 oz steps
            if qty < 1:
                return {"label": body["label"], "state": "REJECTED", "reason": "size below the minimum 1.0"}
            long = body["side"] == "long"
            self.orders[body["label"]] = {
                "label": body["label"], "id": f"id{len(self.orders)}", "state": "FILLED", "side": body["side"],
                "qty": qty, "requested_qty": qty, "open_price": self.ask if long else self.bid,
                "fill_time": int(T0.timestamp() * 1000), "close_price": 0.0, "close_time": 0,
                "stop": body["stop"], "target": body["target"] or 0.0, "pnl": 0.0, "commission": 1.5}
            return self.orders[body["label"]]
        if method == "GET" and path.startswith("/orders/"):
            return self.orders.get(path.split("/")[2])
        raise AssertionError(f"unexpected call {method} {path}")

    def close(self, order_id, price, pnl):
        # As Dukascopy's history has it: the stop and target are gone once it's closed
        self.orders[label(order_id)].update(state="CLOSED", close_price=price, pnl=pnl, stop=0.0, target=0.0,
                                            close_time=int((T0 + pd.Timedelta("2h")).timestamp() * 1000),
                                            commission=3.0)


def order(side="long", stop=99.0, target=103.0, risk=100.0):
    oid = signal_id(STRATEGY, "1h", "XAUUSD", T0)
    return Order(order_id=oid, date=T0.date(), strategy=STRATEGY, timeframe="1h", symbol="XAUUSD", side=side,
                 broker="dukascopy", status="pending", stop=stop, target=target, risk=risk, signal_ts=T0,
                 created_at=T0, updated_at=T0)


def test_label_fits_jforex():
    assert label("supply_demand:1h:XAUUSD:20261103T0100") == "supply_demand_1h_XAUUSD_20261103T0100"


def test_submit_sizes_from_the_live_price_and_labels_the_order():
    bridge, o = FakeBridge(), order()
    bridge.submit(o)
    [sent] = bridge.sent
    # A long fills on the ask: 100 risk / (100.2 - 99.0) = 83.3 oz, rounded down by the bridge
    assert sent == {"label": "supply_demand_1h_XAUUSD_20261103T1000", "symbol": "XAUUSD", "side": "long",
                    "qty": pytest.approx(100 / 1.2), "stop": 99.0, "target": 103.0}
    assert (o.status, o.broker_order_id) == ("pending", "id0")  # the fill is recorded by sync

    bridge.submit(o)  # a retry finds the label and sends nothing new
    assert len(bridge.orders) == 1


def test_submit_rejects_past_the_stop_or_below_the_minimum_size():
    past = order(side="short", stop=99.5)  # a short sells the bid, 100.0, already above its stop
    FakeBridge().submit(past)
    assert past.status == "rejected" and "past the stop" in past.reason
    tiny = order(risk=0.5)
    FakeBridge().submit(tiny)
    assert (tiny.status, tiny.reason) == ("rejected", "size below the minimum 1.0")


def test_sync_records_the_fill_then_the_exit_at_dukascopy():
    bridge, o = FakeBridge(), order()
    bridge.submit(o)
    orders, positions = [o], []
    changed = bridge.sync("1h", "XAUUSD", None, orders, positions)
    [pos] = positions
    assert changed == [o, pos]
    assert (o.status, o.qty, o.fill_price, o.commission, o.fill_ts) == ("filled", 83.0, 100.2, 1.5, T0)
    assert (pos.status, pos.qty, pos.entry_price, pos.stop, pos.target) == ("open", 83.0, 100.2, 99.0, 103.0)
    assert bridge.sync("1h", "XAUUSD", None, orders, positions) == []  # nothing new

    bridge.close(o.order_id, price=99.0, pnl=-99.6)
    assert bridge.sync("1h", "XAUUSD", None, orders, positions) == [pos]
    assert (pos.status, pos.exit_price, pos.exit_reason, pos.commission) == ("closed", 99.0, "stop", 3.0)
    assert pos.pnl == pytest.approx(-102.6) and pos.r_multiple == pytest.approx(-1.026)
    assert pos.exit_ts == T0 + pd.Timedelta("2h")


def test_exit_reason_comes_from_our_stop_and_target():
    bridge, positions = FakeBridge(), []
    long, short = order(), order(side="short", stop=101.0, target=97.0)
    short.order_id += "s"
    for o in (long, short):
        bridge.submit(o)
    bridge.sync("1h", "XAUUSD", None, [long, short], positions)
    lp, sp = positions
    assert [exit_reason(lp, p) for p in (98.7, 99.0, 101.0, 103.0, 103.4)] == [
        "stop", "stop", "closed", "target", "target"]  # a gap fills past the level
    assert [exit_reason(sp, p) for p in (101.3, 101.0, 99.0, 97.0, 96.5)] == [
        "stop", "stop", "closed", "target", "target"]


def test_an_order_dukascopy_never_filled_is_rejected():
    bridge, o = FakeBridge(), order()
    o.broker_order_id = "lost"  # submitted, but the bridge has no such label
    bridge.sync("1h", "XAUUSD", None, [o], [])
    assert o.status == "rejected" and "not found" in o.reason


def test_unreachable_bridge_says_how_to_start_it():
    with pytest.raises(BridgeError, match="bridge/run.sh"):
        DukascopyBroker("http://127.0.0.1:9", timeout=2).balance()


def test_step_orders_only_on_the_newest_bar_sized_from_the_account(env):
    cloud, market, backtest, bars = env
    sig = strategies.run(STRATEGY, bars, strategies.resolve_params(STRATEGY))
    hits = sig.index[(sig["signal"] != 0) & (sig.index > 300)]
    first, last = hits[0], hits[-1]
    approve(cloud, after(bars, len(bars) - 1))
    close = float(bars["close"].iloc[last])
    bridge = FakeBridge(bid=close, ask=close, balance=50_000.0)

    # Catching up over many bars: every earlier signal is stale, only the newest bar may trade
    set_cursor(cloud, bars["ts"].iloc[first - 1])
    result = step(NO_LIMITS, backtest, market, cloud, broker=bridge, now=after(bars, last))
    signals = store.read(cloud, "trading", "signals").set_index("signal_id")
    newest = signal_id(STRATEGY, "1h", "EURUSD", bars["ts"].iloc[last])
    assert (signals.drop(newest)["reason"] == "stale bar (not the newest)").all()
    assert signals.loc[newest, "status"] == "ordered" and result["ordered"] == 1
    [o] = store.records(store.read(cloud, "trading", "orders"), Order)
    assert o.risk == pytest.approx(50_000.0 * backtest.risk_per_trade)  # the account's balance, not paper capital
    assert result["balance"] == 50_000.0


def test_account_snapshot_records_the_brokers_equity(env):
    # A test order closed at Dukascopy outside this system: the equity has the loss, the balance not yet
    cloud, market, backtest, bars = env
    set_cursor(cloud, bars["ts"].iloc[-2])
    result = step(NO_LIMITS, backtest, market, cloud, broker=FakeBridge(balance=10_000.0, equity=9_999.56),
                  now=after(bars, len(bars) - 1))
    [row] = store.read(cloud, "trading", "account").itertuples()
    assert (row.balance, row.equity) == (10_000.0, 9_999.56)
    assert result["equity"] == 9_999.56


# --- live bars ---

def test_bars_come_back_as_a_frame_and_mid_averages_both_sides():
    ts = pd.date_range("2026-11-03 10:00", periods=3, freq="5min", tz="UTC")
    bid = pd.DataFrame({"ts": ts, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 3.0})
    ask = bid.assign(open=1.2, high=2.2, low=0.7, close=1.7).iloc[1:]  # ask lacks the first bar
    bridge = FakeBridge(live={"bid": bid, "ask": ask})
    got = bridge.bars("XAUUSD", "5min", ts[0], "bid")
    assert got["ts"].tolist() == list(ts[1:])  # opening after `after`
    assert bridge.bar_requests[0] == {"side": "bid", "symbol": "XAUUSD", "period": "5min",
                                      "from": int(ts[1].timestamp() * 1000)}
    mid = bridge.bars("XAUUSD", "5min", ts[0] - pd.Timedelta("5min"), "mid")
    assert mid["ts"].tolist() == list(ts[1:])  # only where both sides have the bar
    assert mid[["open", "high", "low", "close", "volume"]].iloc[0].tolist() == pytest.approx([1.1, 2.1, 0.6, 1.6, 3.0])


@pytest.fixture
def env5(tmp_path):
    """5min XAUUSD bars, with a silver_bullet approval that signals on most hours."""
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    market = Settings(local_data_dir=str(tmp_path / "data"), datasets=["bars"], bar_timeframes=["5min"])
    market_setup(market, cloud)
    backtest = BacktestSettings()
    backtest_setup(backtest, cloud)
    trading_setup(TradingSettings(), cloud)
    bars = random_bars(n=12 * 24 * 6, freq="5min")
    params = strategies.resolve_params("silver_bullet", SILVER_BULLET_5MIN)
    sig = strategies.run("silver_bullet", bars, params)
    store.write(cloud, "trading", "approvals", [{
        "strategy": "silver_bullet", "timeframe": "5min", "symbol": "XAUUSD", "status": "approved",
        "run_id": "daily-x", "params_json": json.dumps(params), "decided_at": bars["ts"].iloc[-1]}])
    return cloud, market, backtest, bars, sig


def to_warehouse(cloud, bars):
    bars = bars.assign(date=bars["ts"].dt.date)
    for day, df in bars.groupby("date"):
        shaped = shape(df.drop(columns="date"), "bars", "XAUUSD", day, pd.Timestamp.now(tz="UTC"),
                       timeframe="5min", price_side="bid")
        cloud.bq.replace_from_dataframe(shaped, bq_table_id("market", "bars_5min"), {"symbol": "XAUUSD", "date": day})


def test_5min_signal_is_ordered_from_the_bridge_before_the_warehouse_has_its_hour(env5):
    cloud, market, backtest, bars, sig = env5
    hit = sig.index[(sig["signal"] != 0) & (sig.index > 12 * 24 * 3)][0]
    # The hourly load has the warehouse up to 40 minutes before the signal bar; the bridge has the rest
    to_warehouse(cloud, bars.iloc[:hit - 7])
    close = float(bars["close"].iloc[hit])
    bridge = FakeBridge(bid=close, ask=close, live={"bid": bars.iloc[:hit + 1]})
    store.write(cloud, "trading", "cursors", [{"timeframe": "5min", "symbol": "XAUUSD",
                                               "last_bar_ts": bars["ts"].iloc[hit - 1], "updated_at": T0}])
    now = bars["ts"].iloc[hit] + pd.Timedelta("5min") + pd.Timedelta("30s")  # the minute the bar closed
    result = step(NO_LIMITS, backtest, market, cloud, broker=bridge, now=now)

    assert bridge.bar_requests[0]["from"] == int(bars["ts"].iloc[hit - 7].timestamp() * 1000)
    assert result["bars"] == 1 and result["ordered"] == 1
    [row] = store.read(cloud, "trading", "signals").itertuples()
    assert row.signal_id == signal_id("silver_bullet", "5min", "XAUUSD", bars["ts"].iloc[hit])
    assert (row.status, row.side) == ("ordered", "long" if sig["signal"][hit] > 0 else "short")
    assert row.stop == pytest.approx(sig["stop"][hit])  # the signal the backtest computes on the full bars


def test_step_skips_a_timeframe_whose_next_bar_has_not_closed(env5, monkeypatch):
    cloud, market, backtest, bars, _sig = env5
    to_warehouse(cloud, bars)
    last = bars["ts"].iloc[-1]
    store.write(cloud, "trading", "cursors", [{"timeframe": "5min", "symbol": "XAUUSD",
                                               "last_bar_ts": last, "updated_at": T0}])
    loads = []
    monkeypatch.setattr(executor, "load_bars", lambda *a, **k: loads.append(a) or bars)
    bridge = FakeBridge(live={"bid": bars})
    # One minute after the newest bar closed, the next one is 4 minutes away: no query at all
    result = step(NO_LIMITS, backtest, market, cloud, broker=bridge, now=last + pd.Timedelta("6min"))
    assert result["bars"] == 0 and loads == [] and bridge.bar_requests == []


def test_on_the_5_minute_schedule_15min_bars_are_pulled_every_third_run(env5, monkeypatch):
    cloud, market, backtest, _bars, _sig = env5
    bars = random_bars(n=4 * 24 * 6, freq="15min").assign(price_side="bid", ingested_at=T0)
    five = _bars.assign(price_side="bid", ingested_at=T0)
    cursor = bars["ts"].iloc[-2]  # the bar before the newest; the newest closes at last + 15min
    store.write(cloud, "trading", "approvals", [{
        "strategy": "supply_demand", "timeframe": "15min", "symbol": "XAUUSD", "status": "approved",
        "run_id": "daily-x", "params_json": json.dumps(strategies.resolve_params("supply_demand")),
        "decided_at": bars["ts"].iloc[-1]}])
    loads = []
    monkeypatch.setattr(executor, "load_bars",
                        lambda cloud_, market_, sym, tf, *a: loads.append(tf) or (bars if tf == "15min" else five))
    pulled = []
    for minute in (1, 6, 11, 16):  # the runs of 1-59/5 after the cursor bar closed
        store.write(cloud, "trading", "cursors", [{"timeframe": "15min", "symbol": "XAUUSD",
                                                   "last_bar_ts": cursor, "updated_at": T0}])
        loads.clear()
        now = cursor + pd.Timedelta("15min") + pd.Timedelta(minutes=minute)
        step(NO_LIMITS, backtest, market, cloud, broker=FakeBridge(live={"bid": bars}), now=now)
        pulled.append("15min" in loads)
    # The bar after the cursor closes at +30min: only the run at :16 past it (+31min) pulls it
    assert pulled == [False, False, False, True]


# --- broker down, reconciliation, per-symbol risk ---

class DownBridge(FakeBridge):
    up = False

    def _call(self, method, path, body=None, **query):
        if not self.up:
            raise BridgeError("bridge not reachable at http://bridge.test (refused); start it with bridge/run.sh")
        return super()._call(method, path, body, **query)


def test_a_down_broker_skips_the_step_and_alerts_once_per_outage(env, monkeypatch):
    from ops import alerts

    cloud, market, backtest, bars = env
    sent = []
    monkeypatch.setattr(alerts, "notify", lambda subject, text="", key=None: sent.append(subject) or ["test"])
    set_cursor(cloud, bars["ts"].iloc[-2])
    bridge = DownBridge()
    for _ in range(3):
        result = step(NO_LIMITS, backtest, market, cloud, broker=bridge, now=after(bars, len(bars) - 1))
        assert result["broker"] == "down"
    assert sent == ["DLH broker dukascopy is down"]
    controls = store.read(cloud, "trading", "controls").set_index("key")
    assert controls.loc["broker_status", "value"].startswith("down since")

    bridge.up = True
    result = step(NO_LIMITS, backtest, market, cloud, broker=bridge, now=after(bars, len(bars) - 1))
    assert "broker" not in result and sent[-1] == "DLH broker dukascopy is back"
    assert store.read(cloud, "trading", "controls").set_index("key").loc["broker_status", "value"] is None
    kinds = store.read(cloud, "trading", "events")["kind"].tolist()
    assert kinds.count("broker_down") == 1 and kinds.count("broker_up") == 1


def test_a_position_opened_outside_the_system_counts_toward_the_limits(env, monkeypatch):
    from ops import alerts

    cloud, market, backtest, bars = env
    monkeypatch.setattr(alerts, "notify", lambda *a, **k: [])
    sig = strategies.run(STRATEGY, bars, strategies.resolve_params(STRATEGY))
    last = sig.index[(sig["signal"] != 0) & (sig.index > 300)][-1]
    approve(cloud, after(bars, len(bars) - 1))
    close = float(bars["close"].iloc[last])
    bridge = FakeBridge(bid=close, ask=close, balance=10_000.0)
    # Opened by hand, 5% of the balance at risk to its stop
    bridge.external.append({"label": "manual_1", "state": "FILLED", "side": "long", "qty": 100.0,
                            "open_price": 100.0, "stop": 95.0})
    set_cursor(cloud, bars["ts"].iloc[last - 1])
    settings = replace(NO_LIMITS, max_open_risk=0.05)
    result = step(settings, backtest, market, cloud, broker=bridge, now=after(bars, last))
    assert result["external_positions"] == 1 and result["ordered"] == 0
    [row] = store.read(cloud, "trading", "signals").itertuples()
    assert row.reason.startswith("max open risk")
    [ev] = store.read(cloud, "trading", "events", "kind = 'unknown_position'").itertuples()
    assert ev.ref == "manual_1" and "stop 95" in ev.message


def test_per_symbol_risk_cap_counts_same_direction_only():
    settings = TradingSettings(max_open_positions=None, max_open_risk=None, max_symbol_risk=0.01)
    ok = dict(kill_switch=False, busy=False, stale=False, balance=10_000.0, open_count=1, open_risk=100.0,
              new_risk=100.0)
    assert risk.check(settings, **ok, symbol_risk=0.0) is None
    assert risk.check(settings, **ok, symbol_risk=100.0) == "max risk on the symbol (2.0% of balance)"
