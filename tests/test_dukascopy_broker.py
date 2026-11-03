"""The Dukascopy broker against a fake dlh-bridge: sizing, labels, fills, exits, the account
balance, and a trading step that only acts on the newest bar. No network."""

from datetime import datetime, timezone

import pandas as pd
import pytest

from backtest import strategies
from trading import store
from trading.brokers.dukascopy import BridgeError, DukascopyBroker, exit_reason, label
from trading.executor import signal_id, step
from trading.store import Order
from tests.test_trading import NO_LIMITS, STRATEGY, after, approve, env, set_cursor  # noqa: F401 (fixture)

T0 = pd.Timestamp("2026-11-03 10:00", tz="UTC")


class FakeBridge(DukascopyBroker):
    """Answers like bridge/ does: market orders fill at once at the bid/ask, labels are unique."""

    def __init__(self, bid=100.0, ask=100.2, balance=50_000.0, equity=None):
        super().__init__("http://bridge.test")
        self.bid, self.ask, self.account_balance = bid, ask, balance
        self.account_equity = balance if equity is None else equity
        self.orders: dict[str, dict] = {}
        self.sent: list[dict] = []

    def _call(self, method, path, body=None, **query):
        if path == "/account":
            return {"balance": self.account_balance, "equity": self.account_equity, "currency": "USD"}
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
