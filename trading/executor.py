"""One trading step: new bars -> broker sync -> signals of approved strategies -> orders.

    1. Load state: approved strategies (fresher than TRADING_APPROVAL_MAX_AGE_DAYS),
       pending orders, open positions, each timeframe/symbol's cursor (newest bar processed).
    2. Pending orders of strategies no longer approved are cancelled. Open positions keep
       running to their stop or target.
    3. For every timeframe/symbol with an approved strategy or something open: load the new
       closed bars plus TRADING_WARMUP_DAYS before them, and run each approved strategy over
       them with its approved parameters. Strategies have no lookahead, so the signal on a
       bar is the one the backtest would have produced there.
    4. Walk the new bars of every timeframe/symbol in close-time order. On each bar the
       broker fills pending orders and applies stops/targets, the daily loss limit is
       checked, and each signal goes through the risk checks and becomes an order that
       fills on the next bar.
    5. Write orders, positions, cursors, an account snapshot and events.

The first time a timeframe/symbol is seen, its cursor is set to its newest bar and
nothing is traded: trading starts from the bars that arrive after that.

No double orders: signal_id = order_id is built from strategy, timeframe, symbol and bar
time; a signal is only ordered if inserting it into trading.signals actually inserted it
(insert_if_absent), and the order is written right after it is submitted. If a step dies
between the two, that signal is never ordered (missed, not doubled). Re-running a step
over bars it already processed finds the same ids and changes nothing.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pandas as pd

from backtest import strategies
from backtest.config import BacktestSettings
from backtest.data import load_bars
from cloud import Cloud
from ingestion.dukascopy.config import Settings
from trading import brokers, risk, store
from trading.brokers.base import Broker
from trading.config import TradingSettings
from trading.store import Order, Position
from trading.tables import table_id

log = logging.getLogger(__name__)

KILL_SWITCH = "kill_switch"


def signal_id(strategy: str, timeframe: str, symbol: str, bar_ts: pd.Timestamp) -> str:
    return f"{strategy}:{timeframe}:{symbol}:{bar_ts:%Y%m%dT%H%M}"


def _utc(ts) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    return ts.tz_convert("UTC") if ts.tzinfo else ts.tz_localize("UTC")


@dataclass
class Book:
    """The account as the step sees it. Balance = capital + realized PnL - entry
    commission of open positions (as the backtest engine's cash)."""

    capital: float
    realized_before: float  # PnL of positions closed before the ones held in `positions`
    orders: list[Order]
    positions: list[Position]  # open ones, plus those closed since the first day being processed
    dirty: dict[str, Order | Position] = field(default_factory=dict)

    def balance(self) -> float:
        closed = sum(p.pnl for p in self.positions if p.status == "closed")
        open_commission = sum(p.entry_commission for p in self.positions if p.status == "open")
        return self.capital + self.realized_before + closed - open_commission

    def live(self) -> list[Order | Position]:
        return ([o for o in self.orders if o.status == "pending"]
                + [p for p in self.positions if p.status == "open"])

    def busy(self, strategy: str, timeframe: str, symbol: str) -> bool:
        return any((r.strategy, r.timeframe, r.symbol) == (strategy, timeframe, symbol) for r in self.live())

    def day_pnl(self, day) -> float:
        return sum(p.pnl for p in self.positions if p.status == "closed" and p.exit_ts.date() == day)

    def touch(self, record: Order | Position) -> None:
        self.dirty[record.order_id if isinstance(record, Order) else "pos:" + record.position_id] = record


def step(settings: TradingSettings, backtest: BacktestSettings, market: Settings, cloud: Cloud,
         *, broker: Broker | None = None, now: datetime | None = None) -> dict:
    """Process every bar that closed since the last step. Returns a summary."""
    settings.validate()
    backtest.validate()
    now = _utc(now or datetime.now(timezone.utc))
    ds = settings.dataset
    broker = broker or brokers.get(settings, backtest, market)
    events: list[dict] = []
    counts = {"bars": 0, "signals": 0, "ordered": 0, "skipped": 0, "duplicate": 0, "filled": 0,
              "rejected": 0, "cancelled": 0, "closed": 0}

    def event(kind: str, ref: str, message: str, ts: pd.Timestamp | None = None, key: str | None = None) -> None:
        events.append({"event_id": f"{kind}:{key or ref}", "ts": ts or now, "date": (ts or now).date(),
                       "kind": kind, "ref": ref, "message": message})
        log.info("%s %s: %s", kind, ref, message)

    # 1. State
    approvals = store.read(cloud, ds, "approvals", "status = 'approved'")
    fresh = approvals["decided_at"] >= now - timedelta(days=settings.approval_max_age_days)
    approved = {(r.strategy, r.timeframe, r.symbol): json.loads(r.params_json)
                for r in approvals[fresh].itertuples()}
    cursors = {(r.timeframe, r.symbol): r.last_bar_ts for r in store.read(cloud, ds, "cursors").itertuples()}
    since = min(cursors.values()).floor("D") if cursors else now.floor("D")
    orders = store.records(store.read(cloud, ds, "orders", "status = 'pending'"), Order)
    positions = store.records(
        store.read(cloud, ds, "positions", "status = 'open' OR exit_ts >= @since", {"since": since}), Position)
    realized = cloud.bq.query(
        f"SELECT SUM(pnl) AS pnl FROM `{table_id(ds, 'positions')}` WHERE status = 'closed' AND exit_ts < @since",
        {"since": since})["pnl"].iloc[0]
    book = Book(settings.paper_capital, float(store.clean(realized) or 0.0), orders, positions)
    controls = store.read(cloud, ds, "controls")
    kill = controls.loc[controls["key"] == KILL_SWITCH, "value"].dropna()
    kill_reason = kill.iloc[0] if len(kill) and kill.iloc[0] else None
    kill_was_on = kill_reason is not None

    # 2. Strategies that lost their approval open nothing new
    for order in book.orders:
        if order.status == "pending" and (order.strategy, order.timeframe, order.symbol) not in approved:
            order.status, order.reason, order.updated_at = "cancelled", "strategy no longer approved", now
            book.touch(order)
            counts["cancelled"] += 1
            event("cancelled", order.order_id, order.reason)

    # 3. New bars and signals per timeframe/symbol
    groups = {(tf, sym) for _s, tf, sym in approved} | {(r.timeframe, r.symbol) for r in book.live()}
    feeds: dict[tuple[str, str], tuple[pd.DataFrame, dict[str, pd.DataFrame]]] = {}
    timeline = []
    for tf, sym in sorted(groups):
        cursor = cursors.get((tf, sym))
        start = (cursor if cursor is not None else now) - timedelta(days=settings.warmup_days)
        bars = load_bars(cloud, market, sym, tf, start.date(), now.date())
        bars = bars[bars["ts"] + pd.Timedelta(tf) <= now].reset_index(drop=True)  # closed bars only
        if bars.empty:
            log.warning("%s %s: no closed bars to trade on", sym, tf)
            continue
        if cursor is None:
            cursors[(tf, sym)] = bars["ts"].iloc[-1]
            event("tracking", f"{tf}:{sym}", f"trading starts after {bars['ts'].iloc[-1]}")
            continue
        sigs = {s: strategies.run(s, bars, params) for (s, t, y), params in approved.items() if (t, y) == (tf, sym)}
        feeds[(tf, sym)] = (bars, sigs)
        closes = bars["ts"] + pd.Timedelta(tf)
        timeline += [(closes[i], tf, sym, i) for i in bars.index[bars["ts"] > cursor]]
    timeline.sort()

    # 4. Walk the bars in the order they closed
    marks: dict[tuple[str, str], pd.Series] = {}
    for _close, tf, sym, i in timeline:
        bars, sigs = feeds[(tf, sym)]
        bar = bars.iloc[i]
        counts["bars"] += 1
        for record in broker.sync(tf, sym, bar, book.orders, book.positions):
            book.touch(record)
            if isinstance(record, Order):
                counts[record.status] += 1
                event(record.status, record.order_id,
                      f"{record.side} {record.qty:g} at {record.fill_price:g}" if record.status == "filled"
                      else record.reason or record.status, bar["ts"])
            elif record.status == "closed":
                counts["closed"] += 1
                event("closed", record.position_id, f"{record.exit_reason} at {record.exit_price:g}, "
                      f"pnl {record.pnl:+.2f} ({record.r_multiple:+.2f}R)", bar["ts"])
        marks[(tf, sym)] = bar

        if kill_reason is None and settings.max_daily_loss is not None:
            day = bar["ts"].date()
            day_pnl = book.day_pnl(day)
            opening = book.balance() - day_pnl
            if day_pnl < 0 and -day_pnl >= settings.max_daily_loss * opening:
                kill_reason = f"daily loss {day_pnl:+.2f} on {day} reached {settings.max_daily_loss:.1%} of {opening:.2f}"
                event("kill_switch", KILL_SWITCH, kill_reason, bar["ts"], key=f"{KILL_SWITCH}:{day}")

        for strategy, sig in sigs.items():
            side = int(sig["signal"].iloc[i])
            if side == 0:
                continue
            counts["signals"] += 1
            _handle_signal(strategy, tf, sym, bar, sig.iloc[i], side, now=now, settings=settings,
                           backtest=backtest, cloud=cloud, broker=broker, book=book, counts=counts, event=event,
                           kill=kill_reason is not None,
                           stale=broker.realtime and bar["ts"] != bars["ts"].iloc[-1])
        cursors[(tf, sym)] = bar["ts"]

    # 5. Write
    store.write(cloud, ds, "orders", [r for r in book.dirty.values() if isinstance(r, Order)])
    store.write(cloud, ds, "positions", [r for r in book.dirty.values() if isinstance(r, Position)])
    store.write(cloud, ds, "cursors", [{"timeframe": tf, "symbol": sym, "last_bar_ts": ts, "updated_at": now}
                                       for (tf, sym), ts in cursors.items()])
    if kill_reason and not kill_was_on:
        store.write(cloud, ds, "controls", [{"key": KILL_SWITCH, "value": kill_reason, "updated_at": now}])
    open_positions = [p for p in book.positions if p.status == "open"]
    balance = book.balance()
    equity = balance + sum(broker.unrealized(p, marks[(p.timeframe, p.symbol)])
                           for p in open_positions if (p.timeframe, p.symbol) in marks)
    open_risk = sum(r.risk for r in book.live())
    store.write(cloud, ds, "account", [{
        "ts": now, "date": now.date(), "broker": broker.name, "balance": balance, "equity": equity,
        "open_positions": len(open_positions), "open_risk": open_risk, "kill_switch": kill_reason is not None,
    }])
    store.write(cloud, ds, "events", list({e["event_id"]: e for e in events}.values()))
    return {**counts, "balance": balance, "equity": equity, "open_positions": len(open_positions),
            "pending_orders": sum(o.status == "pending" for o in book.orders), "kill_switch": kill_reason}


def _handle_signal(strategy, tf, sym, bar, sig, side, *, now, settings, backtest, cloud, broker, book,
                   counts, event, kill, stale) -> None:
    sid = signal_id(strategy, tf, sym, bar["ts"])
    balance = book.balance()
    new_risk = balance * backtest.risk_per_trade
    reason = risk.check(settings, kill_switch=kill, busy=book.busy(strategy, tf, sym), stale=stale,
                        balance=balance, open_count=len(book.live()), open_risk=sum(r.risk for r in book.live()),
                        new_risk=new_risk)
    target = store.clean(float(sig["target"]))
    row = {"signal_id": sid, "date": bar["ts"].date(), "strategy": strategy, "timeframe": tf, "symbol": sym,
           "bar_ts": bar["ts"], "side": "long" if side > 0 else "short", "stop": float(sig["stop"]),
           "target": target, "close": float(bar["close"]), "status": "skipped" if reason else "ordered",
           "reason": reason, "created_at": now}
    if not store.insert_new(cloud, settings.dataset, "signals", row):
        counts["duplicate"] += 1  # handled by an earlier step
        return
    if reason:
        counts["skipped"] += 1
        return
    order = Order(order_id=sid, date=row["date"], strategy=strategy, timeframe=tf, symbol=sym, side=row["side"],
                  broker=broker.name, status="pending", stop=row["stop"], target=target, risk=new_risk,
                  signal_ts=bar["ts"], created_at=now, updated_at=now)
    broker.submit(order)
    store.write(cloud, settings.dataset, "orders", [order])
    book.orders.append(order)
    book.touch(order)
    counts["ordered" if order.status == "pending" else order.status] += 1
    event("ordered", sid, f"{order.side} risking {new_risk:.2f}, stop {order.stop:g}"
          + (f", target {target:g}" if target is not None else ""), bar["ts"])
