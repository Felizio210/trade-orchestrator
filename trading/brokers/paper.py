"""Simulated broker with the backtest's fill rules (see backtest/engine.py), so a paper
account trading an approved strategy reproduces its backtest on the same bars:

  * an order from bar t's signal fills at bar t+1's open, on the ask for a long and the
    bid for a short; if that open is already past the stop, the order is rejected
  * qty = order risk / distance from the fill to the stop
  * stops and targets are checked on the side that would fill them; a bar that hits both
    exits at the stop, and a gap through either fills at the open
  * commission is a fraction of notional on entry and on exit
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

import pandas as pd

from backtest.config import BacktestSettings
from backtest.engine import Costs
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.instruments import point_factor
from trading.brokers.base import Broker
from trading.store import Order, Position


class PaperBroker(Broker):
    name = "paper"
    realtime = False

    def __init__(self, backtest: BacktestSettings, market: Settings):
        self.backtest = backtest
        self.market = market

    def costs(self, symbol: str, price_side: str) -> Costs:
        return Costs(spread=self.backtest.spread_for(symbol) / point_factor(symbol, self.market.point_overrides),
                     commission_rate=self.backtest.commission_rate, price_side=price_side)

    def submit(self, order: Order) -> None:
        order.broker_order_id = order.order_id  # stays pending until the next bar's open

    def sync(self, timeframe, symbol, bar, orders, positions):
        costs = self.costs(symbol, bar["price_side"])
        ts = bar["ts"]
        o, h, l, c = (float(bar[k]) for k in ("open", "high", "low", "close"))
        now = pd.Timestamp(datetime.now(timezone.utc))
        changed: list[Order | Position] = []

        for order in orders:
            if (order.status != "pending" or (order.timeframe, order.symbol) != (timeframe, symbol)
                    or order.signal_ts >= ts):
                continue
            d = order.direction
            fill = o + costs.to_ask if d > 0 else o - costs.to_bid
            distance = (fill - order.stop) * d
            order.updated_at = now
            changed.append(order)
            if distance <= 0:
                order.status, order.reason = "rejected", f"opened at {fill:g}, already past the stop {order.stop:g}"
                continue
            qty = order.risk / distance
            commission = qty * fill * costs.commission_rate
            order.status, order.qty, order.fill_ts, order.fill_price, order.commission = (
                "filled", qty, ts, fill, commission)
            position = Position(
                position_id=order.order_id, date=ts.date(), strategy=order.strategy, timeframe=timeframe,
                symbol=symbol, side=order.side, status="open", qty=qty, entry_ts=ts, entry_price=fill,
                stop=order.stop, target=order.target, risk=order.risk, entry_commission=commission, updated_at=now,
            )
            positions.append(position)
            changed.append(position)

        for pos in positions:
            if pos.status != "open" or (pos.timeframe, pos.symbol) != (timeframe, symbol):
                continue
            target = pos.target if pos.target is not None and not math.isnan(pos.target) else None
            exit_ = None
            if pos.direction > 0:  # a long exits by selling the bid
                b_open, b_low, b_high = o - costs.to_bid, l - costs.to_bid, h - costs.to_bid
                if b_low <= pos.stop:
                    exit_ = (min(pos.stop, b_open), "stop")
                elif target is not None and b_high >= target:
                    exit_ = (max(target, b_open), "target")
            else:  # a short exits by buying the ask
                a_open, a_low, a_high = o + costs.to_ask, l + costs.to_ask, h + costs.to_ask
                if a_high >= pos.stop:
                    exit_ = (max(pos.stop, a_open), "stop")
                elif target is not None and a_low <= target:
                    exit_ = (min(target, a_open), "target")
            if exit_ is not None:
                price, reason = exit_
                exit_commission = pos.qty * price * costs.commission_rate
                pos.pnl = pos.direction * pos.qty * (price - pos.entry_price) - pos.entry_commission - exit_commission
                pos.status, pos.exit_ts, pos.exit_price, pos.exit_reason = "closed", ts, price, reason
                pos.commission = pos.entry_commission + exit_commission
                pos.r_multiple = pos.pnl / pos.risk
                pos.updated_at = now
                if not any(r is pos for r in changed):  # filled and closed on this same bar
                    changed.append(pos)
        return changed

    def unrealized(self, position, bar):
        costs = self.costs(position.symbol, bar["price_side"])
        mark = float(bar["close"]) - costs.to_bid if position.direction > 0 else float(bar["close"]) + costs.to_ask
        return position.direction * position.qty * (mark - position.entry_price)
