"""Bar-by-bar simulation of one symbol.

Rules (see docs/backtest_plan.md):
  * a signal on bar t fills at bar t+1's open; one position at a time, signals while in
    a position are ignored
  * bars are one price side; bid and ask are rebuilt from a fixed spread. Longs buy the
    ask and sell the bid, shorts the reverse, and stops/targets are checked on the side
    that would fill them
  * a bar that hits both stop and target exits at the stop; a gap through the stop or
    target fills at the open
  * size = risk_per_trade x equity / stop distance; a position still open on the last bar
    is closed at its close (exit_reason "end")
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Costs:
    spread: float  # in price units
    commission_rate: float = 0.0  # fraction of notional per side
    price_side: str = "bid"  # which side the bars are: bid | ask | mid

    @property
    def to_bid(self) -> float:
        """Subtract from a bar price to get the bid."""
        return {"bid": 0.0, "mid": self.spread / 2, "ask": self.spread}[self.price_side]

    @property
    def to_ask(self) -> float:
        """Add to a bar price to get the ask."""
        return self.spread - self.to_bid


def simulate(bars: pd.DataFrame, sig: pd.DataFrame, costs: Costs, initial_capital: float,
             risk_per_trade: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (trades, equity). equity has one row per bar: ts, position, equity."""
    ts = bars["ts"].to_numpy()
    o, h, l, c = (bars[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close"))
    signal, stops, targets = sig["signal"].to_numpy(), sig["stop"].to_numpy(), sig["target"].to_numpy()
    bid, ask = costs.to_bid, costs.to_ask

    cash = initial_capital  # realized equity
    trades: list[dict] = []
    position = np.zeros(len(bars))
    equity = np.zeros(len(bars))
    pos = None  # dict while a trade is open
    pending = None  # (side, stop, target) from the previous bar's signal

    def close(i: int, price: float, reason: str) -> None:
        nonlocal cash, pos
        commission = pos["qty"] * price * costs.commission_rate
        pnl = pos["side"] * pos["qty"] * (price - pos["entry_price"]) - pos["commission"] - commission
        cash += pnl + pos["commission"]  # entry commission was already taken from cash
        trades.append({
            "symbol": None, "side": "long" if pos["side"] > 0 else "short", "qty": pos["qty"],
            "entry_ts": ts[pos["i"]], "entry_price": pos["entry_price"], "exit_ts": ts[i], "exit_price": price,
            "exit_reason": reason, "stop": pos["stop"], "target": pos["target"],
            "commission": pos["commission"] + commission, "pnl": pnl, "r_multiple": pnl / pos["risk"],
        })
        pos = None

    for i in range(len(bars)):
        # 1. Fill yesterday's signal at this open
        if pending is not None:
            side, stop, target = pending
            pending = None
            fill = o[i] + ask if side > 0 else o[i] - bid
            distance = (fill - stop) * side
            if distance > 0 and cash > 0:  # the open may already be past the stop
                risk = cash * risk_per_trade
                qty = risk / distance
                commission = qty * fill * costs.commission_rate
                cash -= commission
                pos = {"side": side, "qty": qty, "entry_price": fill, "stop": stop, "target": target,
                       "risk": risk, "commission": commission, "i": i}

        # 2. Stop / target inside this bar
        if pos is not None:
            stop, target = pos["stop"], pos["target"]
            if pos["side"] > 0:  # a long exits by selling the bid
                b_open, b_low, b_high = o[i] - bid, l[i] - bid, h[i] - bid
                if b_low <= stop:
                    close(i, min(stop, b_open), "stop")
                elif not np.isnan(target) and b_high >= target:
                    close(i, max(target, b_open), "target")
            else:  # a short exits by buying the ask
                a_open, a_low, a_high = o[i] + ask, l[i] + ask, h[i] + ask
                if a_high >= stop:
                    close(i, max(stop, a_open), "stop")
                elif not np.isnan(target) and a_low <= target:
                    close(i, min(target, a_open), "target")

        # 3. A new signal at this close, filled at the next open
        if pos is None and signal[i] != 0 and i + 1 < len(bars):
            pending = (int(signal[i]), stops[i], targets[i])

        # 4. Mark to market at the close, on the side the position would exit on
        if pos is not None:
            mark = c[i] - bid if pos["side"] > 0 else c[i] + ask
            position[i] = pos["side"] * pos["qty"]
            equity[i] = cash + pos["side"] * pos["qty"] * (mark - pos["entry_price"])
        else:
            equity[i] = cash

    if pos is not None:
        last = len(bars) - 1
        close(last, c[last] - bid if pos["side"] > 0 else c[last] + ask, "end")
        position[last], equity[last] = 0.0, cash

    trades_df = pd.DataFrame(trades, columns=[
        "symbol", "side", "qty", "entry_ts", "entry_price", "exit_ts", "exit_price", "exit_reason",
        "stop", "target", "commission", "pnl", "r_multiple",
    ])
    equity_df = pd.DataFrame({"ts": bars["ts"], "position": position, "equity": equity})
    return trades_df, equity_df
