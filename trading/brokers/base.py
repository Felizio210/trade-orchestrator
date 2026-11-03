"""What the executor needs from a broker.

Orders are market orders with the stop and target attached, so the broker holds the
protective orders and a late or crashed step never leaves a position unprotected. The
order_id doubles as the broker's client order id, so a broker that dedupes on it is a
second guard against sending the same order twice.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from trading.store import Order, Position


class Broker(ABC):
    name: str
    # True when orders fill at live prices: only a signal from the newest bar is acted on.
    # False (paper) replays every new bar, filling at the next bar's open like the backtest.
    realtime: bool

    @abstractmethod
    def submit(self, order: Order) -> None:
        """Send a pending order. May set its status to rejected (with a reason)."""

    @abstractmethod
    def sync(self, timeframe: str, symbol: str, bar: pd.Series, orders: list[Order],
             positions: list[Position]) -> list[Order | Position]:
        """Bring orders and positions of this timeframe/symbol up to the close of `bar`:
        fills (new positions are appended to `positions`), stop and target exits. Updates
        the records in place and returns the ones that changed."""

    @abstractmethod
    def unrealized(self, position: Position, bar: pd.Series) -> float:
        """Open PnL of a position at the close of `bar`, on the side it would exit on."""

    def balance(self) -> float | None:
        """The account balance when the broker holds one; None means the executor computes it
        from TRADING_PAPER_CAPITAL and the recorded positions."""
        return None

    def equity(self) -> float | None:
        """The account equity when the broker holds one, so it includes whatever happened outside
        this system (manual trades, swaps, fees); None means balance + open PnL of our positions."""
        return None
