"""Backtest settings, read from environment variables (optionally a .env file).

The market data side (which dataset bars live in, which timeframes exist) comes from
ingestion.dukascopy.config.Settings, so both always agree.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from ingestion.dukascopy.config import _point_overrides

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


@dataclass(frozen=True)
class BacktestSettings:
    # Results go to {dataset}.runs / .trades / .equity / .metrics
    dataset: str = "backtest"
    initial_capital: float = 10_000.0
    risk_per_trade: float = 0.01  # fraction of equity lost if the stop is hit
    # Spread in points (1 point = 1 / point factor, e.g. 0.00001 for EURUSD, so 10 = 1 pip)
    spread_points: float = 10.0
    spread_overrides: dict[str, int] = field(default_factory=dict)
    commission_rate: float = 0.0  # fraction of notional, charged on entry and on exit

    @classmethod
    def from_env(cls) -> "BacktestSettings":
        env = os.environ.get
        return cls(
            dataset=env("BACKTEST_DATASET") or "backtest",
            initial_capital=float(env("BACKTEST_INITIAL_CAPITAL", "10000")),
            risk_per_trade=float(env("BACKTEST_RISK_PER_TRADE", "0.01")),
            spread_points=float(env("BACKTEST_SPREAD_POINTS", "10")),
            spread_overrides=_point_overrides(env("BACKTEST_SPREAD_OVERRIDES")),
            commission_rate=float(env("BACKTEST_COMMISSION_RATE", "0")),
        )

    def validate(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError("BACKTEST_INITIAL_CAPITAL must be positive.")
        if not 0 < self.risk_per_trade < 1:
            raise ValueError("BACKTEST_RISK_PER_TRADE must be between 0 and 1, e.g. 0.01 for 1%.")
        if self.spread_points < 0 or self.commission_rate < 0:
            raise ValueError("BACKTEST_SPREAD_POINTS and BACKTEST_COMMISSION_RATE can't be negative.")

    def spread_for(self, symbol: str) -> float:
        """Spread in points for one symbol."""
        return float(self.spread_overrides.get(symbol.upper(), self.spread_points))
