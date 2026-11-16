"""Backtest settings, read from environment variables (optionally a .env file).

The market data side (which dataset bars live in, which timeframes exist) comes from
ingestion.dukascopy.config.Settings, so both always agree.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from ingestion.dukascopy.config import _point_overrides

# Units in one standard lot, used only to report position size in lots. Anything not
# listed is 100,000 (FX); set others with BACKTEST_CONTRACT_SIZES, e.g. "BTCUSD=1".
DEFAULT_CONTRACT_SIZE = 100_000
KNOWN_CONTRACT_SIZES: dict[str, int] = {"XAUUSD": 100, "XAGUSD": 5_000}

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
    contract_size_overrides: dict[str, int] = field(default_factory=dict)
    # Metrics are also computed over this many most recent days (window "recent"), so the gate can
    # tell a strategy that still works from one that worked a year ago
    recent_days: int = 90
    # Walk-forward: choose parameters from the strategy's GRID on train_days, trade them on the next
    # test_days, roll forward by test_days; the stitched test periods are the out-of-sample result
    walk_forward: bool = False
    wf_train_days: int = 365
    wf_test_days: int = 90
    wf_min_trades: int = 20  # a parameter set needs this many trades in a training window to be chosen
    # A run still "running" this long after it started was killed: it's marked failed
    run_timeout_hours: float = 3.0
    # Equity curves: kept bar by bar for the newest daily run of each strategy/timeframe and for runs
    # younger than this; older ones are thinned to one row per day
    equity_full_days: int = 7

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
            contract_size_overrides=_point_overrides(env("BACKTEST_CONTRACT_SIZES")),
            recent_days=int(env("BACKTEST_RECENT_DAYS", "90")),
            walk_forward=env("BACKTEST_WALK_FORWARD", "false").strip().lower() in ("1", "true", "yes"),
            wf_train_days=int(env("BACKTEST_WF_TRAIN_DAYS", "365")),
            wf_test_days=int(env("BACKTEST_WF_TEST_DAYS", "90")),
            wf_min_trades=int(env("BACKTEST_WF_MIN_TRADES", "20")),
            run_timeout_hours=float(env("BACKTEST_RUN_TIMEOUT_HOURS", "3")),
            equity_full_days=int(env("BACKTEST_EQUITY_FULL_DAYS", "7")),
        )

    def validate(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError("BACKTEST_INITIAL_CAPITAL must be positive.")
        if not 0 < self.risk_per_trade < 1:
            raise ValueError("BACKTEST_RISK_PER_TRADE must be between 0 and 1, e.g. 0.01 for 1%.")
        if self.spread_points < 0 or self.commission_rate < 0:
            raise ValueError("BACKTEST_SPREAD_POINTS and BACKTEST_COMMISSION_RATE can't be negative.")
        if self.recent_days < 1 or self.wf_train_days < 1 or self.wf_test_days < 1:
            raise ValueError("BACKTEST_RECENT_DAYS, BACKTEST_WF_TRAIN_DAYS and BACKTEST_WF_TEST_DAYS must be positive.")

    def spread_for(self, symbol: str) -> float:
        """Spread in points for one symbol."""
        return float(self.spread_overrides.get(symbol.upper(), self.spread_points))

    def contract_size_for(self, symbol: str) -> int:
        """Units in one lot for one symbol."""
        symbol = symbol.upper()
        return self.contract_size_overrides.get(symbol, KNOWN_CONTRACT_SIZES.get(symbol, DEFAULT_CONTRACT_SIZE))
