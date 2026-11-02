"""Trading settings, read from environment variables (optionally a .env file).

Sizing (risk per trade), spread and commission come from backtest.config.BacktestSettings,
so paper trading uses the same costs the strategy was approved under.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

BROKERS = ("paper",)


def _float(name: str, default: str) -> float | None:
    """Empty means "don't check"."""
    raw = os.environ.get(name, default).strip()
    return float(raw) if raw else None


@dataclass(frozen=True)
class TradingSettings:
    dataset: str = "trading"
    broker: str = "paper"
    paper_capital: float = 10_000.0
    # Bars loaded before the new ones so indicators and zones are warmed up
    warmup_days: int = 60
    # Only daily backtests (run_id daily-...) can approve a strategy, not ad-hoc runs
    run_prefix: str = "daily-"
    # An approval older than this is ignored, e.g. when backtest_daily stopped running
    approval_max_age_days: float = 3.0

    # The standards a backtest must pass, per strategy, timeframe and symbol. None = not checked.
    min_trades: float | None = 50
    min_sharpe: float | None = 1.0
    min_profit_factor: float | None = 1.3
    max_drawdown: float | None = 0.15  # |max_drawdown| at most this
    min_avg_r: float | None = 0.1
    min_win_rate: float | None = None
    min_return: float | None = 0.0

    # Risk limits on live positions. None = no limit.
    max_open_positions: float | None = 5
    max_open_risk: float | None = 0.05  # sum of risk-to-stop over open positions, fraction of balance
    max_daily_loss: float | None = 0.03  # realized loss in a UTC day, fraction of that day's opening balance

    @classmethod
    def from_env(cls) -> "TradingSettings":
        env = os.environ.get
        return cls(
            dataset=env("TRADING_DATASET") or "trading",
            broker=(env("TRADING_BROKER") or "paper").lower(),
            paper_capital=float(env("TRADING_PAPER_CAPITAL", "10000")),
            warmup_days=int(env("TRADING_WARMUP_DAYS", "60")),
            run_prefix=env("TRADING_RUN_PREFIX", "daily-"),
            approval_max_age_days=float(env("TRADING_APPROVAL_MAX_AGE_DAYS", "3")),
            min_trades=_float("TRADING_MIN_TRADES", "50"),
            min_sharpe=_float("TRADING_MIN_SHARPE", "1.0"),
            min_profit_factor=_float("TRADING_MIN_PROFIT_FACTOR", "1.3"),
            max_drawdown=_float("TRADING_MAX_DRAWDOWN", "0.15"),
            min_avg_r=_float("TRADING_MIN_AVG_R", "0.1"),
            min_win_rate=_float("TRADING_MIN_WIN_RATE", ""),
            min_return=_float("TRADING_MIN_RETURN", "0"),
            max_open_positions=_float("TRADING_MAX_OPEN_POSITIONS", "5"),
            max_open_risk=_float("TRADING_MAX_OPEN_RISK", "0.05"),
            max_daily_loss=_float("TRADING_MAX_DAILY_LOSS", "0.03"),
        )

    def validate(self) -> None:
        if self.broker not in BROKERS:
            raise ValueError(f"TRADING_BROKER must be one of {BROKERS}, got {self.broker!r}")
        if self.paper_capital <= 0:
            raise ValueError("TRADING_PAPER_CAPITAL must be positive.")
        if self.warmup_days < 1:
            raise ValueError("TRADING_WARMUP_DAYS must be at least 1.")
        for name in ("max_drawdown", "max_open_risk", "max_daily_loss"):
            value = getattr(self, name)
            if value is not None and not 0 < value <= 1:
                raise ValueError(f"TRADING_{name.upper()} must be a fraction between 0 and 1, e.g. 0.15.")
