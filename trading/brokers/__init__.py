"""Broker registry: TRADING_BROKER picks one."""

from __future__ import annotations

from backtest.config import BacktestSettings
from ingestion.dukascopy.config import Settings
from trading.brokers.base import Broker
from trading.brokers.paper import PaperBroker
from trading.config import TradingSettings


def get(settings: TradingSettings, backtest: BacktestSettings, market: Settings) -> Broker:
    if settings.broker == "paper":
        return PaperBroker(backtest, market)
    raise ValueError(f"Unknown broker {settings.broker!r}")


__all__ = ["Broker", "PaperBroker", "get"]
