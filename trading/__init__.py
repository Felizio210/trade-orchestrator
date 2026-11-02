"""Trading: take positions with strategies whose backtest passed the standards.

    gate      backtest.metrics -> trading.approvals (TRADING_MIN_* / TRADING_MAX_* thresholds)
    step      new bars -> signals of approved strategies -> risk checks -> orders -> broker
    brokers   paper (simulated, fills like backtest/engine.py); real brokers plug in later

State lives in the `trading` warehouse dataset, next to the bars and backtest results.
See docs/trading_plan.md.
"""
