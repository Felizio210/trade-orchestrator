"""Pre-trade checks. Every new signal goes through check(); the first failed rule is
the reason the signal is skipped."""

from __future__ import annotations

from trading.config import TradingSettings


def check(settings: TradingSettings, *, kill_switch: bool, busy: bool, stale: bool, balance: float,
          open_count: int, open_risk: float, new_risk: float) -> str | None:
    """None if the signal may be ordered, otherwise why not.

    busy: this strategy already has an open position or pending order on this
    timeframe/symbol (one at a time, as in the backtest). open_count / open_risk count
    open positions and pending orders across the account.
    """
    if kill_switch:
        return "kill switch on"
    if busy:
        return "position already open"
    if stale:
        return "stale bar (not the newest)"
    if balance <= 0:
        return "no balance"
    if settings.max_open_positions is not None and open_count >= settings.max_open_positions:
        return f"max open positions ({open_count:.0f})"
    if settings.max_open_risk is not None and open_risk + new_risk > settings.max_open_risk * balance:
        return f"max open risk ({(open_risk + new_risk) / balance:.1%} of balance)"
    return None
