"""Strategy registry.

A strategy module has DEFAULTS (every parameter with its default) and
signals(bars, **params) -> DataFrame with one row per bar:
    signal  +1 long, -1 short, 0 nothing; decided at the bar's close, filled at the next open
    stop    absolute stop price (required with a signal)
    target  absolute target price (NaN = no target)

To add one: write backtest/strategies/<name>.py and list it below.
"""

from __future__ import annotations

from types import ModuleType
from typing import Any

import pandas as pd

from backtest.strategies import supply_demand, trendline

STRATEGIES: dict[str, ModuleType] = {
    "supply_demand": supply_demand,
    "trendline": trendline,
}


def resolve_params(name: str, overrides: dict[str, str] | None = None) -> dict[str, Any]:
    """DEFAULTS with string overrides ("rr" -> "2.5") cast to each default's type."""
    module = get(name)
    params = dict(module.DEFAULTS)
    for key, raw in (overrides or {}).items():
        if key not in params:
            raise ValueError(f"{name} has no parameter {key!r}; it has {sorted(params)}")
        default = params[key]
        if isinstance(default, bool):
            params[key] = raw.strip().lower() in ("1", "true", "yes")
        else:
            params[key] = type(default)(raw)
    return params


def get(name: str) -> ModuleType:
    if name not in STRATEGIES:
        raise ValueError(f"Unknown strategy {name!r}; available: {sorted(STRATEGIES)}")
    return STRATEGIES[name]


def run(name: str, bars: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    return get(name).signals(bars, **params)
