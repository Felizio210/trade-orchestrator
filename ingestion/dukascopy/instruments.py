"""Price scaling per instrument.

Dukascopy stores prices as integers; divide by the instrument's point factor to get
the real price. Most FX pairs use 1e5, JPY crosses 1e3. Anything not covered here
can be set with DUKASCOPY_POINT_OVERRIDES, e.g. "BTCUSD=10,USA500IDXUSD=1000".
Sanity-check the first output for a new symbol: a price off by 10x/100x means the
factor is wrong.
"""

from __future__ import annotations

DEFAULT_POINT_FACTOR = 100_000

KNOWN_POINT_FACTORS: dict[str, int] = {
    "XAUUSD": 1_000,
    "XAGUSD": 1_000,
    "BTCUSD": 10,
    "ETHUSD": 10,
    "USA500IDXUSD": 1_000,
    "USA30IDXUSD": 1_000,
    "USATECHIDXUSD": 1_000,
    "DEUIDXEUR": 1_000,
    "GBRIDXGBP": 1_000,
    "JPNIDXJPY": 1_000,
    "LIGHTCMDUSD": 1_000,
    "BRENTCMDUSD": 1_000,
}


def point_factor(symbol: str, overrides: dict[str, int] | None = None) -> int:
    symbol = symbol.upper()
    if overrides and symbol in overrides:
        return overrides[symbol]
    if symbol in KNOWN_POINT_FACTORS:
        return KNOWN_POINT_FACTORS[symbol]
    if len(symbol) == 6 and symbol.endswith("JPY"):
        return 1_000
    return DEFAULT_POINT_FACTOR
