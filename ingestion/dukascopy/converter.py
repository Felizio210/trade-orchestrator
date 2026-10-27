"""Step 2: convert .bi5 tick files to CSV.

Accepts a single .bi5 file or a directory (searched recursively). Symbol and hour are
read from the path, which may follow either layout:

    local (this project):  .../EURUSD/2026-09-03/10h_ticks.bi5
    Dukascopy native:      .../EURUSD/2026/08/03/10h_ticks.bi5   (zero-based month)

For a file outside those layouts, pass symbol= and hour= explicitly.

Output:
    single file -> {out_dir}/{SYMBOL}/{SYMBOL}_{YYYY-MM-DD}_{HH}h_ticks.csv
    directory   -> one file per symbol/day: {out_dir}/{SYMBOL}/{SYMBOL}_{YYYY-MM-DD}_ticks.csv
                   (+ {SYMBOL}_{YYYY-MM-DD}_bars_{tf}.csv when bars_timeframe is set)

CSV columns: ts (UTC, millisecond precision), bid, ask, bid_volume, ask_volume.
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ingestion.dukascopy.client import decode_ticks
from ingestion.dukascopy.instruments import point_factor
from ingestion.dukascopy.transform import ticks_to_bars

_LOCAL_RE = re.compile(
    r"(?P<symbol>[A-Z0-9]+)/(?P<date>\d{4}-\d{2}-\d{2})/(?P<hour>\d{2})h_ticks\.bi5$", re.IGNORECASE
)
_NATIVE_RE = re.compile(
    r"(?P<symbol>[A-Z0-9]+)/(?P<year>\d{4})/(?P<month0>\d{2})/(?P<day>\d{2})/(?P<hour>\d{2})h_ticks\.bi5$",
    re.IGNORECASE,
)


def parse_bi5_path(path: str | Path) -> tuple[str, datetime]:
    """Return (SYMBOL, hour start in UTC) from a .bi5 path, or raise ValueError."""
    posix = Path(path).as_posix()
    if m := _LOCAL_RE.search(posix):
        d = datetime.strptime(m["date"], "%Y-%m-%d")
        return m["symbol"].upper(), d.replace(hour=int(m["hour"]), tzinfo=timezone.utc)
    if m := _NATIVE_RE.search(posix):
        hour = datetime(int(m["year"]), int(m["month0"]) + 1, int(m["day"]), int(m["hour"]), tzinfo=timezone.utc)
        return m["symbol"].upper(), hour
    raise ValueError(
        f"Can't infer symbol/hour from {path}; expected .../SYMBOL/YYYY-MM-DD/HHh_ticks.bi5 "
        "or pass symbol and hour explicitly."
    )


def read_bi5(
    path: str | Path,
    symbol: str | None = None,
    hour: datetime | None = None,
    point_overrides: dict[str, int] | None = None,
) -> pd.DataFrame:
    """Decode one .bi5 file into a tick DataFrame."""
    if symbol is None or hour is None:
        parsed_symbol, parsed_hour = parse_bi5_path(path)
        symbol, hour = symbol or parsed_symbol, hour or parsed_hour
    if hour.tzinfo is None:
        hour = hour.replace(tzinfo=timezone.utc)
    return decode_ticks(Path(path).read_bytes(), hour, point_factor(symbol, point_overrides))


def read_day_dir(day_dir: str | Path, point_overrides: dict[str, int] | None = None) -> pd.DataFrame:
    """All ticks from one downloaded day folder (.../SYMBOL/YYYY-MM-DD/), sorted by time."""
    frames = [read_bi5(f, point_overrides=point_overrides) for f in sorted(Path(day_dir).glob("*h_ticks.bi5"))]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return decode_ticks(b"", datetime(1970, 1, 1, tzinfo=timezone.utc), 1)
    return pd.concat(frames, ignore_index=True).sort_values("ts", kind="stable").reset_index(drop=True)


def write_csv(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    if not out.empty:
        out["ts"] = out["ts"].dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3]
    for col in out.select_dtypes("float").columns:  # plain decimals, no 1e-05 notation
        out[col] = out[col].map(lambda v: "" if pd.isna(v) else f"{v:.8f}".rstrip("0").rstrip("."))
    out.to_csv(path, index=False)
    return path


def convert(
    path: str | Path,
    out_dir: str | Path,
    *,
    symbol: str | None = None,
    hour: datetime | None = None,
    bars_timeframe: str | None = None,
    bar_price_side: str = "bid",
    point_overrides: dict[str, int] | None = None,
) -> list[Path]:
    """Convert a .bi5 file or a directory of them to CSV. Returns the files written."""
    path, out_dir = Path(path), Path(out_dir)

    if path.is_file():
        sym, hr = symbol, hour
        if sym is None or hr is None:
            parsed_sym, parsed_hr = parse_bi5_path(path)
            sym, hr = sym or parsed_sym, hr or parsed_hr
        sym = sym.upper()
        ticks = read_bi5(path, sym, hr, point_overrides)
        stem = f"{sym}_{hr:%Y-%m-%d}_{hr:%H}h"
        written = [write_csv(ticks, out_dir / sym / f"{stem}_ticks.csv")]
        if bars_timeframe:
            bars = ticks_to_bars(ticks, bars_timeframe, bar_price_side)
            written.append(write_csv(bars, out_dir / sym / f"{stem}_bars_{bars_timeframe}.csv"))
        return written

    if not path.is_dir():
        raise FileNotFoundError(path)

    groups: dict[tuple[str, str], list[pd.DataFrame]] = defaultdict(list)
    for f in sorted(path.rglob("*h_ticks.bi5")):
        sym, hr = parse_bi5_path(f)
        groups[(sym, hr.date().isoformat())].append(read_bi5(f, sym, hr, point_overrides))
    if not groups:
        raise FileNotFoundError(f"No *h_ticks.bi5 files under {path}")

    written = []
    for (sym, day), frames in sorted(groups.items()):
        frames = [f for f in frames if not f.empty]
        if not frames:
            continue
        ticks = pd.concat(frames, ignore_index=True).sort_values("ts", kind="stable").reset_index(drop=True)
        written.append(write_csv(ticks, out_dir / sym / f"{sym}_{day}_ticks.csv"))
        if bars_timeframe:
            bars = ticks_to_bars(ticks, bars_timeframe, bar_price_side)
            written.append(write_csv(bars, out_dir / sym / f"{sym}_{day}_bars_{bars_timeframe}.csv"))
    return written
