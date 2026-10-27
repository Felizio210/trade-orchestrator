"""Step 1: pull raw .bi5 files from Dukascopy into local storage and verify them.

Layout:
    {local_data_dir}/raw/{SYMBOL}/{YYYY-MM-DD}/{HH}h_ticks.bi5            (bar_source=ticks)
    {local_data_dir}/raw/{SYMBOL}/{YYYY-MM-DD}/{BID|ASK}_candles_min_1.bi5 (bar_source=candles)
    {local_data_dir}/raw/{SYMBOL}/{YYYY-MM-DD}/_manifest.json

Every hour is decoded right after download, so a corrupt or non-bi5 response (e.g. an
HTML error page from a proxy) is caught here instead of later in the pipeline. The
manifest records per-hour byte size, tick count and first/last prices so you can see
at a glance whether the pull worked.

Non-empty files already on disk are reused (pass overwrite=True to force a re-pull).
Empty hours are never cached, because a recent hour can be empty simply because
Dukascopy hasn't published it yet.
"""

from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from ingestion.dukascopy.client import CANDLE_SIDES, DukascopyClient, DukascopyError, decode_candles, decode_ticks
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.instruments import point_factor

log = logging.getLogger(__name__)


def raw_day_dir(raw_dir: str | Path, symbol: str, day: date) -> Path:
    return Path(raw_dir) / symbol.upper() / day.isoformat()


def raw_hour_path(raw_dir: str | Path, symbol: str, hour: datetime) -> Path:
    return raw_day_dir(raw_dir, symbol, hour.date()) / f"{hour.hour:02d}h_ticks.bi5"


def _write_atomic(path: Path, data: bytes) -> None:
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def download_day(
    symbol: str,
    day: date,
    settings: Settings | None = None,
    *,
    overwrite: bool = False,
    client: DukascopyClient | None = None,
) -> dict:
    """Download and verify the 24 hourly files for one UTC day. Returns the manifest.

    Hours that fail to download or decode are recorded in the manifest as "failed" /
    "corrupt" rather than raising, so one bad hour doesn't lose the rest of the day.
    Use problems(manifest) to check; re-running only re-fetches what's missing.
    """
    settings = settings or Settings.from_env()
    client = client or DukascopyClient(settings)
    if settings.bar_source == "candles":
        return download_day_candles(symbol, day, settings, overwrite=overwrite, client=client)
    symbol = symbol.upper()
    factor = point_factor(symbol, settings.point_overrides)
    day_dir = raw_day_dir(settings.raw_dir, symbol, day)
    day_dir.mkdir(parents=True, exist_ok=True)
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)

    def one(hour: datetime) -> dict:
        path = raw_hour_path(settings.raw_dir, symbol, hour)
        entry: dict = {"hour": f"{hour.hour:02d}", "file": path.name}
        if path.exists() and path.stat().st_size and not overwrite:
            raw, entry["source"] = path.read_bytes(), "cached"
        else:
            entry["source"] = "downloaded"
            try:
                raw = client.fetch_hour_raw(symbol, hour)
            except DukascopyError as exc:
                return {**entry, "status": "failed", "error": str(exc)}
        entry["bytes"] = len(raw)
        try:
            ticks = decode_ticks(raw, hour, factor)
        except Exception as exc:  # lzma.LZMAError, DukascopyError, ...
            path.unlink(missing_ok=True)
            return {**entry, "status": "corrupt", "error": f"{type(exc).__name__}: {exc}"}
        if ticks.empty:
            path.unlink(missing_ok=True)
            return {**entry, "status": "empty", "ticks": 0}
        if entry["source"] == "downloaded":
            _write_atomic(path, raw)
        return {
            **entry,
            "status": "ok",
            "ticks": len(ticks),
            "first_ts": ticks["ts"].iloc[0].isoformat(),
            "last_ts": ticks["ts"].iloc[-1].isoformat(),
            "first_bid": float(ticks["bid"].iloc[0]),
            "last_bid": float(ticks["bid"].iloc[-1]),
        }

    hours = [start + timedelta(hours=h) for h in range(24)]
    with ThreadPoolExecutor(max_workers=settings.max_workers) as pool:
        entries = list(pool.map(one, hours))

    manifest = {
        "symbol": symbol,
        "date": day.isoformat(),
        "point_factor": factor,
        "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "hours_ok": sum(e["status"] == "ok" for e in entries),
        "hours_empty": sum(e["status"] == "empty" for e in entries),
        "hours_corrupt": sum(e["status"] == "corrupt" for e in entries),
        "hours_failed": sum(e["status"] == "failed" for e in entries),
        "total_ticks": sum(e.get("ticks", 0) for e in entries),
        "dir": str(day_dir),
        "hours": entries,
    }
    (day_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2))
    log.info("%s %s: %d ticks, %d/24 hours with data", symbol, day, manifest["total_ticks"], manifest["hours_ok"])

    return manifest


def candle_path(raw_dir: str | Path, symbol: str, day: date, side: str) -> Path:
    return raw_day_dir(raw_dir, symbol, day) / f"{side}_candles_min_1.bi5"


def download_day_candles(
    symbol: str,
    day: date,
    settings: Settings,
    *,
    overwrite: bool = False,
    client: DukascopyClient | None = None,
) -> dict:
    """Download and verify the BID and ASK daily 1-minute candle files. Returns the manifest.

    Same contract as download_day: failures are recorded, not raised, and a re-run only
    re-fetches what's missing. The manifest has one entry per file under "hours" (keyed
    by "side") so problems() and the publish checks work unchanged.
    """
    client = client or DukascopyClient(settings)
    symbol = symbol.upper()
    factor = point_factor(symbol, settings.point_overrides)
    day_dir = raw_day_dir(settings.raw_dir, symbol, day)
    day_dir.mkdir(parents=True, exist_ok=True)

    def one(side: str) -> dict:
        path = candle_path(settings.raw_dir, symbol, day, side)
        entry: dict = {"side": side, "file": path.name}
        if path.exists() and path.stat().st_size and not overwrite:
            raw, entry["source"] = path.read_bytes(), "cached"
        else:
            entry["source"] = "downloaded"
            try:
                raw = client.fetch_candles_raw(symbol, day, side)
            except DukascopyError as exc:
                return {**entry, "status": "failed", "error": str(exc)}
        entry["bytes"] = len(raw)
        try:
            candles = decode_candles(raw, day, factor)
        except Exception as exc:
            path.unlink(missing_ok=True)
            return {**entry, "status": "corrupt", "error": f"{type(exc).__name__}: {exc}"}
        traded = candles[candles["volume"] > 0]
        if traded.empty:
            path.unlink(missing_ok=True)
            return {**entry, "status": "empty", "bars": 0}
        if entry["source"] == "downloaded":
            _write_atomic(path, raw)
        return {
            **entry,
            "status": "ok",
            "bars": len(traded),
            "first_ts": traded["ts"].iloc[0].isoformat(),
            "last_ts": traded["ts"].iloc[-1].isoformat(),
            "first_open": float(traded["open"].iloc[0]),
            "last_close": float(traded["close"].iloc[-1]),
        }

    with ThreadPoolExecutor(max_workers=min(settings.max_workers, len(CANDLE_SIDES))) as pool:
        entries = list(pool.map(one, CANDLE_SIDES))

    manifest = {
        "symbol": symbol,
        "date": day.isoformat(),
        "bar_source": "candles",
        "point_factor": factor,
        "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "files_ok": sum(e["status"] == "ok" for e in entries),
        "hours_failed": sum(e["status"] == "failed" for e in entries),
        "hours_corrupt": sum(e["status"] == "corrupt" for e in entries),
        "bars": entries[0].get("bars", 0),
        "dir": str(day_dir),
        "hours": entries,
    }
    (day_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2))
    log.info("%s %s: %d one-minute candles with volume", symbol, day, manifest["bars"])
    return manifest


def problems(manifest: dict) -> list[str]:
    """Human-readable list of hours/files that didn't download cleanly (empty means all good)."""
    def label(e: dict) -> str:
        return f"{e['hour']}h" if "hour" in e else e["side"]

    return [f"{label(e)} {e['status']}: {e['error']}" for e in manifest["hours"]
            if e["status"] in ("corrupt", "failed")]


def download_range(
    symbols: list[str], start: date, end: date, settings: Settings | None = None, *, overwrite: bool = False
) -> list[dict]:
    settings = settings or Settings.from_env()
    client = DukascopyClient(settings)
    manifests = []
    day = start
    while day <= end:
        for symbol in symbols:
            m = download_day(symbol, day, settings, overwrite=overwrite, client=client)
            manifests.append(m)
            if m["hours_failed"] == len(m["hours"]):
                raise DukascopyError(
                    f"Every hour of {symbol} {day} failed to download ({m['hours'][0]['error']}). "
                    "The datafeed looks unreachable from this network; run "
                    "`python -m ingestion.dukascopy check` to diagnose."
                )
        day += timedelta(days=1)
    return manifests
