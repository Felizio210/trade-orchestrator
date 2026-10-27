"""Downloads and decodes Dukascopy hourly tick files (.bi5).

Datafeed layout (public, no authentication):
    {base}/{SYMBOL}/{YYYY}/{MM0}/{DD}/{HH}h_ticks.bi5
where MM0 is the zero-based month (January = 00) and HH is the UTC hour.

Each file is LZMA-compressed; the payload is a sequence of 20-byte big-endian records:
    uint32 ms offset from the start of the hour
    uint32 ask (scaled int)
    uint32 bid (scaled int)
    float32 ask volume
    float32 bid volume
Hours with no trading (weekends, holidays) come back empty or as 404.

Daily 1-minute candles live next to the tick files, one per price side:
    {base}/{SYMBOL}/{YYYY}/{MM0}/{DD}/{BID|ASK}_candles_min_1.bi5
24-byte big-endian records: uint32 seconds from midnight UTC, uint32 open, close, low,
high (scaled int), float32 volume. Minutes without trading are present with volume 0.
"""

from __future__ import annotations

import logging
import lzma
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter

from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.instruments import point_factor

log = logging.getLogger(__name__)

TICK_DTYPE = np.dtype(
    [("ms", ">u4"), ("ask", ">u4"), ("bid", ">u4"), ("ask_volume", ">f4"), ("bid_volume", ">f4")]
)
TICK_COLUMNS = ["ts", "bid", "ask", "bid_volume", "ask_volume"]
CANDLE_DTYPE = np.dtype(
    [("s", ">u4"), ("open", ">u4"), ("close", ">u4"), ("low", ">u4"), ("high", ">u4"), ("volume", ">f4")]
)
CANDLE_COLUMNS = ["ts", "open", "high", "low", "close", "volume"]
CANDLE_SIDES = ("BID", "ASK")
RETRY_STATUS = {429, 500, 502, 503, 504}


class DukascopyError(RuntimeError):
    pass


def hour_url(base_url: str, symbol: str, hour: datetime) -> str:
    return (
        f"{base_url}/{symbol.upper()}/{hour.year:04d}/{hour.month - 1:02d}/"
        f"{hour.day:02d}/{hour.hour:02d}h_ticks.bi5"
    )


def candle_url(base_url: str, symbol: str, day: date, side: str) -> str:
    return (
        f"{base_url}/{symbol.upper()}/{day.year:04d}/{day.month - 1:02d}/"
        f"{day.day:02d}/{side.upper()}_candles_min_1.bi5"
    )


def decode_candles(raw: bytes, day: date, factor: int) -> pd.DataFrame:
    """Decode one compressed daily 1-minute candle file (all 1440 minutes, UTC)."""
    if not raw:
        return pd.DataFrame(columns=CANDLE_COLUMNS)
    payload = lzma.decompress(raw)
    if len(payload) % CANDLE_DTYPE.itemsize:
        raise DukascopyError(f"Corrupt candle file for {day}: {len(payload)} bytes")
    rec = np.frombuffer(payload, dtype=CANDLE_DTYPE)
    midnight = pd.Timestamp(day.isoformat(), tz="UTC")
    return pd.DataFrame(
        {
            "ts": midnight + pd.to_timedelta(rec["s"].astype("int64"), unit="s"),
            **{c: rec[c].astype("float64") / factor for c in ("open", "high", "low", "close")},
            "volume": rec["volume"].astype("float64").round(6),
        }
    )


def decode_ticks(raw: bytes, hour: datetime, factor: int) -> pd.DataFrame:
    """Decode one compressed .bi5 hour into a tick DataFrame (UTC timestamps)."""
    if not raw:
        return pd.DataFrame(columns=TICK_COLUMNS)
    payload = lzma.decompress(raw)
    if len(payload) % TICK_DTYPE.itemsize:
        raise DukascopyError(f"Corrupt tick file for {hour:%Y-%m-%d %H}h: {len(payload)} bytes")
    rec = np.frombuffer(payload, dtype=TICK_DTYPE)
    hour_start = pd.Timestamp(hour).tz_convert("UTC") if hour.tzinfo else pd.Timestamp(hour, tz="UTC")
    return pd.DataFrame(
        {
            "ts": hour_start + pd.to_timedelta(rec["ms"].astype("int64"), unit="ms"),
            "bid": rec["bid"].astype("float64") / factor,
            "ask": rec["ask"].astype("float64") / factor,
            # float32 on disk; round off the conversion noise (5.400000095 -> 5.4)
            "bid_volume": rec["bid_volume"].astype("float64").round(6),
            "ask_volume": rec["ask_volume"].astype("float64").round(6),
        }
    )


def _retry_after(resp: requests.Response) -> float | None:
    try:
        return float(resp.headers["Retry-After"])
    except (KeyError, ValueError):
        return None


class DukascopyClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_maxsize=max(settings.max_workers, 10))
        self.session.mount("https://", adapter)
        self.session.headers["User-Agent"] = "dlh-dukascopy-ingest/1.0"
        # Shared across worker threads: spaces requests out and pauses everyone after a 429
        self._lock = threading.Lock()
        self._next_request_at = 0.0

    def _throttle(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next_request_at - now
            self._next_request_at = max(now, self._next_request_at) + self.settings.min_interval_s
        if wait > 0:
            time.sleep(wait)

    def _pause_all(self, seconds: float) -> None:
        with self._lock:
            self._next_request_at = max(self._next_request_at, time.monotonic() + seconds)

    def _get(self, url: str) -> bytes:
        """GET with throttling and backoff. Returns b"" for missing/empty hours.

        429 (rate limited) pauses all workers, honouring Retry-After when present.
        Connection errors usually mean a network/ISP block; they retry with short backoff.
        """
        for attempt in range(self.settings.max_retries + 1):
            self._throttle()
            try:
                resp = self.session.get(url, timeout=self.settings.timeout_s)
            except requests.RequestException as exc:
                err: object = exc
                delay = min(self.settings.error_backoff_s * 2**attempt, 120)
            else:
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code == 404:
                    return b""
                if resp.status_code not in RETRY_STATUS:
                    raise DukascopyError(f"HTTP {resp.status_code} for {url}")
                err = f"HTTP {resp.status_code}"
                if resp.status_code == 429:
                    delay = _retry_after(resp) or min(self.settings.rate_limit_backoff_s * 2**attempt, 300)
                    self._pause_all(delay)
                else:
                    delay = min(self.settings.error_backoff_s * 2**attempt, 120)
            if attempt == self.settings.max_retries:
                raise DukascopyError(f"Giving up on {url} after {attempt + 1} attempts: {err}")
            log.warning("Retrying %s in %.0fs (%s)", url, delay, err)
            time.sleep(delay)
        raise AssertionError("unreachable")

    def fetch_hour_raw(self, symbol: str, hour: datetime) -> bytes:
        """Compressed .bi5 bytes for one hour (b"" when the hour has no data)."""
        return self._get(hour_url(self.settings.base_url, symbol, hour))

    def fetch_candles_raw(self, symbol: str, day: date, side: str) -> bytes:
        """Compressed daily 1-minute candle file for one price side (b"" when missing)."""
        return self._get(candle_url(self.settings.base_url, symbol, day, side))

    def fetch_hour(self, symbol: str, hour: datetime) -> pd.DataFrame:
        raw = self.fetch_hour_raw(symbol, hour)
        factor = point_factor(symbol, self.settings.point_overrides)
        return decode_ticks(raw, hour, factor)

    def fetch_day_ticks(self, symbol: str, day: date) -> pd.DataFrame:
        """All ticks for one UTC day, sorted and de-duplicated."""
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        hours = [start + timedelta(hours=h) for h in range(24)]
        with ThreadPoolExecutor(max_workers=self.settings.max_workers) as pool:
            frames = list(pool.map(lambda h: self.fetch_hour(symbol, h), hours))
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame(columns=TICK_COLUMNS).astype({"ts": "datetime64[ns, UTC]"})
        df = pd.concat(frames, ignore_index=True)
        return df.drop_duplicates().sort_values("ts", kind="stable").reset_index(drop=True)
