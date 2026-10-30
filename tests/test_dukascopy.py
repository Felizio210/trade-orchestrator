import lzma
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from ingestion.dukascopy.client import TICK_DTYPE, decode_ticks, hour_url
from ingestion.dukascopy.instruments import point_factor
from ingestion.dukascopy.transform import ticks_to_bars

HOUR = datetime(2026, 9, 3, 10, tzinfo=timezone.utc)


def make_bi5(rows):
    rec = np.array(rows, dtype=TICK_DTYPE)
    return lzma.compress(rec.tobytes(), format=lzma.FORMAT_ALONE)


def test_hour_url_uses_zero_based_month():
    assert hour_url("https://x/datafeed", "eurusd", HOUR) == "https://x/datafeed/EURUSD/2026/08/03/10h_ticks.bi5"


def test_point_factor():
    assert point_factor("EURUSD") == 100_000
    assert point_factor("USDJPY") == 1_000
    assert point_factor("XAUUSD") == 1_000
    assert point_factor("BTCUSD", {"BTCUSD": 100}) == 100


def test_decode_ticks():
    raw = make_bi5([(0, 117012, 117010, 1.5, 2.25), (61_500, 117020, 117015, 0.75, 1.0)])
    df = decode_ticks(raw, HOUR, 100_000)
    assert list(df["ts"]) == [pd.Timestamp("2026-09-03 10:00:00", tz="UTC"),
                              pd.Timestamp("2026-09-03 10:01:01.500", tz="UTC")]
    assert df["bid"].tolist() == [1.1701, 1.17015]
    assert df["ask"].tolist() == [1.17012, 1.1702]
    assert df["bid_volume"].tolist() == [2.25, 1.0]


def test_decode_empty_hour():
    assert decode_ticks(b"", HOUR, 100_000).empty


def test_ticks_to_bars():
    ts = pd.to_datetime(["2026-09-03 10:00:01", "2026-09-03 10:00:30", "2026-09-03 10:00:59",
                         "2026-09-03 10:02:00"], utc=True)
    ticks = pd.DataFrame({"ts": ts, "bid": [1.0, 3.0, 2.0, 5.0], "ask": [1.1, 3.1, 2.1, 5.2],
                          "bid_volume": [1.0] * 4, "ask_volume": [1.0] * 4})
    bars = ticks_to_bars(ticks, "1min", "bid")
    assert len(bars) == 2  # empty 10:01 bar dropped
    first = bars.iloc[0]
    assert (first.open, first.high, first.low, first.close) == (1.0, 3.0, 1.0, 2.0)
    assert first.volume == 6.0


def _client_raising(monkeypatch, outcome):
    """A client whose every GET raises `outcome` (an exception) or returns it (a status code)."""
    import requests

    from ingestion.dukascopy.client import DukascopyClient
    from ingestion.dukascopy.config import Settings

    client = DukascopyClient(Settings(max_retries=1, error_backoff_s=0, min_interval_s=0))

    def get(url, timeout):
        if isinstance(outcome, Exception):
            raise outcome
        resp = requests.Response()
        resp.status_code = outcome
        return resp

    monkeypatch.setattr(client.session, "get", get)
    monkeypatch.setattr("time.sleep", lambda s: None)
    return client


def test_download_errors_carry_their_kind(monkeypatch):
    import pytest
    import requests

    from ingestion.dukascopy.client import DukascopyError

    cases = {
        "SSL": requests.exceptions.SSLError("certificate verify failed: Hostname mismatch"),
        "DNS": requests.ConnectionError("NameResolutionError: Failed to resolve 'datafeed.dukascopy.com'"),
        "TIMEOUT": requests.exceptions.ReadTimeout("Read timed out"),
        "CONNECTION": requests.ConnectionError("Connection refused"),
        "SERVER": 503,
        "HTTP": 403,
    }
    for kind, outcome in cases.items():
        with pytest.raises(DukascopyError, match=rf"^\[{kind}\]") as info:
            _client_raising(monkeypatch, outcome).fetch_hour_raw("EURUSD", HOUR)
        assert info.value.kind == kind


def test_problem_summary_groups_by_kind():
    from ingestion.dukascopy.downloader import problem_summary

    manifest = {"hours": [
        {"side": "BID", "status": "failed", "kind": "SSL", "error": "x"},
        {"side": "ASK", "status": "failed", "kind": "SSL", "error": "x"},
        {"hour": "03", "status": "corrupt", "kind": "CORRUPT", "error": "x"},
        {"hour": "04", "status": "failed", "error": "x"},  # old manifest, no kind
        {"hour": "05", "status": "ok"},
    ]}
    summary = problem_summary(manifest)
    assert summary.startswith("4 bad files: SSL x2 (HTTPS is being intercepted")
    assert "CORRUPT x1 (" in summary and "ERROR x1 (" in summary
