import json
import lzma
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import pytest

from ingestion.dukascopy.__main__ import main
from ingestion.dukascopy.client import TICK_DTYPE, DukascopyClient, DukascopyError
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.converter import convert, parse_bi5_path
from ingestion.dukascopy.downloader import download_day

DAY = date(2026, 9, 3)


def make_bi5(rows):
    return lzma.compress(np.array(rows, dtype=TICK_DTYPE).tobytes(), format=lzma.FORMAT_ALONE)


BLOB = make_bi5([(0, 117012, 117010, 1.5, 2.25), (61_500, 117020, 117015, 0.75, 1.0)])


def fake_client(settings, monkeypatch, responses):
    """responses: hour -> bytes; other hours return b"". Records calls."""
    client = DukascopyClient(settings)
    calls = []

    def fetch(symbol, hour):
        calls.append(hour.hour)
        return responses.get(hour.hour, b"")

    monkeypatch.setattr(client, "fetch_hour_raw", fetch)
    return client, calls


def test_parse_bi5_path_both_layouts():
    assert parse_bi5_path("data/raw/eurusd/2026-09-03/10h_ticks.bi5") == (
        "EURUSD", datetime(2026, 9, 3, 10, tzinfo=timezone.utc))
    assert parse_bi5_path("dl/EURUSD/2026/08/03/07h_ticks.bi5") == (
        "EURUSD", datetime(2026, 9, 3, 7, tzinfo=timezone.utc))
    with pytest.raises(ValueError):
        parse_bi5_path("random.bi5")


def test_download_day_writes_files_and_manifest(tmp_path, monkeypatch):
    settings = Settings(local_data_dir=str(tmp_path))
    client, calls = fake_client(settings, monkeypatch, {10: BLOB, 11: BLOB})

    m = download_day("EURUSD", DAY, settings, client=client)

    day_dir = tmp_path / "raw" / "EURUSD" / "2026-09-03"
    assert sorted(p.name for p in day_dir.glob("*.bi5")) == ["10h_ticks.bi5", "11h_ticks.bi5"]
    assert (m["hours_ok"], m["hours_empty"], m["total_ticks"]) == (2, 22, 4)
    assert json.loads((day_dir / "_manifest.json").read_text())["total_ticks"] == 4
    assert m["hours"][10]["first_bid"] == 1.1701

    # Second run reuses the cached non-empty hours and re-checks only the empty ones
    calls.clear()
    m2 = download_day("EURUSD", DAY, settings, client=client)
    assert 10 not in calls and 11 not in calls and len(calls) == 22
    assert m2["hours"][10]["source"] == "cached"


def test_download_day_flags_corrupt_hours(tmp_path, monkeypatch):
    from ingestion.dukascopy.downloader import problems

    settings = Settings(local_data_dir=str(tmp_path))
    client, _ = fake_client(settings, monkeypatch, {10: BLOB, 12: b"<html>blocked</html>"})

    m = download_day("EURUSD", DAY, settings, client=client)

    assert m["hours_corrupt"] == 1 and m["hours_ok"] == 1
    assert problems(m)[0].startswith("12h corrupt")
    day_dir = tmp_path / "raw" / "EURUSD" / "2026-09-03"
    assert not (day_dir / "12h_ticks.bi5").exists()
    assert json.loads((day_dir / "_manifest.json").read_text())["hours"][12]["status"] == "corrupt"


def test_convert_single_file(tmp_path):
    f = tmp_path / "raw" / "EURUSD" / "2026-09-03" / "10h_ticks.bi5"
    f.parent.mkdir(parents=True)
    f.write_bytes(BLOB)

    [out] = convert(f, tmp_path / "csv")

    assert out == tmp_path / "csv" / "EURUSD" / "EURUSD_2026-09-03_10h_ticks.csv"
    lines = out.read_text().splitlines()
    assert lines[0] == "ts,bid,ask,bid_volume,ask_volume"
    assert lines[1] == "2026-09-03 10:00:00.000,1.1701,1.17012,2.25,1.5"
    assert lines[2] == "2026-09-03 10:01:01.500,1.17015,1.1702,1,0.75"


def test_convert_unrecognised_path_needs_symbol_and_hour(tmp_path):
    f = tmp_path / "whatever.bi5"
    f.write_bytes(BLOB)
    with pytest.raises(ValueError):
        convert(f, tmp_path / "csv")
    [out] = convert(f, tmp_path / "csv", symbol="usdjpy", hour=datetime(2026, 9, 3, 10))
    assert out.name == "USDJPY_2026-09-03_10h_ticks.csv"
    assert pd.read_csv(out)["bid"].iloc[0] == 117.01  # JPY scale (1e3)


def test_convert_directory_groups_per_day_with_bars(tmp_path):
    day_dir = tmp_path / "raw" / "EURUSD" / "2026-09-03"
    day_dir.mkdir(parents=True)
    (day_dir / "11h_ticks.bi5").write_bytes(BLOB)
    (day_dir / "10h_ticks.bi5").write_bytes(BLOB)

    written = convert(tmp_path / "raw", tmp_path / "csv", bars_timeframe="1min")

    assert [p.name for p in written] == ["EURUSD_2026-09-03_ticks.csv", "EURUSD_2026-09-03_bars_1min.csv"]
    ticks = pd.read_csv(written[0])
    assert len(ticks) == 4 and ticks["ts"].is_monotonic_increasing
    bars = pd.read_csv(written[1])
    assert list(bars.columns) == ["ts", "open", "high", "low", "close", "volume"]
    assert bars["volume"].sum() == (ticks["bid_volume"] + ticks["ask_volume"]).sum()


def test_cli_convert(tmp_path, capsys):
    f = tmp_path / "EURUSD" / "2026-09-03" / "10h_ticks.bi5"
    f.parent.mkdir(parents=True)
    f.write_bytes(BLOB)
    main(["convert", str(f), "--out", str(tmp_path / "csv")])
    assert "EURUSD_2026-09-03_10h_ticks.csv" in capsys.readouterr().out


def test_download_day_records_failed_hours_and_keeps_going(tmp_path, monkeypatch):
    settings = Settings(local_data_dir=str(tmp_path))
    client = DukascopyClient(settings)

    def fetch(symbol, hour):
        if hour.hour == 5:
            raise DukascopyError("Giving up: connection refused")
        return BLOB if hour.hour == 10 else b""

    monkeypatch.setattr(client, "fetch_hour_raw", fetch)
    m = download_day("EURUSD", DAY, settings, client=client)
    assert (m["hours_ok"], m["hours_failed"]) == (1, 1)
    assert m["hours"][5]["status"] == "failed"


def test_download_range_aborts_when_network_is_down(tmp_path, monkeypatch):
    from ingestion.dukascopy import downloader

    settings = Settings(local_data_dir=str(tmp_path))

    def always_fail(self, symbol, hour):
        raise DukascopyError("connection refused")

    monkeypatch.setattr(DukascopyClient, "fetch_hour_raw", always_fail)
    with pytest.raises(DukascopyError, match="unreachable"):
        downloader.download_range(["EURUSD", "XAUUSD"], DAY, date(2026, 9, 5), settings)


def test_client_pauses_on_429(monkeypatch):
    settings = Settings(min_interval_s=0, rate_limit_backoff_s=7, max_retries=1)
    client = DukascopyClient(settings)

    class Resp:
        def __init__(self, code, body=b""):
            self.status_code, self.content, self.headers = code, body, {}

    responses = iter([Resp(429), Resp(200, BLOB)])
    monkeypatch.setattr(client.session, "get", lambda url, timeout: next(responses))
    sleeps = []
    monkeypatch.setattr("ingestion.dukascopy.client.time.sleep", sleeps.append)
    assert client._get("https://x/EURUSD/2026/08/03/10h_ticks.bi5") == BLOB
    assert 7 in sleeps
