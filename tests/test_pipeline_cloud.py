"""End-to-end: downloaded day -> GCS + Pub/Sub -> BigQuery, on the local emulation."""

import json
import lzma
from datetime import date, timedelta

import numpy as np
import pytest

from cloud import Cloud, CloudSettings
from ingestion.dukascopy.client import CANDLE_DTYPE, TICK_DTYPE, DukascopyClient, DukascopyError
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.downloader import download_day, problems
from ingestion.dukascopy.pipeline import (
    IncompleteDayError, backfill_range, lake_uri, load_pending, publish_day, setup,
)

DAY = date(2026, 9, 3)


def make_bi5(rows):
    return lzma.compress(np.array(rows, dtype=TICK_DTYPE).tobytes(), format=lzma.FORMAT_ALONE)


# Two ticks in 10:00, one in 10:01 -> 3 ticks, 2 one-minute bars
BLOB = make_bi5([(0, 117012, 117010, 1.0, 1.0), (30_000, 117022, 117020, 1.0, 1.0),
                 (61_000, 117032, 117030, 1.0, 1.0)])


@pytest.fixture
def env(tmp_path, monkeypatch):
    settings = Settings(local_data_dir=str(tmp_path / "data"))
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    setup(settings, cloud)
    return settings, cloud


def download(settings, monkeypatch, fetch):
    client = DukascopyClient(settings)
    monkeypatch.setattr(client, "fetch_hour_raw", fetch)
    return download_day("EURUSD", DAY, settings, client=client)


def test_setup_creates_everything(env):
    settings, cloud = env
    assert cloud.pubsub.list_topics() == ["dukascopy-files"]
    assert cloud.bq.list_tables("market") == ["dukascopy_ticks"]
    for tf in ("1min", "5min", "15min", "1h"):  # one BigQuery dataset per timeframe
        assert cloud.bq.list_tables(f"market_{tf}") == ["dukascopy_bars"]


def test_publish_then_load(env, monkeypatch):
    settings, cloud = env
    download(settings, monkeypatch, lambda s, h: BLOB if h.hour == 10 else b"")

    summary = publish_day("EURUSD", DAY, settings, cloud)

    assert [(p["dataset"], p["rows"]) for p in summary["published"]] == [
        ("ticks", 3), ("bars_1min", 2), ("bars_5min", 1), ("bars_15min", 1), ("bars_1h", 1)]
    # Only the CSV lake files land in GCS by default, no raw .bi5
    assert cloud.storage.list("gs://dlh-lake/dukascopy/") == [
        f"gs://dlh-lake/dukascopy/csv/{ds}/symbol=EURUSD/date=2026-09-03/part-0.csv"
        for ds in ("bars_15min", "bars_1h", "bars_1min", "bars_5min", "ticks")
    ]
    [msg] = [m for m in cloud.pubsub.pull("dukascopy-files-bq-loader", 1)]
    assert json.loads(msg.data)["table"] == "market.dukascopy_ticks"
    cloud.pubsub.nack("dukascopy-files-bq-loader", [msg.ack_id])

    result = load_pending(settings, cloud)

    assert [(r["table"], r["rows"]) for r in result["loaded"]] == [
        ("market.dukascopy_ticks", 3), ("market_1min.dukascopy_bars", 2), ("market_5min.dukascopy_bars", 1),
        ("market_15min.dukascopy_bars", 1), ("market_1h.dukascopy_bars", 1)]
    assert result["failed"] == []
    bars = cloud.bq.query("SELECT ts, open, close, timeframe FROM market_1min.dukascopy_bars ORDER BY ts")
    assert bars.to_dict("records")[0] == {"ts": "2026-09-03 10:00:00.000000", "open": 1.1701,
                                          "close": 1.1702, "timeframe": "1min"}

    hourly = cloud.bq.query("SELECT open, high, low, close, volume, timeframe FROM market_1h.dukascopy_bars")
    one = cloud.bq.query("SELECT open, high, low, close, volume FROM market_1min.dukascopy_bars ORDER BY ts")
    assert hourly.to_dict("records") == [{"open": one.open[0], "high": one.high.max(), "low": one.low.min(),
                                          "close": one.close.iloc[-1], "volume": one.volume.sum(),
                                          "timeframe": "1h"}]

    # Publishing + loading the same day again replaces rather than duplicates
    publish_day("EURUSD", DAY, settings, cloud)
    load_pending(settings, cloud)
    assert cloud.bq.query("SELECT COUNT(*) AS n FROM market.dukascopy_ticks").n[0] == 3
    assert load_pending(settings, cloud) == {"loaded": [], "failed": []}


def test_raw_upload_and_parquet_are_opt_in(tmp_path, monkeypatch):
    settings = Settings(local_data_dir=str(tmp_path / "data"), lake_format="parquet", upload_raw=True)
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    setup(settings, cloud)
    download(settings, monkeypatch, lambda s, h: BLOB if h.hour == 10 else b"")
    publish_day("EURUSD", DAY, settings, cloud)
    raw = cloud.storage.list("gs://dlh-lake/dukascopy/raw/EURUSD/2026-09-03/")
    assert [u.rsplit("/", 1)[1] for u in raw] == ["10h_ticks.bi5", "_manifest.json"]
    assert cloud.storage.exists(lake_uri(settings, "ticks", "EURUSD", DAY))
    assert lake_uri(settings, "ticks", "EURUSD", DAY).endswith(".parquet")
    assert [r["rows"] for r in load_pending(settings, cloud)["loaded"]] == [3, 2, 1, 1, 1]


def test_publish_refuses_incomplete_day(env, monkeypatch):
    settings, cloud = env

    def fetch(symbol, hour):
        if hour.hour == 5:
            raise DukascopyError("connection refused", "CONNECTION")
        return BLOB if hour.hour == 10 else b""

    download(settings, monkeypatch, fetch)
    with pytest.raises(IncompleteDayError, match=r"1 bad files: CONNECTION x1 \(couldn.t connect"):
        publish_day("EURUSD", DAY, settings, cloud)
    assert publish_day("EURUSD", DAY, settings, cloud, allow_partial=True)["published"]


def test_publish_requires_download(env):
    settings, cloud = env
    with pytest.raises(FileNotFoundError, match="download"):
        publish_day("EURUSD", DAY, settings, cloud)


def test_closed_market_day_publishes_nothing(env, monkeypatch):
    settings, cloud = env
    download(settings, monkeypatch, lambda s, h: b"")
    assert publish_day("EURUSD", DAY, settings, cloud)["published"] == []
    assert load_pending(settings, cloud)["loaded"] == []


def test_failed_load_is_not_acked(env, monkeypatch):
    settings, cloud = env
    download(settings, monkeypatch, lambda s, h: BLOB if h.hour == 10 else b"")
    publish_day("EURUSD", DAY, settings, cloud)
    cloud.storage.delete(lake_uri(settings, "ticks", "EURUSD", DAY))

    result = load_pending(settings, cloud)

    assert len(result["loaded"]) == 4 and len(result["failed"]) == 1
    assert "NotFound" in result["failed"][0]["error"]
    sub = [s for s in cloud.pubsub.list_subscriptions() if s["name"] == settings.pubsub_subscription][0]
    assert (sub["backlog"], sub["leased"]) == (1, 1)  # will be redelivered after the ack deadline


def test_backfill_skips_saturdays_and_resumes(env, monkeypatch):
    settings, cloud = env
    client = DukascopyClient(settings)
    calls = []
    broken = {date(2026, 9, 2)}

    def fetch(symbol, hour):
        calls.append(hour.date())
        if hour.date() in broken and hour.hour == 3:
            raise DukascopyError("HTTP 429")
        return BLOB if hour.hour == 10 else b""

    monkeypatch.setattr(client, "fetch_hour_raw", fetch)
    start, end = date(2026, 8, 31), date(2026, 9, 6)  # Mon .. Sun, spans a month boundary

    result = backfill_range(["eurusd"], start, end, settings, cloud, client=client)

    assert date(2026, 9, 5) not in calls  # Saturday never requested
    assert [s["date"] for s in result["skipped"]] == ["2026-09-02"]
    assert len(result["published"]) == 5  # Mon, Thu, Fri, Sun + Tue
    n = cloud.bq.query("SELECT COUNT(DISTINCT date) AS n FROM market_1min.dukascopy_bars").n[0]
    assert n == 5

    # Re-run: only the incomplete day is fetched again
    broken.clear()
    calls.clear()
    result = backfill_range(["EURUSD"], start, end, settings, cloud, client=client)
    assert set(calls) == {date(2026, 9, 2)}
    assert (result["existing"], len(result["published"]), result["skipped"]) == (5, 1, [])
    bars = cloud.bq.query("SELECT date, COUNT(*) AS n FROM market_1min.dukascopy_bars GROUP BY 1 ORDER BY 1")
    assert bars.n.tolist() == [2] * 6


def make_candles(rows):
    """rows: (minute, open, close, low, high, volume) in scaled ints; other minutes stay flat with no volume."""
    rec = np.zeros(1440, dtype=CANDLE_DTYPE)
    rec["s"] = np.arange(1440) * 60
    for minute, o, c, lo, hi, v in rows:
        rec[minute] = (minute * 60, o, c, lo, hi, v)
    return lzma.compress(rec.tobytes(), format=lzma.FORMAT_ALONE)


# 10:00 and 10:01 traded; ask = bid + 20 points (0.0002)
CANDLES = {"BID": make_candles([(600, 117010, 117020, 117005, 117025, 1.5), (601, 117020, 117030, 117020, 117030, 0.5)]),
           "ASK": make_candles([(600, 117030, 117040, 117025, 117045, 1.0), (601, 117040, 117050, 117040, 117050, 0.5)])}


@pytest.fixture
def candle_env(tmp_path):
    settings = Settings(local_data_dir=str(tmp_path / "data"), datasets=["bars"], bar_source="candles")
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    setup(settings, cloud)
    return settings, cloud


def test_candle_source_publish_then_load(candle_env, monkeypatch):
    settings, cloud = candle_env
    client = DukascopyClient(settings)
    requested = []
    monkeypatch.setattr(client, "fetch_hour_raw", lambda *a: pytest.fail("ticks must not be fetched"))
    monkeypatch.setattr(client, "fetch_candles_raw",
                        lambda symbol, day, side: requested.append(side) or CANDLES[side])

    manifest = download_day("EURUSD", DAY, settings, client=client)
    assert (manifest["files_ok"], manifest["bars"], problems(manifest)) == (2, 2, [])
    download_day("EURUSD", DAY, settings, client=client)  # second run uses the cache
    assert sorted(requested) == ["ASK", "BID"]

    publish_day("EURUSD", DAY, settings, cloud)
    assert [r["rows"] for r in load_pending(settings, cloud)["loaded"]] == [2, 1, 1, 1]
    bars = cloud.bq.query("SELECT ts, open, high, low, close, volume "
                          "FROM market_1min.dukascopy_bars ORDER BY ts").to_dict("records")
    assert bars[0] == {"ts": "2026-09-03 10:00:00.000000", "open": 1.1701, "high": 1.17025, "low": 1.17005,
                       "close": 1.1702, "volume": 2.5}


def test_candle_source_failed_side_blocks_publish(candle_env, monkeypatch):
    settings, cloud = candle_env
    client = DukascopyClient(settings)

    def fetch(symbol, day, side):
        if side == "ASK":
            raise DukascopyError("HTTP 503")
        return CANDLES[side]

    monkeypatch.setattr(client, "fetch_candles_raw", fetch)
    manifest = download_day("EURUSD", DAY, settings, client=client)
    assert problems(manifest) == ["ASK failed: HTTP 503"]
    with pytest.raises(IncompleteDayError):
        publish_day("EURUSD", DAY, settings, cloud)


def test_candle_source_requires_bars_only():
    with pytest.raises(ValueError, match="DATASETS=bars"):
        Settings(bar_source="candles").validate()


def test_repair_task_fills_gaps_and_retries_incomplete(tmp_path, monkeypatch):
    import sys
    from pathlib import Path

    from ingestion.dukascopy import pipeline

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "plugins"))
    import dlh_tasks

    settings = Settings(local_data_dir=str(tmp_path / "data"), symbols=["EURUSD"])
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    setup(settings, cloud)
    monkeypatch.setattr(dlh_tasks, "_pipeline", lambda: (pipeline, settings, cloud))
    monkeypatch.setenv("DUKASCOPY_REPAIR_DAYS", "3")  # Fri 09-04 .. Sun 09-06
    broken = {date(2026, 9, 6)}

    def fetch(self, symbol, hour):
        if hour.date() in broken and hour.hour == 3:
            raise DukascopyError("HTTP 429")
        return BLOB if hour.hour == 10 else b""

    monkeypatch.setattr(DukascopyClient, "fetch_hour_raw", fetch)

    with pytest.raises(RuntimeError, match="EURUSD 2026-09-06: 1 bad files: ERROR x1"):
        dlh_tasks.repair(ds="2026-09-06")

    broken.clear()
    result = dlh_tasks.repair(ds="2026-09-06")
    assert (result["start"], result["existing"], result["published"]) == ("2026-09-04", 1, ["EURUSD 2026-09-06"])

    result = dlh_tasks.repair(ds="2026-09-06")
    assert (result["existing"], result["published"], result["loaded"]) == (2, [], 0)


def test_hourly_task_reads_ticks_even_when_daily_uses_candles(tmp_path, monkeypatch):
    """DUKASCOPY_BAR_SOURCE=candles is for the daily runs; the hourly DAG must still work."""
    import sys
    from datetime import datetime, timezone
    from pathlib import Path

    from ingestion.dukascopy import pipeline

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "plugins"))
    import dlh_tasks

    settings = Settings(local_data_dir=str(tmp_path / "data"), symbols=["EURUSD"], datasets=["bars"],
                        bar_source="candles", bar_timeframes=["1min", "1h"])
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    setup(settings, cloud)
    monkeypatch.setattr(dlh_tasks, "_pipeline", lambda: (pipeline, settings, cloud))
    fetched = []

    def fetch(self, symbol, hour):
        fetched.append(hour.hour)
        return lzma.compress(np.array([(0, 117012, 117010, 1.0, 1.0)], dtype=TICK_DTYPE).tobytes(),
                             format=lzma.FORMAT_ALONE)

    monkeypatch.setattr(DukascopyClient, "fetch_hour_raw", fetch)
    # a Thursday; 03:00 just closed
    result = dlh_tasks.hourly(data_interval_end=datetime(2026, 10, 29, 3, 10, tzinfo=timezone.utc))
    assert result["EURUSD"].startswith("loaded") and sorted(fetched) == [0, 1, 2]
    assert len(cloud.bq.query("SELECT ts FROM market_1h.dukascopy_bars")) == 3
    assert settings.bar_source == "candles"  # the daily runs' settings are untouched
