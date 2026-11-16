"""End-to-end: downloaded day -> GCS + Pub/Sub -> BigQuery, on the local emulation."""

import json
import lzma
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pytest

from cloud import Cloud, CloudSettings
from ingestion.dukascopy.client import CANDLE_DTYPE, TICK_DTYPE, DukascopyClient, DukascopyError
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.downloader import download_day, problems
from ingestion.dukascopy.pipeline import (
    IncompleteDayError, backfill_range, lake_uri, last_complete_day, load_pending, publish_day, run_range, setup,
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
    assert cloud.bq.list_tables("market") == ["dukascopy_ticks", "quality"]
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
    assert load_pending(settings, cloud) == {"loaded": [], "failed": [], "quality_failed": [], "quality_warnings": []}


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


def test_unfinished_day_is_never_published(env, monkeypatch):
    """Hours that haven't happened yet download as empty, so today's manifest looks clean."""
    settings, cloud = env
    today = datetime.now(timezone.utc).date()
    client = DukascopyClient(settings)
    calls = []
    monkeypatch.setattr(client, "fetch_hour_raw", lambda s, h: calls.append(h.date()) or (BLOB if h.hour == 10 else b""))
    download_day("EURUSD", today, settings, client=client)
    assert last_complete_day() == today - timedelta(days=1)

    with pytest.raises(IncompleteDayError, match="hasn't ended yet"):
        publish_day("EURUSD", today, settings, cloud)

    calls.clear()
    result = backfill_range(["EURUSD"], today - timedelta(days=1), today, settings, cloud, client=client)
    assert today not in calls  # stops at yesterday, which the next run treats as new
    assert not any(cloud.storage.exists(lake_uri(settings, d, "EURUSD", today)) for d in ("ticks", "bars_1h"))
    assert today.isoformat() not in [p["date"] for p in result["published"]]

    assert run_range(["EURUSD"], today, today, settings, cloud, load=False)["skipped"] == [
        {"symbol": "EURUSD", "date": today.isoformat(), "problems": [], "summary": "day hasn't ended yet (UTC)"}]


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


def _tasks(tmp_path, monkeypatch, **overrides):
    import sys
    from pathlib import Path

    from ingestion.dukascopy import pipeline

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "plugins"))
    import dlh_tasks

    settings = Settings(local_data_dir=str(tmp_path / "data"), symbols=["EURUSD"], datasets=["bars"],
                        bar_source="candles", bar_timeframes=["1min", "1h"], **overrides)
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    setup(settings, cloud)
    monkeypatch.setattr(dlh_tasks, "_pipeline", lambda: (pipeline, settings, cloud))
    fetched = []

    def fetch(self, symbol, hour):
        fetched.append(hour)
        return lzma.compress(np.array([(0, 117012, 117010, 1.0, 1.0)], dtype=TICK_DTYPE).tobytes(),
                             format=lzma.FORMAT_ALONE)

    monkeypatch.setattr(DukascopyClient, "fetch_hour_raw", fetch)
    return dlh_tasks, settings, cloud, fetched


def test_intraday_task_reads_ticks_even_when_cli_uses_candles(tmp_path, monkeypatch):
    """DUKASCOPY_BAR_SOURCE=candles is for the CLI; the DAGs always build from ticks."""
    from datetime import datetime, timezone

    dlh_tasks, settings, cloud, fetched = _tasks(tmp_path, monkeypatch)
    # a Thursday; 03:00 just closed
    result = dlh_tasks.intraday(data_interval_end=datetime(2026, 10, 29, 3, 10, tzinfo=timezone.utc))
    assert result["EURUSD"].startswith("loaded") and sorted(h.hour for h in fetched) == [0, 1, 2]
    assert len(cloud.bq.query("SELECT ts FROM market_1h.dukascopy_bars")) == 3
    assert settings.bar_source == "candles"  # the CLI's settings are untouched


def test_finalize_publishes_yesterday_once_from_cached_hours(tmp_path, monkeypatch):
    from datetime import datetime, timezone

    from ingestion.dukascopy.pipeline import lake_uri

    dlh_tasks, settings, cloud, fetched = _tasks(tmp_path, monkeypatch)
    dlh_tasks.intraday(data_interval_end=datetime(2026, 10, 29, 23, 10, tzinfo=timezone.utc))
    assert len(fetched) == 23  # 00h..22h cached, provisional rows in BigQuery
    fetched.clear()

    midnight = datetime(2026, 10, 30, 0, 10, tzinfo=timezone.utc)
    result = dlh_tasks.finalize(data_interval_end=midnight)
    assert result == {"date": "2026-10-29", "published": ["EURUSD"], "existing": 0}
    assert [h.hour for h in fetched] == [23]  # only the hour not cached yet
    assert cloud.storage.exists(lake_uri(settings, "bars_1h", "EURUSD", date(2026, 10, 29)))
    assert dlh_tasks.load()["loaded"] == 2  # bars_1min + bars_1h
    assert len(cloud.bq.query("SELECT ts FROM market_1h.dukascopy_bars")) == 24

    assert dlh_tasks.intraday(data_interval_end=midnight)["EURUSD"].startswith("complete")
    fetched.clear()
    result = dlh_tasks.finalize(data_interval_end=datetime(2026, 10, 30, 1, 10, tzinfo=timezone.utc))
    assert (result["published"], result["existing"], fetched) == ([], 1, [])
    assert dlh_tasks.load()["loaded"] == 0


def test_finalize_fails_on_incomplete_day_and_skips_saturday(tmp_path, monkeypatch):
    from datetime import datetime, timezone

    dlh_tasks, settings, cloud, fetched = _tasks(tmp_path, monkeypatch)

    def fetch(self, symbol, hour):
        raise DukascopyError("HTTP 429")

    monkeypatch.setattr(DukascopyClient, "fetch_hour_raw", fetch)
    with pytest.raises(RuntimeError, match="2026-10-29 incomplete"):
        dlh_tasks.finalize(data_interval_end=datetime(2026, 10, 30, 0, 10, tzinfo=timezone.utc))
    from ingestion.dukascopy.pipeline import lake_uri
    assert not cloud.storage.exists(lake_uri(settings, "bars_1h", "EURUSD", date(2026, 10, 29)))

    result = dlh_tasks.finalize(data_interval_end=datetime(2026, 11, 1, 5, 10, tzinfo=timezone.utc))
    assert result == {"date": "2026-10-31", "skipped": "saturday"}


def test_freshness_passes_on_current_bars_and_fails_on_a_stall(tmp_path, monkeypatch):
    from datetime import datetime, timezone

    dlh_tasks, settings, cloud, fetched = _tasks(tmp_path, monkeypatch)
    # a Thursday: bars up to the 02h bar, loaded at 03:10 (the fake has one tick per hour, at :00)
    dlh_tasks.intraday(data_interval_end=datetime(2026, 10, 29, 3, 10, tzinfo=timezone.utc))

    ok = dlh_tasks.freshness(data_interval_end=datetime(2026, 10, 29, 3, 10, tzinfo=timezone.utc))
    assert ok == {"EURUSD 1min": "2026-10-29 02:00 (0h behind)", "EURUSD 1h": "2026-10-29 02:00 (0h behind)"}
    # two hours later with no new bars: 2h behind, still within the default 2h
    dlh_tasks.freshness(data_interval_end=datetime(2026, 10, 29, 5, 10, tzinfo=timezone.utc))
    with pytest.raises(RuntimeError, match="EURUSD 1h: newest bar 2026-10-29 02:00, 3 market hours behind"):
        dlh_tasks.freshness(data_interval_end=datetime(2026, 10, 29, 6, 10, tzinfo=timezone.utc))
    monkeypatch.setenv("DUKASCOPY_MAX_LAG_HOURS", "5")
    dlh_tasks.freshness(data_interval_end=datetime(2026, 10, 29, 6, 10, tzinfo=timezone.utc))


def test_freshness_ignores_closed_market_hours(tmp_path, monkeypatch):
    from datetime import datetime, timezone

    dlh_tasks, settings, cloud, fetched = _tasks(tmp_path, monkeypatch)
    # Friday's last open hour is 20h; New York's clocks go back on Sunday Nov 1, so the market
    # reopens at 23:00 UTC that day, not 22:00
    dlh_tasks.intraday(data_interval_end=datetime(2026, 10, 30, 21, 10, tzinfo=timezone.utc))
    weekend = dlh_tasks.freshness(data_interval_end=datetime(2026, 11, 1, 23, 10, tzinfo=timezone.utc))
    assert weekend["EURUSD 1h"].endswith("(0h behind)")
    monday = dlh_tasks.freshness(data_interval_end=datetime(2026, 11, 2, 1, 10, tzinfo=timezone.utc))
    assert monday["EURUSD 1h"].endswith("(2h behind)")  # Sunday 23h and Monday 00h


def test_market_hours_follow_new_york_time():
    """The daily break and the weekend move by an hour in UTC with US daylight saving."""
    import sys
    from datetime import datetime, timezone
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "plugins"))
    from dlh_tasks import _market_open

    def is_open(*args):
        return _market_open(datetime(*args, tzinfo=timezone.utc))

    # summer (Wed 2026-07-08): the break is 21:00 UTC
    assert [is_open(2026, 7, 8, h) for h in (20, 21, 22)] == [True, False, True]
    # winter (Wed 2026-11-04): the break is 22:00 UTC
    assert [is_open(2026, 11, 4, h) for h in (21, 22, 23)] == [True, False, True]
    # weekend: closes Friday 17:00 New York, reopens Sunday 18:00
    assert [is_open(2026, 7, 10, h) for h in (20, 21)] == [True, False]  # summer Friday
    assert [is_open(2026, 11, 6, h) for h in (21, 22)] == [True, False]  # winter Friday
    assert not is_open(2026, 11, 7, 12)  # Saturday
    assert [is_open(2026, 7, 12, h) for h in (21, 22)] == [False, True]  # summer Sunday
    assert [is_open(2026, 11, 8, h) for h in (22, 23)] == [False, True]  # winter Sunday
    # Christmas, New Year's Day and Good Friday don't trade at all
    assert not is_open(2026, 12, 25, 10) and not is_open(2027, 1, 1, 10) and not is_open(2026, 4, 3, 10)
    assert is_open(2026, 12, 24, 10) and is_open(2026, 12, 28, 10)


def test_finalize_waits_for_the_last_hour_before_publishing(tmp_path, monkeypatch):
    """At 00:10 yesterday's 23h may not be out yet; publishing then would keep the day short for good."""
    from datetime import datetime, timezone

    dlh_tasks, settings, cloud, fetched = _tasks(tmp_path, monkeypatch)
    blob = make_bi5([(0, 117012, 117010, 1.0, 1.0)])
    late = {23}
    monkeypatch.setattr(DukascopyClient, "fetch_hour_raw",
                        lambda self, symbol, hour: b"" if hour.hour in late else blob)
    uri = lake_uri(settings, "bars_1h", "EURUSD", date(2026, 10, 29))  # a Thursday

    with pytest.raises(RuntimeError, match="EURUSD: 23h not published yet"):
        dlh_tasks.finalize(data_interval_end=datetime(2026, 10, 30, 0, 10, tzinfo=timezone.utc))
    assert not cloud.storage.exists(uri)

    late.clear()  # the hour arrives: the next hourly run publishes the whole day
    dlh_tasks.finalize(data_interval_end=datetime(2026, 10, 30, 1, 10, tzinfo=timezone.utc))
    assert dlh_tasks.load()["loaded"] == 2
    assert len(cloud.bq.query("SELECT ts FROM market_1h.dukascopy_bars")) == 24


def test_finalize_accepts_an_empty_last_hour_when_it_is_closed_or_stays_empty(tmp_path, monkeypatch):
    from datetime import datetime, timezone

    dlh_tasks, settings, cloud, fetched = _tasks(tmp_path, monkeypatch)
    blob = make_bi5([(0, 117012, 117010, 1.0, 1.0)])
    monkeypatch.setattr(DukascopyClient, "fetch_hour_raw",
                        lambda self, symbol, hour: b"" if hour.hour >= 21 else blob)
    # Friday: 23h is after the weekly close, nothing to wait for
    result = dlh_tasks.finalize(data_interval_end=datetime(2026, 10, 31, 0, 10, tzinfo=timezone.utc))
    assert result["published"] == ["EURUSD"]
    # Thursday, still empty a few hours later (e.g. a holiday evening): published as it is
    with pytest.raises(RuntimeError, match="23h not published yet"):
        dlh_tasks.finalize(data_interval_end=datetime(2026, 10, 30, 2, 10, tzinfo=timezone.utc))
    result = dlh_tasks.finalize(data_interval_end=datetime(2026, 10, 30, 3, 10, tzinfo=timezone.utc))
    assert result["published"] == ["EURUSD"]


def test_repair_reaches_back_to_the_last_published_day_after_long_downtime(tmp_path, monkeypatch):
    """Downtime longer than DUKASCOPY_REPAIR_DAYS must not leave a hole behind the window."""
    import sys
    from pathlib import Path

    from ingestion.dukascopy import pipeline

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "plugins"))
    import dlh_tasks

    settings = Settings(local_data_dir=str(tmp_path / "data"), symbols=["EURUSD"])
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    setup(settings, cloud)
    monkeypatch.setattr(dlh_tasks, "_pipeline", lambda: (pipeline, settings, cloud))
    monkeypatch.setenv("DUKASCOPY_REPAIR_DAYS", "3")
    monkeypatch.setattr(DukascopyClient, "fetch_hour_raw",
                        lambda self, symbol, hour: BLOB if hour.hour == 10 else b"")

    # A new install has nothing published: the window stays as configured
    assert pipeline.repair_start(["EURUSD"], date(2026, 9, 14), settings, cloud) == date(2026, 9, 14)

    backfill_range(["EURUSD"], date(2026, 9, 1), date(2026, 9, 2), settings, cloud)  # last days before the outage
    result = dlh_tasks.repair(ds="2026-09-16")  # two weeks later; the window alone is 09-14 .. 09-16
    assert result["start"] == "2026-09-03"
    assert len(result["published"]) == 12  # 09-03 .. 09-16 without the two Saturdays
    days = cloud.bq.query("SELECT DISTINCT date FROM market_1h.dukascopy_bars ORDER BY date")["date"].tolist()
    assert days[0] == "2026-09-01" and len(days) == 14

    monkeypatch.setenv("DUKASCOPY_REPAIR_MAX_DAYS", "5")  # the cap: older gaps stay manual
    assert dlh_tasks.repair(ds="2026-10-16")["start"] == "2026-10-14"


def test_freshness_fails_when_a_symbol_has_no_bars(tmp_path, monkeypatch):
    from datetime import datetime, timezone

    dlh_tasks, settings, cloud, fetched = _tasks(tmp_path, monkeypatch)
    with pytest.raises(RuntimeError, match="EURUSD 1min: no bars"):
        dlh_tasks.freshness(data_interval_end=datetime(2026, 10, 29, 3, 10, tzinfo=timezone.utc))



# --- empty days, bar source, quality ---

def test_a_closed_day_is_marked_empty_and_never_fetched_again(tmp_path, monkeypatch):
    from ingestion.dukascopy.pipeline import empty_uri, in_lake

    settings = Settings(local_data_dir=str(tmp_path / "data"), symbols=["EURUSD"], datasets=["bars"],
                        bar_timeframes=["1h"])
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    setup(settings, cloud)
    fetched = []
    monkeypatch.setattr(DukascopyClient, "fetch_hour_raw", lambda self, s, h: fetched.append(h) or b"")
    good_friday, thursday = date(2026, 4, 3), date(2026, 4, 2)
    backfill_range(["EURUSD"], good_friday, good_friday, settings, cloud)
    assert cloud.storage.exists(empty_uri(settings, "EURUSD", good_friday)) and len(fetched) == 24
    backfill_range(["EURUSD"], good_friday, good_friday, settings, cloud)
    assert len(fetched) == 24  # not fetched again

    # A day the market was open with no data at all is suspicious: not marked, so it's retried
    backfill_range(["EURUSD"], thursday, thursday, settings, cloud)
    assert not in_lake(settings, cloud, "EURUSD", thursday)


def test_ticks_and_candles_build_the_same_bars(tmp_path):
    """The CLI may build bars from candle files, the DAGs from ticks: both must agree, and say which they were."""
    from ingestion.dukascopy.downloader import candle_path, raw_hour_path
    from ingestion.dukascopy.pipeline import _day_frames

    rng = np.random.default_rng(3)
    day = date(2026, 9, 3)
    ticks = []
    for hour in (9, 10, 13):
        ms = np.sort(rng.choice(3_600_000, size=400, replace=False))
        bid = 117_000 + np.cumsum(rng.integers(-3, 4, size=400))
        ticks.append((hour, [(int(m), int(b) + 2, int(b), float(rng.integers(1, 4)), float(rng.integers(1, 4)))
                             for m, b in zip(ms, bid)]))
    base = dict(local_data_dir=str(tmp_path / "data"), symbols=["EURUSD"], datasets=["bars"],
                bar_timeframes=["1min", "1h"])
    tick_settings, candle_settings = Settings(**base), Settings(**base, bar_source="candles")
    for hour, rows in ticks:
        path = raw_hour_path(tick_settings.raw_dir, "EURUSD", datetime(2026, 9, 3, hour, tzinfo=timezone.utc))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(make_bi5(rows))
    # Candle files as Dukascopy builds them from the same ticks: every minute, volume 0 when nothing traded
    for side, price_col, vol_col in (("BID", 2, 4), ("ASK", 1, 3)):
        minutes = {}
        for hour, rows in ticks:
            for r in rows:
                minutes.setdefault(hour * 60 + r[0] // 60_000, []).append((r[price_col], r[vol_col]))
        records = []
        for m in range(1440):
            if m in minutes:
                p = [x[0] for x in minutes[m]]
                records.append((m * 60, p[0], p[-1], min(p), max(p), sum(x[1] for x in minutes[m])))
            else:
                records.append((m * 60, 0, 0, 0, 0, 0.0))
        path = candle_path(candle_settings.raw_dir, "EURUSD", day, side)
        path.write_bytes(lzma.compress(np.array(records, dtype=CANDLE_DTYPE).tobytes(), format=lzma.FORMAT_ALONE))

    now = datetime(2026, 9, 4, tzinfo=timezone.utc)
    from_ticks, from_candles = _day_frames("EURUSD", day, tick_settings, now), _day_frames("EURUSD", day, candle_settings, now)
    for ds in ("bars_1min", "bars_1h"):
        cols = ["ts", "open", "high", "low", "close", "volume"]
        assert len(from_ticks[ds]) > 0
        assert from_ticks[ds][cols].reset_index(drop=True).equals(from_candles[ds][cols].reset_index(drop=True))
        assert set(from_ticks[ds]["source"]) == {"ticks"} and set(from_candles[ds]["source"]) == {"candles"}


def test_quality_checks_after_each_load(tmp_path):
    import pandas as pd

    from ingestion.dukascopy import quality
    from ingestion.dukascopy.tables import bq_table_id, shape

    settings = Settings(local_data_dir=str(tmp_path / "data"), symbols=["EURUSD"], datasets=["bars"],
                        bar_timeframes=["1h"])
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    setup(settings, cloud)
    table = bq_table_id("market", "bars_1h")

    def load(day, hours, bad_hour=None, close=1.1):
        ts = pd.date_range(f"{day} 00:00", periods=24, freq="h", tz="UTC")[hours]
        df = pd.DataFrame({"ts": ts, "open": 1.1, "high": 1.2, "low": 1.0, "close": close, "volume": 1.0})
        if bad_hour is not None:
            df.loc[df["ts"].dt.hour == bad_hour, "high"] = 0.9  # high below low
        cloud.bq.replace_from_dataframe(shape(df, "bars", "EURUSD", day, ts[0], timeframe="1h", price_side="bid"),
                                        table, {"symbol": "EURUSD", "date": day})

    wed, thu = date(2026, 11, 4), date(2026, 11, 5)  # winter: open every hour but 22:00 UTC
    load(wed, [h for h in range(24) if h != 22])
    load(thu, list(range(10)))  # provisional: the intraday load's first 10 hours
    before = quality.provisional_snapshot(cloud, table, "EURUSD", thu)
    load(thu, [h for h in range(24) if h != 22], bad_hour=5, close=1.15)
    rows = {r["rule"]: r for r in quality.check_bars(cloud, table, "1h", "EURUSD", thu, provisional=before)}
    assert rows["ohlc"]["status"] == "fail" and "1 bad bars" in rows["ohlc"]["detail"]
    assert rows["hours"]["status"] == "ok" and rows["previous_day"]["status"] == "ok"
    assert rows["duplicates"]["status"] == rows["in_day"]["status"] == "ok"
    assert rows["provisional"]["detail"] == "of 10 provisional bars, 10 changed close, 0 gone"
    quality.write(cloud, "market", list(rows.values()))
    assert len(cloud.bq.query("SELECT * FROM market.quality")) == 6

    # Monday's previous trading day is Sunday (the session opens 23:00 UTC in winter), not Friday
    monday = date(2026, 11, 9)
    load(monday, [h for h in range(24) if h != 22])
    rows = {r["rule"]: r for r in quality.check_bars(cloud, table, "1h", "EURUSD", monday)}
    assert (rows["previous_day"]["status"], rows["previous_day"]["detail"]) == ("warn", "no bars on 2026-11-08")
