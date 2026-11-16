"""Alerts and the health view. No network: the senders are replaced."""

from datetime import date, datetime, timezone

import pandas as pd
import pytest

from cloud import Cloud, CloudSettings
from ingestion.dukascopy import market_hours
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.pipeline import setup as market_setup
from ingestion.dukascopy.tables import bq_table_id, shape
from ops import alerts
from trading import store
from trading.config import TradingSettings
from trading.gate import setup as trading_setup


@pytest.fixture
def channels(monkeypatch, tmp_path):
    for name in ("ALERT_TELEGRAM_BOT_TOKEN", "ALERT_TELEGRAM_CHAT_ID", "ALERT_WEBHOOK_URL", "ALERT_EMAIL_TO",
                 "ALERT_SMTP_HOST"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("LOCAL_DATA_DIR", str(tmp_path))
    sent = []
    monkeypatch.setattr(alerts, "_send", lambda channel, subject, text: sent.append((channel, subject)))
    return sent


def test_alerts_go_to_every_configured_channel_and_repeats_are_throttled(channels, monkeypatch):
    assert alerts.notify("nothing configured") == [] and channels == []
    monkeypatch.setenv("ALERT_TELEGRAM_BOT_TOKEN", "t")
    monkeypatch.setenv("ALERT_TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://hook.test")
    assert alerts.notify("a", key="dag.task") == ["telegram", "webhook"]
    assert alerts.notify("a again", key="dag.task") == []  # within ALERT_REPEAT_MINUTES
    assert alerts.notify("other", key="dag.other") == ["telegram", "webhook"]
    monkeypatch.setenv("ALERT_REPEAT_MINUTES", "0")
    assert alerts.notify("a third time", key="dag.task") == ["telegram", "webhook"]


def test_a_failing_channel_never_breaks_the_caller(channels, monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://hook.test")

    def boom(channel, subject, text):
        raise OSError("down")

    monkeypatch.setattr(alerts, "_send", boom)
    assert alerts.notify("x") == []


def test_airflow_failure_callback_names_the_task(channels, monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://hook.test")

    class TI:
        dag_id, task_id, try_number, log_url = "dukascopy_hourly", "intraday", 4, "http://x"

    alerts.airflow_failure({"task_instance": TI(), "run_id": "scheduled__1", "exception": RuntimeError("boom")})
    assert channels == [("webhook", "DLH failed: dukascopy_hourly.intraday")]


def test_early_closes_match_dukascopy():
    utc = timezone.utc
    ny = market_hours.NEW_YORK
    # Thanksgiving 2025 (Thu 27 Nov) and the Friday after: no ticks from 15:00 New York
    assert market_hours.is_open(datetime(2025, 11, 27, 14, tzinfo=ny))
    assert not market_hours.is_open(datetime(2025, 11, 27, 15, tzinfo=ny))
    assert not market_hours.is_open(datetime(2025, 11, 28, 16, tzinfo=ny))
    # July 3rd 2026, Independence Day observed on a Friday: from 13:00
    assert market_hours.is_open(datetime(2026, 7, 3, 12, tzinfo=ny))
    assert not market_hours.is_open(datetime(2026, 7, 3, 13, tzinfo=ny))
    # Christmas Eve: from 14:00; the evening session after it is Christmas Day's, which doesn't trade
    assert not market_hours.is_open(datetime(2025, 12, 24, 14, tzinfo=ny))
    assert not market_hours.is_open(datetime(2025, 12, 24, 19, tzinfo=ny))
    # The evening session after an early close is a normal one
    assert market_hours.is_open(datetime(2025, 11, 27, 18, tzinfo=ny))
    assert market_hours.open_hours(date(2026, 4, 3)) == 0  # Good Friday
    assert market_hours.open_hours(date(2026, 11, 4)) == 23  # an ordinary winter Wednesday
    assert market_hours.is_open(datetime(2026, 11, 4, 23, tzinfo=utc))


def test_health_flags_stale_data_old_approvals_and_the_kill_switch(tmp_path, monkeypatch):
    from backtest.config import BacktestSettings
    from ops.health import collect

    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "no-airflow"))
    cloud = Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))
    market = Settings(local_data_dir=str(tmp_path / "data"), symbols=["EURUSD"], datasets=["bars"],
                      bar_timeframes=["1h"], gcs_bucket="b")
    market_setup(market, cloud)
    trading = TradingSettings()
    trading_setup(trading, cloud)
    ts = pd.date_range("2026-11-04 00:00", periods=10, freq="h", tz="UTC")
    df = pd.DataFrame({"ts": ts, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})
    cloud.bq.replace_from_dataframe(shape(df, "bars", "EURUSD", date(2026, 11, 4), ts[0], timeframe="1h",
                                          price_side="bid"), bq_table_id("market", "bars_1h"),
                                    {"symbol": "EURUSD", "date": date(2026, 11, 4)})
    now = pd.Timestamp("2026-11-04 12:30", tz="UTC")
    store.write(cloud, "trading", "approvals", [{
        "strategy": "breakout", "timeframe": "1h", "symbol": "EURUSD", "status": "approved", "run_id": "daily-x",
        "params_json": "{}", "decided_at": now - pd.Timedelta(days=5)}])
    store.write(cloud, "trading", "controls", [{"key": "kill_switch", "value": "by hand", "updated_at": now}])

    h = collect(now, cloud=cloud, market=market, trading=trading, backtest=BacktestSettings(), airflow=False)
    text = h.render()
    assert text.startswith("DLH health 2026-11-04 12:30 UTC: ATTENTION")
    assert "EURUSD 1h: newest bar 2026-11-04 09:00, 2 market hours behind" in text  # 10h and 11h
    assert any("decided 5.0 days ago" in p for p in h.problems)
    assert any("kill switch: ON: by hand" in p for p in h.problems)
    assert "lake" in h.sections
