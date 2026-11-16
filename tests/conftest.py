"""Tests run the same with or without a .env: every project setting it may have loaded into the
environment is removed before each test, so only what a test sets itself applies (as in CI)."""

import os

import pytest

PREFIXES = ("DLH_", "DUKASCOPY_", "BACKTEST_", "TRADING_", "ALERT_", "GCP_", "GCS_", "BQ_", "PUBSUB_",
            "LOCAL_GCP_DIR", "LOCAL_DATA_DIR")


@pytest.fixture(autouse=True)
def _no_project_env(monkeypatch):
    for name in list(os.environ):
        if name.startswith(PREFIXES):
            monkeypatch.delenv(name)
