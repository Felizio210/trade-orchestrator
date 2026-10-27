from datetime import date, datetime, timezone

import pandas as pd
import pytest

from cloud import Cloud, CloudSettings, Field, NotFound
from cloud.gcp.bigquery import query_parameter
from cloud.local.bigquery import to_sqlite_sql


@pytest.fixture
def cloud(tmp_path):
    return Cloud(CloudSettings(env="local", local_dir=str(tmp_path / ".local_gcp")))


# --- config -----------------------------------------------------------------

def test_gcp_env_requires_project():
    with pytest.raises(ValueError, match="GCP_PROJECT"):
        Cloud(CloudSettings(env="gcp"))
    with pytest.raises(ValueError, match="DLH_ENV"):
        Cloud(CloudSettings(env="aws"))


# --- storage ----------------------------------------------------------------

def test_storage_roundtrip(cloud):
    s = cloud.storage
    with pytest.raises(NotFound, match="setup"):
        s.upload_bytes("gs://lake/a.txt", b"x")
    s.ensure_bucket("lake")
    s.upload_bytes("gs://lake/dir/a.txt", b"hello")
    s.upload_bytes("gs://lake/dir/b.txt", b"world")
    s.upload_bytes("gs://lake/other.txt", b"!")
    assert s.download_bytes("gs://lake/dir/a.txt") == b"hello"
    assert s.list("gs://lake/dir/") == ["gs://lake/dir/a.txt", "gs://lake/dir/b.txt"]
    assert s.exists("gs://lake/other.txt") and not s.exists("gs://lake/nope.txt")
    s.delete("gs://lake/dir/a.txt")
    with pytest.raises(NotFound):
        s.download_bytes("gs://lake/dir/a.txt")


def test_storage_rejects_path_escape(cloud):
    cloud.storage.ensure_bucket("lake")
    with pytest.raises(ValueError, match="escapes"):
        cloud.storage.upload_bytes("gs://lake/../evil.txt", b"x")


# --- pub/sub ----------------------------------------------------------------

def test_pubsub_fanout_ack_and_redelivery(cloud, monkeypatch):
    ps = cloud.pubsub
    ps.ensure_topic("t")
    ps.publish("t", b"before-subs")  # no subscriptions yet -> dropped, like GCP
    ps.ensure_subscription("a", "t", ack_deadline_s=30)
    ps.ensure_subscription("b", "t", ack_deadline_s=30)
    ps.publish("t", b"m1", kind="x")
    ps.publish("t", b"m2")

    [m1, m2] = ps.pull("a")
    assert (m1.data, m1.attributes, m1.delivery_attempt) == (b"m1", {"kind": "x"}, 1)
    assert ps.pull("a") == []  # leased
    assert [m.data for m in ps.pull("b")] == [b"m1", b"m2"]  # independent copy

    ps.ack("a", [m1.ack_id])
    ps.nack("a", [m2.ack_id])
    [again] = ps.pull("a")
    assert again.data == b"m2" and again.delivery_attempt == 2

    # Lease expiry -> redelivered with a new ack id; the old ack id no longer works
    import cloud.local.pubsub as mod
    real_time = mod.time.time
    monkeypatch.setattr(mod.time, "time", lambda: real_time() + 31)
    [third] = ps.pull("a")
    assert third.delivery_attempt == 3 and third.ack_id != again.ack_id
    ps.ack("a", [again.ack_id])  # stale: ignored
    assert [s["backlog"] for s in ps.list_subscriptions() if s["name"] == "a"] == [1]
    ps.ack("a", [third.ack_id])
    assert [s["backlog"] for s in ps.list_subscriptions() if s["name"] == "a"] == [0]


def test_pubsub_unknown_names(cloud):
    with pytest.raises(NotFound):
        cloud.pubsub.publish("missing", b"x")
    with pytest.raises(NotFound):
        cloud.pubsub.pull("missing")


# --- bigquery ---------------------------------------------------------------

SCHEMA = [Field("ts", "TIMESTAMP", "REQUIRED"), Field("date", "DATE", "REQUIRED"),
          Field("symbol", "STRING", "REQUIRED"), Field("price", "FLOAT64"), Field("n", "INT64")]


def _frame(symbol, day, prices):
    ts = pd.date_range(f"{day} 10:00", periods=len(prices), freq="min", tz="UTC")
    return pd.DataFrame({"ts": ts, "date": date.fromisoformat(day), "symbol": symbol,
                         "price": prices, "n": range(len(prices))})


def test_bq_replace_is_idempotent_per_match(cloud):
    bq = cloud.bq
    with pytest.raises(NotFound):
        bq.ensure_table("market.t", SCHEMA)
    bq.ensure_dataset("market")
    bq.ensure_table("market.t", SCHEMA, partition_field="date", cluster_fields=["symbol"])

    d = date(2026, 9, 3)
    assert bq.replace_from_dataframe(_frame("EURUSD", "2026-09-03", [1.1, 1.2]), "market.t",
                                     {"symbol": "EURUSD", "date": d}) == 2
    bq.replace_from_dataframe(_frame("XAUUSD", "2026-09-03", [2500.0]), "market.t", {"symbol": "XAUUSD", "date": d})
    # Reload EURUSD with different data: replaces only EURUSD's rows
    bq.replace_from_dataframe(_frame("EURUSD", "2026-09-03", [1.3, 1.4, 1.5]), "market.t",
                              {"symbol": "EURUSD", "date": d})

    counts = bq.query("SELECT symbol, COUNT(*) AS n FROM market.t GROUP BY symbol ORDER BY symbol")
    assert counts.to_dict("records") == [{"symbol": "EURUSD", "n": 3}, {"symbol": "XAUUSD", "n": 1}]
    row = bq.query("SELECT ts, date, price FROM `my-project.market.t` WHERE symbol = @s AND date = @d "
                   "ORDER BY ts LIMIT 1", {"s": "EURUSD", "d": d}).iloc[0]
    assert (row.ts, row.date, row.price) == ("2026-09-03 10:00:00.000000", "2026-09-03", 1.3)
    assert bq.list_datasets() == ["market"] and bq.list_tables("market") == ["t"]


def test_bq_timestamp_params_and_schema_evolution(cloud):
    bq = cloud.bq
    bq.ensure_dataset("market")
    bq.ensure_table("market.t", SCHEMA)
    bq.replace_from_dataframe(_frame("EURUSD", "2026-09-03", [1.1, 1.2, 1.3]), "market.t", {"symbol": "EURUSD"})
    since = datetime(2026, 9, 3, 10, 1, tzinfo=timezone.utc)
    assert bq.query("SELECT COUNT(*) AS n FROM market.t WHERE ts >= @since", {"since": since}).n[0] == 2

    bq.ensure_table("market.t", [*SCHEMA, Field("note", "STRING")])
    assert "note" in bq.query("SELECT * FROM market.t LIMIT 1").columns


def test_bq_rejects_bad_rows(cloud):
    bq = cloud.bq
    bq.ensure_dataset("market")
    bq.ensure_table("market.t", SCHEMA)
    bad = _frame("EURUSD", "2026-09-03", [1.1]).drop(columns=["symbol"])
    with pytest.raises(ValueError, match="symbol"):
        bq.replace_from_dataframe(bad, "market.t", {"date": date(2026, 9, 3)})
    with pytest.raises(ValueError, match="not in"):
        bq.replace_from_dataframe(_frame("EURUSD", "2026-09-03", [1.1]), "market.t", {"nope": 1})


def test_bq_load_from_storage_uri(cloud):
    cloud.storage.ensure_bucket("lake")
    cloud.bq.ensure_dataset("market")
    cloud.bq.ensure_table("market.t", SCHEMA)
    import io
    buf = io.BytesIO()
    _frame("EURUSD", "2026-09-03", [1.1, 1.2]).to_parquet(buf, index=False)
    cloud.storage.upload_bytes("gs://lake/x.parquet", buf.getvalue())
    assert cloud.bq.replace_from_uri("gs://lake/x.parquet", "market.t", {"symbol": "EURUSD"}) == 2


def test_sql_translation():
    assert to_sqlite_sql("SELECT * FROM `proj.market.t` WHERE a = @a AND b = '@x `y`' AND c=@c") == \
        "SELECT * FROM market.t WHERE a = :a AND b = '@x `y`' AND c=:c"
    assert to_sqlite_sql("SELECT 'it''s @x', @y FROM `market.t`") == "SELECT 'it''s @x', :y FROM market.t"


def test_gcp_query_parameter_types():
    assert query_parameter("a", True).type_ == "BOOL"
    assert query_parameter("a", 3).type_ == "INT64"
    assert query_parameter("a", 1.5).type_ == "FLOAT64"
    assert query_parameter("a", date(2026, 9, 3)).type_ == "DATE"
    assert query_parameter("a", datetime(2026, 9, 3, tzinfo=timezone.utc)).type_ == "TIMESTAMP"
    assert query_parameter("a", "EURUSD").type_ == "STRING"
