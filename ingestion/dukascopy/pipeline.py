"""Cloud steps of the pipeline. Identical code for DLH_ENV=local and DLH_ENV=gcp.

    download (downloader.py)  Dukascopy -> local disk, verified, with _manifest.json
    publish                   local day -> GCS (CSV or Parquet) -> Pub/Sub message per file
    load                      Pub/Sub -> BigQuery (replaces that symbol/day; safe to re-run)
    backfill                  download + publish + load over a long range, resumable

GCS layout ({fmt} = csv | parquet, from DUKASCOPY_LAKE_FORMAT):
    gs://{bucket}/{prefix}/{fmt}/{dataset}/symbol={SYMBOL}/date={YYYY-MM-DD}/part-0.{fmt}
    gs://{bucket}/{prefix}/raw/{SYMBOL}/{YYYY-MM-DD}/{HH}h_ticks.bi5 (+ _manifest.json), only with upload_raw

Message (JSON body):
    {"uri", "dataset", "table", "symbol", "date", "rows"}
"""

from __future__ import annotations

import io
import json
import logging
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from cloud import Cloud
from ingestion.dukascopy.client import DukascopyClient
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.client import CANDLE_SIDES, decode_candles
from ingestion.dukascopy.converter import read_day_dir
from ingestion.dukascopy.downloader import candle_path, download_day, problem_summary, problems, raw_day_dir
from ingestion.dukascopy.instruments import point_factor
from ingestion.dukascopy.tables import CLUSTER_FIELDS, PARTITION_FIELD, bq_table_id, dataset_name, schema_for, shape
from ingestion.dukascopy.transform import candles_to_bars, resample_bars, ticks_to_bars

log = logging.getLogger(__name__)


class IncompleteDayError(RuntimeError):
    pass


def _dataset_names(settings: Settings) -> list[str]:
    names = []
    for kind in settings.datasets:
        names += [dataset_name(kind, tf) for tf in settings.bar_timeframes] if kind == "bars" else [kind]
    return names


def setup(settings: Settings, cloud: Cloud) -> list[str]:
    """Create bucket, topic, subscription, dataset and tables if they don't exist yet."""
    settings.validate()
    cloud.storage.ensure_bucket(settings.gcs_bucket)
    cloud.pubsub.ensure_topic(settings.pubsub_topic)
    # Loading a big tick file can take a while; give the loader time before redelivery
    cloud.pubsub.ensure_subscription(settings.pubsub_subscription, settings.pubsub_topic, ack_deadline_s=600)
    created = [f"gs://{settings.gcs_bucket}", f"topic {settings.pubsub_topic}",
               f"subscription {settings.pubsub_subscription}"]
    for ds in _dataset_names(settings):
        table_id = bq_table_id(settings.bq_dataset, ds)
        cloud.bq.ensure_dataset(table_id.split(".")[0])
        cloud.bq.ensure_table(table_id, schema_for(ds), PARTITION_FIELD, CLUSTER_FIELDS)
        created.append(f"table {table_id}")
    return created


def lake_uri(settings: Settings, dataset: str, symbol: str, day: date) -> str:
    fmt = settings.lake_format
    return (f"gs://{settings.gcs_bucket}/{settings.gcs_prefix}/{fmt}/{dataset}/"
            f"symbol={symbol}/date={day.isoformat()}/part-0.{fmt}")


def _to_parquet(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy",
                  coerce_timestamps="us", allow_truncated_timestamps=True)
    return buf.getvalue()


def _to_csv(df: pd.DataFrame) -> bytes:
    """CSV that both the local loader and BigQuery's CSV loader read back exactly."""
    out = df.copy()
    for col in out.columns:
        if isinstance(out[col].dtype, pd.DatetimeTZDtype):
            out[col] = out[col].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    return out.to_csv(index=False, float_format="%.10g").encode()


def publish_day(
    symbol: str,
    day: date,
    settings: Settings,
    cloud: Cloud,
    *,
    allow_partial: bool = False,
    upload_raw: bool | None = None,
) -> dict:
    """Upload one downloaded day to GCS and announce each file on Pub/Sub.

    Refuses days whose download had failed/corrupt hours (so BigQuery never gets a
    silently incomplete day) unless allow_partial=True. upload_raw defaults to settings.upload_raw.
    """
    settings.validate()
    if upload_raw is None:
        upload_raw = settings.upload_raw
    symbol = symbol.upper()
    day_dir = raw_day_dir(settings.raw_dir, symbol, day)
    manifest_path = day_dir / "_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"{manifest_path} not found; run `download` for {symbol} {day} first")
    manifest = json.loads(manifest_path.read_text())
    if problems(manifest) and not allow_partial:
        raise IncompleteDayError(
            f"{symbol} {day} has {problem_summary(manifest)}; re-run download, "
            "or publish with allow_partial"
        )

    summary: dict = {"symbol": symbol, "date": day.isoformat(), "published": []}
    if upload_raw:
        raw_prefix = f"gs://{settings.gcs_bucket}/{settings.gcs_prefix}/raw/{symbol}/{day.isoformat()}"
        for f in sorted(day_dir.iterdir()):
            if f.name.endswith(".bi5") or f.name == "_manifest.json":  # ticks or candle files
                cloud.storage.upload_file(f"{raw_prefix}/{f.name}", str(f))
        summary["raw_prefix"] = raw_prefix

    ingested_at = datetime.now(timezone.utc)
    frames: dict[str, pd.DataFrame] = {}
    if settings.bar_source == "candles":
        bars = _read_candle_bars(symbol, day, settings)
        if bars.empty:
            log.info("%s %s: no candles with volume (market closed), nothing to publish", symbol, day)
            return summary
    else:
        ticks = read_day_dir(day_dir, settings.point_overrides)
        if ticks.empty:
            log.info("%s %s: no ticks (market closed), nothing to publish", symbol, day)
            return summary
        if "ticks" in settings.datasets:
            frames["ticks"] = shape(ticks, "ticks", symbol, day, ingested_at)
        bars = None
    if "bars" in settings.datasets:
        base = bars if bars is not None else ticks_to_bars(ticks, "1min", settings.bar_price_side)
        for tf in settings.bar_timeframes:
            # Coarser bars are built from the 1min bars for candles, straight from ticks otherwise
            tf_bars = resample_bars(base, tf) if bars is not None else ticks_to_bars(ticks, tf, settings.bar_price_side)
            frames[dataset_name("bars", tf)] = shape(
                tf_bars, "bars", symbol, day, ingested_at, timeframe=tf, price_side=settings.bar_price_side,
            )

    for dataset, df in frames.items():
        uri = lake_uri(settings, dataset, symbol, day)
        if settings.lake_format == "csv":
            cloud.storage.upload_bytes(uri, _to_csv(df), "text/csv")
        else:
            cloud.storage.upload_bytes(uri, _to_parquet(df), "application/vnd.apache.parquet")
        message = {"uri": uri, "dataset": dataset, "table": bq_table_id(settings.bq_dataset, dataset),
                   "symbol": symbol, "date": day.isoformat(), "rows": len(df)}
        message_id = cloud.pubsub.publish(
            settings.pubsub_topic, json.dumps(message).encode(),
            dataset=dataset, symbol=symbol, date=day.isoformat(),
        )
        summary["published"].append({**message, "message_id": message_id})
    log.info("%s %s: published %s", symbol, day, [p["dataset"] for p in summary["published"]])
    return summary


def _read_candle_bars(symbol: str, day: date, settings: Settings) -> pd.DataFrame:
    factor = point_factor(symbol, settings.point_overrides)
    sides = {}
    for side in CANDLE_SIDES:
        path = candle_path(settings.raw_dir, symbol, day, side)
        sides[side] = decode_candles(path.read_bytes(), day, factor) if path.exists() else pd.DataFrame()
    return candles_to_bars(sides["BID"], sides["ASK"], settings.bar_price_side)


def load_message(payload: dict, cloud: Cloud) -> int:
    """Load one published file into BigQuery, replacing that symbol/day."""
    match = {"symbol": payload["symbol"], "date": date.fromisoformat(payload["date"])}
    rows = cloud.bq.replace_from_uri(payload["uri"], payload["table"], match)
    if "rows" in payload and rows != payload["rows"]:
        raise RuntimeError(f"Loaded {rows} rows from {payload['uri']}, expected {payload['rows']}")
    return rows


def load_pending(settings: Settings, cloud: Cloud, *, max_messages: int | None = None) -> dict:
    """Drain the loader subscription into BigQuery.

    A message is acked only after its load succeeds. Failed messages are left un-acked,
    so Pub/Sub redelivers them after the ack deadline (i.e. on a later run); they are
    not retried in this run.
    """
    loaded, failed = [], []
    empty_pulls = 0
    while max_messages is None or len(loaded) + len(failed) < max_messages:
        batch = cloud.pubsub.pull(settings.pubsub_subscription, max_messages=10)
        if not batch:
            empty_pulls += 1
            # Real Pub/Sub can return an empty pull while messages exist; try once more
            if cloud.is_local or empty_pulls >= 2:
                break
            continue
        empty_pulls = 0
        for msg in batch:
            try:
                payload = json.loads(msg.data)
                rows = load_message(payload, cloud)
            except Exception as exc:
                log.exception("Failed to load message %s", msg.message_id)
                failed.append({"message_id": msg.message_id, "error": f"{type(exc).__name__}: {exc}",
                               "delivery_attempt": msg.delivery_attempt})
                continue
            cloud.pubsub.ack(settings.pubsub_subscription, [msg.ack_id])
            loaded.append({"table": payload["table"], "symbol": payload["symbol"], "date": payload["date"],
                           "rows": rows})
            log.info("Loaded %s rows into %s (%s %s)", rows, payload["table"], payload["symbol"], payload["date"])
    return {"loaded": loaded, "failed": failed}


def run_range(
    symbols: list[str],
    start: date,
    end: date,
    settings: Settings,
    cloud: Cloud,
    *,
    load: bool = True,
    allow_partial: bool = False,
) -> dict:
    """download -> publish (-> load) for every symbol/day. Incomplete days are skipped, not published."""
    published, skipped = [], []
    day = start
    while day <= end:
        for symbol in symbols:
            manifest = download_day(symbol, day, settings)
            if problems(manifest) and not allow_partial:
                skipped.append({"symbol": symbol, "date": day.isoformat(), "problems": problems(manifest),
                                "summary": problem_summary(manifest)})
                continue
            published.append(publish_day(symbol, day, settings, cloud, allow_partial=allow_partial))
        day += timedelta(days=1)
    result = {"published": published, "skipped": skipped}
    if load:
        result.update(load_pending(settings, cloud))
    return result


def backfill_range(
    symbols: list[str],
    start: date,
    end: date,
    settings: Settings,
    cloud: Cloud,
    *,
    force: bool = False,
    client: DukascopyClient | None = None,
    on_month_done=None,
) -> dict:
    """download -> publish -> load for a long range; re-run the same call to resume.

    Skips Saturdays (FX and metals never trade, and empty hours are never cached, so they
    would be re-requested on every run) and days whose lake file already exists unless
    force=True. Incomplete days are skipped, not published, so a re-run retries just those.
    Loads after every month, so BigQuery fills up as it goes.
    """
    settings.validate()
    client = client or DukascopyClient(settings)
    datasets = _dataset_names(settings)
    result: dict = {"published": [], "skipped": [], "existing": 0, "loaded": [], "failed": []}

    def flush(month: str) -> None:
        loaded = load_pending(settings, cloud)
        result["loaded"] += loaded["loaded"]
        result["failed"] += loaded["failed"]
        if on_month_done:
            on_month_done(month, result)

    day, month = start, start.strftime("%Y-%m")
    while day <= end:
        if day.strftime("%Y-%m") != month:
            flush(month)
            month = day.strftime("%Y-%m")
        if day.weekday() != 5:
            for symbol in (s.upper() for s in symbols):
                if not force and all(cloud.storage.exists(lake_uri(settings, ds, symbol, day)) for ds in datasets):
                    result["existing"] += 1
                    continue
                manifest = download_day(symbol, day, settings, client=client)
                if problems(manifest):
                    result["skipped"].append({"symbol": symbol, "date": day.isoformat(),
                                              "problems": problems(manifest),
                                              "summary": problem_summary(manifest)})
                    continue
                result["published"].append(publish_day(symbol, day, settings, cloud))
        day += timedelta(days=1)
    flush(month)
    return result
