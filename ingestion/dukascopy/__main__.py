"""CLI.

    # 1. Pull raw .bi5 to ./data/raw and verify (add --csv to convert right away)
    python -m ingestion.dukascopy download --start 2026-09-01 --end 2026-09-05 --symbols EURUSD,XAUUSD

    # 2. Convert .bi5 -> CSV (a file or a folder)
    python -m ingestion.dukascopy convert data/raw/EURUSD --bars 1min

    # Can this machine reach Dukascopy at all?
    python -m ingestion.dukascopy check

    # Cloud steps (DLH_ENV=local -> emulation in .local_gcp/, DLH_ENV=gcp -> real GCP)
    python -m ingestion.dukascopy setup                       # bucket, topic, subscription, tables
    python -m ingestion.dukascopy publish --start 2026-09-03  # local day -> GCS + Pub/Sub
    python -m ingestion.dukascopy load                        # Pub/Sub -> BigQuery
    python -m ingestion.dukascopy run --start 2026-09-03      # download + publish + load

    # History: resumable, skips Saturdays and days already in the lake; re-run to retry gaps
    python -m ingestion.dukascopy backfill --start 2024-10-16 --end 2026-10-15
"""

from __future__ import annotations

import argparse
import logging
import socket
import sys
from datetime import date, datetime, timedelta, timezone

import requests

from cloud import Cloud
from ingestion.dukascopy.config import Settings, _csv
from ingestion.dukascopy.converter import convert
from ingestion.dukascopy.client import DukascopyClient, decode_ticks, hour_url
from ingestion.dukascopy.downloader import download_range, problems, raw_day_dir
from ingestion.dukascopy.instruments import point_factor
from ingestion.dukascopy.pipeline import backfill_range, load_pending, publish_day, run_range, setup


def _add_range_args(p: argparse.ArgumentParser) -> None:
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    p.add_argument("--start", type=date.fromisoformat, default=yesterday, help="UTC date, default yesterday")
    p.add_argument("--end", type=date.fromisoformat, help="UTC date inclusive, default = --start")
    p.add_argument("--symbols", help="Comma-separated, overrides DUKASCOPY_SYMBOLS")


def _print_manifest_table(manifests: list[dict]) -> bool:
    """Print a per-day summary. Returns True if every hour downloaded cleanly."""
    print(f"\n{'SYMBOL':<10} {'DATE':<11} {'HOURS':>5} {'FAILED':>6} {'TICKS':>10}  "
          f"{'FIRST BID':>12} {'LAST BID':>12}  DIR")
    clean = True
    for m in manifests:
        ok = [h for h in m["hours"] if h["status"] == "ok"]
        bad = m["hours_failed"] + m["hours_corrupt"]
        clean &= bad == 0
        if m.get("bar_source") == "candles":  # one BID + one ASK file; TICKS column shows bars
            first = f"{ok[0]['first_open']:.6g}" if ok else "-"
            last = f"{ok[0]['last_close']:.6g}" if ok else "-"
            got, count = f"{m['files_ok']:>2}/2 ", m["bars"]
        else:
            first = f"{ok[0]['first_bid']:.6g}" if ok else "-"
            last = f"{ok[-1]['last_bid']:.6g}" if ok else "-"
            got, count = f"{m['hours_ok']:>2}/24", m["total_ticks"]
        print(f"{m['symbol']:<10} {m['date']:<11} {got} {bad:>6} {count:>10,}  "
              f"{first:>12} {last:>12}  {m['dir']}")
    for m in manifests:
        for line in problems(m):
            print(f"  ! {m['symbol']} {m['date']} {line}")
    if not clean:
        print("\nSome hours failed. Re-run the same command to retry only the missing hours.")
    return clean


def _check(settings: Settings) -> bool:
    """One-request connectivity check with a plain-English diagnosis."""
    host = settings.base_url.split("/")[2]
    try:
        ips = sorted({a[4][0] for a in socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)})
        print(f"DNS      {host} -> {', '.join(ips)}")
    except OSError as exc:
        print(f"DNS      FAILED: {exc}\n=> Can't resolve {host}. Check your internet/DNS.")
        return False

    # A recent weekday 10:00 UTC hour, which always has EURUSD ticks
    day = datetime.now(timezone.utc).date() - timedelta(days=3)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    hour = datetime(day.year, day.month, day.day, 10, tzinfo=timezone.utc)
    url = hour_url(settings.base_url, "EURUSD", hour)
    print(f"GET      {url}")
    try:
        resp = DukascopyClient(settings).session.get(url, timeout=settings.timeout_s)
    except requests.exceptions.SSLError as exc:
        print(f"TLS      FAILED: {exc}\n=> Something between you and Dukascopy is intercepting HTTPS "
              "(ISP filter, firewall, or proxy). Try another network, a VPN, or run from GCP.")
        return False
    except requests.RequestException as exc:
        print(f"CONNECT  FAILED: {exc}\n=> Connection refused/timed out. Dukascopy is likely blocked on this "
              "network (some ISPs block forex sites). Try another network, a VPN, or run from GCP.")
        return False
    print(f"HTTP     {resp.status_code} ({len(resp.content)} bytes)")
    if resp.status_code == 429:
        print(f"=> Reachable, but rate limited: {resp.text[:200]}\n=> Wait a few minutes before pulling "
              "again, and keep DUKASCOPY_MAX_WORKERS low.")
        return False
    if resp.status_code != 200:
        print(f"=> Unexpected response: {resp.text[:200]}")
        return False
    try:
        ticks = decode_ticks(resp.content, hour, point_factor("EURUSD"))
    except Exception as exc:
        print(f"DECODE   FAILED: {exc}\n=> Got a response, but it isn't a .bi5 file (probably a block page).")
        return False
    print(f"DECODE   {len(ticks):,} ticks, EURUSD bid {ticks['bid'].iloc[0]:.5f} -> {ticks['bid'].iloc[-1]:.5f}")
    print("=> OK: Dukascopy datafeed is reachable from this machine.")
    return True


def _print_cloud_result(result: dict) -> None:
    for p in result.get("published", []):
        for f in p["published"]:
            print(f"published  {f['symbol']} {f['date']} {f['dataset']:<10} {f['rows']:>9,} rows  {f['uri']}")
        if not p["published"]:
            print(f"published  {p['symbol']} {p['date']} (no ticks, market closed)")
    for s in result.get("skipped", []):
        print(f"SKIPPED    {s['symbol']} {s['date']}: {s.get('reason') or s.get('problems')}")
    for item in result.get("loaded", []):
        print(f"loaded     {item['symbol']} {item['date']} {item['rows']:>9,} rows -> {item['table']}")
    for item in result.get("failed", []):
        print(f"FAILED     message {item['message_id']}: {item['error']}")
    if "loaded" in result and not result["loaded"] and not result["failed"]:
        print("(nothing waiting to load)")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m ingestion.dukascopy")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_dl = sub.add_parser("download", help="Pull raw .bi5 files to local disk and verify them")
    _add_range_args(p_dl)
    p_dl.add_argument("--overwrite", action="store_true", help="Re-download files already on disk")
    p_dl.add_argument("--csv", action="store_true", help="Also convert each downloaded day to CSV")

    p_cv = sub.add_parser("convert", help="Convert a .bi5 file or folder to CSV")
    p_cv.add_argument("path", help=".bi5 file or directory containing them")
    p_cv.add_argument("--out", help="Output directory, default {LOCAL_DATA_DIR}/csv")
    p_cv.add_argument("--symbol", help="Symbol, if it can't be read from the path")
    p_cv.add_argument("--hour", type=datetime.fromisoformat,
                      help="Hour start in UTC (e.g. 2026-09-03T10), if it can't be read from the path")
    p_cv.add_argument("--bars", metavar="TIMEFRAME", help="Also write OHLCV bars, e.g. 1min, 5min, 1h")

    sub.add_parser("check", help="Test whether this machine can reach the Dukascopy datafeed")

    sub.add_parser("setup", help="Create bucket, Pub/Sub topic + subscription, BigQuery dataset + tables")

    p_pub = sub.add_parser("publish", help="Upload downloaded days to GCS and announce them on Pub/Sub")
    _add_range_args(p_pub)
    p_pub.add_argument("--allow-partial", action="store_true", help="Publish days with failed hours anyway")
    raw = p_pub.add_mutually_exclusive_group()
    raw.add_argument("--raw", dest="raw", action="store_true", default=None,
                     help="Also upload the raw .bi5 files (default: DUKASCOPY_UPLOAD_RAW)")
    raw.add_argument("--no-raw", dest="raw", action="store_false", help="Don't upload the raw .bi5 files")

    p_load = sub.add_parser("load", help="Load pending Pub/Sub messages into BigQuery")
    p_load.add_argument("--max", type=int, help="Stop after this many messages")

    p_run = sub.add_parser("run", help="download + publish + load")
    _add_range_args(p_run)
    p_run.add_argument("--allow-partial", action="store_true")
    p_run.add_argument("--no-load", action="store_true", help="Stop after publishing")

    p_bf = sub.add_parser("backfill", help="Resumable download + publish + load over a long range")
    _add_range_args(p_bf)
    p_bf.add_argument("--force", action="store_true", help="Redo days already in the lake")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()

    if args.cmd == "download":
        symbols = _csv(args.symbols) if args.symbols else settings.symbols
        manifests = download_range(symbols, args.start, args.end or args.start, settings, overwrite=args.overwrite)
        clean = _print_manifest_table(manifests)
        if args.csv:
            for m in manifests:
                if m.get("hours_ok"):  # tick downloads only
                    day_dir = raw_day_dir(settings.raw_dir, m["symbol"], date.fromisoformat(m["date"]))
                    for tf in settings.bar_timeframes:
                        for f in convert(day_dir, settings.csv_dir, bars_timeframe=tf,
                                         bar_price_side=settings.bar_price_side,
                                         point_overrides=settings.point_overrides):
                            print(f"wrote {f}")
        if not clean:
            sys.exit(1)

    elif args.cmd == "check":
        sys.exit(0 if _check(settings) else 1)

    elif args.cmd == "convert":
        written = convert(args.path, args.out or settings.csv_dir, symbol=args.symbol, hour=args.hour,
                          bars_timeframe=args.bars, bar_price_side=settings.bar_price_side,
                          point_overrides=settings.point_overrides)
        for f in written:
            print(f"wrote {f}")

    elif args.cmd == "backfill":
        cloud = Cloud.from_env()
        print(f"Cloud: {cloud.describe()}")
        symbols = _csv(args.symbols) if args.symbols else settings.symbols
        end = args.end or args.start

        def progress(month: str, r: dict) -> None:
            print(f"{month}  published={len(r['published'])} already_there={r['existing']} "
                  f"incomplete={len(r['skipped'])} loaded={len(r['loaded'])} failed_loads={len(r['failed'])}",
                  flush=True)

        result = backfill_range(symbols, args.start, end, settings, cloud, force=args.force,
                                on_month_done=progress)
        for s in result["skipped"]:
            print(f"INCOMPLETE {s['symbol']} {s['date']}: {s['summary']}")
        for item in result["failed"]:
            print(f"FAILED     message {item['message_id']}: {item['error']}")
        if result["skipped"] or result["failed"]:
            print("\nRe-run the same command to retry only what's missing.")
            sys.exit(1)
        print("\nDone: every trading day in the range is in the lake and BigQuery.")

    elif args.cmd in ("setup", "publish", "load", "run"):
        cloud = Cloud.from_env()
        print(f"Cloud: {cloud.describe()}")
        if args.cmd == "setup":
            for item in setup(settings, cloud):
                print(f"ready  {item}")
            return
        if args.cmd == "load":
            result = load_pending(settings, cloud, max_messages=args.max)
        else:
            symbols = _csv(args.symbols) if args.symbols else settings.symbols
            start, end = args.start, args.end or args.start
            if args.cmd == "run":
                result = run_range(symbols, start, end, settings, cloud,
                                   load=not args.no_load, allow_partial=args.allow_partial)
            else:
                result = {"published": [], "skipped": []}
                day = start
                while day <= end:
                    for symbol in symbols:
                        try:
                            result["published"].append(publish_day(
                                symbol, day, settings, cloud,
                                allow_partial=args.allow_partial, upload_raw=args.raw))
                        except (FileNotFoundError, RuntimeError) as exc:
                            result["skipped"].append({"symbol": symbol, "date": day.isoformat(), "reason": str(exc)})
                    day += timedelta(days=1)
        _print_cloud_result(result)
        if result.get("failed") or result.get("skipped"):
            sys.exit(1)

if __name__ == "__main__":
    main()
