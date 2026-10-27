"""Inspect Cloud Storage / Pub/Sub / BigQuery. Works the same for DLH_ENV=local and gcp.

    python -m cloud status
    python -m cloud gcs ls gs://dlh-lake/dukascopy/
    python -m cloud gcs cat gs://dlh-lake/dukascopy/raw/EURUSD/2026-09-03/_manifest.json
    python -m cloud pubsub ls
    python -m cloud pubsub peek dukascopy-files-bq-loader
    python -m cloud bq ls [dataset]
    python -m cloud bq query "SELECT symbol, date, COUNT(*) AS n FROM market.dukascopy_ticks GROUP BY 1, 2"
    python -m cloud reset --yes          # local only: wipe the emulation folder
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys

import pandas as pd

from cloud import Cloud, NotFound


def _status(c: Cloud) -> None:
    print(f"Backend: {c.describe()}\n")
    print("Pub/Sub")
    topics = c.pubsub.list_topics()
    subs = c.pubsub.list_subscriptions()
    for t in topics:
        print(f"  topic {t}")
        for s in (s for s in subs if s["topic"] == t):
            extra = f"  backlog={s['backlog']} leased={s['leased']}" if "backlog" in s else ""
            print(f"    sub {s['name']}  ack_deadline={s['ack_deadline_s']}s{extra}")
    if not topics:
        print("  (no topics)")
    print("\nBigQuery")
    datasets = c.bq.list_datasets()
    for ds in datasets:
        for t in c.bq.list_tables(ds):
            n = c.bq.query(f"SELECT COUNT(*) AS n FROM `{ds}.{t}`")["n"].iloc[0]
            print(f"  {ds}.{t}  rows={n:,}")
    if not datasets:
        print("  (no datasets)")
    if c.is_local:
        root = c.storage.root  # type: ignore[attr-defined]
        print("\nCloud Storage")
        buckets = sorted(p.name for p in root.iterdir()) if root.is_dir() else []
        for b in buckets:
            print(f"  gs://{b}  objects={len(c.storage.list(f'gs://{b}/'))}")
        if not buckets:
            print("  (no buckets)")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m cloud", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="service", required=True)
    sub.add_parser("status", help="Overview of topics, subscriptions, tables (and local buckets)")

    gcs = sub.add_parser("gcs").add_subparsers(dest="cmd", required=True)
    gcs.add_parser("ls").add_argument("prefix", help="gs://bucket/prefix")
    gcs.add_parser("cat").add_argument("uri")

    ps = sub.add_parser("pubsub").add_subparsers(dest="cmd", required=True)
    ps.add_parser("ls")
    peek = ps.add_parser("peek", help="Show waiting messages without consuming them")
    peek.add_argument("subscription")
    peek.add_argument("-n", type=int, default=10)

    bq = sub.add_parser("bq").add_subparsers(dest="cmd", required=True)
    bq.add_parser("ls").add_argument("dataset", nargs="?")
    q = bq.add_parser("query")
    q.add_argument("sql")
    q.add_argument("--max-rows", type=int, default=50)

    reset = sub.add_parser("reset", help="Delete the local emulation folder (local only)")
    reset.add_argument("--yes", action="store_true")

    args = parser.parse_args(argv)
    c = Cloud.from_env()

    try:
        if args.service == "status":
            _status(c)
        elif args.service == "gcs" and args.cmd == "ls":
            for uri in c.storage.list(args.prefix):
                print(uri)
        elif args.service == "gcs" and args.cmd == "cat":
            data = c.storage.download_bytes(args.uri)
            if args.uri.endswith(".parquet"):
                import io
                print(pd.read_parquet(io.BytesIO(data)).to_string(max_rows=40))
            else:
                sys.stdout.buffer.write(data)
        elif args.service == "pubsub" and args.cmd == "ls":
            for s in c.pubsub.list_subscriptions():
                print(json.dumps(s))
        elif args.service == "pubsub" and args.cmd == "peek":
            msgs = c.pubsub.pull(args.subscription, args.n)
            c.pubsub.nack(args.subscription, [m.ack_id for m in msgs])  # put them straight back
            for m in msgs:
                print(json.dumps({"message_id": m.message_id, "attributes": m.attributes,
                                  "data": m.data.decode(errors="replace"), "delivery_attempt": m.delivery_attempt}))
            if not msgs:
                print("(no messages waiting)")
        elif args.service == "bq" and args.cmd == "ls":
            for ds in [args.dataset] if args.dataset else c.bq.list_datasets():
                for t in c.bq.list_tables(ds):
                    print(f"{ds}.{t}")
        elif args.service == "bq" and args.cmd == "query":
            print(c.bq.query(args.sql).to_string(max_rows=args.max_rows))
        elif args.service == "reset":
            if not c.is_local:
                parser.error("reset only works with DLH_ENV=local")
            if not args.yes:
                parser.error(f"this deletes {c.settings.local_dir}/; pass --yes to confirm")
            shutil.rmtree(c.settings.local_dir, ignore_errors=True)
            print(f"Deleted {c.settings.local_dir}/")
    except NotFound as exc:
        sys.exit(f"Not found: {exc}")


if __name__ == "__main__":
    main()
