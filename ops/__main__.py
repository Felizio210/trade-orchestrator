"""CLI.

    python -m ops status          # data, lake, Airflow, strategies, broker, kill switch, quality, drift
    python -m ops alert-test      # send a test message to every configured alert channel
"""

from __future__ import annotations

import argparse
import logging
import sys


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m ops")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", help="Is everything healthy? (exit code 1 when something needs attention)")
    sub.add_parser("alert-test", help="Send a test alert to every configured channel")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

    if args.cmd == "status":
        from ops.health import collect

        health = collect()
        print(health.render())
        sys.exit(0 if health.ok else 1)
    elif args.cmd == "alert-test":
        from ops.alerts import channels, notify

        if not channels():
            print("no alert channel configured: set ALERT_TELEGRAM_BOT_TOKEN + ALERT_TELEGRAM_CHAT_ID, "
                  "ALERT_WEBHOOK_URL, or ALERT_EMAIL_TO + ALERT_SMTP_HOST in .env")
            sys.exit(1)
        sent = notify("DLH test alert", "If you can read this, alerts work.")
        print(f"sent via {', '.join(sent) or 'nothing (see the errors above)'}")
        sys.exit(0 if sent else 1)


if __name__ == "__main__":
    main()
