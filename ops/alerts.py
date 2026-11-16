"""Send an alert to whichever channels are configured; with none configured it is only logged.

    ALERT_TELEGRAM_BOT_TOKEN + ALERT_TELEGRAM_CHAT_ID   Telegram bot message
    ALERT_WEBHOOK_URL                                   POST {"text": ...} (Slack incoming webhooks,
                                                        Discord /slack endpoints, Mattermost, ...)
    ALERT_EMAIL_TO + ALERT_SMTP_HOST (+ _PORT, _USER, _PASSWORD, ALERT_EMAIL_FROM)   email

A key (e.g. "dukascopy_hourly.intraday") throttles repeats: the same key alerts at most once
per ALERT_REPEAT_MINUTES (default 60). Throttle state is a small JSON file in LOCAL_DATA_DIR.
Sending never raises: an alert that can't be delivered is logged instead.
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import time
import urllib.parse
import urllib.request
from email.message import EmailMessage
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

log = logging.getLogger(__name__)
MAX_CHARS = 3500  # Telegram's limit is 4096


def _state_file() -> Path:
    return Path(os.environ.get("LOCAL_DATA_DIR") or "data") / "alerts_state.json"


def _throttled(key: str) -> bool:
    minutes = float(os.environ.get("ALERT_REPEAT_MINUTES", "60") or 0)
    if not key or minutes <= 0:
        return False
    path = _state_file()
    try:
        state = json.loads(path.read_text())
    except (OSError, ValueError):
        state = {}
    now = time.time()
    if now - state.get(key, 0) < minutes * 60:
        return True
    state[key] = now
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state))
    except OSError:
        log.warning("can't write %s; alerts won't be throttled", path)
    return False


def channels() -> list[str]:
    env = os.environ.get
    out = []
    if env("ALERT_TELEGRAM_BOT_TOKEN") and env("ALERT_TELEGRAM_CHAT_ID"):
        out.append("telegram")
    if env("ALERT_WEBHOOK_URL"):
        out.append("webhook")
    if env("ALERT_EMAIL_TO") and env("ALERT_SMTP_HOST"):
        out.append("email")
    return out


def _post(url: str, payload: dict) -> None:
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        resp.read()


def _send(channel: str, subject: str, text: str) -> None:
    env = os.environ.get
    if channel == "telegram":
        url = f"https://api.telegram.org/bot{env('ALERT_TELEGRAM_BOT_TOKEN')}/sendMessage"
        _post(url, {"chat_id": env("ALERT_TELEGRAM_CHAT_ID"), "text": f"{subject}\n\n{text}"[:MAX_CHARS],
                    "disable_web_page_preview": True})
    elif channel == "webhook":
        _post(env("ALERT_WEBHOOK_URL"), {"text": f"*{subject}*\n{text}"[:MAX_CHARS]})
    elif channel == "email":
        msg = EmailMessage()
        msg["Subject"], msg["To"] = subject, env("ALERT_EMAIL_TO")
        msg["From"] = env("ALERT_EMAIL_FROM") or env("ALERT_SMTP_USER") or "dlh@localhost"
        msg.set_content(text)
        port = int(env("ALERT_SMTP_PORT") or 587)
        with smtplib.SMTP(env("ALERT_SMTP_HOST"), port, timeout=20) as smtp:
            if port != 25:
                smtp.starttls()
            if env("ALERT_SMTP_USER"):
                smtp.login(env("ALERT_SMTP_USER"), env("ALERT_SMTP_PASSWORD") or "")
            smtp.send_message(msg)


def notify(subject: str, text: str = "", *, key: str | None = None) -> list[str]:
    """Send to every configured channel. Returns the channels it reached (empty when throttled,
    unconfigured, or every send failed)."""
    if key and _throttled(key):
        log.info("alert %r throttled: %s", key, subject)
        return []
    log.warning("ALERT %s %s", subject, text)
    sent = []
    for channel in channels():
        try:
            _send(channel, subject, text)
            sent.append(channel)
        except Exception as exc:  # never let alerting break the caller
            log.error("alert via %s failed: %s: %s", channel, type(exc).__name__, exc)
    return sent


def airflow_failure(context) -> None:
    """on_failure_callback for every DAG: one message per task that failed for good (after its retries)."""
    ti = context.get("task_instance")
    dag_id, task_id = getattr(ti, "dag_id", "?"), getattr(ti, "task_id", "?")
    exc = context.get("exception")
    text = "\n".join(filter(None, [
        f"run: {context.get('run_id')}",
        f"try: {getattr(ti, 'try_number', '?')}",
        f"error: {type(exc).__name__}: {exc}"[:1500] if exc else None,
        f"log: {getattr(ti, 'log_url', '')}",
    ]))
    notify(f"DLH failed: {dag_id}.{task_id}", text, key=f"{dag_id}.{task_id}")
