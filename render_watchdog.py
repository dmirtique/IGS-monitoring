import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

DB = "https://igs-monitoring-default-rtdb.europe-west1.firebasedatabase.app"
CHAT_ID = "-1004425577425"
OFFLINE_AFTER_MS = 5 * 60 * 1000
KYIV_TZ = ZoneInfo("Europe/Kyiv")
WATCHDOG_VERSION = "2.0-render-cron"

STATIONS = {
    "zir8": {"label": "ST106", "path": "/public/status.json", "field": "updated_ms"},
    "st107": {"label": "ST107", "path": "/public/st107/status.json", "field": "station_time_ms"},
    "mag": {"label": "MAG", "path": "/public/mag/status.json", "field": "updated_ms"},
    "s2dw": {"label": "S2DW", "path": "/public/s2dw/status.json", "field": "station_time_ms"},
}

STATE_ROOT = "/watchdog_render"
STARTUP_META_URL = f"{DB}{STATE_ROOT}/meta.json"


def request_json(url, method="GET", payload=None, attempts=3):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"User-Agent": f"IGS-Render-Watchdog/{WATCHDOG_VERSION}"}
    if data is not None:
        headers["Content-Type"] = "application/json"

    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=20) as response:
                body = response.read().decode("utf-8")
            return json.loads(body) if body else None
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(1.0 * attempt)

    raise last_error


def send_telegram(token, message):
    data = urllib.parse.urlencode({"chat_id": CHAT_ID, "text": message}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=data,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        result = json.loads(response.read().decode("utf-8"))
    if not result.get("ok"):
        raise RuntimeError(f"Telegram API error: {result}")


def fmt_time(ms):
    if not isinstance(ms, (int, float)):
        return "невідомо"
    return (
        datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        .astimezone(KYIV_TZ)
        .strftime("%d.%m.%Y %H:%M:%S")
    )


def state_url(key):
    return f"{DB}{STATE_ROOT}/stations/{key}.json"


def read_saved_state(key):
    value = request_json(state_url(key))
    return value if isinstance(value, dict) else {}


def save_state(key, state, updated_ms, now_ms):
    request_json(
        state_url(key),
        method="PUT",
        payload={
            "state": state,
            "last_data_ms": updated_ms,
            "changed_ms": now_ms,
            "watchdog_version": WATCHDOG_VERSION,
        },
    )


def read_station(config, now_ms):
    status = request_json(DB + config["path"]) or {}
    raw = status.get(config["field"])
    if raw is None:
        return None, None, "offline"

    updated_ms = int(raw)
    age_ms = max(0, now_ms - updated_ms)
    state = "online" if age_ms < OFFLINE_AFTER_MS else "offline"
    return updated_ms, age_ms, state


def startup_message_once(token, now_ms):
    meta = request_json(STARTUP_META_URL)
    if isinstance(meta, dict) and meta.get("version") == WATCHDOG_VERSION:
        return

    send_telegram(
        token,
        "✅ IGS watchdog активовано на Render\n"
        "ST106, ST107, MAG і S2DW перевіряються щохвилини.\n"
        "Офлайн = понад 5 хв без нових даних.",
    )
    request_json(
        STARTUP_META_URL,
        method="PUT",
        payload={
            "version": WATCHDOG_VERSION,
            "activated_ms": now_ms,
        },
    )


def build_message(label, new_state, updated_ms, age_ms, now_ms):
    if new_state == "offline":
        mins = "невідомо" if age_ms is None else f"{age_ms / 60000:.1f} хв"
        return (
            f"🔴 {label} офлайн\n"
            f"Останнє оновлення: {fmt_time(updated_ms)}\n"
            f"Немає нових даних: {mins}\n"
            f"Виявлено: {fmt_time(now_ms)}"
        )

    delay_s = "невідомо" if age_ms is None else f"{age_ms / 1000:.1f} с"
    return (
        f"🟢 {label} онлайн\n"
        f"Передача даних відновлена.\n"
        f"Нові дані: {fmt_time(updated_ms)}\n"
        f"Виявлено: {fmt_time(now_ms)}\n"
        f"Затримка виявлення: {delay_s}"
    )


def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")

    now_ms = int(time.time() * 1000)
    startup_message_once(token, now_ms)

    failures = []

    for key, config in STATIONS.items():
        try:
            updated_ms, age_ms, new_state = read_station(config, now_ms)
            saved = read_saved_state(key)
            old_state = saved.get("state")

            if old_state is None:
                save_state(key, new_state, updated_ms, now_ms)
                print(
                    f"{config['label']}: initialized {new_state}, "
                    f"age={'unknown' if age_ms is None else f'{age_ms / 1000:.1f}s'}"
                )
                continue

            if new_state == old_state:
                age_text = "unknown" if age_ms is None else f"{age_ms / 1000:.1f}s"
                print(f"{config['label']}: unchanged {new_state}, age={age_text}")
                continue

            message = build_message(
                config["label"],
                new_state,
                updated_ms,
                age_ms,
                now_ms,
            )

            # State advances only after Telegram confirms delivery.
            send_telegram(token, message)
            save_state(key, new_state, updated_ms, now_ms)
            print(f"{config['label']}: notified {new_state}")

        except Exception as exc:
            failures.append(key)
            print(f"{config['label']}: ERROR {exc}", file=sys.stderr, flush=True)

    if failures:
        raise RuntimeError("Station checks failed: " + ", ".join(failures))


if __name__ == "__main__":
    main()
