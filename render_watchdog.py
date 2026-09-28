import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

DB = "https://igs-monitoring-default-rtdb.europe-west1.firebasedatabase.app"
CHAT_ID = "-1004425577425"
OFFLINE_AFTER_MS = 5 * 60 * 1000
KYIV_TZ = ZoneInfo("Europe/Kyiv")

STATIONS = {
    "zir8": {"label": "ST106", "path": "/public/status.json", "field": "updated_ms"},
    "st107": {"label": "ST107", "path": "/public/st107/status.json", "field": "station_time_ms"},
    "mag": {"label": "MAG", "path": "/public/mag/status.json", "field": "updated_ms"},
    "s2dw": {"label": "S2DW", "path": "/public/s2dw/status.json", "field": "station_time_ms"},
}


def request_json(url, method="GET", payload=None):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"User-Agent": "IGS-Render-Watchdog/1.0"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=20) as response:
        body = response.read().decode("utf-8")
    return json.loads(body) if body else None


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
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(KYIV_TZ).strftime("%d.%m.%Y %H:%M:%S")


def state_url(key):
    return f"{DB}/watchdog_render/stations/{key}.json"


def read_saved_state(key):
    value = request_json(state_url(key))
    return value if isinstance(value, dict) else {}


def save_state(key, state, updated_ms, now_ms):
    request_json(
        state_url(key),
        method="PUT",
        payload={"state": state, "last_data_ms": updated_ms, "changed_ms": now_ms},
    )


def read_station(config, now_ms):
    status = request_json(DB + config["path"]) or {}
    raw = status.get(config["field"])
    if raw is None:
        return None, None, "offline"
    updated_ms = int(raw)
    age_ms = max(0, now_ms - updated_ms)
    return updated_ms, age_ms, "online" if age_ms < OFFLINE_AFTER_MS else "offline"


def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")

    if os.getenv("WATCHDOG_STARTUP_MESSAGE", "0") == "1":
        send_telegram(
            token,
            "✅ IGS watchdog запущено у Render\n"
            "ST106, ST107, MAG і S2DW перевіряються щохвилини. "
            "Офлайн = понад 5 хв без нових даних.",
        )
        print("startup message sent")

    now_ms = int(time.time() * 1000)
    failures = []

    for key, config in STATIONS.items():
        try:
            updated_ms, age_ms, new_state = read_station(config, now_ms)
            saved = read_saved_state(key)
            old_state = saved.get("state")

            if old_state is None:
                if new_state == "offline":
                    mins = "невідомо" if age_ms is None else f"{age_ms / 60000:.1f} хв"
                    send_telegram(
                        token,
                        f"🔴 {config['label']} офлайн\n"
                        f"Останнє оновлення: {fmt_time(updated_ms)}\n"
                        f"Немає нових даних: {mins}",
                    )
                save_state(key, new_state, updated_ms, now_ms)
                print(f"{config['label']}: initialized {new_state}")
                continue

            if new_state == old_state:
                age_text = "unknown" if age_ms is None else f"{age_ms / 1000:.1f}s"
                print(f"{config['label']}: unchanged {new_state}, age={age_text}")
                continue

            if new_state == "offline":
                mins = "невідомо" if age_ms is None else f"{age_ms / 60000:.1f} хв"
                message = (
                    f"🔴 {config['label']} офлайн\n"
                    f"Останнє оновлення: {fmt_time(updated_ms)}\n"
                    f"Немає нових даних: {mins}"
                )
            else:
                message = f"🟢 {config['label']} онлайн\nПередача даних відновлена."

            send_telegram(token, message)
            save_state(key, new_state, updated_ms, now_ms)
            print(f"{config['label']}: notified {new_state}")

        except Exception as exc:
            failures.append(key)
            print(f"{config['label']}: ERROR {exc}", flush=True)

    if failures:
        raise RuntimeError("Station checks failed: " + ", ".join(failures))


if __name__ == "__main__":
    main()
