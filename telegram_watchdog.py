import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

DB = "https://igs-monitoring-default-rtdb.europe-west1.firebasedatabase.app"
CHAT_ID = "-1004425577425"
OFFLINE_AFTER_MS = 5 * 60 * 1000
STATE_FILE = ".watchdog_state.json"
KYIV_TZ = ZoneInfo("Europe/Kyiv")

# These are the same Firebase status nodes and timestamps used by the pages.
# Therefore the page and Telegram watchdog switch offline on the same 5-minute rule.
STATIONS = {
    "zir8": {
        "label": "ST106",
        "path": "/public/status.json",
        "timestamp_field": "updated_ms",
    },
    "st107": {
        "label": "ST107",
        "path": "/public/st107/status.json",
        "timestamp_field": "station_time_ms",
    },
    "mag": {
        "label": "MAG",
        "path": "/public/mag/status.json",
        "timestamp_field": "updated_ms",
    },
}


def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "IGS-monitoring-watchdog/1.2"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def send_telegram(token, text):
    data = urllib.parse.urlencode({"chat_id": CHAT_ID, "text": text}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=data,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        payload = json.loads(r.read().decode("utf-8"))
    if not payload.get("ok"):
        raise RuntimeError(payload)


def fmt_time(ms):
    if not isinstance(ms, (int, float)):
        return "невідомо"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(KYIV_TZ).strftime("%d.%m.%Y %H:%M:%S")


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


def offline_text(label, updated_ms, age_ms):
    mins = "невідомо" if age_ms is None else f"{age_ms / 60000:.1f} хв"
    return (
        f"🔴 {label} офлайн\n"
        f"Останнє оновлення: {fmt_time(updated_ms)}\n"
        f"Немає нових даних: {mins}"
    )


def read_station(config, now_ms):
    status = get_json(DB + config["path"]) or {}
    raw_updated_ms = status.get(config["timestamp_field"])
    if raw_updated_ms is None:
        return None, None, "offline"

    updated_ms = int(raw_updated_ms)
    age_ms = max(0, now_ms - updated_ms)
    state = "online" if age_ms < OFFLINE_AFTER_MS else "offline"
    return updated_ms, age_ms, state


def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        print("TELEGRAM_BOT_TOKEN is missing; state is not changed", file=sys.stderr)
        sys.exit(2)

    previous = load_state()
    current = dict(previous)
    had_error = False

    if not previous:
        send_telegram(
            token,
            "✅ IGS watchdog активовано\n"
            "ST106, ST107 і MAG перевіряються щохвилини. "
            "Станція вважається офлайн після 5 хв без нових даних.",
        )

    now_ms = int(time.time() * 1000)

    for key, config in STATIONS.items():
        label = config["label"]
        try:
            updated_ms, age_ms, new_state = read_station(config, now_ms)
            old_state = previous.get(key)

            if old_state is None:
                if new_state == "offline":
                    send_telegram(token, offline_text(label, updated_ms, age_ms))
                    print(f"{label}: initial offline notified")
                else:
                    print(f"{label}: initial online")
                current[key] = new_state
                continue

            if new_state == old_state:
                age_text = "unknown" if age_ms is None else f"{age_ms / 1000:.1f}s"
                print(f"{label}: unchanged {new_state}, age={age_text}")
                continue

            if new_state == "offline":
                send_telegram(token, offline_text(label, updated_ms, age_ms))
            else:
                send_telegram(token, f"🟢 {label} онлайн\nПередача даних відновлена.")

            current[key] = new_state
            print(f"{label}: notified {new_state}")

        except Exception as e:
            print(f"{label}: check failed: {e}", file=sys.stderr)
            had_error = True

    if current != previous:
        save_state(current)

    if had_error:
        sys.exit(1)


if __name__ == "__main__":
    main()
