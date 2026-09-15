import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

DB = "https://igs-monitoring-default-rtdb.europe-west1.firebasedatabase.app"
CHAT_ID = "-1004425577425"
OFFLINE_AFTER_MS = 15 * 60 * 1000
STATE_FILE = ".watchdog_state.json"

STATIONS = {
    "zir8": ("ZIR-8", "/public/status.json"),
    "st107": ("ST107", "/public/st107/status.json"),
    "mag": ("MAG", "/public/mag/status.json"),
}


def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "IGS-monitoring-watchdog/1.0"})
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
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone().strftime("%d.%m.%Y %H:%M:%S")


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


def main():
    now_ms = int(time.time() * 1000)
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    previous = load_state()
    current = dict(previous)
    first_run = not bool(previous)
    had_error = False

    for key, (label, path) in STATIONS.items():
        try:
            status = get_json(DB + path) or {}
            updated_ms = status.get("updated_ms")
            age_ms = now_ms - int(updated_ms) if updated_ms is not None else None
            online = age_ms is not None and age_ms < OFFLINE_AFTER_MS
            new_state = "online" if online else "offline"
            old_state = previous.get(key)

            if first_run or old_state is None:
                current[key] = new_state
                print(f"{label}: initial {new_state}")
                continue

            if new_state == old_state:
                print(f"{label}: unchanged {new_state}")
                continue

            if not token:
                print(f"{label}: state changed to {new_state}, but TELEGRAM_BOT_TOKEN is missing", file=sys.stderr)
                had_error = True
                continue

            if new_state == "offline":
                mins = "невідомо" if age_ms is None else f"{age_ms / 60000:.1f} хв"
                text = f"🔴 {label} офлайн\nОстаннє оновлення: {fmt_time(updated_ms)}\nНемає нових даних: {mins}"
            else:
                text = f"🟢 {label} онлайн\nПередача даних відновлена."

            send_telegram(token, text)
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
