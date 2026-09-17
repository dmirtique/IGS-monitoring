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
# GitHub Actions runs every 5 minutes. Without a follow-up check, a station that
# becomes stale just after one run can miss the 5-minute boundary and be
# reported only on the next run (almost 10 minutes later). If a station is
# already within 2 minutes of the offline boundary, this run waits only until
# that boundary and checks it once more.
FOLLOW_UP_WINDOW_MS = 2 * 60 * 1000
FOLLOW_UP_GRACE_SECONDS = 3.0
STATE_FILE = ".watchdog_state.json"
KYIV_TZ = ZoneInfo("Europe/Kyiv")

STATIONS = {
    "zir8": ("ST106", "/public/status.json"),
    "st107": ("ST107", "/public/st107/status.json"),
    "mag": ("MAG", "/public/mag/status.json"),
}


def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "IGS-monitoring-watchdog/1.1"})
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


def read_station(key, path, now_ms):
    status = get_json(DB + path) or {}
    updated_ms = status.get("station_time_ms") if key == "st107" else status.get("updated_ms")
    if updated_ms is None:
        return None, None, "offline"

    updated_ms = int(updated_ms)
    # A small clock skew must not turn into a negative data age.
    age_ms = max(0, now_ms - updated_ms)
    state = "online" if age_ms < OFFLINE_AFTER_MS else "offline"
    return updated_ms, age_ms, state


def apply_check(token, key, label, path, current):
    """Check one station, notify on a state transition, and return freshness."""
    now_ms = int(time.time() * 1000)
    updated_ms, age_ms, new_state = read_station(key, path, now_ms)
    old_state = current.get(key)

    if old_state is None:
        if new_state == "offline":
            send_telegram(token, offline_text(label, updated_ms, age_ms))
            print(f"{label}: initial offline notified")
        else:
            print(f"{label}: initial online")
        current[key] = new_state
        return updated_ms, age_ms, new_state

    if new_state == old_state:
        print(f"{label}: unchanged {new_state}")
        return updated_ms, age_ms, new_state

    if new_state == "offline":
        send_telegram(token, offline_text(label, updated_ms, age_ms))
    else:
        send_telegram(token, f"🟢 {label} онлайн\nПередача даних відновлена.")

    current[key] = new_state
    print(f"{label}: notified {new_state}")
    return updated_ms, age_ms, new_state


def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        print("TELEGRAM_BOT_TOKEN is missing; state is not changed", file=sys.stderr)
        sys.exit(2)

    previous = load_state()
    current = dict(previous)
    had_error = False
    follow_up = {}

    if not previous:
        send_telegram(
            token,
            "✅ IGS watchdog активовано\nПеревірка ST106, ST107 і MAG кожні 5 хв. Офлайн — після 5 хв без нових даних.",
        )

    # Normal scheduled pass.
    for key, (label, path) in STATIONS.items():
        try:
            updated_ms, age_ms, state = apply_check(token, key, label, path, current)
            if state == "online" and age_ms is not None:
                remaining_ms = OFFLINE_AFTER_MS - age_ms
                if 0 < remaining_ms <= FOLLOW_UP_WINDOW_MS:
                    follow_up[key] = (label, path, remaining_ms)
                    print(
                        f"{label}: stale for {age_ms / 1000:.1f}s; "
                        f"follow-up at 5-minute boundary in {remaining_ms / 1000:.1f}s"
                    )
        except Exception as e:
            print(f"{label}: check failed: {e}", file=sys.stderr)
            had_error = True

    # Important edge-case fix: if the scheduled run happened just before the
    # 5-minute boundary, do not wait another whole GitHub cron interval.
    if follow_up:
        wait_seconds = max(item[2] for item in follow_up.values()) / 1000.0
        wait_seconds += FOLLOW_UP_GRACE_SECONDS
        print(f"Follow-up check in {wait_seconds:.1f}s for: {', '.join(follow_up)}")
        time.sleep(wait_seconds)

        for key, (label, path, _remaining_ms) in follow_up.items():
            try:
                apply_check(token, key, label, path, current)
            except Exception as e:
                print(f"{label}: follow-up check failed: {e}", file=sys.stderr)
                had_error = True

    if current != previous:
        save_state(current)

    if had_error:
        sys.exit(1)


if __name__ == "__main__":
    main()
