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
ONLINE_CONFIRM_MS = 5 * 60 * 1000
RECOVERY_FRESH_MS = 90 * 1000
MAX_CHECK_GAP_MS = 130 * 1000
RECOVERY_PROGRESS_SLACK_MS = 40 * 1000
STATE_FILE = ".watchdog_state.json"
RECOVERY_FILE = ".watchdog_recovery.json"  # Local runner checkpoint; not committed to Git.

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
        "timestamp_field": "updated_ms",
    },
    "mag": {
        "label": "MAG",
        "path": "/public/mag/status.json",
        "timestamp_field": "updated_ms",
        "data_field": "station_time_ms",
        "points_path": "/public/st107/points.json",
    },
    "s2dw": {
        "label": "S2DW",
        "path": "/public/s2dw/status.json",
        "timestamp_field": "updated_ms",
        "data_field": "station_time_ms",
        "points_path": "/public/s2dw/points.json",
    },
}


def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "IGS-monitoring-watchdog/1.3"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def send_telegram(token, text):
    data = urllib.parse.urlencode({"chat_id": CHAT_ID, "text": text}).encode()
    last_error = None
    for attempt in range(1, 4):
        try:
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=data,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=20) as r:
                payload = json.loads(r.read().decode("utf-8"))
            if not payload.get("ok"):
                raise RuntimeError(payload)
            return
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(attempt * 2)
    raise RuntimeError(f"Telegram send failed after 3 attempts: {last_error}")


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


def offline_text(label, updated_ms, age_ms, detected_ms=None):
    mins = "невідомо" if age_ms is None else f"{age_ms / 60000:.1f} хв"
    detected = ""
    if detected_ms is not None:
        detected = f"\nВиявлено: {fmt_time(detected_ms)}"
    return (
        f"🔴 {label} офлайн\n"
        f"Останнє оновлення: {fmt_time(updated_ms)}\n"
        f"Немає нових даних: {mins}"
        f"{detected}"
    )



def load_recovery():
    """Pending online confirmation survives each one-minute Python invocation."""
    try:
        with open(RECOVERY_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, ValueError, OSError):
        return {}


def save_recovery(recovery):
    temp = RECOVERY_FILE + ".tmp"
    with open(temp, "w", encoding="utf-8") as handle:
        json.dump(recovery, handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.write("\n")
    os.replace(temp, RECOVERY_FILE)


def has_five_minute_sample_coverage(config, data_ms):
    """Verify a near-continuous five-minute one-second data series.

    This complements five minutes of successive *delivery* observations.
    It blocks false recoveries caused by a brief upload of cached old packets.
    Only ST107 and S2DW have this one-second circular buffer.
    """
    points_path = config.get("points_path")
    if not points_path:
        return True
    raw = get_json(DB + points_path) or {}
    if not isinstance(raw, dict):
        return False
    end_second = int(data_ms // 1000)
    valid_seconds = sorted({
        int(float(record[0]) // 1000)
        for record in raw.values()
        if isinstance(record, list)
        and record
        and isinstance(record[0], (int, float))
        and end_second - 301 <= float(record[0]) / 1000 <= end_second + 1
    })
    if len(valid_seconds) < 295:
        return False
    if valid_seconds[-1] - valid_seconds[0] < 294:
        return False
    return all(b - a <= 3 for a, b in zip(valid_seconds, valid_seconds[1:]))


def recovery_check(pending, *, now_ms, data_ms, updated_ms):
    """Five minutes of fresh, advancing data checked about once per minute."""
    checkpoint = {
        "start_ms": now_ms,
        "checked_ms": now_ms,
        "last_data_ms": data_ms,
        "last_update_ms": updated_ms,
        "observations": 1,
    }
    if not isinstance(pending, dict):
        return checkpoint, False
    elapsed = now_ms - int(pending.get("checked_ms", 0))
    data_progress = data_ms - int(pending.get("last_data_ms", 0))
    update_progress = updated_ms - int(pending.get("last_update_ms", 0))
    if (
        elapsed <= 0 or elapsed > MAX_CHECK_GAP_MS
        or data_progress < max(1000, elapsed - RECOVERY_PROGRESS_SLACK_MS)
        or data_progress > elapsed + RECOVERY_PROGRESS_SLACK_MS
        or update_progress <= 0
    ):
        return checkpoint, False
    pending["checked_ms"] = now_ms
    pending["last_data_ms"] = data_ms
    pending["last_update_ms"] = updated_ms
    pending["observations"] = int(pending.get("observations", 1)) + 1
    confirmed = (
        now_ms - int(pending["start_ms"]) >= ONLINE_CONFIRM_MS
        and pending["observations"] >= 5
    )
    return pending, confirmed


def read_station(config, now_ms):
    status = get_json(DB + config["path"]) or {}
    if not isinstance(status, dict):
        raise ValueError("Invalid Firebase station status")
    raw_updated_ms = status.get(config["timestamp_field"])
    if raw_updated_ms is None:
        return None, None, "offline", None, False
    updated_ms = int(raw_updated_ms)
    data_ms = int(status.get(config.get("data_field", config["timestamp_field"])) or 0)
    if updated_ms <= 0 or data_ms <= 0:
        return data_ms or None, None, "offline", updated_ms or None, False
    if max(updated_ms, data_ms) > now_ms + 120_000:
        raise ValueError("Firebase timestamp is too far in the future")

    age_ms = max(0, now_ms - data_ms)
    delivery_age_ms = max(0, now_ms - updated_ms)
    data_available = age_ms < OFFLINE_AFTER_MS
    # A fresh HTTP write without any new sensor data does not mean
    # the station has recovered. Restrict green confirmation accordingly.
    recovery_fresh = (
        data_available
        and age_ms <= RECOVERY_FRESH_MS
        and delivery_age_ms <= RECOVERY_FRESH_MS
        and status.get("station_online") is not False
    )
    raw_state = "online" if data_available else "offline"
    return data_ms, age_ms, raw_state, updated_ms, recovery_fresh


def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        print("TELEGRAM_BOT_TOKEN is missing; state is not changed", file=sys.stderr)
        sys.exit(2)

    previous = load_state()
    current = dict(previous)
    recovery = load_recovery()
    had_error = False
    now_ms = int(time.time() * 1000)

    for key, config in STATIONS.items():
        label = config["label"]
        try:
            data_ms, age_ms, raw_state, updated_ms, fresh = read_station(config, now_ms)
            old_state = previous.get(key)

            if old_state is None:
                # Never announce online on the very first packet.
                if raw_state == "offline":
                    send_telegram(token, offline_text(label, data_ms, age_ms, now_ms))
                    current[key] = "offline"
                    recovery.pop(key, None)
                    print(f"{label}: initial offline notified")
                else:
                    current[key] = "offline"
                    if fresh:
                        recovery[key], _ = recovery_check(
                            None, now_ms=now_ms, data_ms=data_ms, updated_ms=updated_ms
                        )
                    print(f"{label}: initial fresh data; verifying 5 minutes")
                continue

            if raw_state == "offline":
                recovery.pop(key, None)
                if old_state == "online":
                    send_telegram(token, offline_text(label, data_ms, age_ms, now_ms))
                    current[key] = "offline"
                    print(f"{label}: notified offline")
                else:
                    print(f"{label}: still offline")
                continue

            if old_state == "online":
                recovery.pop(key, None)
                print(f"{label}: still online, data age={age_ms / 1000:.1f}s")
                continue

            # We previously sent/offline and have some data again.
            # Wait for 5 minutes of independent regular observations, not
            # a few brief bursts or late uploads of old queued packets.
            if not fresh:
                recovery.pop(key, None)
                print(f"{label}: offline, stale data/delivery; recovery reset")
                continue

            recovery[key], verified = recovery_check(
                recovery.get(key),
                now_ms=now_ms,
                data_ms=data_ms,
                updated_ms=updated_ms,
            )
            elapsed = max(0, now_ms - int(recovery[key]["start_ms"]))
            if not verified:
                print(
                    f"{label}: recovery {elapsed / 60000:.1f}/5.0 min "
                    f"(observations={recovery[key]['observations']}); no alert yet"
                )
                continue

            if not has_five_minute_sample_coverage(config, data_ms):
                # Requiring continuous one-second records prevents green
                # on a sequence of sporadic reconnects and cached uploads.
                recovery.pop(key, None)
                print(f"{label}: no complete five-minute sample history; recovery reset")
                continue

            delay = f"{age_ms / 1000:.1f} с"
            send_telegram(
                token,
                f"🟢 {label} онлайн\n"
                f"Передача даних стабільна щонайменше 5 хвилин.\n"
                f"Нові дані: {fmt_time(data_ms)}\n"
                f"Виявлено: {fmt_time(now_ms)}\n"
                f"Затримка даних: {delay}"
            )
            current[key] = "online"
            recovery.pop(key, None)
            print(f"{label}: notified online after verified 5 minutes")

        except Exception as exc:
            print(f"{label}: check failed: {exc}", file=sys.stderr)
            had_error = True

    # Persist only confirmed online/offline transitions to Git; local pending
    # does not generate a commit or trigger unnecessary Render deployments.
    save_recovery(recovery)
    if current != previous:
        save_state(current)
    if had_error:
        sys.exit(1)


if __name__ == "__main__":
    main()
