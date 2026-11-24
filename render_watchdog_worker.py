"""Continuous Render worker for the IGS station watchdog.

No credentials in source; no HTTP listener. Starts in read-only verification
mode unless WATCHDOG_ENABLED=1. State is kept on a mounted persistent disk.
"""
import fcntl
import json
import os
from pathlib import Path
import signal
import threading
import time
import urllib.request

import telegram_watchdog as watchdog

STATE_DIR = Path(os.getenv("WATCHDOG_STATE_DIR", "/var/data/igs-watchdog"))
SEED_URL = "https://raw.githubusercontent.com/dmirtique/IGS-monitoring/main/.watchdog_state.json"
STOP = threading.Event()


def stop(*_):
    STOP.set()


def atomic_save_state(state):
    path = Path(watchdog.STATE_FILE)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def validate_state(state):
    if not isinstance(state, dict) or any(
        state.get(key) not in ("online", "offline") for key in watchdog.STATIONS
    ):
        raise ValueError("State does not contain valid states for every station")
    return state


def seed_state():
    path = Path(watchdog.STATE_FILE)
    if path.exists():
        validate_state(json.loads(path.read_text(encoding="utf-8")))
        print("Persistent confirmed states loaded", flush=True)
        return
    # Only seed when the worker is actually enabled, after GitHub cutover.
    state = validate_state(watchdog.get_json(SEED_URL))
    atomic_save_state(state)
    print("Confirmed states imported from GitHub", flush=True)


def verify_token(token):
    # Telegram getMe verifies authentication without sending a chat message.
    try:
        with urllib.request.urlopen(
            f"https://api.telegram.org/bot{token}/getMe", timeout=20
        ) as response:
            result = json.load(response)
        if result.get("ok") is not True:
            raise RuntimeError("Bot authentication failed")
    except Exception:
        raise RuntimeError("Telegram getMe failed; check secret and connectivity") from None
    print("Telegram bot authentication OK", flush=True)


def observe():
    now = int(time.time() * 1000)
    for cfg in watchdog.STATIONS.values():
        try:
            _, age, status, _, fresh = watchdog.read_station(cfg, now)
            print(f"Verification: {cfg['label']} status={status}, fresh={fresh}, age_s={None if age is None else round(age / 1000, 1)}", flush=True)
        except Exception as exc:
            print(f"Verification: {cfg['label']} read failed: {type(exc).__name__}", flush=True)


def run():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN secret")
    if not STATE_DIR.parent.is_mount() and not STATE_DIR.is_mount():
        # A Render mount may be /var/data (parent of STATE_DIR).
        raise RuntimeError("Persistent state disk is not mounted")
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    watchdog.STATE_FILE = str(STATE_DIR / "confirmed.json")
    watchdog.RECOVERY_FILE = str(STATE_DIR / "recovery.json")
    watchdog.save_state = atomic_save_state

    with (STATE_DIR / "watchdog.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        enabled = os.getenv("WATCHDOG_ENABLED", "0") == "1"
        verify_token(token)
        print(f"Worker starting: {'active' if enabled else 'read-only verification'}", flush=True)
        if enabled:
            while not STOP.is_set():
                try:
                    seed_state()
                    break
                except Exception as exc:
                    print(f"State initialization failed: {type(exc).__name__}; retry in 60s", flush=True)
                    STOP.wait(60)

        while not STOP.is_set():
            began = time.monotonic()
            try:
                if enabled:
                    watchdog.main()
                else:
                    observe()
            except SystemExit as exc:
                print(f"Watchdog check returned {exc.code}; retrying", flush=True)
            except Exception as exc:
                print(f"Watchdog check failed: {type(exc).__name__}; retrying", flush=True)
            STOP.wait(max(1.0, 60.0 - (time.monotonic() - began)))
        print("Worker stopped", flush=True)


if __name__ == "__main__":
    run()
