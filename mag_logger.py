#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IGS MAG — passive TCP logger + HDF5 + Firebase live.

Source:
  192.168.7.108:16031
Protocol observed:
  16-byte header: <4i = sample_rate_whole, sample_rate_fraction, channels, samples_per_frame
  frame: 19 x int32 metadata (76 B) + channel-major int32 samples

Active channels:
  CH00=MGX, CH02=MGY, CH04=MGZ, CH06=LSU,
  CH08=LSV, CH10=LSW, CH12=TPR, CH14=PRS

Important:
  - read-only: nothing is sent to the device
  - device clock is ignored for recording time
  - timestamps come from this Windows laptop
  - full 8-channel raw counts are stored locally in HDF5
  - only MGX/MGY/MGZ are sent to Firebase for the live page
"""
from __future__ import annotations

import json
import os
import socket
import struct
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import h5py
    import numpy as np
except ImportError as exc:
    print(f"Missing package: {exc}")
    print("Install with: py -3 -m pip install numpy h5py")
    input("Press Enter to exit...")
    raise SystemExit(2)

HOST = "192.168.7.108"
PORT = 16031

RECORD_ROOT = Path(r"D:\IGS_MAG_RECORDS")
FILE_SECONDS = 3600.0
FLUSH_SECONDS = 2.0
MIN_FREE_GB = 10.0

FIREBASE_URL = "https://igs-monitoring-default-rtdb.europe-west1.firebasedatabase.app"
FIREBASE_ROOT = "/public/mag"
LIVE_BUCKET_SECONDS = 1.0
LIVE_HISTORY_SLOTS = 360  # ring buffer, page itself filters 10/60/300 s

CHANNEL_NAMES = ("MGX", "MGY", "MGZ", "LSU", "LSV", "LSW", "TPR", "PRS")
SOURCE_CHANNELS = (0, 2, 4, 6, 8, 10, 12, 14)
LIVE_CHANNELS = ("MGX", "MGY", "MGZ")
LIVE_INDICES = (0, 1, 2)

EXPECTED_RATE = 32.0
EXPECTED_CHANNELS = 16
EXPECTED_SAMPLES_PER_FRAME = 16


def recv_exact(sock: socket.socket, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise EOFError("device closed TCP connection")
        out.extend(chunk)
    return bytes(out)


def free_gb(path: Path) -> float:
    import shutil
    return shutil.disk_usage(path).free / (1024 ** 3)


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    i = 2
    while True:
        p = path.with_name(f"{path.stem}_{i:02d}{path.suffix}")
        if not p.exists():
            return p
        i += 1


class HDF5Recorder:
    def __init__(self) -> None:
        self.root = RECORD_ROOT
        self.root.mkdir(parents=True, exist_ok=True)
        self._recover_parts()

        self.file: h5py.File | None = None
        self.part_path: Path | None = None
        self.final_path: Path | None = None
        self.time_ds: h5py.Dataset | None = None
        self.raw_ds: h5py.Dataset | None = None

        self.current_hour_key: str | None = None
        self.samples_written = 0
        self.last_flush = 0.0

    def _recover_parts(self) -> None:
        for p in self.root.rglob("*.h5.part"):
            target = unique_path(p.with_name(p.name[:-8] + ".incomplete.h5"))
            try:
                p.replace(target)
                print(f"[RECOVER] {p.name} -> {target.name}", flush=True)
            except OSError as exc:
                print(f"[RECOVER] cannot rename {p}: {exc}", flush=True)

    @staticmethod
    def _hour_key(epoch: float) -> str:
        return datetime.fromtimestamp(epoch).astimezone().strftime("%Y%m%d_%H")

    def _build_paths(self, epoch: float) -> tuple[Path, Path]:
        dt = datetime.fromtimestamp(epoch).astimezone()
        day_dir = self.root / dt.strftime("%Y") / dt.strftime("%m") / dt.strftime("%d")
        day_dir.mkdir(parents=True, exist_ok=True)
        final_path = unique_path(day_dir / f"MAG_{dt.strftime('%Y%m%d_%H0000')}.h5")
        part_path = final_path.with_name(final_path.name + ".part")
        return part_path, final_path

    def _open(self, epoch: float, fs: float) -> None:
        if free_gb(self.root) < MIN_FREE_GB:
            raise RuntimeError(f"less than {MIN_FREE_GB:g} GiB free on recording disk")

        self.part_path, self.final_path = self._build_paths(epoch)
        self.current_hour_key = self._hour_key(epoch)
        self.samples_written = 0
        self.last_flush = time.monotonic()

        self.file = h5py.File(self.part_path, "w", libver="latest")
        attrs = self.file.attrs
        attrs["format_name"] = "IGS MAG raw archive"
        attrs["format_version"] = "1.0"
        attrs["source_host"] = HOST
        attrs["source_port"] = PORT
        attrs["sample_rate_hz"] = float(fs)
        attrs["channel_names_json"] = json.dumps(CHANNEL_NAMES)
        attrs["raw_dtype"] = "int32 counts"
        attrs["time_source"] = "Windows laptop wall clock"
        attrs["device_time_note"] = "Device timestamp is intentionally ignored because its clock is incorrect."
        attrs["start_epoch_utc"] = float(epoch)
        attrs["start_time_utc"] = datetime.fromtimestamp(epoch, timezone.utc).isoformat()
        attrs["start_time_local"] = datetime.fromtimestamp(epoch).astimezone().isoformat()
        attrs["end_epoch_utc"] = float(epoch)
        attrs["samples_per_channel"] = 0
        attrs["complete"] = 0

        chunk_samples = max(320, int(round(fs * 10.0)))
        compression = dict(compression="gzip", compression_opts=1, shuffle=True, fletcher32=True)
        self.time_ds = self.file.create_dataset(
            "time_epoch_utc",
            shape=(0,), maxshape=(None,), chunks=(chunk_samples,),
            dtype="<f8", **compression
        )
        self.raw_ds = self.file.create_dataset(
            "raw_counts",
            shape=(0, len(CHANNEL_NAMES)), maxshape=(None, len(CHANNEL_NAMES)),
            chunks=(chunk_samples, len(CHANNEL_NAMES)), dtype="<i4", **compression
        )
        self.raw_ds.attrs["columns_json"] = json.dumps(CHANNEL_NAMES)

        print(f"[HDF5] OPEN {self.part_path}", flush=True)

    def append(self, times: np.ndarray, values: np.ndarray, fs: float) -> None:
        if len(times) == 0:
            return
        hour_key = self._hour_key(float(times[0]))
        if self.file is None or self.current_hour_key != hour_key:
            self.close()
            self._open(float(times[0]), fs)

        assert self.file is not None and self.time_ds is not None and self.raw_ds is not None
        start = self.samples_written
        stop = start + len(times)
        self.time_ds.resize((stop,))
        self.raw_ds.resize((stop, len(CHANNEL_NAMES)))
        self.time_ds[start:stop] = times
        self.raw_ds[start:stop, :] = values
        self.samples_written = stop

        self.file.attrs["end_epoch_utc"] = float(times[-1])
        self.file.attrs["samples_per_channel"] = int(self.samples_written)

        now = time.monotonic()
        if now - self.last_flush >= FLUSH_SECONDS:
            self.file.flush()
            self.last_flush = now

    def close(self) -> None:
        if self.file is None:
            return
        try:
            self.file.attrs["complete"] = 1
            self.file.attrs["samples_per_channel"] = int(self.samples_written)
            self.file.flush()
            self.file.close()
            if self.part_path and self.final_path:
                self.part_path.replace(self.final_path)
                print(f"[HDF5] CLOSE {self.final_path}", flush=True)
        finally:
            self.file = None
            self.part_path = None
            self.final_path = None
            self.time_ds = None
            self.raw_ds = None
            self.current_hour_key = None


def find_firebase_credentials() -> Path | None:
    script_dir = Path(__file__).resolve().parent
    desktop = Path.home() / "Desktop"
    candidates = [
        script_dir / "firebase-service-account.json",
        desktop / "IGS_ST107" / "firebase-service-account.json",
        desktop / "IGS_ST107" / "service-account.json",
    ]
    for p in candidates:
        if p.is_file():
            return p

    if desktop.exists():
        patterns = ("*service-account*.json", "*firebase*.json")
        found: list[Path] = []
        for pattern in patterns:
            found.extend(desktop.glob(f"*\\{pattern}"))
            found.extend(desktop.glob(pattern))
        for p in found:
            if p.is_file():
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                    if data.get("type") == "service_account" and "private_key" in data:
                        return p
                except Exception:
                    pass
    return None


class FirebaseLive:
    def __init__(self) -> None:
        self.enabled = False
        self.db = None
        self.root = None
        self.buffer_times: list[float] = []
        self.buffer_values: list[np.ndarray] = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.last_error = ""

        try:
            import firebase_admin
            from firebase_admin import credentials, db
        except ImportError:
            print("[FIREBASE] firebase-admin is not installed.", flush=True)
            print("[FIREBASE] Local HDF5 recording will continue.", flush=True)
            print("[FIREBASE] Install: py -3 -m pip install firebase-admin", flush=True)
            return

        cred_path = find_firebase_credentials()
        if cred_path is None:
            print("[FIREBASE] Service-account JSON not found.", flush=True)
            print("[FIREBASE] Local HDF5 recording will continue.", flush=True)
            return

        try:
            app_name = "igs-mag-logger"
            try:
                app = firebase_admin.get_app(app_name)
            except ValueError:
                app = firebase_admin.initialize_app(
                    credentials.Certificate(str(cred_path)),
                    {"databaseURL": FIREBASE_URL},
                    name=app_name,
                )
            self.db = db
            self.root = db.reference(FIREBASE_ROOT, app=app)
            self.enabled = True
            self.thread = threading.Thread(target=self._worker, name="MAG Firebase", daemon=True)
            self.thread.start()
            print(f"[FIREBASE] ON {FIREBASE_ROOT} using {cred_path}", flush=True)
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            print(f"[FIREBASE] init error: {self.last_error}", flush=True)

    def add(self, times: np.ndarray, values: np.ndarray) -> None:
        if not self.enabled:
            return
        with self.lock:
            self.buffer_times.extend(float(x) for x in times)
            self.buffer_values.extend(np.asarray(row[:3], dtype=np.int32).copy() for row in values)

    def _take_one_second(self):
        with self.lock:
            if len(self.buffer_times) < int(EXPECTED_RATE):
                return None
            n = int(EXPECTED_RATE)
            tt = np.asarray(self.buffer_times[:n], dtype=np.float64)
            vv = np.asarray(self.buffer_values[:n], dtype=np.int32)
            del self.buffer_times[:n]
            del self.buffer_values[:n]
        return tt, vv

    def _worker(self) -> None:
        assert self.root is not None
        failures = 0
        while not self.stop_event.is_set():
            packet = self._take_one_second()
            if packet is None:
                self.stop_event.wait(0.05)
                continue

            tt, vv = packet
            t0_ms = int(round(float(tt[0]) * 1000.0))
            slot = f"{(t0_ms // 1000) % LIVE_HISTORY_SLOTS:03d}"
            record = {
                "t": t0_ms,
                "dt_ms": 1000.0 / EXPECTED_RATE,
                "MGX": [int(x) for x in vv[:, 0]],
                "MGY": [int(x) for x in vv[:, 1]],
                "MGZ": [int(x) for x in vv[:, 2]],
            }
            status = {
                "updated_ms": int(time.time() * 1000),
                "laptop_time_ms": int(round(float(tt[-1]) * 1000.0)),
                "sample_rate_hz": EXPECTED_RATE,
                "bucket_seconds": LIVE_BUCKET_SECONDS,
                "source": f"{HOST}:{PORT}",
                "time_source": "Windows laptop",
            }

            try:
                self.root.child("points").child(slot).set(record)
                self.root.child("status").update(status)
                failures = 0
            except Exception as exc:
                failures += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
                delay = min(30.0, max(1.0, 1.7 ** min(failures, 7)))
                print(f"[FIREBASE] send error: {self.last_error}; retry {delay:.1f}s", flush=True)
                with self.lock:
                    self.buffer_times = list(tt) + self.buffer_times
                    self.buffer_values = [row.copy() for row in vv] + self.buffer_values
                    cap = int(EXPECTED_RATE * 300)
                    if len(self.buffer_times) > cap:
                        self.buffer_times = self.buffer_times[-cap:]
                        self.buffer_values = self.buffer_values[-cap:]
                self.stop_event.wait(delay)

    def close(self) -> None:
        if not self.enabled:
            return
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=5.0)


class LaptopClock:
    """Continuous 32 Hz timeline anchored to the laptop, not to the device clock."""
    def __init__(self, fs: float, frame_samples: int) -> None:
        self.fs = float(fs)
        self.frame_samples = int(frame_samples)
        self.next_epoch: float | None = None

    def frame_times(self, recv_end_epoch: float) -> np.ndarray:
        frame_duration = self.frame_samples / self.fs
        measured_start = recv_end_epoch - frame_duration

        if self.next_epoch is None:
            start = measured_start
        else:
            if abs(measured_start - self.next_epoch) > 2.0:
                print(
                    f"[TIME] discontinuity {measured_start - self.next_epoch:+.3f}s; "
                    "re-anchor to laptop clock",
                    flush=True,
                )
                start = measured_start
            else:
                start = self.next_epoch

        t = start + np.arange(self.frame_samples, dtype=np.float64) / self.fs
        self.next_epoch = start + frame_duration
        return t


def decode_frame(body: bytes, nch: int, ns: int) -> np.ndarray:
    sample_bytes = body[76:]
    raw = np.frombuffer(sample_bytes, dtype="<i4", count=nch * ns).reshape(nch, ns)
    selected = raw[np.asarray(SOURCE_CHANNELS), :].T.copy()
    return selected


def run() -> None:
    recorder = HDF5Recorder()
    firebase = FirebaseLive()
    clock: LaptopClock | None = None
    fs: float | None = None
    total_samples = 0
    connections = 0
    last_report = time.monotonic()

    print("=" * 78)
    print("IGS MAG — HDF5 + FIREBASE LIVE")
    print(f"Source: {HOST}:{PORT} (READ ONLY)")
    print(f"HDF5:  {RECORD_ROOT}")
    print(f"Channels: {', '.join(CHANNEL_NAMES)}")
    print("Time: Windows laptop clock; device 2028 clock is ignored")
    print("Stop: Ctrl+C")
    print("=" * 78)

    try:
        while True:
            sock: socket.socket | None = None
            try:
                sock = socket.create_connection((HOST, PORT), timeout=5.0)
                sock.settimeout(3.0)
                connections += 1

                header = recv_exact(sock, 16)
                whole, frac_micro, nch, ns = struct.unpack("<4i", header)
                fs_now = float(whole) + float(frac_micro) / 1_000_000.0

                if nch != EXPECTED_CHANNELS or ns != EXPECTED_SAMPLES_PER_FRAME:
                    raise RuntimeError(f"unexpected stream header: fs={fs_now}, nch={nch}, ns={ns}")
                if abs(fs_now - EXPECTED_RATE) > 1e-6:
                    raise RuntimeError(f"unexpected sample rate: {fs_now}")

                if fs is None:
                    fs = fs_now
                    clock = LaptopClock(fs, ns)

                frame_size = 76 + nch * ns * 4

                while True:
                    body = recv_exact(sock, frame_size)
                    recv_end = time.time()
                    values = decode_frame(body, nch, ns)
                    assert clock is not None and fs is not None
                    times = clock.frame_times(recv_end)

                    recorder.append(times, values, fs)
                    firebase.add(times, values)
                    total_samples += ns

                    now = time.monotonic()
                    if now - last_report >= 10.0:
                        dt = datetime.fromtimestamp(float(times[-1])).astimezone()
                        print(
                            f"[OK] {dt:%Y-%m-%d %H:%M:%S} | "
                            f"samples/ch={total_samples} | conn={connections} | "
                            f"MGX={int(values[-1,0])} MGY={int(values[-1,1])} MGZ={int(values[-1,2])}",
                            flush=True,
                        )
                        last_report = now

            except EOFError:
                pass
            except (ConnectionError, TimeoutError, OSError, RuntimeError) as exc:
                print(f"[NET] {type(exc).__name__}: {exc}", flush=True)
                time.sleep(1.0)
            finally:
                if sock is not None:
                    try:
                        sock.close()
                    except OSError:
                        pass

    except KeyboardInterrupt:
        print("\nStopping...", flush=True)
    finally:
        firebase.close()
        recorder.close()
        print("Stopped cleanly.", flush=True)


if __name__ == "__main__":
    run()
