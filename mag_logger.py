#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IGS MAG — passive TCP logger + calibrated HDF5 + Firebase live.

Source: 192.168.7.108:16031
Read-only: the script never sends anything to the device.

Mapped active channels:
  CH00=MGX, CH02=MGY, CH04=MGZ, CH06=LSU,
  CH08=LSV, CH10=LSW, CH12=TPR, CH14=PRS

Timing:
  Absolute date/time comes from the Windows laptop.
  The device's incorrect 2028 timestamp is NEVER used as absolute time.
  Its frame timestamp is used only as a relative interval counter so TCP jitter
  or short buffering cannot distort the 32 Hz time grid.

Storage on D:\\IGS_MAG_RECORDS:
  - raw_counts      : original int32 ADC counts, preserved unchanged
  - physical_values : calibrated float64 values in physical units
  Files are aligned to local clock hours where possible.

Physical units:
  MGX/MGY/MGZ -> nT
  LSU/LSV/LSW -> um
  TPR         -> degC
  PRS         -> hPa

Live:
  Calibrated physical values -> Firebase /public/mag
  Record format marker: physical_v1
"""
from __future__ import annotations

import json
import shutil
import socket
import struct
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
FLUSH_SECONDS = 2.0
MIN_FREE_GB = 10.0

FIREBASE_URL = "https://igs-monitoring-default-rtdb.europe-west1.firebasedatabase.app"
FIREBASE_ROOT = "/public/mag"
LIVE_HISTORY_SECONDS = 300
LIVE_PACKET_SAMPLES = 32

CHANNEL_NAMES = ("MGX", "MGY", "MGZ", "LSU", "LSV", "LSW", "TPR", "PRS")
SOURCE_CHANNELS = (0, 2, 4, 6, 8, 10, 12, 14)
PHYSICAL_UNITS = ("nT", "nT", "nT", "um", "um", "um", "degC", "hPa")

EXPECTED_RATE = 32.0
EXPECTED_CHANNELS = 16
EXPECTED_SAMPLES_PER_FRAME = 16

# -----------------------------------------------------------------------------
# Calibration used by both HDF5 and Firebase.
# Laser coefficients are from the available factory calibration.
# MAG, TPR and PRS coefficients are current provisional empirical calibration.
# Raw ADC counts are also kept in every HDF5 file so calibration can be revised.
# -----------------------------------------------------------------------------
CALIBRATION_VERSION = "2026-09-17-v1"
MAG_NT_PER_COUNT = 13.7065

CALIBRATION_STATUS = {
    "MGX": "provisional empirical",
    "MGY": "provisional empirical",
    "MGZ": "provisional empirical",
    "LSU": "factory calibration",
    "LSV": "factory calibration",
    "LSW": "factory calibration",
    "TPR": "provisional empirical",
    "PRS": "provisional empirical",
}

CALIBRATION_FORMULAS = {
    "MGX": "nT = 13.7065 * counts",
    "MGY": "nT = 13.7065 * counts",
    "MGZ": "nT = 13.7065 * counts",
    "LSU": "um = -1.642418e-5 * counts + 8.802597",
    "LSV": "um = -1.61601e-5 * counts - 1.36095",
    "LSW": "um = -1.6510e-5 * counts - 6.0113",
    "TPR": "degC = 0.00753296 * counts + 14627.07345",
    "PRS": "hPa = 0.000410678 * counts + 240.70595",
}


def calibrate_values(raw_values: np.ndarray) -> np.ndarray:
    """Convert Nx8 raw ADC counts to Nx8 physical values."""
    raw = np.asarray(raw_values, dtype=np.float64)
    if raw.ndim != 2 or raw.shape[1] != len(CHANNEL_NAMES):
        raise ValueError(f"expected Nx8 raw values, got shape {raw.shape}")

    out = np.empty(raw.shape, dtype=np.float64)
    out[:, 0] = raw[:, 0] * MAG_NT_PER_COUNT
    out[:, 1] = raw[:, 1] * MAG_NT_PER_COUNT
    out[:, 2] = raw[:, 2] * MAG_NT_PER_COUNT
    out[:, 3] = -1.642418e-5 * raw[:, 3] + 8.802597
    out[:, 4] = -1.61601e-5 * raw[:, 4] - 1.36095
    out[:, 5] = -1.6510e-5 * raw[:, 5] - 6.0113
    out[:, 6] = 0.00753296 * raw[:, 6] + 14627.07345
    out[:, 7] = 0.000410678 * raw[:, 7] + 240.70595
    return out


def recv_exact(sock: socket.socket, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise EOFError("device closed TCP connection")
        out.extend(chunk)
    return bytes(out)


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    i = 2
    while True:
        candidate = path.with_name(f"{path.stem}_{i:02d}{path.suffix}")
        if not candidate.exists():
            return candidate
        i += 1


def source_frame_epoch(metadata: np.ndarray) -> float | None:
    """Read the source frame clock only for RELATIVE spacing."""
    try:
        sec = int(metadata[0])
        usec = int(metadata[1])
    except Exception:
        return None
    if not (1_000_000_000 <= sec <= 3_000_000_000):
        return None
    if not (0 <= usec < 1_000_000):
        return None
    return float(sec) + float(usec) / 1_000_000.0


class LaptopAnchoredTimeline:
    """Windows provides absolute time; source clock contributes relative deltas."""

    def __init__(self, fs: float, frame_samples: int) -> None:
        self.fs = float(fs)
        self.frame_samples = int(frame_samples)
        self.frame_duration = self.frame_samples / self.fs
        self.source_anchor: float | None = None
        self.laptop_anchor_start: float | None = None
        self.last_source: float | None = None
        self.fallback_next: float | None = None
        self.gap_count = 0

    def frame_times(
        self,
        source_epoch: float | None,
        receive_end_laptop_epoch: float,
    ) -> np.ndarray:
        measured_start = receive_end_laptop_epoch - self.frame_duration

        if source_epoch is not None:
            if self.source_anchor is None:
                self.source_anchor = source_epoch
                self.laptop_anchor_start = measured_start
                self.last_source = source_epoch
                start = measured_start
                print(
                    "[TIME] Windows clock anchored; source 2028 clock used only for relative spacing",
                    flush=True,
                )
            else:
                assert self.laptop_anchor_start is not None
                assert self.last_source is not None

                step = source_epoch - self.last_source
                if step <= 0 or abs(step) > 86400.0:
                    print(
                        f"[TIME] source clock reset ({step:+.3f}s); re-anchor to Windows clock",
                        flush=True,
                    )
                    self.source_anchor = source_epoch
                    self.laptop_anchor_start = measured_start
                    start = measured_start
                else:
                    if abs(step - self.frame_duration) > 0.001:
                        self.gap_count += 1
                        missing = max(0.0, step - self.frame_duration)
                        print(
                            f"[STREAM] source interval {step:.3f}s "
                            f"(expected {self.frame_duration:.3f}s, gap {missing:.3f}s)",
                            flush=True,
                        )
                    start = self.laptop_anchor_start + (source_epoch - self.source_anchor)

                self.last_source = source_epoch

            self.fallback_next = start + self.frame_duration
        else:
            start = measured_start if self.fallback_next is None else self.fallback_next
            self.fallback_next = start + self.frame_duration

        return start + np.arange(self.frame_samples, dtype=np.float64) / self.fs


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
        self.physical_ds: h5py.Dataset | None = None
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
        final = unique_path(day_dir / f"MAG_{dt.strftime('%Y%m%d_%H0000')}.h5")
        return final.with_name(final.name + ".part"), final

    def _open(self, epoch: float, fs: float) -> None:
        free = shutil.disk_usage(self.root).free / (1024 ** 3)
        if free < MIN_FREE_GB:
            raise RuntimeError(
                f"recording paused: only {free:.2f} GiB free, need {MIN_FREE_GB:.0f} GiB"
            )

        self.part_path, self.final_path = self._build_paths(epoch)
        self.current_hour_key = self._hour_key(epoch)
        self.samples_written = 0
        self.last_flush = time.monotonic()

        # Use broadly compatible HDF5 layout rather than libver='latest'.
        self.file = h5py.File(self.part_path, "w", libver="earliest")
        attrs = self.file.attrs
        attrs["format_name"] = "IGS MAG raw + calibrated archive"
        attrs["format_version"] = "2.0"
        attrs["source_host"] = HOST
        attrs["source_port"] = PORT
        attrs["sample_rate_hz"] = float(fs)
        attrs["channel_names_json"] = json.dumps(CHANNEL_NAMES)
        attrs["physical_units_json"] = json.dumps(dict(zip(CHANNEL_NAMES, PHYSICAL_UNITS)))
        attrs["raw_dtype"] = "int32 counts"
        attrs["physical_dtype"] = "float64"
        attrs["calibration_version"] = CALIBRATION_VERSION
        attrs["calibration_status_json"] = json.dumps(CALIBRATION_STATUS)
        attrs["calibration_formulas_json"] = json.dumps(CALIBRATION_FORMULAS)
        attrs["calibration_note"] = (
            "Raw counts are preserved. Laser channels use factory calibration; "
            "MAG, TPR and PRS use provisional empirical calibration and can be recalculated later."
        )
        attrs["time_source"] = "Windows laptop absolute clock"
        attrs["timing_method"] = (
            "Absolute time anchored to Windows laptop; source frame clock is used "
            "only for relative frame spacing and gap detection."
        )
        attrs["device_time_note"] = (
            "Device absolute timestamp is incorrect (2028) and is not used as real date/time."
        )
        attrs["start_epoch_utc"] = float(epoch)
        attrs["start_time_utc"] = datetime.fromtimestamp(epoch, timezone.utc).isoformat()
        attrs["start_time_local"] = datetime.fromtimestamp(epoch).astimezone().isoformat()
        attrs["end_epoch_utc"] = float(epoch)
        attrs["samples_per_channel"] = 0
        attrs["complete"] = 0

        chunk = max(320, int(round(fs * 10)))
        opts = dict(
            compression="gzip",
            compression_opts=1,
            shuffle=True,
            fletcher32=True,
        )
        self.time_ds = self.file.create_dataset(
            "time_epoch_utc",
            shape=(0,),
            maxshape=(None,),
            chunks=(chunk,),
            dtype="<f8",
            **opts,
        )
        self.raw_ds = self.file.create_dataset(
            "raw_counts",
            shape=(0, len(CHANNEL_NAMES)),
            maxshape=(None, len(CHANNEL_NAMES)),
            chunks=(chunk, len(CHANNEL_NAMES)),
            dtype="<i4",
            **opts,
        )
        self.physical_ds = self.file.create_dataset(
            "physical_values",
            shape=(0, len(CHANNEL_NAMES)),
            maxshape=(None, len(CHANNEL_NAMES)),
            chunks=(chunk, len(CHANNEL_NAMES)),
            dtype="<f8",
            **opts,
        )
        self.raw_ds.attrs["columns_json"] = json.dumps(CHANNEL_NAMES)
        self.raw_ds.attrs["units_json"] = json.dumps(["counts"] * len(CHANNEL_NAMES))
        self.physical_ds.attrs["columns_json"] = json.dumps(CHANNEL_NAMES)
        self.physical_ds.attrs["units_json"] = json.dumps(PHYSICAL_UNITS)
        self.physical_ds.attrs["calibration_version"] = CALIBRATION_VERSION
        print(f"[HDF5] OPEN {self.part_path}", flush=True)

    def _append_same_hour(
        self,
        times: np.ndarray,
        raw_values: np.ndarray,
        physical_values: np.ndarray,
        fs: float,
    ) -> None:
        if len(times) == 0:
            return
        key = self._hour_key(float(times[0]))
        if self.file is None or self.current_hour_key != key:
            self.close()
            self._open(float(times[0]), fs)

        assert self.file is not None
        assert self.time_ds is not None
        assert self.raw_ds is not None
        assert self.physical_ds is not None

        start = self.samples_written
        stop = start + len(times)
        self.time_ds.resize((stop,))
        self.raw_ds.resize((stop, len(CHANNEL_NAMES)))
        self.physical_ds.resize((stop, len(CHANNEL_NAMES)))
        self.time_ds[start:stop] = times
        self.raw_ds[start:stop, :] = raw_values
        self.physical_ds[start:stop, :] = physical_values
        self.samples_written = stop

        self.file.attrs["end_epoch_utc"] = float(times[-1])
        self.file.attrs["samples_per_channel"] = int(self.samples_written)

        now = time.monotonic()
        if now - self.last_flush >= FLUSH_SECONDS:
            self.file.flush()
            self.last_flush = now

    def append(
        self,
        times: np.ndarray,
        raw_values: np.ndarray,
        physical_values: np.ndarray,
        fs: float,
    ) -> None:
        if len(times) == 0:
            return

        keys = [self._hour_key(float(t)) for t in times]
        start = 0
        for i in range(1, len(times) + 1):
            if i == len(times) or keys[i] != keys[start]:
                self._append_same_hour(
                    times[start:i],
                    raw_values[start:i],
                    physical_values[start:i],
                    fs,
                )
                start = i

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
            self.physical_ds = None
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
        found: list[Path] = []
        for pattern in ("*service-account*.json", "*firebase*.json"):
            found.extend(desktop.glob(f"*\\{pattern}"))
            found.extend(desktop.glob(pattern))
        for p in found:
            if not p.is_file():
                continue
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
        self.root = None
        self.buffer_times: list[float] = []
        self.buffer_values: list[np.ndarray] = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.last_error = ""
        self.last_success_print = 0.0

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
            self.root = db.reference(FIREBASE_ROOT, app=app)
            self.enabled = True
            self.thread = threading.Thread(
                target=self._worker,
                name="MAG Firebase",
                daemon=True,
            )
            self.thread.start()
            print(f"[FIREBASE] ON {FIREBASE_ROOT} using {cred_path}", flush=True)
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            print(f"[FIREBASE] init error: {self.last_error}", flush=True)

    def add(self, times: np.ndarray, physical_values: np.ndarray) -> None:
        if not self.enabled:
            return
        with self.lock:
            if self.buffer_times and len(times):
                expected = 1.0 / EXPECTED_RATE
                if float(times[0]) - self.buffer_times[-1] > expected * 1.5:
                    self.buffer_times.clear()
                    self.buffer_values.clear()

            self.buffer_times.extend(float(x) for x in times)
            self.buffer_values.extend(
                np.asarray(row, dtype=np.float64).copy() for row in physical_values
            )

    def _take_packet(self):
        with self.lock:
            if len(self.buffer_times) < LIVE_PACKET_SAMPLES:
                return None
            n = LIVE_PACKET_SAMPLES
            tt = np.asarray(self.buffer_times[:n], dtype=np.float64)
            vv = np.asarray(self.buffer_values[:n], dtype=np.float64)

            expected = 1.0 / EXPECTED_RATE
            diffs = np.diff(tt)
            bad = np.where(np.abs(diffs - expected) > 1e-5)[0]
            if len(bad):
                cut = int(bad[0]) + 1
                del self.buffer_times[:cut]
                del self.buffer_values[:cut]
                return None

            del self.buffer_times[:n]
            del self.buffer_values[:n]
        return tt, vv

    def _worker(self) -> None:
        assert self.root is not None
        failures = 0
        slots = LIVE_HISTORY_SECONDS

        while not self.stop_event.is_set():
            packet = self._take_packet()
            if packet is None:
                self.stop_event.wait(0.05)
                continue

            tt, vv = packet
            t0_ms = int(round(float(tt[0]) * 1000.0))
            slot = f"{(t0_ms // 1000) % slots:03d}"

            record = {
                "t": t0_ms,
                "dt_ms": 1000.0 / EXPECTED_RATE,
                "format": "physical_v1",
            }
            for i, name in enumerate(CHANNEL_NAMES):
                # Six decimals are far beyond the practical resolution here and keep JSON compact.
                record[name] = [round(float(x), 6) for x in vv[:, i]]

            status = {
                "updated_ms": int(time.time() * 1000),
                "laptop_time_ms": int(round(float(tt[-1]) * 1000.0)),
                "sample_rate_hz": EXPECTED_RATE,
                "bucket_seconds": 1.0,
                "source": f"{HOST}:{PORT}",
                "time_source": "Windows laptop",
                "channels": list(CHANNEL_NAMES),
                "data_format": "physical_v1",
                "calibration_version": CALIBRATION_VERSION,
                "units": dict(zip(CHANNEL_NAMES, PHYSICAL_UNITS)),
            }

            try:
                self.root.child("points").child(slot).set(record)
                self.root.child("status").update(status)
                failures = 0

                now = time.monotonic()
                if self.last_success_print == 0.0 or now - self.last_success_print >= 30.0:
                    print(
                        f"[FIREBASE] OK {datetime.now().astimezone():%H:%M:%S} "
                        f"| physical_v1 | 8 channels | slot={slot}",
                        flush=True,
                    )
                    self.last_success_print = now

            except Exception as exc:
                failures += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
                delay = min(30.0, max(1.0, 1.7 ** min(failures, 7)))
                print(
                    f"[FIREBASE] send error: {self.last_error}; retry {delay:.1f}s",
                    flush=True,
                )
                with self.lock:
                    self.buffer_times = list(tt) + self.buffer_times
                    self.buffer_values = [row.copy() for row in vv] + self.buffer_values
                    cap = int(EXPECTED_RATE * LIVE_HISTORY_SECONDS)
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


def decode_frame(body: bytes, nch: int, ns: int) -> tuple[np.ndarray, np.ndarray]:
    metadata = np.frombuffer(body[:76], dtype="<i4", count=19).copy()
    raw = np.frombuffer(
        body[76:],
        dtype="<i4",
        count=nch * ns,
    ).reshape(nch, ns)
    selected = raw[np.asarray(SOURCE_CHANNELS), :].T.copy()
    return metadata, selected


def format_physical_row(row: np.ndarray) -> str:
    return " ".join(
        [
            f"MGX={row[0]:.2f}nT",
            f"MGY={row[1]:.2f}nT",
            f"MGZ={row[2]:.2f}nT",
            f"LSU={row[3]:.4f}um",
            f"LSV={row[4]:.4f}um",
            f"LSW={row[5]:.4f}um",
            f"TPR={row[6]:.2f}C",
            f"PRS={row[7]:.2f}hPa",
        ]
    )


def run() -> None:
    recorder = HDF5Recorder()
    firebase = FirebaseLive()

    timeline: LaptopAnchoredTimeline | None = None
    fs: float | None = None
    total_samples = 0
    successful_sessions = 0
    last_report = time.monotonic()

    print("=" * 78)
    print("IGS MAG — CALIBRATED HDF5 + FIREBASE LIVE")
    print(f"Source: {HOST}:{PORT} (READ ONLY)")
    print(f"HDF5:  {RECORD_ROOT}")
    print("HDF5 datasets: raw_counts + physical_values")
    print(f"Physical channels: {', '.join(f'{n}[{u}]' for n, u in zip(CHANNEL_NAMES, PHYSICAL_UNITS))}")
    print(f"Calibration: {CALIBRATION_VERSION}")
    print("Time: Windows laptop absolute clock")
    print("Device 2028 timestamp: relative spacing/gap detection only")
    print("Stop: Ctrl+C")
    print("=" * 78)

    try:
        while True:
            sock: socket.socket | None = None
            try:
                sock = socket.create_connection((HOST, PORT), timeout=5.0)
                sock.settimeout(4.0)

                header = recv_exact(sock, 16)
                whole, frac_micro, nch, ns = struct.unpack("<4i", header)
                fs_now = float(whole) + float(frac_micro) / 1_000_000.0

                if nch != EXPECTED_CHANNELS or ns != EXPECTED_SAMPLES_PER_FRAME:
                    raise RuntimeError(
                        f"unexpected stream header: fs={fs_now}, nch={nch}, ns={ns}"
                    )
                if abs(fs_now - EXPECTED_RATE) > 1e-6:
                    raise RuntimeError(f"unexpected sample rate: {fs_now}")

                if fs is None:
                    fs = fs_now
                    timeline = LaptopAnchoredTimeline(fs, ns)

                successful_sessions += 1
                frame_size = 76 + nch * ns * 4

                while True:
                    body = recv_exact(sock, frame_size)
                    receive_end = time.time()
                    metadata, raw_values = decode_frame(body, nch, ns)
                    src_epoch = source_frame_epoch(metadata)

                    assert timeline is not None and fs is not None
                    times = timeline.frame_times(src_epoch, receive_end)
                    physical_values = calibrate_values(raw_values)

                    recorder.append(times, raw_values, physical_values, fs)
                    firebase.add(times, physical_values)
                    total_samples += ns

                    now = time.monotonic()
                    if now - last_report >= 10.0:
                        dt = datetime.fromtimestamp(float(times[-1])).astimezone()
                        vals = format_physical_row(physical_values[-1])
                        print(
                            f"[OK] {dt:%Y-%m-%d %H:%M:%S} | "
                            f"samples/ch={total_samples} | sessions={successful_sessions} | {vals}",
                            flush=True,
                        )
                        last_report = now

            except EOFError:
                pass
            except (ConnectionError, TimeoutError, socket.timeout, OSError, RuntimeError) as exc:
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
