from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field


APP_TITLE = "IGS Monitoring"
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "")
MAX_POINTS = int(os.getenv("MAX_POINTS", "12000"))
OFFLINE_AFTER_SECONDS = float(os.getenv("OFFLINE_AFTER_SECONDS", "15"))

app = FastAPI(title=APP_TITLE)

# In-memory short online buffer. Full raw files remain on the station HDD.
buffers: dict[str, deque[dict[str, float]]] = {
    "HHV": deque(maxlen=MAX_POINTS),
    "HHW": deque(maxlen=MAX_POINTS),
    "HHU": deque(maxlen=MAX_POINTS),
    "ACC": deque(maxlen=MAX_POINTS),
}

state: dict[str, Any] = {
    "last_ingest_unix": 0.0,
    "station_time": None,
    "sample_rate_hz": None,
    "gps_ok": None,
    "source": "ZIR-8",
}


class Point(BaseModel):
    t: float
    y: float


class IngestPayload(BaseModel):
    station_time: str | None = None
    sample_rate_hz: float | None = None
    gps_ok: bool | None = None
    channels: dict[str, list[Point]] = Field(default_factory=dict)


def require_ingest_token(authorization: str | None) -> None:
    if not INGEST_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="INGEST_TOKEN is not configured on the server.",
        )
    expected = f"Bearer {INGEST_TOKEN}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Invalid ingest token.")


@app.get("/health")
async def health() -> dict[str, Any]:
    age = time.time() - float(state["last_ingest_unix"] or 0.0)
    return {
        "ok": True,
        "station_online": age <= OFFLINE_AFTER_SECONDS,
        "last_data_age_seconds": None if state["last_ingest_unix"] == 0 else round(age, 3),
    }


@app.post("/api/ingest")
async def ingest(
    payload: IngestPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    require_ingest_token(authorization)

    accepted = 0
    for channel, points in payload.channels.items():
        if channel not in buffers:
            continue
        target = buffers[channel]
        for point in points:
            target.append({"t": float(point.t), "y": float(point.y)})
            accepted += 1

    state["last_ingest_unix"] = time.time()
    state["station_time"] = payload.station_time
    state["sample_rate_hz"] = payload.sample_rate_hz
    state["gps_ok"] = payload.gps_ok

    return {"ok": True, "accepted_points": accepted}


@app.get("/api/data")
async def data() -> JSONResponse:
    age = time.time() - float(state["last_ingest_unix"] or 0.0)
    online = state["last_ingest_unix"] > 0 and age <= OFFLINE_AFTER_SECONDS

    return JSONResponse(
        {
            "station_online": online,
            "last_data_age_seconds": None if state["last_ingest_unix"] == 0 else age,
            "station_time": state["station_time"],
            "sample_rate_hz": state["sample_rate_hz"],
            "gps_ok": state["gps_ok"],
            "channels": {name: list(values) for name, values in buffers.items()},
        },
        headers={"Cache-Control": "no-store"},
    )


DASHBOARD_HTML = r"""
<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>IGS Monitoring</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.9/dist/chart.umd.min.js"></script>
  <style>
    :root {
      color-scheme: light dark;
      --bg: #0b1220;
      --card: #121b2e;
      --text: #edf3ff;
      --muted: #9fb0cc;
      --border: #263554;
      --ok: #38d996;
      --bad: #ff6b6b;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: system-ui, -apple-system, Segoe UI, sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    .wrap { max-width: 1500px; margin: 0 auto; padding: 18px; }
    .top {
      display: flex; flex-wrap: wrap; gap: 12px;
      justify-content: space-between; align-items: center;
      margin-bottom: 14px;
    }
    h1 { font-size: 22px; margin: 0; font-weight: 650; }
    .meta { color: var(--muted); font-size: 14px; }
    .badge {
      display: inline-flex; align-items: center; gap: 8px;
      padding: 7px 11px; border: 1px solid var(--border);
      border-radius: 999px; background: var(--card);
    }
    .dot { width: 10px; height: 10px; border-radius: 50%; background: var(--bad); }
    .dot.ok { background: var(--ok); }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
    .card {
      border: 1px solid var(--border);
      border-radius: 14px;
      background: var(--card);
      padding: 12px;
      min-height: 320px;
    }
    .card h2 { margin: 0 0 8px; font-size: 16px; font-weight: 600; }
    .sub { color: var(--muted); font-size: 13px; margin-bottom: 8px; }
    .chart-box { height: 260px; }
    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div>
      <h1>Онлайн-моніторинг ZIR-8</h1>
      <div class="meta" id="meta">Очікування даних…</div>
    </div>
    <div class="badge">
      <span class="dot" id="statusDot"></span>
      <span id="statusText">Станція офлайн</span>
    </div>
  </div>

  <div class="grid">
    <section class="card">
      <h2>HHV</h2>
      <div class="sub">Швидкість, м/с</div>
      <div class="chart-box"><canvas id="HHV"></canvas></div>
    </section>
    <section class="card">
      <h2>HHW</h2>
      <div class="sub">Швидкість, м/с</div>
      <div class="chart-box"><canvas id="HHW"></canvas></div>
    </section>
    <section class="card">
      <h2>HHU</h2>
      <div class="sub">Швидкість, м/с</div>
      <div class="chart-box"><canvas id="HHU"></canvas></div>
    </section>
    <section class="card">
      <h2>ACC</h2>
      <div class="sub">Прискорення, м/с²</div>
      <div class="chart-box"><canvas id="ACC"></canvas></div>
    </section>
  </div>
</div>

<script>
const names = ["HHV", "HHW", "HHU", "ACC"];
const charts = {};

for (const name of names) {
  const ctx = document.getElementById(name);
  charts[name] = new Chart(ctx, {
    type: "line",
    data: { datasets: [{ data: [], borderWidth: 1, pointRadius: 0, tension: 0 }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      parsing: false,
      normalized: true,
      plugins: { legend: { display: false } },
      scales: {
        x: {
          type: "linear",
          ticks: {
            maxTicksLimit: 6,
            callback: v => new Date(v * 1000).toLocaleTimeString("uk-UA")
          },
          grid: { color: "rgba(150,170,200,0.12)" }
        },
        y: {
          grid: { color: "rgba(150,170,200,0.12)" }
        }
      }
    }
  });
}

async function refresh() {
  try {
    const res = await fetch("/api/data", { cache: "no-store" });
    const payload = await res.json();

    document.getElementById("statusDot").classList.toggle("ok", payload.station_online);
    document.getElementById("statusText").textContent =
      payload.station_online ? "Станція онлайн" : "Станція офлайн";

    const age = payload.last_data_age_seconds == null
      ? "дані ще не надходили"
      : `останні дані ${payload.last_data_age_seconds.toFixed(1)} с тому`;

    const gps = payload.gps_ok === true ? "GPS: OK"
      : payload.gps_ok === false ? "GPS: немає"
      : "GPS: невідомо";

    const fs = payload.sample_rate_hz == null
      ? "частота невідома"
      : `${payload.sample_rate_hz} Гц`;

    document.getElementById("meta").textContent =
      `${age} · ${fs} · ${gps}`;

    for (const name of names) {
      const points = (payload.channels[name] || []).map(p => ({x: p.t, y: p.y}));
      charts[name].data.datasets[0].data = points;
      charts[name].update("none");
    }
  } catch (err) {
    document.getElementById("statusDot").classList.remove("ok");
    document.getElementById("statusText").textContent = "Сервер недоступний";
  }
}

setInterval(refresh, 1000);
refresh();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return DASHBOARD_HTML
