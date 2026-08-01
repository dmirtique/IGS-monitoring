from __future__ import annotations

import os
import time
from collections import deque
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field


APP_TITLE = "IGS Monitoring"
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "")
MAX_POINTS = int(os.getenv("MAX_POINTS", "12000"))
OFFLINE_AFTER_SECONDS = float(os.getenv("OFFLINE_AFTER_SECONDS", "15"))

app = FastAPI(title=APP_TITLE)

# Short in-memory online buffer. Full raw files remain on the station HDD.
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
        "station_online": (
            state["last_ingest_unix"] > 0 and age <= OFFLINE_AFTER_SECONDS
        ),
        "last_data_age_seconds": (
            None if state["last_ingest_unix"] == 0 else round(age, 3)
        ),
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
            "last_data_age_seconds": (
                None if state["last_ingest_unix"] == 0 else age
            ),
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
      color-scheme: dark;
      --bg: #08111f;
      --panel: #101b2e;
      --panel-2: #0c1728;
      --text: #edf4ff;
      --muted: #94a7c5;
      --border: #263a5c;
      --ok: #3ddc97;
      --bad: #ff6b78;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: Inter, system-ui, -apple-system, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
    }

    .wrap {
      width: min(1540px, calc(100% - 28px));
      margin: 0 auto;
      padding: 18px 0 34px;
    }

    .top {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: flex-start;
      gap: 14px;
      margin-bottom: 14px;
    }

    h1 {
      margin: 0 0 5px;
      font-size: clamp(21px, 2vw, 29px);
      font-weight: 720;
    }

    .meta {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.55;
    }

    .top-right {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: flex-end;
      gap: 9px;
    }

    .badge,
    .control {
      min-height: 38px;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 7px 11px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: var(--panel);
      color: var(--text);
      font-size: 14px;
    }

    .control select,
    .control button {
      border: 0;
      outline: 0;
      background: transparent;
      color: var(--text);
      font: inherit;
      cursor: pointer;
    }

    .dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      background: var(--bad);
      box-shadow: 0 0 0 3px rgba(255, 107, 120, 0.12);
    }

    .dot.ok {
      background: var(--ok);
      box-shadow: 0 0 0 3px rgba(61, 220, 151, 0.12);
    }

    .summary {
      display: grid;
      grid-template-columns: repeat(4, minmax(170px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }

    .summary-item {
      padding: 11px 13px;
      border: 1px solid var(--border);
      border-radius: 12px;
      background: var(--panel);
    }

    .summary-label {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 4px;
    }

    .summary-value {
      font-size: 15px;
      font-weight: 650;
      overflow-wrap: anywhere;
    }

    .stack {
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }

    .card {
      width: 100%;
      padding: 14px;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: var(--panel);
      overflow: hidden;
    }

    .channel-head {
      display: flex;
      flex-wrap: wrap;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 10px;
    }

    .channel-title h2 {
      margin: 0 0 4px;
      font-size: 18px;
      font-weight: 700;
    }

    .channel-unit {
      color: var(--muted);
      font-size: 13px;
    }

    .stats {
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 8px;
      flex: 1 1 900px;
    }

    .stat {
      min-width: 0;
      padding: 8px 10px;
      border: 1px solid rgba(118, 149, 192, 0.22);
      border-radius: 9px;
      background: var(--panel-2);
    }

    .stat-label {
      color: var(--muted);
      font-size: 11px;
      margin-bottom: 3px;
      white-space: nowrap;
    }

    .stat-value {
      font-size: 13px;
      font-weight: 650;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .chart-box {
      height: 340px;
      position: relative;
    }

    .empty-note {
      display: none;
      position: absolute;
      inset: 0;
      place-items: center;
      color: var(--muted);
      font-size: 14px;
      pointer-events: none;
    }

    .empty-note.show { display: grid; }

    .footer-note {
      margin-top: 13px;
      color: var(--muted);
      font-size: 12px;
      text-align: center;
    }

    @media (max-width: 1100px) {
      .summary { grid-template-columns: repeat(2, minmax(180px, 1fr)); }
      .stats { grid-template-columns: repeat(3, minmax(130px, 1fr)); }
      .chart-box { height: 310px; }
    }

    @media (max-width: 650px) {
      .wrap { width: min(100% - 16px, 1540px); }
      .summary { grid-template-columns: 1fr; }
      .stats { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
      .chart-box { height: 270px; }
      .top-right { justify-content: flex-start; }
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

    <div class="top-right">
      <label class="control">
        Інтервал:
        <select id="windowSeconds">
          <option value="30">30 с</option>
          <option value="60">60 с</option>
          <option value="120" selected>120 с</option>
          <option value="180">180 с</option>
        </select>
      </label>

      <label class="control">
        <input type="checkbox" id="pauseUpdates">
        Пауза
      </label>

      <div class="badge">
        <span class="dot" id="statusDot"></span>
        <span id="statusText">Станція офлайн</span>
      </div>
    </div>
  </div>

  <section class="summary">
    <div class="summary-item">
      <div class="summary-label">Час станції</div>
      <div class="summary-value" id="stationTime">—</div>
    </div>
    <div class="summary-item">
      <div class="summary-label">Частота дискретизації</div>
      <div class="summary-value" id="sampleRate">—</div>
    </div>
    <div class="summary-item">
      <div class="summary-label">GPS</div>
      <div class="summary-value" id="gpsState">—</div>
    </div>
    <div class="summary-item">
      <div class="summary-label">Останнє оновлення</div>
      <div class="summary-value" id="lastUpdate">—</div>
    </div>
  </section>

  <main class="stack">
    <section class="card">
      <div class="channel-head">
        <div class="channel-title">
          <h2>HHV</h2>
          <div class="channel-unit">Швидкість, м/с</div>
        </div>
        <div class="stats" id="stats-HHV"></div>
      </div>
      <div class="chart-box">
        <canvas id="HHV"></canvas>
        <div class="empty-note" id="empty-HHV">У вибраному інтервалі немає даних</div>
      </div>
    </section>

    <section class="card">
      <div class="channel-head">
        <div class="channel-title">
          <h2>HHW</h2>
          <div class="channel-unit">Швидкість, м/с</div>
        </div>
        <div class="stats" id="stats-HHW"></div>
      </div>
      <div class="chart-box">
        <canvas id="HHW"></canvas>
        <div class="empty-note" id="empty-HHW">У вибраному інтервалі немає даних</div>
      </div>
    </section>

    <section class="card">
      <div class="channel-head">
        <div class="channel-title">
          <h2>HHU</h2>
          <div class="channel-unit">Швидкість, м/с</div>
        </div>
        <div class="stats" id="stats-HHU"></div>
      </div>
      <div class="chart-box">
        <canvas id="HHU"></canvas>
        <div class="empty-note" id="empty-HHU">У вибраному інтервалі немає даних</div>
      </div>
    </section>

    <section class="card">
      <div class="channel-head">
        <div class="channel-title">
          <h2>ACC</h2>
          <div class="channel-unit">Прискорення, м/с²</div>
        </div>
        <div class="stats" id="stats-ACC"></div>
      </div>
      <div class="chart-box">
        <canvas id="ACC"></canvas>
        <div class="empty-note" id="empty-ACC">У вибраному інтервалі немає даних</div>
      </div>
    </section>
  </main>

  <div class="footer-note">
    Повні сирі записи зберігаються локально на станції; сторінка показує
    проріджений онлайн-потік зі збереженням мінімумів і максимумів.
  </div>
</div>

<script>
const CHANNELS = ["HHV", "HHW", "HHU", "ACC"];
const G_STD = 9.80665;
const charts = {};
let lastPayload = null;

function localTime(epochSeconds, withMs = false) {
  if (!Number.isFinite(epochSeconds)) return "—";
  return new Date(epochSeconds * 1000).toLocaleTimeString("uk-UA", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: withMs ? 3 : undefined
  });
}

function scientific(value, digits = 4) {
  if (!Number.isFinite(value)) return "—";
  if (value === 0) return "0";
  const abs = Math.abs(value);
  if (abs >= 0.001 && abs < 1000) {
    return value.toLocaleString("uk-UA", {
      maximumSignificantDigits: digits,
      minimumSignificantDigits: Math.min(2, digits)
    });
  }
  return value.toExponential(Math.max(1, digits - 1));
}

function detailValue(channel, value) {
  if (!Number.isFinite(value)) return "—";
  if (channel === "ACC") {
    return `${scientific(value)} м/с² · ${scientific(value / G_STD)} g`;
  }
  return `${scientific(value)} м/с · ${scientific(value * 1000)} мм/с`;
}

function computeStats(points) {
  if (!points.length) return null;

  let minPoint = points[0];
  let maxPoint = points[0];
  let peakPoint = points[0];
  let sumSquares = 0;

  for (const point of points) {
    if (point.y < minPoint.y) minPoint = point;
    if (point.y > maxPoint.y) maxPoint = point;
    if (Math.abs(point.y) > Math.abs(peakPoint.y)) peakPoint = point;
    sumSquares += point.y * point.y;
  }

  return {
    latest: points[points.length - 1],
    minPoint,
    maxPoint,
    peakPoint,
    rms: Math.sqrt(sumSquares / points.length),
    count: points.length,
    span: points[points.length - 1].t - points[0].t
  };
}

function statBox(label, value, title = "") {
  return `
    <div class="stat" title="${title || value}">
      <div class="stat-label">${label}</div>
      <div class="stat-value">${value}</div>
    </div>
  `;
}

function updateStats(channel, points) {
  const target = document.getElementById(`stats-${channel}`);
  const stats = computeStats(points);

  if (!stats) {
    target.innerHTML = [
      statBox("Поточне", "—"),
      statBox("Макс. |значення|", "—"),
      statBox("RMS", "—"),
      statBox("Мінімум", "—"),
      statBox("Максимум", "—"),
      statBox("Точок / інтервал", "0 / —")
    ].join("");
    return;
  }

  target.innerHTML = [
    statBox(
      "Поточне",
      detailValue(channel, stats.latest.y),
      `Час: ${localTime(stats.latest.t, true)}`
    ),
    statBox(
      "Макс. |значення|",
      detailValue(channel, Math.abs(stats.peakPoint.y)),
      `Пік: ${stats.peakPoint.y >= 0 ? "+" : ""}${detailValue(channel, stats.peakPoint.y)}; час ${localTime(stats.peakPoint.t, true)}`
    ),
    statBox("RMS", detailValue(channel, stats.rms)),
    statBox(
      "Мінімум",
      detailValue(channel, stats.minPoint.y),
      `Час: ${localTime(stats.minPoint.t, true)}`
    ),
    statBox(
      "Максимум",
      detailValue(channel, stats.maxPoint.y),
      `Час: ${localTime(stats.maxPoint.t, true)}`
    ),
    statBox(
      "Точок / інтервал",
      `${stats.count} / ${stats.span.toFixed(1)} с`
    )
  ].join("");
}

function insertGapSeparators(points, gapSeconds = 2.0) {
  if (points.length < 2) return points.map(p => ({x: p.t, y: p.y}));

  const series = [{x: points[0].t, y: points[0].y}];
  for (let i = 1; i < points.length; i++) {
    const previous = points[i - 1];
    const current = points[i];
    if ((current.t - previous.t) > gapSeconds) {
      series.push({x: previous.t + 0.000001, y: null});
    }
    series.push({x: current.t, y: current.y});
  }
  return series;
}

function visiblePoints(points, windowSeconds) {
  if (!points.length) return [];
  const newest = points[points.length - 1].t;
  const lower = newest - windowSeconds;
  return points.filter(p => p.t >= lower && p.t <= newest);
}

for (const name of CHANNELS) {
  charts[name] = new Chart(document.getElementById(name), {
    type: "line",
    data: {
      datasets: [{
        data: [],
        borderColor: "#4db2ff",
        backgroundColor: "rgba(77, 178, 255, 0.08)",
        borderWidth: 1.05,
        pointRadius: 0,
        pointHitRadius: 8,
        tension: 0,
        spanGaps: false
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      parsing: false,
      normalized: true,
      interaction: {
        mode: "nearest",
        intersect: false,
        axis: "x"
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          displayColors: false,
          callbacks: {
            title: items => items.length ? localTime(items[0].parsed.x, true) : "",
            label: context => detailValue(name, context.parsed.y)
          }
        }
      },
      scales: {
        x: {
          type: "linear",
          ticks: {
            maxTicksLimit: 9,
            color: "#94a7c5",
            callback: value => localTime(Number(value), false)
          },
          grid: { color: "rgba(147, 169, 204, 0.14)" },
          title: {
            display: true,
            text: "Час",
            color: "#94a7c5"
          }
        },
        y: {
          grace: "8%",
          ticks: {
            color: "#94a7c5",
            callback: value => scientific(Number(value), 4)
          },
          grid: { color: "rgba(147, 169, 204, 0.14)" },
          title: {
            display: true,
            text: name === "ACC" ? "м/с²" : "м/с",
            color: "#94a7c5"
          }
        }
      }
    }
  });

  updateStats(name, []);
}

function renderPayload(payload) {
  lastPayload = payload;

  document.getElementById("statusDot")
    .classList.toggle("ok", payload.station_online);

  document.getElementById("statusText").textContent =
    payload.station_online ? "Станція онлайн" : "Станція офлайн";

  const ageText = payload.last_data_age_seconds == null
    ? "дані ще не надходили"
    : `останні дані ${payload.last_data_age_seconds.toFixed(1)} с тому`;

  document.getElementById("meta").textContent =
    `${ageText} · онлайн-буфер оновлюється раз на секунду`;

  document.getElementById("stationTime").textContent =
    payload.station_time || "—";

  document.getElementById("sampleRate").textContent =
    payload.sample_rate_hz == null
      ? "—"
      : `${Number(payload.sample_rate_hz).toLocaleString("uk-UA")} Гц`;

  document.getElementById("gpsState").textContent =
    payload.gps_ok === true
      ? "Синхронізовано"
      : payload.gps_ok === false
        ? "Немає сигналу"
        : "Невідомо";

  document.getElementById("lastUpdate").textContent =
    payload.last_data_age_seconds == null
      ? "—"
      : `${payload.last_data_age_seconds.toFixed(1)} с тому`;

  const windowSeconds =
    Number(document.getElementById("windowSeconds").value);

  for (const name of CHANNELS) {
    const raw = (payload.channels[name] || [])
      .filter(p => Number.isFinite(p.t) && Number.isFinite(p.y))
      .sort((a, b) => a.t - b.t);

    const visible = visiblePoints(raw, windowSeconds);
    const chartSeries = insertGapSeparators(visible);

    charts[name].data.datasets[0].data = chartSeries;
    charts[name].update("none");

    updateStats(name, visible);
    document.getElementById(`empty-${name}`)
      .classList.toggle("show", visible.length === 0);
  }
}

async function refresh() {
  if (document.getElementById("pauseUpdates").checked) return;

  try {
    const response = await fetch("/api/data", {cache: "no-store"});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const payload = await response.json();
    renderPayload(payload);
  } catch (error) {
    document.getElementById("statusDot").classList.remove("ok");
    document.getElementById("statusText").textContent =
      "Сервер недоступний";
    document.getElementById("meta").textContent =
      `Помилка оновлення: ${error.message}`;
  }
}

document.getElementById("windowSeconds").addEventListener("change", () => {
  if (lastPayload) renderPayload(lastPayload);
});

document.getElementById("pauseUpdates").addEventListener("change", event => {
  if (!event.target.checked) refresh();
});

setInterval(refresh, 1000);
refresh();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return DASHBOARD_HTML
