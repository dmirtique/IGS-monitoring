"use strict";

const { onSchedule } = require("firebase-functions/v2/scheduler");
const { defineSecret } = require("firebase-functions/params");
const { initializeApp } = require("firebase-admin/app");
const { getDatabase } = require("firebase-admin/database");

initializeApp({
  databaseURL: "https://igs-monitoring-default-rtdb.europe-west1.firebasedatabase.app"
});

const TELEGRAM_BOT_TOKEN = defineSecret("TELEGRAM_BOT_TOKEN");
const CHAT_ID = "-1004425577425";
const LIMIT_MS = 300000;
const STATIONS = {
  zir8: { label: "ST106", path: "/public/status", field: "updated_ms" },
  st107: { label: "ST107", path: "/public/st107/status", field: "station_time_ms" },
  mag: { label: "MAG", path: "/public/mag/status", field: "updated_ms" },
  s2dw: { label: "S2DW", path: "/public/s2dw/status", field: "station_time_ms" }
};

async function sendTelegram(token, message) {
  const response = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ chat_id: CHAT_ID, text: message }),
    signal: AbortSignal.timeout(20000)
  });
  const result = await response.json();
  if (!response.ok || !result.ok) throw new Error(`Telegram error: ${response.status} ${JSON.stringify(result)}`);
}

function kyivDate(ms) {
  if (ms === null) return "невідомо";
  return new Intl.DateTimeFormat("uk-UA", {
    timeZone: "Europe/Kyiv", dateStyle: "short", timeStyle: "medium"
  }).format(new Date(ms));
}

exports.stationWatchdog = onSchedule({
  schedule: "* * * * *",
  timeZone: "Etc/UTC",
  region: "europe-west1",
  secrets: [TELEGRAM_BOT_TOKEN],
  timeoutSeconds: 120,
  maxInstances: 1
}, async () => {
  const db = getDatabase();
  const token = TELEGRAM_BOT_TOKEN.value();
  const now = Date.now();
  const errors = [];

  for (const [key, config] of Object.entries(STATIONS)) {
    try {
      const status = (await db.ref(config.path).get()).val() || {};
      const raw = status[config.field];
      const updated = raw == null ? null : Number(raw);
      if (updated !== null && (!Number.isFinite(updated) || updated <= 0))
        throw new Error(`Invalid timestamp for ${key}: ${raw}`);

      const age = updated === null ? null : Math.max(0, now - updated);
      const current = age !== null && age < LIMIT_MS ? "online" : "offline";
      const stateRef = db.ref(`/watchdog/stations/${key}`);
      const old = (await stateRef.get()).val();
      const previous = old && old.state;

      if (previous === current) {
        console.log(`${key}: unchanged ${current}, age=${age}`);
        continue;
      }

      if (previous === undefined && current === "online") {
        await stateRef.set({ state: current, changed_ms: now });
        console.log(`${key}: initial online`);
        continue;
      }

      const message = current === "online"
        ? `🟢 ${config.label} онлайн\nПередача даних відновлена.`
        : `🔴 ${config.label} офлайн\nОстаннє оновлення: ${kyivDate(updated)}\nНемає нових даних: ${age === null ? "невідомо" : (age / 60000).toFixed(1) + " хв"}`;

      // Never advance state if Telegram did not acknowledge the alert.
      await sendTelegram(token, message);
      await stateRef.set({ state: current, changed_ms: now, last_data_ms: updated });
      console.log(`${key}: notified ${current}`);
    } catch (err) {
      console.error(`${key}: ${err.stack || err}`);
      errors.push(key);
    }
  }
  if (errors.length) throw new Error(`Failed stations: ${errors.join(", ")}`);
});
