# IGS external Telegram watchdog

Cloud Function runs independently of the monitoring laptop and local internet.
Every minute checks ST106, ST107, MAG and S2DW in Firebase RTDB. After five
minutes without new data sends a single offline alert; sends one recovery alert
when fresh data resumes. State is stored in RTDB at `/watchdog/stations`.

## Deployment (requires Firebase project Owner/Editor and Blaze billing)

```bash
npm install -g firebase-tools
firebase login
firebase use igs-monitoring
cd firebase-watchdog
npm install
firebase functions:secrets:set TELEGRAM_BOT_TOKEN
cd ..
firebase deploy --only functions:stationWatchdog
```

Use root `firebase.json` for the functions source path. The Telegram token
must be provided via Firebase Secret Manager, never committed to GitHub.
Scheduled Functions require billing and provision Cloud Scheduler.
Disable the old GitHub Actions watchdog **after** the Cloud Function is deployed
and verified, to avoid duplicate alerts.
