# IGS Monitoring

Public Render server for the ZIR-8 online dashboard.

## Render settings

- Runtime: Python 3
- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Health check path: `/health`

## Required environment variable

Create a secret environment variable:

- `INGEST_TOKEN` — long random secret used by the local bridge near the station.

The public dashboard is `/`.
The station-side bridge sends prepared channel points to `POST /api/ingest`.
