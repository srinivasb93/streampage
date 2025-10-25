# Streampage (Streamlit App)

This is a multi-page Streamlit app for financial analytics. It connects to PostgreSQL databases via SQLAlchemy and uses external data sources (Upstox, NSE).

## Prerequisites

- PostgreSQL reachable from the app (create DBs: `nsedata`, `analytics`, `mfdata`, `trading_db`)
- Upstox access token (env var `UPSTOX_ACCESS_TOKEN`)
- `config.ini` with PostgreSQL credentials (see `config.sample.ini`)

## Configure

1. Copy the sample config and edit with your values:

   ```bash
   cp config.sample.ini config.ini
   # edit config.ini
   ```

2. Export Upstox token (or set via Docker `-e`):

   ```bash
export UPSTOX_ACCESS_TOKEN=YOUR_TOKEN
```

Alternatively, the app can read the Upstox token from PostgreSQL `trading_db.public.users`. It looks for a token column among `upstox_access_token`, `access_token`, or `token`, and prefers the most recent row by `updated_at`, `last_updated`, or `modified_at` if present. If the env var is set, it takes precedence.

## Run with Docker (recommended)

```bash
docker build -t streampage .
docker run \
  -p 8501:8501 \
  -e UPSTOX_ACCESS_TOKEN=$UPSTOX_ACCESS_TOKEN \
  # IMPORTANT: When Postgres runs on your host machine, set POSTGRES_HOST so the
  # container can reach it. On macOS/Windows use host.docker.internal. On Linux
  # also add the host gateway mapping shown below.
  -e POSTGRES_HOST=host.docker.internal \
  -v $(pwd)/config.ini:/app/config.ini:ro \
  streampage
```

Open http://localhost:8501

On Linux (native or WSL2), add a host-gateway mapping so `host.docker.internal` resolves **and** mount your `.env` alongside `config.ini` so `python-dotenv` can load it:

```bash
docker run \
  --add-host=host.docker.internal:host-gateway \
  -p 8501:8501 \
  -e UPSTOX_ACCESS_TOKEN=$UPSTOX_ACCESS_TOKEN \
  -e POSTGRES_HOST=host.docker.internal \
  -v $(pwd)/config.ini:/app/config.ini:ro \
  -v $(pwd)/.env:/app/.env:ro \
  streampage
```

> Tip: if you prefer passing all secrets via environment variables, swap the `.env` bind mount for an `--env-file .env` flag.

Notes:
- The app already reads `config.ini` for `[postgres]` settings, but any of these env vars will override it: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE`.
- If Postgres runs in another container on the same user-defined Docker network, set `POSTGRES_HOST` to that service name instead (or use Docker Compose).

## Run on Ubuntu with Python venv

```bash
sudo apt update && sudo apt install -y \
  python3.11 python3.11-venv python3-pip libpq-dev build-essential

python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

export UPSTOX_ACCESS_TOKEN=YOUR_TOKEN
streamlit run streamlit_app.py
```

Then visit http://localhost:8501

## Notes

- The app creates DB engines using the same host/user/password for multiple databases. Ensure the databases exist and the user has privileges.
- If you see PostgreSQL driver errors, ensure `psycopg2-binary` is installed (already in `requirements.txt`).
- The `pages/` directory contains additional Streamlit pages (e.g., data load utilities).
 - Upstox token source: env var `UPSTOX_ACCESS_TOKEN` takes precedence; otherwise the app will query `trading_db.public.users` for a token column.
