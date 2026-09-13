# Connecting Streampage to PostgreSQL on AWS EC2

Streampage resolves its database target from `DB_TARGET`, the same switch
`falcon_app` uses (`backend/app/env_bootstrap.py`). Pointing the app at the EC2
database is a one-variable change, not a code change and not a second env file.

| `DB_TARGET` | Endpoint | Used for |
|---|---|---|
| `local` (default) | `POSTGRES_HOST:POSTGRES_PORT` → `127.0.0.1:5432` | Local development |
| `ec2` | `127.0.0.1:EC2_DB_TUNNEL_PORT` → `127.0.0.1:15432` | The EC2 database, over the SSM tunnel |

## Why a tunnel and not a direct connection

The `ec2` target goes through an **AWS Systems Manager port-forward**, so:

- port 5432 stays closed in the instance security group — the database is not
  reachable from the internet;
- no bastion host, key pair, or public database endpoint is needed;
- the SSM session is already encrypted, so `sslmode` is not required.

Instance `i-0ac6de1a71abf4794`, AWS profile `dev` — the same instance and profile
`falcon_app` uses.

## One-time prerequisites

- AWS CLI v2 on `PATH`
- The AWS Session Manager plugin on `PATH` (`session-manager-plugin --version`)
- An authenticated `dev` profile (`aws sts get-caller-identity --profile dev`)

## Run

Terminal 1 — start the forward:

```powershell
.\scripts\run-ec2-db-tunnel.ps1
```

If a forward already owns port 15432 — from a previous run, or from the legacy
`C:\Users\sba400\start-tunnels.ps1` in the falcon workflow — the script reports
the owning process, starts nothing, stops nothing, and exits 0. That existing
forward works; just reuse it.

Terminal 2 — run the app against EC2:

```powershell
$env:DB_TARGET = 'ec2'
streamlit run streamlit_app.py
```

Unset `DB_TARGET` (or set it to `local`) and the app connects to local
PostgreSQL exactly as before. `DB_TARGET=local` is the default in `.env`, so an
ordinary `streamlit run` is unchanged.

Confirm which server you actually reached — do this before concluding anything
from application behaviour:

```powershell
$env:DB_TARGET = 'ec2'
python scripts\check_db_target.py
```

It prints the resolved target, the password-masked URLs, and a live row count
from both databases. The EC2 `nsedata` holds ~1514 public tables against the
local ~1512, so the counts also tell the two servers apart.

## Running on the instance itself

Deployed on EC2 the app talks to PostgreSQL over the host loopback — no tunnel,
so leave `DB_TARGET` at `local`:

```bash
docker compose --env-file .env -f docker-compose.production.yml up -d --build
```

`network_mode: host` is what makes `127.0.0.1:5432` resolve to the instance's
PostgreSQL. `DB_TARGET=ec2` is for reaching EC2 *from a workstation*; it would be
wrong on the instance, where the tunnel port does not exist.

## Settings reference

| Variable | Default | Notes |
|---|---|---|
| `DB_TARGET` | `local` | `local` or `ec2`. An unrecognized value falls back to `local`. |
| `EC2_DB_TUNNEL_PORT` | `15432` | Local port of the SSM forward. Only used when `DB_TARGET=ec2`. |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `127.0.0.1` / `5432` | The `local` endpoint. |
| `APP_DB_USER` / `APP_DB_PASSWORD` | `trading_user` / … | Percent-encoded into the URL, so `@ / : #` are safe. |
| `EC2_APP_DB_USER` / `EC2_APP_DB_PASSWORD` / `EC2_POSTGRES_HOST` | unset | Honored only when `DB_TARGET=ec2`. Set only if the EC2 credentials differ from local. |
| `NSE_DATA_DB_NAME` / `TRADING_DB_NAME` | `nsedata` / `trading_db` | `EC2_`-prefixed overrides also apply. |
| `POSTGRES_CONNECT_TIMEOUT` | `10` | Keeps a dead tunnel from hanging a Streamlit rerun. |
| `POSTGRES_KEEPALIVES_IDLE` / `_INTERVAL` / `_COUNT` | `30` / `10` / `3` | Detects a dropped idle SSM socket. |
| `POSTGRES_SSLMODE` | unset | Set to `require` only when reaching Postgres without the tunnel. |

All engines also use `pool_pre_ping=True`: after the tunnel restarts, every
pooled socket is dead but still looks checked-in, and the pre-ping reconnects
transparently instead of failing the query.

## Troubleshooting

| Symptom | Check | Resolution |
|---|---|---|
| `could not connect to server` on 15432 | `Get-NetTCPConnection -LocalPort 15432` | Start the tunnel; it is not running. |
| Tunnel script exits saying the port is taken | The reported PID and process name | That forward already works — reuse it. Stop it yourself only if it is stale. |
| `AWS profile 'dev' is not authenticated` | `aws sts get-caller-identity --profile dev` | Refresh the AWS login and retry. |
| `session-manager-plugin was not found` | `session-manager-plugin --version` | Install the official AWS Session Manager plugin. |
| Queries hang, then fail after ~10s | Is the tunnel process still alive? | The SSM session dropped. Restart the tunnel; `pool_pre_ping` recovers the pool. |
| Login fails with "User not found" | `python scripts\check_db_target.py` | Most often the target is still `local`. The two servers hold different accounts — see below. |
| Data looks stale or sparse | `python scripts\check_db_target.py` | Confirm the resolved target is `ec2`, not `local`. |

### The two servers hold different accounts

`trading_db.users` is not the same on both. Logging in against EC2 requires an
account that exists on EC2, and the login field is an **email address**, not a
username. `check_db_target.py` reports which server you are on; if a login fails
there, the account is missing on that server rather than the connection being
broken.

## Safety

Do not rerun `C:\Users\sba400\start-tunnels.ps1` to fix a streampage tunnel: its
startup broadly stops SSM child processes and will tear down the falcon API
(18000) and pgAdmin (15050) forwards. `run-ec2-db-tunnel.ps1` never does this —
it stops only the SSM session it started itself.

Treat the EC2 database as production data. It is the same instance `falcon_app`
trades against, so writes from streampage pages — the Dataload page in
particular — land on live tables.
