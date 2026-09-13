"""Environment bootstrap — load `.env` and resolve which Postgres the app talks
to (local vs the EC2 instance reached over the SSM tunnel), via ``DB_TARGET``.

Mirrors `falcon_app/backend/app/env_bootstrap.py` so both projects switch the
same way. Switch on demand with the ``DB_TARGET`` env var (default ``local``):
  * ``local`` -> ``POSTGRES_HOST:POSTGRES_PORT``           (default 127.0.0.1:5432)
  * ``ec2``   -> ``127.0.0.1:EC2_DB_TUNNEL_PORT``          (default 15432), which
                 the SSM port-forward maps to the EC2 Postgres.

Credential precedence for the ``ec2`` target (first match wins):
  1. ``EC2_APP_DB_USER`` / ``EC2_APP_DB_PASSWORD`` / ``EC2_POSTGRES_HOST`` —
     set these only if the EC2 credentials differ from local.
  2. The generic ``APP_DB_USER`` / ``APP_DB_PASSWORD`` / ``POSTGRES_HOST``.

Unlike falcon, no ``DATABASE_URL`` is synthesized: streampage builds an engine
per database name on demand (``nsedata`` alone holds ~1500 per-symbol tables),
so the target is resolved as host/port/credentials and the URL is assembled per
engine in `postgres_settings.postgres_database_url`.

Kept dependency-free (os + python-dotenv) so it can be imported before any
module that reads settings at import time.
"""
import os
import re
from pathlib import Path

from dotenv import load_dotenv

# common_utils/env_loader.py -> repo root is one level up from `common_utils`.
_ROOT = Path(__file__).resolve().parents[1]
_DONE = False

LOCAL = "local"
EC2 = "ec2"


def db_target():
    """Active target: ``'local'`` or ``'ec2'``."""
    target = (os.getenv("DB_TARGET") or LOCAL).strip().lower()
    return target if target in (LOCAL, EC2) else LOCAL


def load_environment():
    """Load `.env` and return the resolved target. Idempotent.

    Real environment variables always win: `override=False` means a value already
    exported in the shell (or injected by docker compose) is not replaced by the
    file, which is what makes the container case work with no `.env` present.
    """
    global _DONE
    if _DONE:
        return db_target()

    base = _ROOT / ".env"
    if base.exists():
        load_dotenv(base, override=False)
    _DONE = True
    return db_target()


def masked_db_summary():
    """Human-readable, password-masked summary of the resolved DB target."""
    from common_utils.postgres_settings import (
        default_nse_database,
        default_trading_database,
        postgres_database_url,
    )

    def mask(url):
        return re.sub(r"://([^:]+):[^@]+@", r"://\1:****@", url)

    return (
        f"DB_TARGET={db_target()} "
        f"trading_db={mask(postgres_database_url(default_trading_database()))} "
        f"nsedata={mask(postgres_database_url(default_nse_database()))}"
    )
