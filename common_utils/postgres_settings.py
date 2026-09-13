"""PostgreSQL connection settings from the environment (no config.ini).

Which server these point at is decided by ``DB_TARGET`` — see
`common_utils/env_loader.py`. For the ``ec2`` target the connection goes through
an AWS Systems Manager port-forward (`scripts/run-ec2-db-tunnel.ps1`), so the
host stays `127.0.0.1` and only the port changes to the forwarded local port.
That keeps the database off the public internet — no security-group rule for 5432.
"""
import os
from urllib.parse import quote_plus


def _int_env(name, default):
    """Read an int env var, falling back to `default` when unset or malformed."""
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _target_env(name, default=None):
    """Read `name`, preferring an `EC2_`-prefixed override on the ec2 target.

    Set the prefixed variables only when the EC2 credentials differ from local;
    otherwise the generic ones serve both targets.
    """
    from common_utils.env_loader import EC2, db_target

    if db_target() == EC2:
        override = os.getenv(f"EC2_{name}")
        if override:
            return override
    return os.getenv(name) or default


def postgres_connection_kwargs():
    """Host, port, user, password for application DB connections.

    On the ec2 target the port defaults to the SSM forward's local port rather
    than 5432, so switching servers needs no other change.
    """
    from common_utils.env_loader import EC2, db_target

    user = _target_env("APP_DB_USER") or os.getenv("POSTGRES_USER") or "trading_user"
    password = _target_env("APP_DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD") or "password123"
    host = _target_env("POSTGRES_HOST", "127.0.0.1")
    if db_target() == EC2:
        port = os.getenv("EC2_DB_TUNNEL_PORT", "15432")
    else:
        port = os.getenv("POSTGRES_PORT", "5432")
    return {"host": host, "port": port, "user": user, "password": password}


def postgres_connect_args():
    """libpq options passed through to psycopg2.

    `connect_timeout` stops a dead tunnel from hanging a Streamlit rerun
    indefinitely. The TCP keepalives matter on a tunneled connection: an SSM
    port-forward silently drops idle sockets, and without keepalives a pooled
    connection only discovers that on the next query.
    """
    args = {
        "connect_timeout": _int_env("POSTGRES_CONNECT_TIMEOUT", 10),
        "keepalives": 1,
        "keepalives_idle": _int_env("POSTGRES_KEEPALIVES_IDLE", 30),
        "keepalives_interval": _int_env("POSTGRES_KEEPALIVES_INTERVAL", 10),
        "keepalives_count": _int_env("POSTGRES_KEEPALIVES_COUNT", 3),
    }
    sslmode = os.getenv("POSTGRES_SSLMODE")
    if sslmode:
        args["sslmode"] = sslmode
    return args


def postgres_database_url(database_name):
    """SQLAlchemy URL for `database_name`.

    Credentials are percent-encoded so a password containing `@`, `/`, `:` or `#`
    cannot corrupt the URL.
    """
    pg = postgres_connection_kwargs()
    return (
        f"postgresql+psycopg2://{quote_plus(pg['user'])}:{quote_plus(pg['password'])}@"
        f"{pg['host']}:{pg['port']}/{database_name}"
    )


def default_nse_database():
    """Default database for general queries (historical NSE data, etc.)."""
    return _target_env("NSE_DATA_DB_NAME") or os.getenv("POSTGRES_DATABASE", "nsedata")


def default_trading_database():
    """Default database for auth, instruments, tokens, etc."""
    return _target_env("TRADING_DB_NAME", "trading_db")
