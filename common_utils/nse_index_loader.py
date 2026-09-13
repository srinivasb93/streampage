"""Load NSE index bars (charting API) into nsedata — local and/or EC2.

Why this exists
---------------
Falcon Pro runs on AWS and NSE blocks the charting API from AWS ranges, so its
Upstox-based loader cannot fetch what NSE serves. This module runs from a local
machine (the Streamlit app) and writes into the same nsedata schema, either
locally or through the EC2 SSM tunnel.

What NSE actually provides — measured 2026-09-10, not assumed
-------------------------------------------------------------
| interval | history                       | index volume |
|----------|-------------------------------|--------------|
| ``1d``   | **20 years** (from 2006-09-15)| **YES** (NIFTY 50 / BANK 100% of bars) |
| ``5m``   | ~24 sessions, rolling         | no (0 on every bar) |
| ``1m``   | ~24 sessions, rolling         | no (0 on every bar) |

Two consequences that decide how this is used:

1. **Index volume exists ONLY at daily resolution.** Intraday index bars carry
   ``volume = 0`` from NSE exactly as they do from Upstox. There is no source
   here for intraday index volume; that has to come from the futures contract.
2. **NSE intraday is anchored to now and capped at ~24 sessions.** Windows
   further back return empty, so NSE cannot backfill intraday history. Upstox
   serves 1-min ~4 years back, so for intraday indices Upstox is strictly
   better and this module's intraday mode is only for topping up recent bars.

The unique value here is therefore **daily index volume**, including NIFTY BANK,
which has never had volume in nsedata at all.

Write-safety
------------
Intraday writes go to ``<INDEX>_1MIN``-style tables, a namespace no Upstox
loader touches, so they cannot disturb the daily historical load. Daily writes
go to the real daily index tables and are additive per timestamp; note that
Falcon's ``reconcile`` job rewrites the trailing 7 days of any index in its
Upstox universe each morning, which will re-zero the volume on those few most
recent rows. Everything older is durable.
"""
from __future__ import annotations

import datetime as dt
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import create_engine, text

try:
    from . import nse_api
except ImportError:  # script/`streamlit run` context
    import nse_api  # type: ignore

logger = logging.getLogger(__name__)

LOCAL = "local"
EC2 = "ec2"

#: Intervals NSE serves that this module supports, mapped to a table suffix.
#: Daily uses the bare index table name (no suffix) to match nsedata.
INTERVAL_SUFFIX: Dict[str, str] = {
    "1m": "_1MIN",
    "5m": "_5MIN",
    "15m": "_15MIN",
    "1d": "",
}

#: Measured ceiling on NSE intraday history (sessions). Beyond this the API
#: returns an empty payload rather than an error, so a caller that assumes more
#: silently gets a short series.
INTRADAY_SESSION_CAP = 24

#: Intervals for which NSE returns volume. Verified: only daily.
INTERVALS_WITH_VOLUME = {"1d"}

_OHLCV = ["timestamp", "open", "high", "low", "close", "volume"]

#: Pandas floor rule per interval, used to align NSE bar labels onto nsedata's.
#: NSE stamps a bar at its CLOSE (a 09:15-09:16 minute arrives as ``09:15:59``)
#: while Upstox and every nsedata table stamp the OPEN (``09:15:00``). Writing
#: NSE bars unaltered into a shared table therefore does not collide with the
#: Upstox row — it silently inserts a SECOND bar for the same minute. Flooring
#: to the interval maps close-stamps onto open-stamps for every interval
#: (``09:19:59`` -> ``09:15:00`` at 5m), so the two sources upsert onto the
#: same key instead of doubling the series.
_FLOOR_RULE: Dict[str, str] = {"1m": "1min", "5m": "5min", "15m": "15min", "1d": "D"}


#: NSE continuous session. NSE's intraday feed also emits pre-open bars from
#: ~09:09 (flat carry of the auction price, zero volume) which Upstox does not.
#: Left in, recent sessions carry 381 bars against 375 for the whole history —
#: a silent inhomogeneity that breaks opening-range and first-bar work.
SESSION_START = dt.time(9, 15)
SESSION_END = dt.time(15, 29)


def trim_to_session(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    """Drop pre-open / post-close filler, WITHOUT discarding special sessions.

    A naive "keep only 09:15-15:29" rule would delete Muhurat trading entirely —
    those sessions run in the evening (2023-11-12 18:15-19:14, 2024-11-01
    18:00-18:59) and carry real volume. The rule here is therefore per-day: trim
    out-of-window bars only on days that ALSO have regular-session bars. A day
    whose bars lie wholly outside the window is a special session and is kept
    intact.
    """
    if df is None or df.empty or interval == "1d":
        return df
    idx = pd.DatetimeIndex(df.index)
    in_window = pd.Series(
        [(SESSION_START <= x <= SESSION_END) for x in idx.time], index=df.index
    )
    days = pd.Series(idx.date, index=df.index)
    has_regular = in_window.groupby(days).transform("any")
    # Keep a bar if it is in-window, or if its whole day sits outside the window.
    return df[in_window | ~has_regular]


def align_bar_timestamps(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    """Floor NSE close-stamped bar labels onto nsedata's open-stamped grid.

    Keeps the LAST row per resulting key: if two raw bars floor onto the same
    label the later one is the more complete.
    """
    if df is None or df.empty:
        return df
    rule = _FLOOR_RULE.get(interval)
    if not rule:
        return df
    out = df.copy()
    out.index = pd.DatetimeIndex(out.index).floor(rule)
    out.index.name = "Timestamp"
    return out[~out.index.duplicated(keep="last")].sort_index()


# --------------------------------------------------------------------------- #
# Naming
# --------------------------------------------------------------------------- #
#: NSE charting symbol -> the base table name Falcon's intraday store already
#: uses. Falcon names intraday index tables from the Upstox TRADING SYMBOL
#: (``NIFTY``, ``BANKNIFTY``, ``FINNIFTY``), while NSE calls the same indices
#: ``NIFTY 50`` / ``NIFTY BANK`` / ``NIFTY FIN SERVICE``. Without this map the
#: page would build ``NIFTY_50_1MIN`` alongside the existing ``NIFTY_1MIN`` —
#: two tables for one instrument, silently diverging. Daily tables are NOT
#: aliased: nsedata's daily series really are named ``NIFTY_50`` etc.
INTRADAY_BASE_ALIAS: Dict[str, str] = {
    "NIFTY 50": "NIFTY",
    "NIFTY BANK": "BANKNIFTY",
    "NIFTY FIN SERVICE": "FINNIFTY",
}


def index_table_name(index_name: str, interval: str = "1m") -> str:
    """nsedata table for an index at an interval.

    Daily keeps nsedata's own naming (``NIFTY 50`` -> ``NIFTY_50``). Intraday
    follows Falcon's intraday store instead (``NIFTY 50`` -> ``NIFTY_1MIN``) so
    a top-up lands in the existing table rather than creating a duplicate.
    """
    if interval not in INTERVAL_SUFFIX:
        raise ValueError(f"Unsupported interval {interval!r}; use {sorted(INTERVAL_SUFFIX)}")
    name = str(index_name or "").strip()
    if interval != "1d":
        name = INTRADAY_BASE_ALIAS.get(name.upper(), name)
    base = name.replace(" ", "_").replace("-", "_")
    return f"{base}{INTERVAL_SUFFIX[interval]}"


# --------------------------------------------------------------------------- #
# Engines — explicit per target, independent of the app-wide DB_TARGET
# --------------------------------------------------------------------------- #
_ENGINE_CACHE: Dict[Tuple[str, str], Any] = {}


def _target_conn_kwargs(target: str) -> Dict[str, str]:
    """Host/port/user/password for a specific target, ignoring ``DB_TARGET``.

    Deliberately does not go through ``postgres_settings.postgres_connection_kwargs``
    because that resolves whichever target the app was started with; this module
    lets one session write to both.
    """
    import os

    if target == EC2:
        return {
            "host": os.getenv("EC2_POSTGRES_HOST") or os.getenv("POSTGRES_HOST", "127.0.0.1"),
            "port": os.getenv("EC2_DB_TUNNEL_PORT", "15432"),
            "user": os.getenv("EC2_APP_DB_USER") or os.getenv("APP_DB_USER", "trading_user"),
            "password": os.getenv("EC2_APP_DB_PASSWORD") or os.getenv("APP_DB_PASSWORD", "password123"),
        }
    return {
        "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "user": os.getenv("APP_DB_USER", "trading_user"),
        "password": os.getenv("APP_DB_PASSWORD", "password123"),
    }


def get_target_engine(target: str, database: str = "nsedata"):
    """Cached SQLAlchemy engine for (target, database)."""
    key = (target, database)
    if key in _ENGINE_CACHE:
        return _ENGINE_CACHE[key]
    from urllib.parse import quote_plus

    pg = _target_conn_kwargs(target)
    url = (
        f"postgresql+psycopg2://{quote_plus(pg['user'])}:{quote_plus(pg['password'])}@"
        f"{pg['host']}:{pg['port']}/{database}"
    )
    engine = create_engine(
        url, pool_pre_ping=True, pool_size=2, max_overflow=3,
        connect_args={"connect_timeout": 10, "keepalives": 1,
                      "keepalives_idle": 30, "keepalives_interval": 10,
                      "keepalives_count": 3},
    )
    _ENGINE_CACHE[key] = engine
    return engine


def check_target(target: str, database: str = "nsedata") -> Tuple[bool, str]:
    """``(ok, message)`` for a target — used to fail fast in the UI."""
    try:
        eng = get_target_engine(target, database)
        with eng.connect() as conn:
            db = conn.execute(text("select current_database()")).scalar()
            n = conn.execute(text(
                "select count(*) from information_schema.tables where table_schema='public'"
            )).scalar()
        pg = _target_conn_kwargs(target)
        return True, f"{db} @ {pg['host']}:{pg['port']} ({n} tables)"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


# --------------------------------------------------------------------------- #
# Index catalogue
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class IndexRef:
    name: str
    token: str
    instrument_type: str = "0"


def resolve_index(session, name: str) -> Optional[IndexRef]:
    """Resolve an index name to its NSE scripcode.

    Must go through ``nse_api.search_symbol`` (a GET). The PyPI ``openchart``
    package POSTs a JSON body instead, which NSE answers with a fixed 20-index
    list regardless of the query — so a lookup for anything outside those 20
    silently resolves to NIFTY 50.
    """
    try:
        df = nse_api.search_symbol(session, name, "IDX")
    except Exception as exc:  # noqa: BLE001
        logger.warning("index search failed for %s: %s", name, exc)
        return None
    if df is None or df.empty:
        return None
    exact = df[df["symbol"].str.upper() == name.strip().upper()]
    row = exact.iloc[0] if not exact.empty else None
    if row is None:
        return None
    return IndexRef(
        name=str(row["symbol"]),
        token=str(row["scripcode"]),
        instrument_type=str(row.get("instrumentType", "0")),
    )


def resolve_many(names: Sequence[str], session=None) -> Tuple[List[IndexRef], List[str]]:
    """``(resolved, unresolved)``. Only EXACT symbol matches are accepted."""
    session = session or nse_api.create_session()
    found, missing = [], []
    for n in names:
        ref = resolve_index(session, n)
        (found.append(ref) if ref else missing.append(n))
        time.sleep(0.2)
    return found, missing


# --------------------------------------------------------------------------- #
# Schema + write
# --------------------------------------------------------------------------- #
def ensure_table(engine, table: str) -> None:
    """Create the bar table if absent, keyed on ``timestamp``."""
    with engine.begin() as conn:
        conn.execute(text(
            f'''CREATE TABLE IF NOT EXISTS public."{table}" (
                   "timestamp" timestamp without time zone PRIMARY KEY,
                   "open" double precision, "high" double precision,
                   "low" double precision, "close" double precision,
                   "volume" bigint)'''
        ))


def _has_primary_key(engine, table: str) -> bool:
    with engine.connect() as conn:
        return bool(conn.execute(text(
            """
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_schema='public' AND table_name=:t AND constraint_type='PRIMARY KEY'
            """
        ), {"t": table}).scalar())


def upsert_bars(engine, table: str, df: pd.DataFrame) -> int:
    """Insert/update bars keyed on timestamp. Returns rows sent.

    Falls back to delete-then-insert on tables that predate this module and have
    no primary key (the existing daily index tables are in that state), so the
    same call works for both without silently duplicating rows.
    """
    if df is None or df.empty:
        return 0
    rows = df.reset_index()
    rows.columns = [str(c).lower() for c in rows.columns]
    rows = rows.rename(columns={"index": "timestamp"})
    missing = [c for c in _OHLCV if c not in rows.columns]
    if missing:
        raise ValueError(f"bars missing columns {missing}")
    rows = rows[_OHLCV].dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"])
    rows["volume"] = rows["volume"].fillna(0).astype("int64")
    payload = rows.to_dict("records")

    if _has_primary_key(engine, table):
        stmt = text(
            f'''INSERT INTO public."{table}"
                    ("timestamp","open","high","low","close","volume")
                VALUES (:timestamp,:open,:high,:low,:close,:volume)
                ON CONFLICT ("timestamp") DO UPDATE SET
                    "open"=EXCLUDED."open", "high"=EXCLUDED."high",
                    "low"=EXCLUDED."low", "close"=EXCLUDED."close",
                    "volume"=EXCLUDED."volume"'''
        )
        with engine.begin() as conn:
            conn.execute(stmt, payload)
        return len(payload)

    # No PK (legacy daily table): replace exactly the days we are writing.
    days = sorted({r["timestamp"].date() for r in payload})
    with engine.begin() as conn:
        conn.execute(
            text(f'DELETE FROM public."{table}" WHERE ("timestamp")::date = ANY(:days)'),
            {"days": days},
        )
        conn.execute(
            text(f'''INSERT INTO public."{table}"
                        ("timestamp","open","high","low","close","volume")
                     VALUES (:timestamp,:open,:high,:low,:close,:volume)'''),
            payload,
        )
    return len(payload)


def table_summary(engine, table: str) -> Dict[str, Any]:
    """Row count / span / volume coverage for a table, or ``exists=False``."""
    with engine.connect() as conn:
        if not conn.execute(text(
            "select 1 from information_schema.tables "
            "where table_schema='public' and table_name=:t"), {"t": table}
        ).scalar():
            return {"table": table, "exists": False}
        n, first, last, vb = conn.execute(text(
            f'''SELECT count(*), min("timestamp"), max("timestamp"),
                       count(*) FILTER (WHERE "volume" > 0)
                FROM public."{table}"'''
        )).one()
    return {"table": table, "exists": True, "rows": int(n or 0),
            "first": first, "last": last, "volume_bars": int(vb or 0)}


# --------------------------------------------------------------------------- #
# Load
# --------------------------------------------------------------------------- #
@dataclass
class IndexLoadResult:
    index: str
    table: str
    interval: str
    status: str                       # loaded | empty | failed
    fetched: int = 0
    written: Dict[str, int] = field(default_factory=dict)
    first: Optional[dt.datetime] = None
    last: Optional[dt.datetime] = None
    volume_bars: int = 0
    message: str = ""


def load_index(
    ref: IndexRef,
    *,
    interval: str,
    start: dt.datetime,
    end: dt.datetime,
    targets: Sequence[str],
    session=None,
    database: str = "nsedata",
) -> IndexLoadResult:
    """Fetch one index at one interval and write it to each target."""
    table = index_table_name(ref.name, interval)
    res = IndexLoadResult(index=ref.name, table=table, interval=interval, status="loaded")

    # Guard: intraday must never write over a daily index table. A bad interval
    # suffix here would silently overwrite 20 years of daily bars with a month
    # of minutes, so make it impossible rather than unlikely.
    if interval != "1d" and not table.endswith(INTERVAL_SUFFIX[interval]):
        res.status = "failed"
        res.message = f"refusing to write interval {interval} to {table}"
        return res

    session = session or nse_api.create_session()
    try:
        df = nse_api.fetch_historical(
            session, token=ref.token, symbol=ref.name,
            instrument_type=ref.instrument_type,
            start=start, end=end, interval=interval,
        )
    except Exception as exc:  # noqa: BLE001
        res.status = "failed"
        res.message = f"fetch failed: {exc}"
        return res

    if df is None or df.empty:
        res.status = "empty"
        res.message = "NSE returned no candles for this window"
        return res

    # Align NSE's close-stamped labels onto nsedata's open-stamped grid BEFORE
    # writing, or every bar lands as a duplicate alongside the Upstox row for
    # the same minute rather than upserting over it.
    df = align_bar_timestamps(df, interval)
    df = trim_to_session(df, interval)
    if df.empty:
        res.status = "empty"
        res.message = "no bars left inside the continuous session window"
        return res

    res.fetched = len(df)
    res.first, res.last = df.index[0], df.index[-1]
    res.volume_bars = int((df["Volume"] > 0).sum())

    for target in targets:
        try:
            eng = get_target_engine(target, database)
            ensure_table(eng, table)
            res.written[target] = upsert_bars(eng, table, df)
        except Exception as exc:  # noqa: BLE001
            res.status = "failed"
            res.message = f"{target} write failed: {exc}"
            logger.exception("write failed for %s -> %s", table, target)
    return res


def load_indices(
    refs: Sequence[IndexRef],
    *,
    interval: str,
    start: dt.datetime,
    end: dt.datetime,
    targets: Sequence[str],
    database: str = "nsedata",
    pause_sec: float = 0.4,
    progress: Optional[Callable[[IndexLoadResult, int, int], None]] = None,
) -> List[IndexLoadResult]:
    """Load several indices sequentially, pausing between NSE calls."""
    session = nse_api.create_session()
    out: List[IndexLoadResult] = []
    for i, ref in enumerate(refs, 1):
        r = load_index(ref, interval=interval, start=start, end=end,
                       targets=targets, session=session, database=database)
        out.append(r)
        if progress:
            progress(r, i, len(refs))
        time.sleep(pause_sec)
    return out


# --------------------------------------------------------------------------- #
# Self-check — `python -m common_utils.nse_index_loader` (no pytest in this repo)
# --------------------------------------------------------------------------- #
def _self_check() -> int:
    """Assert the pure transforms. Network/DB are not exercised here."""
    fails: List[str] = []

    def eq(label, got, want):
        if got != want:
            fails.append(f"{label}: got {got!r}, want {want!r}")

    # Naming: intraday aliases onto Falcon's tables, daily keeps nsedata's.
    eq("1m NIFTY 50", index_table_name("NIFTY 50", "1m"), "NIFTY_1MIN")
    eq("1m NIFTY BANK", index_table_name("NIFTY BANK", "1m"), "BANKNIFTY_1MIN")
    eq("1m NIFTY FIN SERVICE", index_table_name("NIFTY FIN SERVICE", "1m"), "FINNIFTY_1MIN")
    eq("1d NIFTY 50", index_table_name("NIFTY 50", "1d"), "NIFTY_50")
    eq("1d NIFTY BANK", index_table_name("NIFTY BANK", "1d"), "NIFTY_BANK")
    eq("1m NIFTY CPSE", index_table_name("NIFTY CPSE", "1m"), "NIFTY_CPSE_1MIN")

    def mk(rows):
        return pd.DataFrame(
            [{"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": v} for _, v in rows],
            index=pd.DatetimeIndex([pd.Timestamp(t) for t, _ in rows], name="Timestamp"),
        )

    # Close-stamped labels must floor onto the open-stamped grid.
    a = mk([("2026-09-08 09:15:59", 1), ("2026-09-08 09:19:59", 1)])
    eq("floor 1m", [str(x) for x in align_bar_timestamps(a, "1m").index],
       ["2026-09-08 09:15:00", "2026-09-08 09:19:00"])
    eq("floor 5m", [str(x) for x in align_bar_timestamps(a, "5m").index],
       ["2026-09-08 09:15:00"])

    # Regular day: pre-open and post-close filler goes.
    d = mk([("2026-09-08 09:09", 0), ("2026-09-08 09:15", 100),
            ("2026-09-08 15:29", 100), ("2026-09-08 15:40", 0)])
    eq("trim regular", len(trim_to_session(d, "1m")), 2)
    # Muhurat day: wholly outside the window, so kept intact.
    m = mk([("2023-11-12 18:15", 500), ("2023-11-12 19:14", 300)])
    eq("trim muhurat", len(trim_to_session(m, "1m")), 2)
    eq("trim mixed", len(trim_to_session(pd.concat([d, m]), "1m")), 4)
    # Daily is never trimmed.
    eq("trim daily", len(trim_to_session(d, "1d")), 4)

    for f in fails:
        print("FAIL:", f)
    print("self-check:", "PASS" if not fails else f"{len(fails)} FAILURE(S)")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(_self_check())
