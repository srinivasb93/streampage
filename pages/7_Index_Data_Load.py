"""Index data load from the NSE charting API — local and/or EC2 nsedata.

Runs from this machine because NSE blocks the charting API from AWS ranges, so
Falcon Pro's Upstox-based loader cannot reach it.

The page is split by what NSE actually serves (measured, see the module
docstring in ``common_utils/nse_index_loader.py``):

- **Daily + volume** — 20 years, index volume on ~100% of bars. This is the only
  source of index volume anywhere in the stack, and the only reason to run this
  page on a schedule.
- **Intraday** — last ~24 sessions, and ``volume = 0`` on every bar. Kept for
  topping up recent minute bars; NOT a backfill path, and no volume advantage.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# `streamlit run pages/...` puts the page's own dir on sys.path, not the project
# root, so a plain `from common_utils import ...` fails when the app is launched
# that way rather than through streamlit_app.py.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from common_utils.env_loader import load_environment  # noqa: E402
from common_utils.nse_index_loader import (  # noqa: E402
    EC2,
    INTERVALS_WITH_VOLUME,
    INTRADAY_SESSION_CAP,
    LOCAL,
    check_target,
    get_target_engine,
    index_table_name,
    load_indices,
    resolve_many,
    table_summary,
)

load_environment()
st.set_page_config(layout="wide", page_title="Index Data Load")

st.title("Index Data Load — NSE charting API")
st.caption(
    "Loads NSE index bars into nsedata on this machine's chosen target(s). "
    "Falcon Pro on AWS cannot do this: NSE blocks the charting API from AWS ranges."
)

DEFAULT_INDICES = [
    "NIFTY 50", "NIFTY BANK", "NIFTY FIN SERVICE", "NIFTY NEXT 50",
    "NIFTY 100", "NIFTY 200", "NIFTY 500", "NIFTY MIDCAP 100",
    "NIFTY MIDCAP 150", "NIFTY SMLCAP 100", "NIFTY SMLCAP 250",
    "NIFTY AUTO", "NIFTY IT", "NIFTY PHARMA", "NIFTY FMCG", "NIFTY METAL",
    "NIFTY REALTY", "NIFTY ENERGY", "NIFTY MEDIA", "NIFTY PSU BANK",
    "NIFTY PVT BANK", "NIFTY PSE", "NIFTY CPSE", "NIFTY MNC",
    "NIFTY INFRA", "NIFTY SERV SECTOR", "NIFTY COMMODITIES",
]


# --------------------------------------------------------------------------- #
# Targets
# --------------------------------------------------------------------------- #
st.subheader("1. Target database")
c1, c2 = st.columns([1, 2])
with c1:
    targets = st.multiselect(
        "Write to", [LOCAL, EC2], default=[LOCAL],
        help="EC2 goes through the SSM port-forward — start the tunnel first "
             "(scripts/run-ec2-db-tunnel.ps1). Both can be selected.",
    )
with c2:
    if st.button("Test connections", use_container_width=False):
        for t in ([LOCAL, EC2] if not targets else targets):
            ok, msg = check_target(t)
            (st.success if ok else st.error)(f"**{t}** — {msg}")

if not targets:
    st.warning("Pick at least one target to enable loading.")

tab_daily, tab_intraday, tab_status = st.tabs(
    ["Daily + volume (recommended)", "Intraday (1m/5m/15m)", "What's stored"]
)


def _run(refs, interval, start, end, targets):
    """Shared execution + result rendering."""
    prog = st.progress(0.0)
    line = st.empty()
    rows = []

    def _cb(res, i, n):
        prog.progress(i / n)
        line.write(f"`{i}/{n}` **{res.index}** — {res.status}, {res.fetched:,} bars")

    results = load_indices(
        refs, interval=interval, start=start, end=end, targets=targets,
        progress=_cb,
    )
    prog.empty()
    line.empty()

    for r in results:
        rows.append({
            "Index": r.index,
            "Table": r.table,
            "Status": r.status,
            "Fetched": r.fetched,
            "Vol bars": r.volume_bars,
            "First": r.first,
            "Last": r.last,
            **{f"Written → {t}": v for t, v in r.written.items()},
            "Message": r.message,
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    failed = [r for r in results if r.status == "failed"]
    empty = [r for r in results if r.status == "empty"]
    ok = [r for r in results if r.status == "loaded"]
    st.success(f"Loaded {len(ok)} · empty {len(empty)} · failed {len(failed)}")
    if failed:
        for r in failed:
            st.error(f"{r.index}: {r.message}")
    if empty:
        st.info(
            f"{len(empty)} returned no candles. For intraday that usually means the "
            f"window is older than NSE's ~{INTRADAY_SESSION_CAP}-session cap."
        )
    return results


# --------------------------------------------------------------------------- #
# Daily
# --------------------------------------------------------------------------- #
with tab_daily:
    st.markdown(
        "**This is the tab that matters.** NSE serves ~20 years of daily index bars "
        "**with volume** — the only index-volume source in the stack. Upstox returns "
        "`volume = 0` for indices at every interval, which is why "
        "`NIFTY_100` / `NIFTY_500` volume stops dead at **2026-04-22**, and why "
        "**NIFTY_BANK has never had any volume at all**."
    )
    st.warning(
        "**Falcon's `reconcile` job (daily 06:00) rewrites the trailing 7 days** of any "
        "index in its Upstox universe, re-zeroing volume on those rows. Everything older "
        "is durable. So run this for history; expect the last few days to keep flipping "
        "back to zero until that job changes.",
        icon="⚠️",
    )

    d1, d2, d3 = st.columns(3)
    with d1:
        years = st.slider("Years of history", 1, 20, 20, key="d_years")
    with d2:
        picks_d = st.multiselect(
            "Indices", DEFAULT_INDICES,
            default=["NIFTY 50", "NIFTY BANK", "NIFTY CPSE"], key="d_idx",
        )
    with d3:
        extra_d = st.text_input(
            "Extra (comma-separated)", key="d_extra",
            help="Exact NSE symbol, e.g. 'NIFTY MID SELECT'. Only exact matches resolve.",
        )

    names_d = picks_d + [s.strip() for s in extra_d.split(",") if s.strip()]
    st.caption(f"{len(names_d)} index(es) → tables: "
               + ", ".join(f"`{index_table_name(n, '1d')}`" for n in names_d[:6])
               + (" …" if len(names_d) > 6 else ""))

    if st.button("Load daily + volume", type="primary", disabled=not (targets and names_d)):
        with st.spinner("Resolving index tokens…"):
            refs, missing = resolve_many(names_d)
        if missing:
            st.warning("Could not resolve (exact NSE symbol required): " + ", ".join(missing))
        if refs:
            end = dt.datetime.now()
            start = end - dt.timedelta(days=365 * years)
            _run(refs, "1d", start, end, targets)


# --------------------------------------------------------------------------- #
# Intraday
# --------------------------------------------------------------------------- #
with tab_intraday:
    st.info(
        f"**Read this before using it.** Measured on 2026-09-10:\n\n"
        f"- NSE intraday is **capped at ~{INTRADAY_SESSION_CAP} sessions** and anchored to "
        f"*now* — windows further back return empty, so this cannot backfill history.\n"
        f"- Intraday index bars carry **`volume = 0`** on every bar, same as Upstox. "
        f"There is no intraday index volume from this source; it has to come from the "
        f"futures contract.\n"
        f"- Upstox serves 1-min ~4 years back, so for index *history* Upstox is strictly "
        f"better. Use this tab only to top up recent minute bars.",
        icon="ℹ️",
    )
    st.caption(
        "Writes to `<INDEX>_1MIN` / `_5MIN` / `_15MIN` tables — a namespace no Upstox "
        "loader touches, so this cannot disturb the daily historical load."
    )

    i1, i2, i3 = st.columns(3)
    with i1:
        interval = st.selectbox("Interval", ["1m", "5m", "15m"], key="i_int")
    with i2:
        days = st.slider("Days back", 1, 30, 25, key="i_days",
                         help=f"NSE serves ~{INTRADAY_SESSION_CAP} sessions; more just returns the same.")
    with i3:
        picks_i = st.multiselect(
            "Indices", DEFAULT_INDICES,
            default=["NIFTY 50", "NIFTY BANK"], key="i_idx",
        )

    if interval not in INTERVALS_WITH_VOLUME:
        st.caption(f"⚠️ `{interval}` returns no volume — expect `Vol bars = 0`. That is NSE, not a bug.")

    st.caption(f"{len(picks_i)} index(es) → tables: "
               + ", ".join(f"`{index_table_name(n, interval)}`" for n in picks_i[:6])
               + (" …" if len(picks_i) > 6 else ""))

    if st.button("Load intraday", type="primary", disabled=not (targets and picks_i)):
        with st.spinner("Resolving index tokens…"):
            refs, missing = resolve_many(picks_i)
        if missing:
            st.warning("Could not resolve: " + ", ".join(missing))
        if refs:
            end = dt.datetime.now()
            start = end - dt.timedelta(days=days)
            _run(refs, interval, start, end, targets)


# --------------------------------------------------------------------------- #
# Status
# --------------------------------------------------------------------------- #
with tab_status:
    st.markdown("Volume coverage of what is already stored, per target.")
    s1, s2 = st.columns(2)
    with s1:
        stat_interval = st.selectbox("Interval", ["1d", "1m", "5m", "15m"], key="s_int")
    with s2:
        stat_names = st.multiselect("Indices", DEFAULT_INDICES,
                                    default=DEFAULT_INDICES[:10], key="s_idx")

    if st.button("Refresh status", disabled=not (targets and stat_names)):
        for t in targets:
            st.markdown(f"**Target: `{t}`**")
            try:
                eng = get_target_engine(t)
            except Exception as exc:  # noqa: BLE001
                st.error(f"{t}: {exc}")
                continue
            rows = []
            for n in stat_names:
                tbl = index_table_name(n, stat_interval)
                try:
                    s = table_summary(eng, tbl)
                except Exception as exc:  # noqa: BLE001
                    rows.append({"Index": n, "Table": tbl, "Rows": f"error: {exc}"})
                    continue
                if not s.get("exists"):
                    rows.append({"Index": n, "Table": tbl, "Rows": "(absent)"})
                    continue
                pct = (100 * s["volume_bars"] / s["rows"]) if s["rows"] else 0
                rows.append({
                    "Index": n, "Table": tbl, "Rows": f"{s['rows']:,}",
                    "Vol bars": f"{s['volume_bars']:,}", "Vol %": f"{pct:.0f}%",
                    "First": s["first"], "Last": s["last"],
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
