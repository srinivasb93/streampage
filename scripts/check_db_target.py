r"""Print which PostgreSQL the app resolves to, and prove it responds.

Answers the one question that matters after switching targets: "am I really on
the remote database?" Run it exactly how the app will run.

    python scripts\check_db_target.py            # local
    $env:DB_TARGET = 'ec2'
    python scripts\check_db_target.py            # EC2, over the SSM tunnel
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text

from common_utils.env_loader import load_environment, masked_db_summary


def main():
    target = load_environment()
    print(f"resolved target: {target}")
    print(f"                 {masked_db_summary()}")

    from common_utils.postgres_settings import (
        default_nse_database,
        default_trading_database,
        postgres_connect_args,
        postgres_connection_kwargs,
    )
    from common_utils import read_write_sql_data as rd

    pg = postgres_connection_kwargs()
    print(f"endpoint:        {pg['user']}@{pg['host']}:{pg['port']}")
    print(f"connect_args:    {postgres_connect_args()}")

    probes = (
        (default_nse_database(),
         "select current_database(), "
         "(select count(*) from information_schema.tables where table_schema='public')",
         "public tables"),
        (default_trading_database(),
         "select current_database(), (select count(*) from instruments)",
         "instrument rows"),
    )

    failed = False
    for database, query, label in probes:
        try:
            with rd.get_engine(database).connect() as conn:
                name, count = conn.execute(text(query)).fetchone()
            print(f"  {name:<12} OK   {count} {label}")
        except Exception as exc:
            failed = True
            print(f"  {database:<12} FAIL {type(exc).__name__}: {str(exc).splitlines()[0]}")

    if failed and target == "ec2":
        print("\nCheck the SSM tunnel is up, then retry:")
        print(r"  .\scripts\run-ec2-db-tunnel.ps1")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
