import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
try:
    from .logging_utils import configure_logging
except ImportError:
    from logging_utils import configure_logging
import os
import threading
import datetime as dt
try:
    from . import nse_api
except ImportError:
    import nse_api

try:
    from .postgres_settings import (
        default_nse_database,
        postgres_connect_args,
        postgres_database_url,
    )
except ImportError:
    from postgres_settings import (
        default_nse_database,
        postgres_connect_args,
        postgres_database_url,
    )

configure_logging()
logger = logging.getLogger(__name__)

# --- Multi-Database Connection Management ---

# Global, thread-safe dictionary to store engine instances.
_ENGINES = {}
_LOCK = threading.Lock()

def get_engine(database_name=None):
    """
    Manages and returns the SQLAlchemy engine for a specific database.
    """
    global _ENGINES
    if database_name is None:
        database_name = default_nse_database()

    if database_name in _ENGINES:
        return _ENGINES[database_name]

    with _LOCK:
        if database_name in _ENGINES:
            return _ENGINES[database_name]

        try:
            db_url = postgres_database_url(database_name)

            # FIX: Reduced pool size and overflow to prevent "too many clients" error.
            # This makes the application more conservative with its connection usage.
            # If this error persists, consider increasing `max_connections` in your
            # postgresql.conf file on the database server itself.
            #
            # pool_pre_ping is what makes a remote/tunneled database usable: when the
            # SSM port-forward is restarted, every pooled socket is dead but still
            # looks checked-in. Without the pre-ping the next query fails instead of
            # transparently reconnecting.
            new_engine = create_engine(
                db_url,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                pool_pre_ping=True,
                connect_args=postgres_connect_args()
            )
            _ENGINES[database_name] = new_engine
            logger.info(f"Connection pool created successfully for database: '{database_name}'.")
            return new_engine

        except Exception as e:
            logger.error(f"Failed to create connection pool for '{database_name}': {e}")
            raise

def get_ref_tables(selected_database):
    """
    Retrieves the list of reference tables from a specific schema.
    """
    global ref_data
    try:
        # CHANGED: Standard SQL for information_schema
        query = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'ref' AND table_catalog = :db
        """)
        engine = get_engine(selected_database)
        with engine.connect() as conn:
            ref_tables = pd.read_sql(query, conn, params={'db': selected_database})
        ref_data = ref_tables['table_name'].to_list()
        logger.info("Reference tables retrieved successfully.")
    except Exception as e:
        logger.error(f"Error retrieving reference tables: {e}")
        ref_data = []


def get_database_list():
    """
    CHANGED: Retrieves the list of databases from PostgreSQL.
    """
    try:
        # Excludes system and template databases
        query = text("""
            SELECT datname FROM pg_database
            WHERE datistemplate = false AND datname NOT IN ('postgres');
        """)
        engine = get_engine()
        with engine.connect() as conn:
            databases = pd.read_sql(query, conn)
        return databases['datname'].to_list()
    except Exception as e:
        logger.error(f"Error retrieving database list: {e}")
        return []


def get_database_tables_list(database, schema='public'):
    """
    CHANGED: Retrieves the list of tables in a specific database and schema for PostgreSQL.
    """
    try:
        query = text("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = :schema AND tableowner != 'postgres'
        """)
        engine = get_engine(database)
        with engine.connect() as conn:
            tables = pd.read_sql(query, conn, params={'schema': schema})
        return tables['tablename'].to_list()
    except Exception as e:
        logger.error(f"Error retrieving table list for database {database}: {e}")
        return []


def get_table_data(selected_database='nsedata', selected_table='TATAMOTORS', query=None,
                   sample=False, sample_count=100, sort=False, sort_order='ASC', sort_by='timestamp'):
    """
    Retrieves data from a table or via a custom query from a specific database.
    """
    try:
        # Get the correct engine for the specified database.
        engine = get_engine(selected_database)

        if not query:
            # Assuming 'public' schema. If you use others like 'ref', this logic could be expanded.
            schema = 'public'
            # Use text() for quoting identifiers safely, crucial for PostgreSQL.
            base_query = f'SELECT * FROM {schema}."{selected_table}"'

            # Append clauses as needed
            if sort:
                base_query += f' ORDER BY "{sort_by}" {sort_order}'
            if sample:
                base_query += f" LIMIT {sample_count}"  # LIMIT is standard SQL

            query = text(base_query)

        # Use the specific engine to connect and execute the query.
        with engine.connect() as conn:
            return pd.read_sql(query, conn)

    except Exception as e:
        logger.error(f"Error retrieving data from table '{selected_table}' in database '{selected_database}': {e}")
        return pd.DataFrame()


def load_sql_data(data_to_load, table_name, database='nsedata', load_type='replace',
                  index_required=False, schema='public'):
    """
    Loads a DataFrame into a SQL table in a specific database.
    """
    try:
        # Get the correct engine for the target database.
        engine = get_engine(database)

        data_to_load.to_sql(
            name=table_name,
            con=engine,
            if_exists=load_type,
            index=index_required,
            schema=schema
        )
        logger.info(f"Data loaded successfully into table '{schema}.{table_name}' in database '{database}'.")
        return f"{table_name} table has been loaded successfully"
    except Exception as e:
        logger.error(f"Failed to load data into table '{table_name}' in database '{database}': {e}")
        return f"Failed to {load_type} {table_name} table"


def upsert_record(database, table_name, data_dict, conflict_column):
    """
    GENERIC: Inserts a new record or updates an existing one based on a conflict column.
    """
    try:
        engine = get_engine(database)
        columns = [f'"{col}"' for col in data_dict.keys()]
        placeholders = [f":{col}" for col in data_dict.keys()]
        update_set = [f'"{col}" = EXCLUDED."{col}"' for col in data_dict if col != conflict_column]

        if not update_set:
            raise ValueError("data_dict must contain more than just the conflict_column to perform an update.")

        query_str = f"""
            INSERT INTO public."{table_name}" ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT ("{conflict_column}") DO UPDATE 
            SET {', '.join(update_set)};
        """
        upsert_query = text(query_str)

        with engine.connect() as conn:
            with conn.begin():
                conn.execute(upsert_query, data_dict)

        logger.info(f"Successfully upserted record into '{table_name}'.")
        return True
    except (SQLAlchemyError, ValueError) as e:
        logger.error(f"Failed to upsert record into '{table_name}': {e}")
        return False

# Databases whose STOCKS_IN_DB has been verified this process, so the check costs
# one round-trip per database rather than one per symbol in a data-load loop.
_REGISTRY_READY = set()

STOCKS_IN_DB_SYMBOL_INDEX = "STOCKS_IN_DB_SYMBOL_uidx"


def ensure_registry_table_exists(database='nsedata'):
    """Create STOCKS_IN_DB if absent and guarantee SYMBOL is uniquely indexed.

    `upsert_record`'s ON CONFLICT ("SYMBOL") requires a unique or exclusion
    constraint on that column, and Postgres raises InvalidColumnReference without
    one. CREATE TABLE alone does not get us there: the table is routinely written by
    pandas `to_sql`, which creates no constraints at all and, with
    if_exists='replace', DROPS the table and any constraint it had. So the unique
    index is asserted separately every time rather than inferred from the CREATE.

    A unique index (not a PRIMARY KEY) is used because it can be added to an
    existing table without rewriting it or requiring SYMBOL to be NOT NULL, and it
    satisfies ON CONFLICT inference identically.
    """
    if database in _REGISTRY_READY:
        return True
    try:
        engine = get_engine(database)
        create_statement = text("""
            CREATE TABLE IF NOT EXISTS public."STOCKS_IN_DB" (
                "SYMBOL" VARCHAR(255) PRIMARY KEY,
                "instrument_token" VARCHAR(255),
                "last_updated" TIMESTAMP
            );
        """)
        index_statement = text(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{STOCKS_IN_DB_SYMBOL_INDEX}" '
            'ON public."STOCKS_IN_DB" ("SYMBOL");')
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(create_statement)
                conn.execute(index_statement)
        logger.info("Ensured 'STOCKS_IN_DB' table and unique SYMBOL index exist.")
        _REGISTRY_READY.add(database)
        return True
    except SQLAlchemyError as e:
        # The usual cause is pre-existing duplicate symbols, which must be resolved
        # by hand — silently deleting rows here would destroy real data.
        logger.error(f"Failed to create or verify 'STOCKS_IN_DB' table: {e}")
        try:
            duplicates = get_table_data(
                selected_database=database,
                query='SELECT "SYMBOL", COUNT(*) AS n FROM public."STOCKS_IN_DB" '
                      'GROUP BY 1 HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 20')
            if duplicates is not None and not duplicates.empty:
                logger.error("Duplicate SYMBOLs block the unique index: %s",
                             duplicates.to_dict('records'))
        except SQLAlchemyError:
            pass
        return False


def registry_constraint_invalidated(database='nsedata'):
    """Forget that `database` was verified, after something may have dropped the index.

    Call this after any `to_sql` replace of STOCKS_IN_DB so the next registry write
    re-creates the unique index instead of failing on ON CONFLICT.
    """
    _REGISTRY_READY.discard(database)


def add_stock_to_registry(stock_symbol, instrument_token, database='nsedata'):
    """
    SPECIFIC USE CASE: A wrapper for upsert_record to add a stock to the STOCKS_IN_DB table.
    """
    ensure_registry_table_exists(database=database)
    stock_data = {"SYMBOL": stock_symbol, "instrument_token": instrument_token, "last_updated": dt.datetime.now()}
    if upsert_record(database=database, table_name="STOCKS_IN_DB",
                     data_dict=stock_data, conflict_column="SYMBOL"):
        return True

    # The unique index can disappear under us: any to_sql(if_exists='replace') on
    # STOCKS_IN_DB drops the table, and this process may already have it cached as
    # verified. Re-assert and retry once so a registry write heals itself rather
    # than depending on every replace site remembering to invalidate.
    registry_constraint_invalidated(database=database)
    if not ensure_registry_table_exists(database=database):
        return False
    return upsert_record(database=database, table_name="STOCKS_IN_DB",
                         data_dict=stock_data, conflict_column="SYMBOL")


_NSE_SESSION = None
_INDEX_SYMBOL_MAPPING = {
    "NIFTY100 LIQUID 15": "NIFTY100 LIQUID 15",
    "NIFTY MIDCAP LIQUID 15": "NIFTY MIDCAP LIQUID 15",
    "NIFTY INDIA DIGITAL": "NIFTY INDIA DIGITAL",
    "NIFTY SMALLCAP 250": "NIFTY SMLCAP 250",
    "NIFTY SMALLCAP 50": "NIFTY SMLCAP 50",
    "NIFTY SMALLCAP 100": "NIFTY SMLCAP 100",
    "NIFTY MIDSMALLCAP 400": "NIFTY MIDSML 400",
    "NIFTY MIDCAP SELECT": "NIFTY MID SELECT",
    "NIFTY LARGEMIDCAP 250": "NIFTY LARGEMID250",
    "NIFTY HEALTHCARE INDEX": "NIFTY HEALTHCARE",
    "NIFTY CONSUMER DURABLES": "NIFTY CONSR DURBL",
    "NIFTY FINANCIAL SERVICES": "NIFTY FIN SERVICE",
    "NIFTY PRIVATE BANK": "NIFTY PRIVATE BANK",
    "NIFTY INFRASTRUCTURE": "NIFTY INFRASTRUCTURE",
    "NIFTY SERVICES SECTOR": "NIFTY SERVICES SECTOR",
    "NIFTY INDIA CONSUMPTION": "NIFTY INDIA CONSUMPTION",
}


def _get_nse_session():
    """Returns a cached session for the NSE charting API."""
    global _NSE_SESSION
    if _NSE_SESSION is None:
        _NSE_SESSION = nse_api.create_session()
    return _NSE_SESSION


def _format_nse_chart_data(df):
    """Standardizes the DataFrame from common_utils.nse_api."""
    formatted_df = df.copy()
    formatted_df.reset_index(inplace=True)
    formatted_df.rename(
        columns={
            'Timestamp': 'timestamp',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        },
        inplace=True
    )
    formatted_df['timestamp'] = pd.to_datetime(formatted_df['timestamp']).dt.tz_localize(None)
    return formatted_df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()


def _fetch_historical_data_with_nse_api(symbol, start_date, end_date, segment='EQ', interval='1d'):
    """Fetches historical data for stock or index using common_utils.nse_api."""
    try:
        logger.info(f"Attempting to fetch {segment} data for '{symbol}' using nse_api.")
        session = _get_nse_session()
        search_symbol = _INDEX_SYMBOL_MAPPING.get(symbol, symbol) if segment == 'IDX' else symbol
        if search_symbol != symbol:
            logger.info("Mapped index symbol '%s' to API search symbol '%s'.", symbol, search_symbol)

        search_results = nse_api.search_symbol(session, search_symbol, segment=segment)

        if search_results.empty:
            logger.warning(f"No search results found for '{symbol}' in segment '{segment}'.")
            return pd.DataFrame()

        if segment == 'EQ':
            symbol_upper = symbol.upper()
            candidate_symbols = {symbol_upper, f"{symbol_upper}-EQ"}
            exact_match = search_results[search_results['symbol'].str.upper().isin(candidate_symbols)]
        else:
            exact_match = search_results[search_results['symbol'].str.upper() == search_symbol.upper()]

        row = exact_match.iloc[0] if not exact_match.empty else search_results.iloc[0]

        start_datetime = dt.datetime.combine(start_date, dt.datetime.min.time())
        end_datetime = dt.datetime.combine(end_date, dt.datetime.max.time())
        historical_df = nse_api.fetch_historical(
            session=session,
            token=row['scripcode'],
            symbol=row['symbol'],
            instrument_type=row['instrumentType'],
            start=start_datetime,
            end=end_datetime,
            interval=interval
        )
        if historical_df.empty:
            logger.warning(f"nse_api returned no historical data for '{symbol}' ({segment}).")
            return pd.DataFrame()

        logger.info(f"nse_api fetch successful for '{symbol}' ({segment}).")
        return _format_nse_chart_data(historical_df)
    except Exception as exc:
        logger.error(f"nse_api failed for '{symbol}' ({segment}): {exc}")
        return pd.DataFrame()


def load_index_sector_history(symbol, start_date_obj, end_date_obj, database='nsedata', data_source='nse_api'):
    """Performs a full historical data load for an NSE index or sector using nse_api."""
    logger.info(f"Starting historical load for '{symbol}' from {start_date_obj} to {end_date_obj}.")
    table_name = symbol.replace(" ", "_").replace("-", "_")

    if data_source and data_source != 'nse_api':
        logger.warning("Ignoring unsupported data_source '%s'; using 'nse_api'.", data_source)

    formatted_data = _fetch_historical_data_with_nse_api(
        symbol=symbol,
        start_date=start_date_obj,
        end_date=end_date_obj,
        segment='IDX',
        interval='1d'
    )

    # Drop duplicates based on the timestamp column
    formatted_data = formatted_data.drop_duplicates(subset=['timestamp'])

    if formatted_data.empty:
        return f"Failed to fetch data for {symbol} from all available sources."

    return load_sql_data(
        data_to_load=formatted_data, table_name=table_name, database=database,
        load_type='replace', index_required=False
    )


def update_index_sector_daily(symbol, database='nsedata'):
    """Performs an incremental daily update for an NSE index or sector using nse_api."""
    table_name = symbol.replace(" ", "_").replace("-", "_")
    logger.info(f"Starting daily update for '{table_name}'.")

    try:
        last_date_query = text(f'SELECT MAX("timestamp") FROM public."{table_name}"')
        engine = get_engine(database)
        with engine.connect() as conn:
            last_date_result = conn.execute(last_date_query).scalar_one_or_none()

        if last_date_result is None:
            return f"Table '{table_name}' is empty. Please run a full historical load first."

        start_date_obj = last_date_result.date() + dt.timedelta(days=1)
        end_date_obj = dt.date.today()

        if start_date_obj > end_date_obj:
            return f"Data for '{table_name}' is already up to date."

        formatted_data = _fetch_historical_data_with_nse_api(
            symbol=symbol,
            start_date=start_date_obj,
            end_date=end_date_obj,
            segment='IDX',
            interval='1d'
        )

        if formatted_data.empty:
            return f"No new data to update for '{table_name}'."

        return load_sql_data(
            data_to_load=formatted_data, table_name=table_name, database=database,
            load_type='append', index_required=False
        )
    except Exception as e:
        logger.error(f"Error during daily update for '{table_name}': {e}")
        if "does not exist" in str(e):
            return f"Table '{table_name}' not found. Please run a full historical load first."
        return f"Failed: {e}"

def load_stock_history(symbol, start_date_obj, end_date_obj, database='nsedata', interval="1d"):
    """Performs a full historical data load for an NSE stock"""
    logger.info(f"Starting historical load for '{symbol}' from {start_date_obj} to {end_date_obj}.")
    table_name = symbol.replace(" ", "_").replace("-", "_")

    formatted_data = _fetch_historical_data_with_nse_api(
        symbol=symbol,
        start_date=start_date_obj,
        end_date=end_date_obj,
        segment='EQ',
        interval=interval
    )
    if formatted_data.empty:
        return f"Failed to fetch data for {symbol} from all available sources."

    return load_sql_data(
        data_to_load=formatted_data, table_name=table_name, database=database,
        load_type='replace', index_required=False
    )

def update_stock_daily(symbol, database='nsedata'):
    """Performs an incremental daily update for a single stock."""
    table_name = symbol
    logger.info(f"Starting daily update for stock: '{table_name}'.")
    try:
        # FIX: Query the 'timestamp' column
        last_date_query = text(f'SELECT MAX("timestamp") FROM public."{table_name.replace("-", "_")}"')
        engine = get_engine(database)
        with engine.connect() as conn:
            last_date_result = conn.execute(last_date_query).scalar_one_or_none()
        if last_date_result is None:
            return f"Table '{table_name}' is empty. Please run a full historical load first."
        start_date_obj = last_date_result.date() + dt.timedelta(days=1)
        end_date_obj = dt.date.today()
        if start_date_obj > end_date_obj:
            return f"Data for '{table_name}' is already up to date."
        formatted_data = _fetch_historical_data_with_nse_api(
            symbol=symbol,
            start_date=start_date_obj,
            end_date=end_date_obj,
            segment='EQ',
            interval='1d'
        )
        if formatted_data.empty:
            return f"No new data to update for '{table_name}'."
        return load_sql_data(data_to_load=formatted_data, table_name=table_name, database=database, load_type='append', index_required=False)
    except Exception as e:
        logger.error(f"Error during daily update for '{table_name}': {e}")
        if "does not exist" in str(e):
             return f"Table '{table_name}' not found. Please run a full historical load first."
        return f"Failed: {e}"


if __name__ == '__main__':
    # Example usage
    db = get_engine('nsedata')

    # Load index data example
    start_date = dt.date(2020, 1, 1)
    end_date = dt.date.today()
    print(load_index_sector_history('NIFTY FINANCIAL SERVICES', start_date, end_date))
    # print(load_stock_history('TATAMOTORS', start_date, end_date))

    # Update index data example
    # print(update_index_sector_daily('NIFTY'))

