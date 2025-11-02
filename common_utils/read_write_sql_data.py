import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from .logging_utils import configure_logging
import configparser
import os
import threading
import datetime as dt

# Import all required data source libraries
from openchart import NSEData
from nsepython import index_history


# --- Configuration ---

def get_config():
    """Reads database credentials from config.ini and applies env var overrides."""
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Allow overriding [postgres] settings via environment variables
    # Supported env vars: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE
    if 'postgres' in config:
        overrides = {
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'database': os.getenv('POSTGRES_DATABASE'),
        }
        for key, value in overrides.items():
            if value:
                config['postgres'][key] = value

    return config


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
        config = get_config()['postgres']
        database_name = config.get('database', 'nsedata')

    if database_name in _ENGINES:
        return _ENGINES[database_name]

    with _LOCK:
        if database_name in _ENGINES:
            return _ENGINES[database_name]

        try:
            config = get_config()['postgres']
            db_url = (
                f"postgresql+psycopg2://{config['user']}:{config['password']}@"
                f"{config['host']}:{config['port']}/{database_name}"
            )

            # FIX: Reduced pool size and overflow to prevent "too many clients" error.
            # This makes the application more conservative with its connection usage.
            # If this error persists, consider increasing `max_connections` in your
            # postgresql.conf file on the database server itself.
            new_engine = create_engine(
                db_url,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800
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

def ensure_registry_table_exists(database='nsedata'):
    """Checks if the STOCKS_IN_DB table exists and creates it if it doesn't."""
    try:
        engine = get_engine(database)
        create_statement = text("""
            CREATE TABLE IF NOT EXISTS public."STOCKS_IN_DB" (
                "SYMBOL" VARCHAR(255) PRIMARY KEY,
                "instrument_token" VARCHAR(255),
                "last_updated" TIMESTAMP
            );
        """)
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(create_statement)
        logger.info("Ensured 'STOCKS_IN_DB' table exists.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Failed to create or verify 'STOCKS_IN_DB' table: {e}")
        return False

def add_stock_to_registry(stock_symbol, instrument_token, database='nsedata'):
    """
    SPECIFIC USE CASE: A wrapper for upsert_record to add a stock to the STOCKS_IN_DB table.
    """
    stock_data = {"SYMBOL": stock_symbol, "instrument_token": instrument_token, "last_updated": dt.datetime.now()}
    return upsert_record(database=database, table_name="STOCKS_IN_DB", data_dict=stock_data, conflict_column="SYMBOL")


# --- ENHANCED: Index and Sector Data Loading with Multi-Level Fallback ---

# FIX: Create a single, cached instance of the NSEData class from openchart
_NSE_DATA_INSTANCE = None


def get_nse_data_instance():
    """Returns a single instance of openchart.NSEData, creating it if necessary."""
    global _NSE_DATA_INSTANCE
    if _NSE_DATA_INSTANCE is None:
        _NSE_DATA_INSTANCE = NSEData()
        _NSE_DATA_INSTANCE.download()  # Download symbol master
    return _NSE_DATA_INSTANCE


def _format_openchart_data(df):
    """Standardizes the DataFrame from openchart."""
    df.reset_index(inplace=True)
    df.rename(columns={'Timestamp': 'timestamp', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close',
                       'Volume': 'volume'}, inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()


def _format_nsepython_data(df):
    """Standardizes the DataFrame from nsepython."""
    df.rename(columns={'HistoricalDate': 'Date', 'OPEN': 'Open', 'HIGH': 'High', 'LOW': 'Low', 'CLOSE': 'Close'},
              inplace=True)
    df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=True)
    df['Volume'] = 0
    return df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()


def _fetch_stock_historical_data_with_openchart(symbol, start_date, end_date, interval="1d"):
    """Tries fetching from openchart """
    try:
        logger.info(f"Attempting to fetch stock data for '{symbol}' using openchart.")
        nse_instance = get_nse_data_instance()
        start_datetime = dt.datetime.combine(start_date, dt.datetime.min.time())
        end_datetime = dt.datetime.combine(end_date, dt.datetime.max.time())

        raw_data_oc = nse_instance.historical(symbol=symbol, start=start_datetime, end=end_datetime, interval=interval)
        if not raw_data_oc.empty:
            logger.info("openchart fetch successful for stock.")
            return _format_openchart_data(raw_data_oc)
        logger.warning("openchart returned no data.")
    except Exception as e_oc:
        logger.error(f"openchart failed for stock '{symbol}': {e_oc}.")

    return pd.DataFrame()


def _fetch_index_data_with_fallback(symbol, start_date, end_date, data_source='openchart'):
    """
    Tries fetching from openchart first, using a dynamic symbol lookup.
    Falls back to other libraries if the primary source fails.
    """
    # 1. Primary: openchart (Provides Volume)
    if data_source == 'openchart':
        try:
            logger.info(f"Attempting to fetch data for '{symbol}' using openchart.")
            nse_instance = get_nse_data_instance()

            # --- NEW: Dynamic Symbol Lookup ---
            api_symbol = symbol  # Default to the input symbol

            # Attempt to look up the symbol in the openchart master list
            symbol_mapping = {"NIFTY100 LIQUID 15": "Nifty100 Liq 15",
                            "NIFTY MIDCAP LIQUID 15": "Nifty Mid Liq 15",
                            "NIFTY INDIA DIGITAL": "Nifty Ind Digital",
                            "NIFTY SMALLCAP 250": "NIFTY SMLCAP 250",
                            "NIFTY SMALLCAP 50": "NIFTY SMLCAP 50",
                            "NIFTY SMALLCAP 100": "NIFTY SMLCAP 100",
                            "NIFTY MIDSMALLCAP 400": "NIFTY MIDSML 400",
                            "NIFTY MIDCAP SELECT": "NIFTY MID SELECT",
                            "NIFTY LARGEMIDCAP 250": "NIFTY LARGEMID250",
                            "NIFTY HEALTHCARE INDEX": "NIFTY HEALTHCARE",
                            "NIFTY CONSUMER DURABLES": "NIFTY CONSR DURBL",
                            "NIFTY FINANCIAL SERVICES": "Nifty Fin Service",
                            "NIFTY PRIVATE BANK": "Nifty Pvt Bank",
                            "NIFTY INFRASTRUCTURE": "Nifty Infra",
                            "NIFTY SERVICES SECTOR": "Nifty Serv Sector",
                            "NIFTY INDIA CONSUMPTION": "Nifty Consumption"
                            }

            try:
                # The master list is stored in the 'nse_data' attribute
                master_df = nse_instance.nse_data
                # Find the row where 'Name' matches the input symbol (case-insensitive)
                if symbol in symbol_mapping.keys():
                    logger.info(f"Using mapped API symbol for '{symbol}': '{symbol_mapping[symbol]}'")
                    match = master_df[master_df['Name'].str.lower() == symbol_mapping[symbol].lower()]
                else:
                    match = master_df[master_df['Name'].str.lower() == symbol.lower()]
                if not match.empty:
                    # Get the corresponding 'Symbol' from that row
                    api_symbol = match['Symbol'].iloc[0]
                    logger.info(f"Found matching API symbol for '{symbol}': '{api_symbol}'")
                else:
                    logger.warning(
                        f"Could not find a matching symbol for '{symbol}' in openchart master. Using original name.")
            except Exception as lookup_error:
                logger.error(f"Error during symbol lookup for '{symbol}': {lookup_error}. Using original name.")
            # --- End of New Logic ---

            start_datetime = dt.datetime.combine(start_date, dt.datetime.min.time())
            end_datetime = dt.datetime.combine(end_date, dt.datetime.max.time())

            raw_data_oc = nse_instance.historical(
                symbol=api_symbol,  # Use the looked-up symbol
                exchange='NSE',
                start=start_datetime,
                end=end_datetime,
                interval='1d'
            )
            if not raw_data_oc.empty:
                logger.info("openchart fetch successful.")
                return _format_openchart_data(raw_data_oc)
            logger.warning("openchart returned no data. Trying fallback 1: nsepython.")
        except Exception as e_oc:
            logger.error(f"openchart failed: {e_oc}. Trying fallback 1: nsepython.")

    # 2. Fallback: nsepython
    if data_source in ['nsepython', 'openchart']:
        try:
            logger.info(f"Attempting to fetch data for '{symbol}' using nsepython.")
            raw_data_nse = index_history(symbol=symbol, start_date=start_date.strftime('%d-%m-%Y'),
                                        end_date=end_date.strftime('%d-%m-%Y'))
            if not raw_data_nse.empty:
                logger.info("nsepython fetch successful.")
                return _format_nsepython_data(raw_data_nse)
            logger.warning("nsepython returned no data.")
        except Exception as e_nse:
            logger.error(f"nsepython failed: {e_nse}")

    logger.warning("All data sources failed for the given period.")
    return pd.DataFrame()


def load_index_sector_history(symbol, start_date_obj, end_date_obj, database='nsedata', data_source='openchart'):
    """Performs a full historical data load for an NSE index or sector with multi-level fallback."""
    logger.info(f"Starting historical load for '{symbol}' from {start_date_obj} to {end_date_obj}.")
    table_name = symbol.replace(" ", "_").replace("-", "_")

    formatted_data = _fetch_index_data_with_fallback(symbol, start_date_obj, end_date_obj, data_source)

    if formatted_data.empty:
        return "Failed to fetch data from all available sources."

    return load_sql_data(
        data_to_load=formatted_data, table_name=table_name, database=database,
        load_type='replace', index_required=False
    )


def update_index_sector_daily(symbol, database='nsedata'):
    """Performs an incremental daily update for an NSE index or sector with multi-level fallback."""
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

        formatted_data = _fetch_index_data_with_fallback(symbol, start_date_obj, end_date_obj)

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

    formatted_data = _fetch_stock_historical_data_with_openchart(symbol, start_date_obj, end_date_obj,
                                                     interval=interval)
    if formatted_data.empty:
        return "Failed to fetch data from all available sources."

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
        formatted_data = _fetch_stock_historical_data_with_openchart(symbol, start_date_obj, end_date_obj)
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
    print(load_index_sector_history('NIFTY 50', start_date, end_date))

    # Update index data example
    # print(update_index_sector_daily('NIFTY'))

