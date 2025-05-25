import asyncio
from functools import partial
from sqlalchemy import create_engine, text
import pandas as pd
import logging
from common_utils.read_write_sql_data import create_connection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Database configuration
DATABASE = "NSEDATA"
engine = create_connection(database=DATABASE)

async def run_in_executor(func, *args):
    """Run a synchronous function in a thread pool."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args))

def execute_query(query, params=None):
    """Synchronous function to execute a SQL query."""
    with engine.connect() as conn:
        if params:
            result = conn.execute(query, params)
        else:
            result = conn.execute(query)
        conn.commit()
        return result

def fetch_query(query, params=None):
    """Synchronous function to fetch query results as DataFrame."""
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)

async def async_execute_query(query, params=None):
    """Async wrapper for executing a SQL query."""
    return await run_in_executor(execute_query, query, params)

async def async_fetch_query(query, params=None):
    """Async wrapper for fetching query results."""
    return await run_in_executor(fetch_query, query, params)