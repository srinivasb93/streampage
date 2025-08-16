import asyncio
from functools import partial
import pandas as pd
import logging
from .read_write_sql_data import create_connection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Database configuration
DATABASE = "nsedata"
# CHANGED: create_connection is now PostgreSQL compatible
engine = create_connection(database=DATABASE)

async def run_in_executor(func, *args):
    """Run a synchronous function in a thread pool."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args))

def execute_query(query, params=None):
    """Synchronous function to execute a SQL query."""
    # ENHANCEMENT: Use a transactional block for safety
    with engine.connect() as conn:
        with conn.begin(): # Start a transaction
            if params:
                result = conn.execute(query, params)
            else:
                result = conn.execute(query)
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