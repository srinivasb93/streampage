import pandas as pd
from sqlalchemy import create_engine
import urllib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ref_data = []
engine = None


# Create a reusable database connection
def create_connection(database='NSEDATA'):
    global engine
    if engine is None:
        try:
            params = urllib.parse.quote_plus(
                "DRIVER={SQL Server Native Client 11.0};"
                "SERVER=IN01-9MCXZH3\\SQLEXPRESS;"
                f"DATABASE={database};"
                "Trusted_Connection=yes"
            )
            db_url = f"mssql+pyodbc:///?odbc_connect={params}"
            engine = create_engine(db_url, pool_size=5, max_overflow=10, pool_timeout=30, pool_recycle=1800)
            logger.info("Database connection created successfully.")
        except Exception as e:
            logger.error(f"Failed to create database connection: {e}")
            raise
    return engine


# Retrieve the list of reference tables
def get_ref_tables(selected_database):
    global ref_data
    try:
        query = f"SELECT table_name FROM {selected_database}.information_schema.tables WHERE table_schema='ref'"
        ref_tables = pd.read_sql(query, create_connection(selected_database))
        ref_data = ref_tables['table_name'].to_list()
        logger.info("Reference tables retrieved successfully.")
    except Exception as e:
        logger.error(f"Error retrieving reference tables: {e}")
        ref_data = []


# Retrieve the list of databases
def get_database_list():
    try:
        query = "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'model', 'msdb', 'tempdb')"
        databases = pd.read_sql(query, create_connection())
        return databases['name'].to_list()
    except Exception as e:
        logger.error(f"Error retrieving database list: {e}")
        return []


# Retrieve the list of tables in a database
def get_database_tables_list(database):
    try:
        query = f"SELECT name FROM {database}.sys.tables"
        tables = pd.read_sql(query, create_connection())
        return tables['name'].to_list()
    except Exception as e:
        logger.error(f"Error retrieving table list for database {database}: {e}")
        return []


# Retrieve data from a table or custom query
def get_table_data(selected_database='NSEDATA', selected_table='TATAMOTORS', query=None,
                   sample=False, sample_count=100, sort=False, sort_order='ASC', sort_by='Date'):
    try:
        if not query:
            schema = '.dbo.' if selected_table not in ref_data else '.ref.'
            query = f"SELECT * FROM {selected_database}{schema}{selected_table}"
            if sample:
                query = f"SELECT TOP {sample_count} * FROM {selected_database}{schema}{selected_table}"
            if sort:
                query += f" ORDER BY {sort_by} {sort_order}"
        return pd.read_sql(query, create_connection(selected_database))
    except Exception as e:
        logger.error(f"Error retrieving data from table {selected_table}: {e}")
        return pd.DataFrame()


# Load data into a SQL table
def load_sql_data(data_to_load, table_name, load_type='replace', index_required=False,
                  database='NSEDATA', schema='dbo'):
    try:
        data_to_load.to_sql(
            name=table_name,
            con=create_connection(database),
            if_exists=load_type,
            index=index_required,
            schema=schema
        )
        logger.info(f"Data loaded successfully into table {table_name}.")
        return f"{table_name} table has been loaded successfully"
    except Exception as e:
        logger.error(f"Failed to load data into table {table_name}: {e}")
        return f"Failed to {load_type} {table_name} table"




