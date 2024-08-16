import pandas as pd
from sqlalchemy import create_engine
import urllib


def create_connection(database='NSEDATA'):
    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                     f"DATABASE={database};"
                                     "Trusted_Connection=yes")
    db_url = "mssql+pyodbc:///?odbc_connect={}".format(params)
    engine = create_engine(db_url)
    return engine


ref_data = []


# Function to retrieve the list of reference tables
def get_ref_tables(selected_database):
    global ref_data
    # Execute a query to retrieve the list of reference tables
    query = 'SELECT table_name FROM ' + selected_database + ".information_schema.tables where table_schema='ref'"
    ref_tables = pd.read_sql(query, create_connection(selected_database))
    ref_data = ref_tables['table_name'].to_list()


# Function to retrieve the list of databases
def get_database_list():
    # Execute a query to retrieve the list of databases
    query = "SELECT name FROM sys.databases where name not in ('master', 'model', 'msdb', 'tempdb')"
    databases = pd.read_sql(query, create_connection())
    rows = databases['name'].to_list()

    # Return a list of database names
    return [row for row in rows]


def get_database_tables_list(database):
    # Execute a query to retrieve the list of databases
    query = f"SELECT name FROM {database}.sys.tables"
    databases = pd.read_sql(query, create_connection())
    rows = databases['name'].to_list()

    # Return a list of database names
    return [row for row in rows]


# Function to retrieve data from the selected table
def get_table_data(selected_database='NSEDATA',
                   selected_table='TATAMOTORS',
                   query=None,
                   sample=False,
                   sample_count=100,
                   sort=False,
                   sort_order='ASC',
                   sort_by='Date'):
    if not query:
        database = selected_database
        table = selected_table
        get_ref_tables(selected_database)
        tab_schema = '.dbo.' if table not in ref_data else '.ref.'
        query = f'SELECT * FROM {database}{tab_schema}{table}'

        if sample:
            query = f'SELECT top {sample_count}* FROM {database}{tab_schema}{table}'

        if sort:
            query += f" ORDER BY {sort_by} {sort_order}"

    table_data = pd.read_sql(query, create_connection(selected_database))

    return table_data


def load_sql_data(data_to_load, table_name, load_type='replace',
                  index_required=False, database='NSEDATA', schema='dbo'):
    try:
        data_to_load.to_sql(name=table_name,
                            con=create_connection(database),
                            if_exists=load_type,
                            index=index_required,
                            schema=schema)
        return f'{table_name} table has been loaded successfully'
    except Exception as e:
        print(e)
        return f'Failed to {load_type} {table_name} table'




