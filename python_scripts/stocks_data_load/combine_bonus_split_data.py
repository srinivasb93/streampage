import pandas as pd
import sqlalchemy as sa
import urllib.parse

# Use this for windows authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                     "DATABASE=NSEDATA;"
                                     "Trusted_Connection=yes")

'''
#Use this for SQL server authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                 "DATABASE=test;"
                                 "UID=user;"
                                 "PWD=password")
'''

# Connection String
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

# Connect to the required SQL Server
conn = engine.connect()

split_data = conn.execute("select [Security Name],[Ex Date],[Purpose],[split_factor] from dbo.SPLIT_DATA").fetchall()
bonus_data = conn.execute("select [Security Name],[Ex Date],[Purpose],[bonus_factor] from dbo.BONUS").fetchall()

# split_df = pd.read_sql(get_split_query, con=conn)

bonus_df = pd.DataFrame(bonus_data, columns=['SYMBOL', 'DATE', 'PURPOSE', 'SPLIT_BONUS_FACTOR'])
split_df = pd.DataFrame(split_data, columns=['SYMBOL', 'DATE', 'PURPOSE', 'SPLIT_BONUS_FACTOR'])

combined_data = pd.concat([split_df, bonus_df])

combined_data.to_sql('SPLIT_BONUS_DATA', con=conn, index=False, if_exists="replace")

conn.close()

print('Split/Bonus data Load is complete for all stocks')
