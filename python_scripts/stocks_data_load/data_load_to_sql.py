import pandas as pd
import sqlalchemy as sa
import urllib.parse
import os

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

curr_dir = 'C:/Users/srinivas/PycharmProjects/data_analysis_all'
file_list = [os.path.join(curr_dir, 'data', f) for f in os.listdir('../data')]

for file in file_list:
    stock = file.split('\\')[-1].split('.')[0]
    file_data = pd.read_csv(file, parse_dates=True)
    # Load file data to SQL Table
    file_data.to_sql(stock, con=conn, if_exists='replace', index=False)
    print('Data Load is complete for stock : {}'.format(stock))

print('Data Load is complete for all stocks')
