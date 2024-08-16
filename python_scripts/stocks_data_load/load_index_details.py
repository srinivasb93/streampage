import pandas as pd
import os
import sqlalchemy as sa
import urllib.parse

filepath = r'F:\Indices'
file_list = [os.path.join(filepath, f) for f in os.listdir(filepath)]

#Use this for windows authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                 "DATABASE=NSEDATA;"
                                 "Trusted_Connection=yes")

'''
#Use this for SQL server authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=dagger;"
                                 "DATABASE=test;"
                                 "UID=user;"
                                 "PWD=password")
'''

# Connection String
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

# Connect to the required SQL Server
conn = engine.connect()

all_data = pd.DataFrame()

for file in file_list:
    data = pd.read_csv(file, parse_dates=True, usecols=[0])
    data.columns = ['SYMBOL']
    # filename = (file.split('-'))
    # stk_index = filename[1] + '_' + filename[2]
    stk_index = data.iloc[0, 0]
    data.drop([0], inplace=True)
    data['STK_INDEX'] = stk_index
    # data.to_sql(stk_index+'_STOCKS', con=conn, index=False, if_exists='replace')
    print('Data Load done for index : {}'.format(stk_index))
    all_data = all_data.append(data)


all_data.to_sql('ALL_STOCKS', con=conn, index=False, if_exists='replace')
print('Data Load is complete for all indices')

