import pandas as pd
from common_utils import read_write_sql_data as rd

split_data = rd.get_table_data(selected_table='SPLIT_DATA')
bonus_data = rd.get_table_data(selected_table='BONUS')
required_cols = ['SYMBOL', 'DATE', 'PURPOSE', 'SPLIT_BONUS_FACTOR']

split_data.rename({'Security Name': 'SYMBOL',
                   'Ex Date': 'DATE',
                   'Purpose': 'PURPOSE',
                   'split_factor': 'SPLIT_BONUS_FACTOR'}, axis=1, inplace=True)

bonus_data.rename({'Security Name': 'SYMBOL',
                   'Ex Date': 'DATE',
                   'Purpose': 'PURPOSE',
                   'bonus_factor': 'SPLIT_BONUS_FACTOR'}, axis=1, inplace=True)

split_data = split_data[required_cols]
bonus_data = bonus_data[required_cols]
# split_df = pd.read_sql(get_split_query, con=conn)
# bonus_df = pd.DataFrame(bonus_data, columns=['SYMBOL', 'DATE', 'PURPOSE', 'SPLIT_BONUS_FACTOR'])
# split_df = pd.DataFrame(split_data, columns=['SYMBOL', 'DATE', 'PURPOSE', 'SPLIT_BONUS_FACTOR'])

combined_data = pd.concat([split_data.dropna(axis=1), bonus_data.dropna(axis=1)])

load_msg = rd.load_sql_data(data_to_load=combined_data, table_name='SPLIT_BONUS_DATA')
print(load_msg)
print('Split/Bonus data Load is complete for all stocks')
