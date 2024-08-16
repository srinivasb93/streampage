import datetime
import pandas as pd

from common_utils import read_write_sql_data as rd

stocks = ['NIFTY_100', 'NIFTY_200', 'NIFTY_500', 'NIFTY_AUTO', 'NIFTY_BANK', 'NIFTY_COMMODITIES', 'NIFTY_CONSUMPTION', 'NIFTY_CPSE', 'NIFTY_ENERGY', 'NIFTY_FIN_SERVICE', 'NIFTY_FMCG', 'NIFTY_INFRA', 'NIFTY_IT', 'NIFTY_MEDIA', 'NIFTY_METAL', 'NIFTY_MIDCAP_100', 'NIFTY_MIDCAP_50', 'NIFTY_MNC', 'NIFTY_NEXT_50', 'NIFTY_PHARMA', 'NIFTY_PSE', 'NIFTY_PSU_BANK', 'NIFTY_REALTY', 'NIFTY_SERV_SECTOR', 'NIFTY_SMLCAP_100', 'NIFTY_SMLCAP_250', 'NIFTY_SMLCAP_50', 'OFSS']

for stock in stocks:
    data = rd.get_table_data(selected_table=stock, sort=True)
    data['Date'] = pd.to_datetime(data['Date'], format='mixed')
    msg = rd.load_sql_data(data_to_load=data, table_name=stock)
    print(msg)

