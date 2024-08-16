# from streamlit_carousel import carousel
#
# test_items = [
#     dict(
#         title="Slide 1",
#         text="A tree in the savannah",
#         img="https://img.freepik.com/free-photo/wide-angle-shot-single-tree-growing-clouded-sky-during-sunset-surrounded-by-grass_181624-22807.jpg?w=1380&t=st=1688825493~exp=1688826093~hmac=cb486d2646b48acbd5a49a32b02bda8330ad7f8a0d53880ce2da471a45ad08a4",
#         link="https://discuss.streamlit.io/t/new-component-react-bootstrap-carousel/46819",
#     ),
#     dict(
#         title="Slide 2",
#         text="A wooden bridge in a forest in Autumn",
#         img="https://img.freepik.com/free-photo/beautiful-wooden-pathway-going-breathtaking-colorful-trees-forest_181624-5840.jpg?w=1380&t=st=1688825780~exp=1688826380~hmac=dbaa75d8743e501f20f0e820fa77f9e377ec5d558d06635bd3f1f08443bdb2c1",
#         link="https://github.com/thomasbs17/streamlit-contributions/tree/master/bootstrap_carousel",
#     ),
#     dict(
#         title="Slide 3",
#         text="A distant mountain chain preceded by a sea",
#         img="https://img.freepik.com/free-photo/aerial-beautiful-shot-seashore-with-hills-background-sunset_181624-24143.jpg?w=1380&t=st=1688825798~exp=1688826398~hmac=f623f88d5ece83600dac7e6af29a0230d06619f7305745db387481a4bb5874a0",
#         link="https://github.com/thomasbs17/streamlit-contributions/tree/master",
#     ),
#     dict(
#         title="Slide 4",
#         text="PANDAS",
#         img="pandas.webp",
#     ),
#     dict(
#         title="Slide 4",
#         text="CAT",
#         img="cat.jpg",
#     ),
# ]
#
# carousel(items=test_items)
# import pandas as pd
# from jugaad_data import nse
# import jugaad_data as jd
# import datetime as dt
# from jugaad_data import nse
# import nsepython as np
#
# # print(np.nse_get_advances_declines())
#
# print(np.nse_eq('SBIN'))
# live_data = nse.NSELive()
#
#
# # print(live_data.trade_info(symbol='SBIN'))
# print(live_data.all_indices())






# extract_date = pd.to_datetime(dt.date(2024, 3, 8)).date()
# cm_holidays = nse.NSELive().holiday_list()['CM']
# nse_holidays = [pd.to_datetime(rec['tradingDate']).date() for rec in cm_holidays]
#
# if extract_date in nse_holidays:
#     print(f"It's a holiday on {extract_date}. No Data found")
# print(nse_holidays)
# import os
# current_file_path = os.pardir.split()
# print(current_file_path)
# # current_file_path = os.path.abspath(__file__)
# required_path = "\\".join(current_file_path.split("\\")[:-1])
# print(required_path)
# file_path = os.path.join(required_path, "logfiles", 'DATALOAD.log')
# print(file_path)
import yfinance as yf
# from common_utils import read_write_sql_data as rd
# stock = 'SBIN'
# extract_date = '11-02-1020'
# data_load_failed_dict = dict()
# query_max_date = "SELECT max(DATE) FROM dbo." + 'SBIN'
# startdate = rd.get_table_data(query=query_max_date)
# start_dt = startdate.loc[0][0]
# start_dt = pd.to_datetime(start_dt).date()
# data_load_failed_dict.update({'stock': (stock, extract_date)})
# data_load_failed_dict.update({'stock': ('TATA', "10")})
# print(start_dt)
# print(data_load_failed_dict)
# data = rd.get_table_data(selected_table='NIFTY_50')
# print(data.dtypes)
# Clean up the "Date" column by stripping extra characters (assuming time part needs to be removed)
# data["Date"] = data["Date"].str.split().str[0]  # Extracting only the date part

# data["Date"] = pd.to_datetime(data["Date"], format='ISO8601')
# # data["Date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d %H:%M:%S")
#
# print(data.dtypes)
# print(data.head())
# print(data.tail())