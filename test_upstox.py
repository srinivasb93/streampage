import json

import pandas as pd
import requests
from dotenv import load_dotenv
import os
from upstox_client import OrderApi
import ast

load_dotenv()
access_token = os.getenv("UPSTOX_ACCESS_TOKEN")
import upstox_client
from upstox_client.rest import ApiException

configuration = upstox_client.Configuration()
configuration.access_token = access_token

api_instance = upstox_client.OrderApi(upstox_client.ApiClient(configuration))
holidays_api = upstox_client.MarketHolidaysAndTimingsApi(upstox_client.ApiClient(configuration))
market_api = upstox_client.MarketQuoteApi(upstox_client.ApiClient(configuration))
api_version = '2.0'
try:
    # Get order book
    # api_response = holidays_api.get_holidays()
    # Convert each string dictionary to an actual dictionary
    token='TATASTEEL'
    # api_response = market_api.get_market_quote_ohlc(symbol='NSE_EQ|INE081A01020', api_version='v2').data
    api_response = market_api.get_market_quote_ohlc(symbol='NSE_EQ|INE081A01020', api_version='v2').data
    latest_data = {}
    for key, data in api_response.items():
        latest_data[data.instrument_token] = data

    print(latest_data)
    # data = [item.to_dict() for item in api_response]

    # Convert to DataFrame
    # df = pd.DataFrame(data)
    #
    # print(df)
    # print(api_response.data)
    # print(api_response)
except ApiException as e:
    print("Exception when calling OrderApi->get_order_book: %s\n" % e)
