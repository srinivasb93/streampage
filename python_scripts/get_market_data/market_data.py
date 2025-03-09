"""
This module fetches data from NSE API
"""
import datetime

import pandas as pd
import requests
from common_utils import read_write_sql_data as rd
import nsepython as np
import datetime as dt

headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,en-IN;q=0.8,en-GB;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0"
        }


def fetch_nse_data(payload):
    try:
        output = requests.get(payload, headers=headers).json()
        # print(output)
    except ValueError:
        s = requests.Session()
        output = s.get("http://nseindia.com", headers=headers)
        output = s.get(payload, headers=headers).json()
    return output


class MarketData:
    def __init__(self):
        self.base_url = "https://www.nseindia.com/api/"
        self.headers = {"User-Agent": "Mozilla/5.0"}
        self.event_type = "dividend"
        self.broad_indices_list = [
            "NIFTY 50", "NIFTY NEXT 50", "NIFTY MIDCAP 50", "NIFTY MIDCAP 100", "NIFTY MIDCAP 150",
            "NIFTY SMALLCAP 250", "NIFTY SMALLCAP 50", "NIFTY SMALLCAP 100", "NIFTY 100", "NIFTY 200",
            "NIFTY 500", "NIFTY MIDSMALLCAP 400", "NIFTY MIDCAP SELECT", "NIFTY LARGEMIDCAP 250"]

        self.sector_indices_list = [
            "NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL", "NIFTY PHARMA",
            "NIFTY PSU BANK", "NIFTY REALTY", "NIFTY AUTO", "NIFTY HEALTHCARE INDEX", "NIFTY FMCG",
            "NIFTY PRIVATE BANK", "NIFTY CONSUMER DURABLES", "NIFTY OIL & GAS"]

        self.thematic_indices_list = [
            "NIFTY ENERGY", "NIFTY CPSE", "NIFTY INFRASTRUCTURE", "NIFTY100 LIQUID 15", "NIFTY PSE",
            "NIFTY COMMODITIES", "NIFTY MNC", "NIFTY INDIA CONSUMPTION",  "NIFTY MIDCAP LIQUID 15",
            "NIFTY SERVICES SECTOR", "NIFTY INDIA DIGITAL", "NIFTY INDIA MANUFACTURING"]

    def get_corporate_actions_data(self, stock_type=None, event='all'):
        """
        Get corporate actions for all stocks or for selected stock
        """
        url = self.base_url
        event_type = self.event_type

        # fetch data from nse api
        response = requests.get(url, headers=headers)
        data = response.json()
        return data

    def fetch_and_load_etf_data(self, as_df=True, load=True):
        url = self.base_url + "etf"
        etf_data = fetch_nse_data(url)
        if as_df:
            etf_data = pd.DataFrame(etf_data.get("data", []))
            exp_columns = ['symbol', 'open', 'high', 'low', 'nav', 'qty', 'meta']
            etf_data = etf_data[exp_columns]
            column_mapping = {'symbol': 'Symbol', 'assets': 'Asset_Type', 'open': 'Open', 'high': 'High',
                              'low': 'Low', 'nav': 'Close', 'qty': 'Volume', 'meta': 'Info'}
            etf_data.rename(columns=column_mapping, inplace=True)
            etf_data['Company_Name'] = etf_data['Info'].apply(lambda x: x['companyName'])
            etf_data['Listing_Date'] = etf_data['Info'].apply(lambda x: x['listingDate'])
            etf_data['Listed_Years'] = round(
                (dt.datetime.today() - pd.to_datetime(etf_data['Listing_Date'])).dt.days/365, 1)
            etf_data['Delisted'] = etf_data['Info'].apply(lambda x: x['isDelisted'])
            etf_data['Suspended'] = etf_data['Info'].apply(lambda x: x['isSuspended'])
            etf_data.drop('Info', axis=1, inplace=True)

            if load:
                msg = rd.load_sql_data(data_to_load=etf_data, table_name='ETF_DATA')
                print(msg)
        return etf_data

    @staticmethod
    def fetch_index_pe_pb_div_data(start_date, end_date, index_name='NIFTY_50'):
        index_data = np.index_pe_pb_div(symbol=index_name, start_date=start_date, end_date=end_date)
        return index_data

    def get_nse_indices_data(self, as_df=True):
        """
        Get nse indices
        :param as_df:
        :return:
        """
        url = self.base_url + "allIndices"
        indices_data = fetch_nse_data(url)
        if as_df:
            indices_data = pd.DataFrame(indices_data.get("data", []))
        return indices_data

    def load_indices_data(self):
        """
        Load indices data
        """
        data_to_load = self.get_nse_indices_data()
        load_msg = rd.load_sql_data(data_to_load, table_name="NSE_INDICES_DATA")
        print(load_msg)
        return "Success" if "success" in load_msg else "Failure"

    @staticmethod
    def get_main_nse_indices_list(all_data=False):
        """
        Get main nse indices list
        :return:
        """
        indices_data = rd.get_table_data(selected_database="NSEDATA", selected_table="NSE_INDICES_DATA")
        req_indices_list = indices_data['index'].to_list()
        return req_indices_list if not all_data else indices_data

    def get_index_stocks_data(self, stock_idx='NIFTY 50', as_df=True):
        """
        Get index stocks data
        :return:
        """
        url = self.base_url + "equity-stockIndices?index=" + stock_idx.replace("&", "%26") if "&" in stock_idx else \
            self.base_url + "equity-stockIndices?index=" + stock_idx
        index_data = fetch_nse_data(url)
        if as_df:
            index_data = pd.DataFrame(index_data.get("data", []))
            index_data = index_data[['symbol', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'previousClose', 'change',
                                     'pChange', 'yearHigh', 'yearLow', 'totalTradedVolume', 'totalTradedValue',
                                     'lastUpdateTime', 'nearWKH', 'nearWKL', 'perChange365d', 'date365dAgo',
                                     'date30dAgo', 'perChange30d']]
            index_data.rename(columns={'symbol': 'Symbol', 'open': 'Open', 'dayHigh': 'High', 'dayLow': 'Low',
                                       'lastPrice': 'Close', 'previousClose': 'Prev_Close', 'change': 'Day_Change',
                                       'pChange': 'Pct_Change', 'yearHigh': 'Year_High', 'yearLow': 'Year_Low',
                                       'totalTradedVolume': 'Traded_Volume', 'totalTradedValue': 'Traded_Value',
                                       'lastUpdateTime': 'Last_Updated', 'nearWKH': 'WK_High', 'nearWKL': 'WK_Low',
                                       'perChange365d': 'Pct_Change_365d', 'date365dAgo': 'Date_365d_Ago',
                                       'date30dAgo': 'Date_30d_Ago', 'perChange30d': 'Pct_Change_30d'}, inplace=True)
        return index_data

    def load_index_stocks_data(self, indices_list):
        """
        Load indices data
        :param indices_list:
        :return:
        """
        status_list = []
        for stock_index in indices_list:
            data_to_load = self.get_index_stocks_data(stock_idx=stock_index)
            table_name = stock_index.replace(" ", "_") + "_REF"
            if '&' in table_name:
                table_name = table_name.replace('&', 'AND')
            load_msg = rd.load_sql_data(data_to_load, table_name=table_name)
            if 'success' in load_msg:
                status_list.append(True)
            else:
                status_list.append(False)

        return "Success" if all(status_list) else "Failure"

    def load_all_stocks_table_with_stock_index(self, all_indices_list):
        """
        Load all stocks along with their index details into sql table
        :param all_indices_list:
        :return:
        """
        all_stocks_df = pd.DataFrame()
        indices_ref = self.get_main_nse_indices_list(all_data=True)
        for stock_index in all_indices_list:
            table_name = stock_index.replace(" ", "_") + "_REF"
            if '&' in table_name:
                table_name = table_name.replace('&', 'AND')
            stock_idx_data = rd.get_table_data(selected_database="NSEDATA", selected_table=table_name)
            stock_index_symbol = indices_ref['indexSymbol'][indices_ref['index'] == stock_index].values[0]
            stock_idx_data['STK_INDEX'] = stock_index
            stock_idx_data['STK_INDEX_SYMBOL'] = stock_index_symbol
            stock_idx_data = stock_idx_data[['Symbol', 'STK_INDEX', 'STK_INDEX_SYMBOL']][1:]
            stock_idx_data.rename(columns={'Symbol': 'SYMBOL'}, inplace=True)

            all_stocks_df = pd.concat([all_stocks_df, stock_idx_data], ignore_index=True, axis=0)
        load_msg = rd.load_sql_data(all_stocks_df, table_name='ALL_STOCKS')
        return "Success" if "success" in load_msg else "Failure"


def load_index_and_stocks_data(load_type="Index_data_load"):
    md = MarketData()
    indices_list = md.broad_indices_list + md.sector_indices_list + md.thematic_indices_list
    data_load_msg = ""
    if load_type == "Index_data_load":
        data_load_msg = md.load_indices_data()
    elif load_type == "Index_Stocks_data_load":
        data_load_msg = md.load_index_stocks_data(indices_list)
    elif load_type == "Stocks_Ref_data_load":
        data_load_msg = md.load_all_stocks_table_with_stock_index(indices_list)
    return data_load_msg


if __name__ == "__main__":
    # broad_indices_list = ["NIFTY 50", "NIFTY NEXT 50", "NIFTY MIDCAP 50", "NIFTY MIDCAP 100", "NIFTY MIDCAP 150",
    #                       "NIFTY SMALLCAP 250", "NIFTY SMALLCAP 50", "NIFTY SMALLCAP 100", "NIFTY 100", "NIFTY 200",
    #                       "NIFTY 500", "NIFTY MIDSMALLCAP 400", "NIFTY MIDCAP SELECT", "NIFTY LARGEMIDCAP 250"]
    #
    # sector_indices_list = ["NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL",
    #                        "NIFTY PHARMA", "NIFTY PSU BANK", "NIFTY REALTY", "NIFTY AUTO", "NIFTY HEALTHCARE INDEX",
    #                        "NIFTY FMCG", "NIFTY PRIVATE BANK", "NIFTY CONSUMER DURABLES", "NIFTY OIL & GAS"]
    #
    # thematic_indices_list = ["NIFTY ENERGY", "NIFTY CPSE", "NIFTY INFRASTRUCTURE", "NIFTY100 LIQUID 15",
    #                          "NIFTY PSE", "NIFTY COMMODITIES", "NIFTY MNC", "NIFTY INDIA CONSUMPTION",
    #                          "NIFTY MIDCAP LIQUID 15", "NIFTY SERVICES SECTOR", "NIFTY INDIA DIGITAL",
    #                          "NIFTY INDIA MANUFACTURING"]
    # sector_indices_list = []
    # broad_indices_list = []
    # thematic_indices_list = []
    # indices_list = broad_indices_list + sector_indices_list + thematic_indices_list
    md = MarketData()
    # print(md.fetch_and_load_etf_data())
    data = md.fetch_index_pe_pb_div_data(start_date="1-1-2020",
                                         end_date="1-1-2025")
    print(data)
    # print(md.load_index_stocks_data(indices_list))
    # print(md.load_all_stocks_table_with_stock_index(indices_list))
    # print(md.get_main_nse_indices_list())
