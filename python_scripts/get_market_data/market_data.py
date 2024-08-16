"""
This module fetches data from NSE API
"""
import pandas as pd
import requests
from common_utils import read_write_sql_data as rd
import nsepython as np

headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36',
    'Sec-Fetch-User': '?1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-Mode': 'navigate',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
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
    def get_main_nse_indices_list():
        """
        Get main nse indices list
        :return:
        """
        indices_data = rd.get_table_data(selected_database="NSEDATA", selected_table="NSE_INDICES_DATA")
        req_indices_list = indices_data['index'].to_list()
        return req_indices_list

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

    @staticmethod
    def load_all_stocks_table_with_stock_index(all_indices_list):
        """
        Load all stocks along with their index details into sql table
        :param all_indices_list:
        :return:
        """
        all_stocks_df = pd.DataFrame()
        for stock_index in all_indices_list:
            table_name = stock_index.replace(" ", "_") + "_REF"
            if '&' in table_name:
                table_name = table_name.replace('&', 'AND')
            stock_idx_data = rd.get_table_data(selected_database="NSEDATA", selected_table=table_name)
            stock_idx_data['STK_INDEX'] = stock_index
            stock_idx_data = stock_idx_data[['Symbol', 'STK_INDEX']][1:]
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
    broad_indices_list = ["NIFTY 50", "NIFTY NEXT 50", "NIFTY MIDCAP 50", "NIFTY MIDCAP 100", "NIFTY MIDCAP 150",
                          "NIFTY SMALLCAP 250", "NIFTY SMALLCAP 50", "NIFTY SMALLCAP 100", "NIFTY 100", "NIFTY 200",
                          "NIFTY 500", "NIFTY MIDSMALLCAP 400", "NIFTY MIDCAP SELECT", "NIFTY LARGEMIDCAP 250"]

    sector_indices_list = ["NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL",
                           "NIFTY PHARMA", "NIFTY PSU BANK", "NIFTY REALTY", "NIFTY AUTO", "NIFTY HEALTHCARE INDEX",
                           "NIFTY FMCG", "NIFTY PRIVATE BANK", "NIFTY CONSUMER DURABLES", "NIFTY OIL & GAS"]

    thematic_indices_list = ["NIFTY ENERGY", "NIFTY CPSE", "NIFTY INFRASTRUCTURE", "NIFTY100 LIQUID 15",
                             "NIFTY PSE", "NIFTY COMMODITIES", "NIFTY MNC", "NIFTY INDIA CONSUMPTION",
                             "NIFTY MIDCAP LIQUID 15", "NIFTY SERVICES SECTOR", "NIFTY INDIA DIGITAL",
                             "NIFTY INDIA MANUFACTURING"]
    # sector_indices_list = []
    # broad_indices_list = []
    # thematic_indices_list = []
    indices_list = broad_indices_list + sector_indices_list + thematic_indices_list
    md = MarketData()
    # print(md.load_indices_data())
    # print(md.load_index_stocks_data(indices_list))
    # print(md.load_all_stocks_table_with_stock_index(indices_list))
    print(md.get_main_nse_indices_list())
