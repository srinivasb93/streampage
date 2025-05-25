import pandas as pd
import numpy as np
import datetime as dt
import statsmodels.api as sm
from common_utils import read_write_sql_data as rd
import pandas_ta as ta


class WeeklyAnalysis:
    def __init__(self, stocks_list, adhoc_date=None, analysis_days=365, analysis_period='by_date', analysis='Weekly'):
        self.stocks_list = stocks_list
        self.adhoc_date = adhoc_date
        self.analysis_days = analysis_days
        self.analysis_period = analysis_period
        self.analysis = analysis
        self.ema_cnt_chk = 0
        self.summary_df = pd.DataFrame()
        self.set_analysis_dates()

    def set_analysis_dates(self):
        if not self.adhoc_date:
            self.analysis_start_date = dt.date.today() - dt.timedelta(days=self.analysis_days)
            self.analysis_end_date = dt.date.today()
        else:
            try:
                self.analysis_start_date = self.adhoc_date - dt.timedelta(days=self.analysis_days)
                self.analysis_end_date = self.adhoc_date
            except Exception as e:
                print(f"Error setting analysis dates: {e}")
                raise

    def process_stock_data(self, stock):
        try:
            query = self.get_query(stock)
            data = rd.get_table_data(query=query)
            data = self.preprocess_data(data, stock)
            data = self.calculate_indicators(data)
            data = self.analyze_price_action(data)
            return data
        except:
            print(f"Error processing {stock}")
            return pd.DataFrame()

    def get_query(self, stock):
        stk_suffix = '_W' if self.analysis == 'Weekly' else '_M'
        if self.analysis_period == 'by_date':
            return f"SELECT * FROM dbo.{stock}{stk_suffix} WHERE DATE BETWEEN '{self.analysis_start_date}' AND '{self.analysis_end_date}' ORDER BY DATE ASC"
        else:
            return f"SELECT top {self.analysis_days} * FROM dbo.{stock}{stk_suffix} ORDER BY DATE DESC"

    def preprocess_data(self, data, stock):
        data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')
        if self.analysis_period != 'by_date':
            data.sort_values(by=['Date'], ascending=False, inplace=True)
        data['Symbol'] = stock
        return data

    @staticmethod
    def calculate_indicators(data):
        data['Pct_Chg'] = round(data['Close'].pct_change() * 100, 1)
        data['Reg_6'] = round(ta.linreg(data['Close'], length=6), 2)
        data['Reg_18'] = round(ta.linreg(data['Close'], length=18), 2)
        data['Reg_Cross'] = round(data['Reg_6'] - data['Reg_18'], 1)
        data['Cls_Abv_Reg6'] = round(data['Close'] - data['Reg_6'], 2)
        return data

    def analyze_price_action(self, data):
        curr_support = prev_support = curr_res = prev_res = 0.0
        data['Curr_Supp'] = data['Prev_Supp'] = data['Curr_Res'] = data['Prev_Res'] = 0.0
        data['Resistance'] = data['Support'] = ''

        for k in range(3, len(data)):
            poc_bl = poc_br = 0
            self.analyze_regression(data, k, poc_bl, poc_br)
            # self.analyze_close_at_support_resistance(data)
            curr_support, prev_support, poc_bl = self.analyze_support(
                data, k, curr_support, prev_support, curr_res, prev_res, poc_bl)
            curr_res, prev_res, poc_br = self.analyze_resistance(
                data, k, curr_res, prev_res, curr_support, prev_support, poc_br)
            self.analyze_breakouts(data, k, poc_bl, poc_br)

        return data

    @staticmethod
    def analyze_regression(data, k, poc_bl, poc_br):
        data.loc[k, 'Reg6_Sig'] = 'Close_GT_Reg6' if data.loc[k, 'Cls_Abv_Reg6'] >= 0 else 'Close_LT_Reg6'

        reg_cross = data.loc[k, 'Reg_Cross']
        prev_reg_cross = data.loc[k - 1, 'Reg_Cross']
        if reg_cross >= 0 > prev_reg_cross:
            data.loc[k, 'Reg_Cross_Sig'] = 'Cross_Up'
            poc_bl += 1
        elif reg_cross < 0 <= prev_reg_cross:
            data.loc[k, 'Reg_Cross_Sig'] = 'Cross_Down'
            poc_br += 1
        elif reg_cross >= 0 and prev_reg_cross >= 0:
            data.loc[k, 'Reg_Cross_Sig'] = 'Reg6_Abv_Reg18'
        elif reg_cross < 0 and prev_reg_cross < 0:
            data.loc[k, 'Reg_Cross_Sig'] = 'Reg6_Blw_Reg18'

        return poc_bl, poc_br

    @staticmethod
    def analyze_support(data, k, curr_support, prev_support, curr_res, prev_res, poc_bl):
        supp_cond = (data.loc[k, 'Reg_6'] > data.loc[k - 1, 'Reg_6']) and (
                data.loc[k - 1, 'Reg_6'] < data.loc[k - 2, 'Reg_6'] < data.loc[k - 3, 'Reg_6'] < data.loc[k - 4, 'Reg_6'])
        if supp_cond:
            prev_support = curr_support
            curr_support = data.loc[k - 1, 'Reg_6']
            data.loc[k, 'Prev_Supp'] = round(prev_support, 2)
            data.loc[k, 'Curr_Supp'] = round(curr_support, 2)

            # Pullback up at Support in the direction of the trend
            if prev_support < curr_support < data.loc[k, 'Low'] and curr_res > prev_res:
                data.loc[k, 'Support'] = 'Price_Abv_Supp'
                poc_bl += 1
            if prev_support < curr_support and data.loc[k, 'Low'] < curr_support and curr_res > prev_res:
                data.loc[k, 'Support'] = 'Price_Crs_Abv_Supp'
                poc_bl += 1
            elif data.loc[k, 'Close'] > curr_support > prev_support and curr_res < prev_res:
                data.loc[k, 'Support'] = 'Price_Abv_Cur_Sup'
                poc_bl += 1

        sup_break_chk = (data.loc[k, 'High'] > curr_support
                         and data.loc[k - 1, 'Close'] > curr_support > data.loc[k, 'Close']
                         and data.loc[k - 2, 'Close'] > curr_support)
        psup_break_chk = (data.loc[k, 'High'] > prev_support
                          and data.loc[k - 1, 'Close'] > prev_support > data.loc[k, 'Close']
                          and data.loc[k - 2, 'Close'] > prev_support)

        if sup_break_chk and psup_break_chk:
            data.loc[k, 'Break_Sup_Res'] = 'Both_Sup_Broken_Down'
        elif sup_break_chk:
            data.loc[k, 'Break_Sup_Res'] = 'Cur_Sup_Broken_Down'

        return curr_support, prev_support, poc_bl

    @staticmethod
    def analyze_resistance(data, k, curr_res, prev_res, curr_support, prev_support, poc_br):
        res_cond = (data.loc[k, 'Reg_6'] < data.loc[k - 1, 'Reg_6']) and (
                data.loc[k - 1, 'Reg_6'] > data.loc[k - 2, 'Reg_6'] > data.loc[k - 3, 'Reg_6'] > data.loc[k - 4, 'Reg_6'])
        if res_cond:
            prev_res = curr_res
            curr_res = data.loc[k - 1, 'Reg_6']
            data.loc[k, 'Prev_Res'] = round(prev_res, 2)
            data.loc[k, 'Curr_Res'] = round(curr_res, 2)

            # Pullback down at Resistance in the direction of the trend
            if prev_res > curr_res > data.loc[k, 'High'] and curr_support < prev_support:
                data.loc[k, 'Resistance'] = 'Price_Blw_Res'
                poc_br += 1
            if prev_res > curr_res and prev_res > data.loc[k, 'Close'] and data.loc[k, 'High'] > curr_res < data.loc[k - 1, 'Close']:
                data.loc[k, 'Resistance'] = 'Price_Crs_Blw_Res'
                poc_br += 1
            elif curr_res < prev_res and prev_support > curr_support > data.loc[k, 'High'] <= data.loc[k - 1, 'Close']:
                data.loc[k, 'Resistance'] = 'Price_Blw_Cur_Sup'
                poc_br += 1

        # Resistance break check
        res_break_chk = (data.loc[k, 'Low'] < curr_res
                         and data.loc[k - 1, 'Close'] < curr_res <= data.loc[k, 'Close']
                         and data.loc[k - 2, 'Close'] < curr_res)
        pres_break_chk = (data.loc[k, 'Low'] < prev_res
                          and data.loc[k - 1, 'Close'] < prev_res <= data.loc[k, 'Close']
                          and data.loc[k - 2, 'Close'] < prev_res)

        if res_break_chk and pres_break_chk:
            data.loc[k, 'Break_Sup_Res'] = 'Both_Res_Broken_Up'
        elif res_break_chk:
            data.loc[k, 'Break_Sup_Res'] = 'Cur_Res_Broken_Up'

        return curr_res, prev_res, poc_br

    @staticmethod
    def analyze_breakouts(data, k, poc_bl, poc_br):
        # Check for 20 days High/Low breakout. Ensure we have at least 20 days of data
        if k >= 20:
            high_20 = data.loc[k - 20:k - 1, 'High'].max()
            low_20 = data.loc[k - 20:k - 1, 'Low'].min()
            high_date = data.loc[k - 20:k - 1, 'High'].idxmax()
            low_date = data.loc[k - 20:k - 1, 'Low'].idxmin()

            days_since_high = k - high_date
            days_since_low = k - low_date

            if (data.loc[k, 'Close'] > high_20 and days_since_high >= 10 and
                    data.loc[k, 'Reg_6'] > data.loc[k, 'Reg_18']):
                data.loc[k, 'Breakout_20'] = 'Breakout_20_Up'
                poc_bl += 1
            elif (data.loc[k, 'Close'] < low_20 and days_since_low >= 10 and
                  data.loc[k, 'Reg_6'] < data.loc[k, 'Reg_18']):
                data.loc[k, 'Breakout_20'] = 'Breakout_20_Down'
                poc_br += 1

        return poc_bl, poc_br

    def run_analysis(self):
        for stock in self.stocks_list:
            stock = stock.replace('&', '').replace('-', '')
            print(f"Processing data for the stock - {stock}")
            data = self.process_stock_data(stock)
            # load_msg = rd.load_sql_data(data_to_load=data, table_name='data_' + stock)
            # print(load_msg)
            if not data.empty:
                self.summary_df = pd.concat([self.summary_df, data.tail(1)], axis=0, ignore_index=True)
        summary_load_msg = rd.load_sql_data(
            data_to_load=self.summary_df,
            table_name='Weekly_Summary' if self.analysis == 'Weekly' else 'Monthly_Summary')
        print(summary_load_msg)
        return 'Success'


if __name__ == '__main__':
    stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
    stock_list = stock_list_df['SYMBOL'].values.tolist()
    indices_df = rd.get_table_data(selected_table="STOCK_INDICES")
    indices_list = indices_df['name'].values.tolist()
    sectors_df = rd.get_table_data(selected_table="STOCK_SECTORS")
    sectors_list = sectors_df['name'].values.tolist()

    stocks_indices_sectors = stock_list + indices_list + sectors_list
    # stocks_indices_sectors = ['RELIANCE']

    # eod_analysis = EODAnalysis(stocks_list=stock_list[:30], adhoc_date=(2024, 9, 9))
    # eod_analysis = WeeklyAnalysis(stocks_list=stocks_indices_sectors, analysis_days=1200, analysis='Monthly')
    eod_analysis = WeeklyAnalysis(stocks_list=stocks_indices_sectors)
    summary_data_load_status = eod_analysis.run_analysis()
    print(summary_data_load_status)
    # eod_analysis.print_summary_data_analysis()
