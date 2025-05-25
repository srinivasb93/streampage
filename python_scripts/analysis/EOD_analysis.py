import pandas as pd
import numpy as np
import datetime as dt
import statsmodels.api as sm
from common_utils import read_write_sql_data as rd
import pandas_ta as ta


class EODAnalysis:
    def __init__(self, stocks_list, adhoc_date=None, analysis_days=365, analysis_period='by_date'):
        self.stocks_list = stocks_list
        self.adhoc_date = adhoc_date
        self.analysis_days = analysis_days
        self.analysis_period = analysis_period
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

    @staticmethod
    def wwma(values, n_days):
        return values.ewm(alpha=1 / n_days, adjust=False).mean()

    @staticmethod
    def atr(df, n_days=14):
        data_copy = df.copy()
        high, low, close = data_copy['High'], data_copy['Low'], data_copy['Close']
        data_copy['tr0'] = abs(high - low)
        data_copy['tr1'] = abs(high - close.shift())
        data_copy['tr2'] = abs(low - close.shift())
        tr = data_copy[['tr0', 'tr1', 'tr2']].max(axis=1)
        return round(EODAnalysis.wwma(tr, n_days), 2)

    @staticmethod
    def slope(ser, n):
        x = np.array(range(len(ser)))
        slopes = [0] * (n - 1)
        reg_prices = [0] * (n - 1)
        for i in range(n, len(ser) + 1):
            y_scaled = ser[i - n:i]
            x_scaled = sm.add_constant(x[i - n:i])
            model = sm.OLS(y_scaled, x_scaled)
            results = model.fit()
            slopes.append(results.params[-1])
            reg_prices.append(model.predict(results.params)[-1])
        return reg_prices

    def process_stock_data(self, stock):
        query = self.get_query(stock)
        data = rd.get_table_data(query=query)
        data = self.preprocess_data(data, stock)
        data = self.calculate_indicators(data)
        data = self.analyze_price_action(data)
        data = self.analyze_narrow_range(data)
        return data

    def get_query(self, stock):
        if self.analysis_period == 'by_date':
            return f"SELECT * FROM dbo.{stock} WHERE DATE BETWEEN '{self.analysis_start_date}' AND '{self.analysis_end_date}' ORDER BY DATE ASC"
        else:
            return f"SELECT top {self.analysis_days} * FROM dbo.{stock} ORDER BY DATE DESC"

    def preprocess_data(self, data, stock):
        data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')
        if self.analysis_period != 'by_date':
            data.sort_values(by=['Date'], ascending=False, inplace=True)
        data['Symbol'] = stock
        return data

    def calculate_indicators(self, data):
        data['Pct_Chg'] = round(data['Close'].pct_change() * 100, 1)
        data['Pct_Chg_5D'] = round(data['Close'].pct_change(5) * 100, 1)
        data['Pct_Chg_20D'] = round(data['Close'].pct_change(20) * 100, 1)
        if self.analysis_days >= 365:
            data['Pct_Chg_365D'] = round(data['Close'].pct_change(240) * 100, 1)
        data['Range'] = round(data['High'] - data['Low'], 2)
        data['HH'] = round(data['High'] - data['High'].shift(), 2)
        data['LL'] = round(data['Low'] - data['Low'].shift(), 2)
        data['High_20'] = data['High'].rolling(20, min_periods=20).max()
        data['Low_20'] = data['Low'].rolling(20, min_periods=20).min()
        data['ATR'] = round(ta.atr(data['High'], data['Low'], data['Close'], length=14), 2)
        data['Range_ATR'] = round(data['Range'] / data['ATR'], 1)
        data['Vol_Avg20'] = round(data['Volume'].rolling(20, min_periods=20).mean(), 0)
        data['EMA_20'] = round(data['Close'].ewm(span=20, min_periods=20).mean(), 2)
        if self.analysis_days >= 200:
            data['EMA_60'] = round(data['Close'].ewm(span=60, min_periods=60).mean(), 2)
            data['EMA_200'] = round(data['Close'].ewm(span=200).mean(), 2)
        data['Reg_6'] = self.slope(data['Close'], n=6)
        data['Reg_6'] = round(data['Reg_6'], 2)
        data['Reg_18'] = round(ta.linreg(data['Close'], length=18), 2)
        data['Reg_6_Chg'] = round(data['Reg_6'] - data['Reg_6'].shift(), 1)
        data['Reg_Cross'] = round(data['Reg_6'] - data['Reg_18'], 1)
        data['Vol_Abv_Avg20'] = round(data['Volume'] / data['Vol_Avg20'], 2)
        data['Cls_Abv_EMA20'] = round(data['Close'] - data['EMA_20'], 2)
        if self.analysis_days >= 200:
            data['Cls_Abv_EMA60'] = round(data['Close'] - data['EMA_60'], 2)
            data['Cls_Abv_EMA200'] = round(data['Close'] - data['EMA_200'], 2)
        data['Cls_Abv_Reg6'] = round(data['Close'] - data['Reg_6'], 2)
        return data

    def analyze_price_action(self, data):
        curr_support = prev_support = curr_res = prev_res = 0.0
        data['Curr_Supp'] = data['Prev_Supp'] = data['Curr_Res'] = data['Prev_Res'] = 0.0
        data['Resistance'] = data['Support'] = ''

        for k in range(3, len(data)):
            poc_bl = poc_br = 0
            self.analyze_ema(data, k, self.ema_cnt_chk, poc_bl, poc_br)
            self.analyze_regression(data, k, poc_bl, poc_br)
            self.analyze_volume(data, k)
            # self.analyze_close_at_support_resistance(data)
            # self.analyze_high_low(data, k)
            curr_support, prev_support, poc_bl = self.analyze_support(
                data, k, curr_support, prev_support, curr_res, prev_res, poc_bl)
            curr_res, prev_res, poc_br = self.analyze_resistance(
                data, k, curr_res, prev_res, curr_support, prev_support, poc_br)
            self.analyze_breakouts(data, k, poc_bl, poc_br)

        return data

    def analyze_ema(self, data, k, ema_cnt_chk, poc_bl, poc_br):
        if data.loc[k, 'Cls_Abv_EMA20'] >= 0:
            data.loc[k, 'EMA20_Sig'] = 'Close_GT_20EMA' if data.loc[k - 1, 'Cls_Abv_EMA20'] >= 0 else 'Cross_Abv_20EMA'
            self.ema_cnt_chk += 1
            poc_bl += 1
        else:
            data.loc[k, 'EMA20_Sig'] = 'Close_LT_20EMA' if data.loc[k - 1, 'Cls_Abv_EMA20'] < 0 else 'Cross_Blw_20EMA'
            self.ema_cnt_chk = 0
            poc_br += 1

        if self.analysis_days >= 200:
            if data.loc[k, 'Cls_Abv_EMA60'] >= 0:
                data.loc[k, 'EMA60_Sig'] = 'Close_GT_60EMA' if data.loc[k - 1, 'Cls_Abv_EMA60'] >= 0 \
                    else 'Cross_Abv_60EMA'
            else:
                data.loc[k, 'EMA60_Sig'] = 'Close_LT_60EMA' if data.loc[k - 1, 'Cls_Abv_EMA60'] < 0 \
                    else 'Cross_Blw_60EMA'

            if data.loc[k, 'Cls_Abv_EMA200'] >= 0:
                data.loc[k, 'EMA200_Sig'] = 'Close_GT_200EMA' if data.loc[k - 1, 'Cls_Abv_EMA200'] >= 0 \
                    else 'Cross_Abv_200EMA'
            else:
                data.loc[k, 'EMA200_Sig'] = 'Close_LT_200EMA' if data.loc[k - 1, 'Cls_Abv_EMA200'] < 0 \
                    else 'Cross_Blw_200EMA'

        data.loc[k, 'EMA20_Cnt'] = self.ema_cnt_chk
        return ema_cnt_chk

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
    def analyze_volume(data, k):
        vol_ratio = data.loc[k, 'Vol_Abv_Avg20']
        if 1 <= vol_ratio <= 1.5:
            data.loc[k, 'Vol20_Sig'] = 'Above Avg Volume'
        elif vol_ratio > 1.5:
            data.loc[k, 'Vol20_Sig'] = 'Super Volume'
        else:
            data.loc[k, 'Vol20_Sig'] = 'Normal Volume'

    # @staticmethod
    # def analyze_high_low(data, k):
    #     hh, ll = data.loc[k, 'HH'], data.loc[k, 'LL']
    #     if hh >= 0 and ll >= 0:
    #         data.loc[k, 'High_Low'] = 'HH_HL'
    #     elif hh >= 0 > ll:
    #         data.loc[k, 'High_Low'] = 'HH_LL'
    #     elif hh < 0 and ll < 0:
    #         data.loc[k, 'High_Low'] = 'LH_LL'
    #     elif hh < 0 <= ll:
    #         data.loc[k, 'High_Low'] = 'LH_HL'

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
            elif curr_support > prev_support and curr_res < prev_res and data.loc[k, 'Close'] > curr_res:
                data.loc[k, 'Support'] = 'Price_Abv_Cur_Res'
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

    def analyze_narrow_range(self, data):
        # Calculate the Average True Range (ATR) for the last 20 days
        data['ATR_20'] = self.atr(data[['High', 'Low', 'Close']], 20)

        # Calculate the range as a percentage of the closing price
        data['Range_Pct'] = round(((data['High'] - data['Low']) / data['Close'])*100, 1)

        # Calculate the 20-day average range percentage
        data['Avg_Range_Pct_20'] = round(data['Range_Pct'].rolling(window=20).mean(), 1)

        # Identify narrow range days (range less than 50% of the 20-day average)
        data['Narrow_Range'] = data['Range_Pct'] < 0.5 * data['Avg_Range_Pct_20']

        # Count consecutive narrow range days
        data['Narrow_Range_Count'] = data['Narrow_Range'].groupby(
            (data['Narrow_Range'] != data['Narrow_Range'].shift()).cumsum()).cumcount() + 1

        # Identify breakouts from narrow range
        data['Narrow_Range_Breakout'] = (data['Narrow_Range_Count'].shift() >= 3) & (~data['Narrow_Range']) & (
                data['Range_ATR'] > 1.5)

        return data

    def run_analysis(self):
        for stock in self.stocks_list:
            stock = stock.replace('&', '').replace('-', '')
            print(f"Processing data for the stock - {stock}")
            data = self.process_stock_data(stock)
            # load_msg = rd.load_sql_data(data_to_load=data, table_name='data_' + stock)
            # print(load_msg)
            self.summary_df = pd.concat([self.summary_df, data.tail(1)], axis=0, ignore_index=True)
        summary_load_msg = rd.load_sql_data(data_to_load=self.summary_df, table_name='EOD_Summary')
        print(summary_load_msg)
        return 'Success'

    def print_summary_data_analysis(self):
        ema_20_abv = ['Close_GT_20EMA', 'Cross_Abv_20EMA']
        ema_200_abv = ['Close_GT_200EMA', 'Cross_Abv_200EMA']
        ema_60_abv = ['Close_GT_60EMA', 'Cross_Abv_60EMA']
        print('Report is generated successfully')

        ema_20_stocks = self.summary_df['Symbol'][self.summary_df['EMA20_Sig'].isin(ema_20_abv)].to_list()
        ema_200_stocks = self.summary_df['Symbol'][self.summary_df['EMA200_Sig'].isin(ema_200_abv)].to_list()
        ema_60_stocks = self.summary_df['Symbol'][self.summary_df['EMA60_Sig'].isin(ema_60_abv)].to_list()
        vol_check_list = self.summary_df['Symbol'][self.summary_df['Vol20_Sig'] == 'Vol_GT_Avg20'].to_list()
        supp_cond_list = ['Price_Abv_Supp', 'Price_Abv_Cur_Res', 'Price_Abv_Prev_Sup', 'Price_Crs_Abv_Supp',
                          'Price_Crs_Abv_Prev_Sup', 'Price_Crs_Abv_Cur_Res']
        supp_crs_stocks = self.summary_df['Symbol'][self.summary_df['Support'].isin(supp_cond_list)].to_list()

        print('Stocks which are at Support Levels')
        print(supp_crs_stocks)
        print('************************************')
        print('Stocks above 20 day, 60 day, 200 days EMA')
        print(set(ema_20_stocks).intersection(set(ema_60_stocks)).intersection(set(ema_200_stocks)))
        print('************************************')
        print('Stocks with greater Volume')
        print(vol_check_list)
        print('************************************')
        narrow_range_stocks = self.summary_df['Symbol'][self.summary_df['Narrow_Range'] == True].to_list()
        narrow_range_breakout_stocks = self.summary_df['Symbol'][
            self.summary_df['Narrow_Range_Breakout'] == True].to_list()

        print('Stocks in Narrow Range')
        print(narrow_range_stocks)
        print('************************************')
        print('Stocks that just broke out of Narrow Range')
        print(narrow_range_breakout_stocks)
        print('************************************')


if __name__ == '__main__':
    stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
    stock_list = stock_list_df['SYMBOL'].values.tolist()
    indices_df = rd.get_table_data(selected_table="STOCK_INDICES")
    indices_list = indices_df['name'].values.tolist()
    sectors_df = rd.get_table_data(selected_table="STOCK_SECTORS")
    sectors_list = sectors_df['name'].values.tolist()

    stocks_indices_sectors = stock_list + indices_list + sectors_list
    # stock_list = ['APOLLOTYRE', 'ITC', 'DHANBANK', 'UNIONBANK', 'MSUMI', 'MOTHERSON', 'GOLDBEES']

    # eod_analysis = EODAnalysis(stocks_list=stock_list[:30], adhoc_date=(2024, 9, 9))
    eod_analysis = EODAnalysis(stocks_list=stocks_indices_sectors)
    summary_data_load_status = eod_analysis.run_analysis()
    print(summary_data_load_status)
    eod_analysis.print_summary_data_analysis()
