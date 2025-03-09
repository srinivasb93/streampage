import pandas as pd
import numpy as np
import datetime as dt
import statsmodels.api as sm
from common_utils import read_write_sql_data as rd


def wwma(values, n_days):
    return values.ewm(alpha=1 / n_days, adjust=False).mean()


def atr(df, n_days=14):
    """
    function to calculate the average true range
    """
    data_copy = df.copy()
    high = data_copy['High']
    low = data_copy['Low']
    close = data_copy['Close']
    data_copy['tr0'] = abs(high - low)
    data_copy['tr1'] = abs(high - close.shift())
    data_copy['tr2'] = abs(low - close.shift())
    tr = data_copy[['tr0', 'tr1', 'tr2']].max(axis=1)
    avg_true_range = wwma(tr, n_days)
    # avg_true_range = tr.rolling(n).mean()
    return round(avg_true_range, 2)


def slope(ser, n):
    """
    function to calculate the slope of regression line for n consecutive points on a plot
    """
    #     ser = (ser - ser.min())/(ser.max() - ser.min())
    x = np.array(range(len(ser)))
    #     x = (x - x.min())/(x.max() - x.min())
    slopes = [i * 0 for i in range(n - 1)]
    reg_prices = [i * 0 for i in range(n - 1)]
    for i in range(n, len(ser) + 1):
        y_scaled = ser[i - n:i]
        x_scaled = x[i - n:i]
        x_scaled = sm.add_constant(x_scaled)
        model = sm.OLS(y_scaled, x_scaled)
        results = model.fit()
        results1 = model.predict(results.params)
        slopes.append(results.params[-1])
        reg_prices.append(results1[-1])
    return reg_prices


def eod_data_analysis(stocks_list, adhoc_date=None, analysis_days=365, analysis_period='by_date'):
    """
    Perform EOD data analysis for stocks
    :param stocks_list:
    :param adhoc_date: if adhoc date, pass the date in a tuple (YYYY, MM, DD)
    :param analysis_days:
    :param analysis_period:
    :return:
    """
    if not adhoc_date:
        analysis_start_date = dt.date.today() - dt.timedelta(days=analysis_days)
        analysis_end_date = dt.date.today()
    else:
        try:
            analysis_start_date = dt.date.today() - dt.timedelta(days=analysis_days)
            analysis_end_date = dt.date.today()
        except Exception as e:
            analysis_start_date = dt.date(
                adhoc_date[0], adhoc_date[1], adhoc_date[2]) - dt.timedelta(days=analysis_days)
            analysis_end_date = dt.date(adhoc_date[0], adhoc_date[1], adhoc_date[2])

    dt_today = dt.date.today().strftime('%d%m%Y')

    summary_df = pd.DataFrame()
    # df6 = pd.DataFrame()

    for stock in stocks_list:
        if '&' in stock or '-' in stock:
            stock = stock.replace('&', '').replace('-', '')
        print(f"Processing data for the stock - {stock}")

        if analysis_period == 'by_date':
            query = f"SELECT * FROM dbo.{stock} WHERE DATE" \
                    f" BETWEEN '{analysis_start_date}' AND '{analysis_end_date}' ORDER BY DATE ASC"
        else:
            query = f"SELECT top {analysis_days} * FROM dbo." + stock + " ORDER BY DATE DESC"
        data = rd.get_table_data(query=query)
        data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')
        if analysis_period != 'by_date':
            data.sort_values(by=['Date'], ascending=False, inplace=True)
        data['Symbol'] = stock
        data['Pct_Chg'] = round(data['Close'].pct_change() * 100, 1)
        data['Pct_Chg_5D'] = round(data['Close'].pct_change(5) * 100, 1)
        data['Pct_Chg_20D'] = round(data['Close'].pct_change(20) * 100, 1)
        if analysis_days >= 365:
            data['Pct_Chg_365D'] = round(data['Close'].pct_change(240) * 100, 1)
        data['Range'] = round(data['High'] - data['Low'], 2)
        data['HH'] = round(data['High'] - data['High'].shift(), 2)
        data['LL'] = round(data['Low'] - data['Low'].shift(), 2)

        data['High_20'] = data['High'].rolling(20, min_periods=20).max()
        data['Low_20'] = data['Low'].rolling(20, min_periods=20).min()

        data['Break_High_20'] = data['Close'] > data['High_20'].shift(1)
        data['Break_Low_20'] = data['Close'] < data['Low_20'].shift(1)

        data['ATR'] = atr(data[['High', 'Low', 'Close']], 14)
        data['Range_ATR'] = round(data['Range'] / data['ATR'], 1)
        data['Vol_Avg20'] = round(data['Volume'].rolling(20, min_periods=20).mean(), 0)

        data['EMA_20'] = round(data['Close'].ewm(span=20, min_periods=20).mean(), 2)

        if analysis_days >= 200:
            data['EMA_60'] = round(data['Close'].ewm(span=60, min_periods=60).mean(), 2)
            data['EMA_200'] = round(data['Close'].ewm(span=200).mean(), 2)

        data['Reg_6'] = slope(data['Close'], 6)
        data['Reg_18'] = slope(data['Close'], 18)
        data['Reg_6_Chg'] = round(data['Reg_6'] - data['Reg_6'].shift(), 1)
        data['Reg_Cross'] = round(data['Reg_6'] - data['Reg_18'], 1)

        data['Vol_Abv_Avg20'] = round(data['Volume'] / data['Vol_Avg20'], 2)
        data['Cls_Abv_EMA20'] = round(data['Close'] - data['EMA_20'], 2)

        if analysis_days >= 200:
            data['Cls_Abv_EMA60'] = round(data['Close'] - data['EMA_60'], 2)
            data['Cls_Abv_EMA200'] = round(data['Close'] - data['EMA_200'], 2)

        data['Cls_Abv_Reg6'] = round(data['Close'] - data['Reg_6'], 2)

        ema_cnt_chk = curr_support = prev_support = curr_res = prev_res = fib382 = fib50 = fib618 = 0.0
        data['Curr_Supp'] = data['Prev_Supp'] = data['Curr_Res'] = data['Prev_Res'] = 0.0

        data['Fib_Status'] = data['Resistance'] = data['Support'] = ''

        for rdata in data.itertuples():
            k = rdata.Index + 2
            poc_bl = poc_br = 0

            if k < len(data.index):
                data.loc[k, 'EMA20_Sig'] = 'Close_GT_20EMA' if data.loc[k, 'Cls_Abv_EMA20'] >= 0 else 'Close_LT_20EMA'
                if data.loc[k, 'Cls_Abv_EMA20'] >= 0 > data.loc[k - 1, 'Cls_Abv_EMA20']:
                    data.loc[k, 'EMA20_Sig'] = 'Cross_Abv_20EMA'
                elif data.loc[k, 'Cls_Abv_EMA20'] < 0 <= data.loc[k - 1, 'Cls_Abv_EMA20']:
                    data.loc[k, 'EMA20_Sig'] = 'Cross_Blw_20EMA'

                if data.loc[k, 'EMA20_Sig'] in ['Cross_Abv_20EMA', 'Close_GT_20EMA']:
                    ema_cnt_chk += 1
                    poc_bl += 1
                    data.loc[k, 'EMA20_Cnt'] = ema_cnt_chk
                if data.loc[k, 'EMA20_Sig'] in ['Cross_Blw_20EMA', 'Close_LT_20EMA']:
                    ema_cnt_chk = 0
                    poc_br += 1
                    data.loc[k, 'EMA20_Cnt'] = ema_cnt_chk

                data.loc[k, 'Reg6_Sig'] = 'Close_GT_Reg6' if data.loc[k, 'Cls_Abv_Reg6'] >= 0 else 'Close_LT_Reg6'
                if data.loc[k, 'Low'] < data.loc[k, 'Reg_6'] <= data.loc[k, 'Close']:
                    data.loc[k, 'Reg6_Sig'] = 'Close_CrossUp_Reg6'
                elif data.loc[k, 'High'] > data.loc[k, 'Reg_6'] >= data.loc[k, 'Close']:
                    data.loc[k, 'Reg6_Sig'] = 'Close_CrossDn_Reg6'

                if 1 <= data.loc[k, 'Vol_Abv_Avg20'] <= 1.5:
                    data.loc[k, 'Vol20_Sig'] = 'Above Avg Volume'
                elif data.loc[k, 'Vol_Abv_Avg20'] > 1.5:
                    data.loc[k, 'Vol20_Sig'] = 'Super Volume'
                else:
                    data.loc[k, 'Vol20_Sig'] = 'Normal Volume'

                if data.loc[k, 'Reg_Cross'] >= 0 > data.loc[k - 1, 'Reg_Cross']:
                    data.loc[k, 'Reg_Cross_Sig'] = 'Cross_Up'
                elif data.loc[k, 'Reg_Cross'] < 0 <= data.loc[k - 1, 'Reg_Cross']:
                    data.loc[k, 'Reg_Cross_Sig'] = 'Cross_Down'
                elif data.loc[k, 'Reg_Cross'] >= 0 and data.loc[k - 1, 'Reg_Cross'] >= 0:
                    data.loc[k, 'Reg_Cross_Sig'] = 'Reg6_Abv_Reg18'
                elif data.loc[k, 'Reg_Cross'] < 0 and data.loc[k - 1, 'Reg_Cross'] < 0:
                    data.loc[k, 'Reg_Cross_Sig'] = 'Reg6_Blw_Reg18'

                if data.loc[k, 'HH'] >= 0 and data.loc[k, 'LL'] >= 0:
                    data.loc[k, 'High_Low'] = 'HH_HL'
                    poc_bl += 1
                elif data.loc[k, 'HH'] >= 0 > data.loc[k, 'LL']:
                    data.loc[k, 'High_Low'] = 'HH_LL'
                    if data.loc[k, 'Close'] > data.loc[k - 1, 'Close']:
                        poc_bl += 1
                    elif data.loc[k, 'Close'] < data.loc[k - 1, 'Close']:
                        poc_br += 1
                elif data.loc[k, 'HH'] < 0 and data.loc[k, 'LL'] < 0:
                    data.loc[k, 'High_Low'] = 'LH_LL'
                    poc_br += 1
                elif data.loc[k, 'HH'] < 0 <= data.loc[k, 'LL']:
                    data.loc[k, 'High_Low'] = 'LH_HL'
                    poc_bl += .5

                supp_cond = data.loc[k, 'Reg_6_Chg'] > 0 >= data.loc[k - 1, 'Reg_6_Chg'] >= data.loc[
                    k - 2, 'Reg_6_Chg']
                if supp_cond:
                    prev_support = curr_support
                    curr_support = data.loc[k - 1, 'Reg_6']
                    data.loc[k, 'Prev_Supp'] = round(prev_support, 2)
                    data.loc[k, 'Curr_Supp'] = round(curr_support, 2)
                    # Condition for recent HH and HL
                    if prev_support < curr_support < data.loc[k, 'Low'] and curr_res > prev_res:
                        data.loc[k, 'Support'] = 'Price_Abv_Supp'
                        poc_bl += 1
                    # Condition for recent LH and HL where cadle is above current LH
                    elif curr_support > prev_support and curr_res < prev_res and data.loc[k, 'Low'] > curr_res:
                        data.loc[k, 'Support'] = 'Price_Abv_Cur_Res'
                        poc_bl += 1
                    elif curr_support < prev_support and curr_res < prev_res and (
                            data.loc[k, 'Low'] > prev_support or data.loc[k, 'Low'] > curr_res) and data.loc[
                        k - 1, 'Close'] < prev_support:
                        data.loc[k, 'Support'] = 'Price_Abv_Prev_Sup'
                        poc_bl += .5

                res_cond = data.loc[k, 'Reg_6_Chg'] < 0 <= data.loc[k - 1, 'Reg_6_Chg'] <= data.loc[
                    k - 2, 'Reg_6_Chg']
                if res_cond:
                    prev_res = curr_res
                    curr_res = data.loc[k - 1, 'Reg_6']
                    data.loc[k, 'Prev_Res'] = round(prev_res, 2)
                    data.loc[k, 'Curr_Res'] = round(curr_res, 2)
                    if prev_res > curr_res > data.loc[k, 'High'] and curr_support < prev_support < data.loc[
                        k - 1, 'Close']:
                        data.loc[k, 'Resistance'] = 'Price_Blw_Res'
                        poc_br += 1
                    elif curr_res < prev_res and prev_support > curr_support > data.loc[k, 'High'] <= data.loc[
                        k - 1, 'Close']:
                        data.loc[k, 'Resistance'] = 'Price_Blw_Cur_Sup'
                        poc_br += 1

                if curr_support < prev_support and curr_res < prev_res and (
                        data.loc[k, 'Low'] < prev_support or data.loc[k, 'Low'] < curr_res) and \
                        (data.loc[k, 'Close'] >= curr_res or data.loc[k, 'Close'] >= prev_support) and data.loc[
                    k - 1, 'Close'] < prev_support:
                    data.loc[k, 'Support'] = 'Price_Crs_Abv_Prev_Sup'
                    poc_bl += .5
                elif curr_support > prev_support and curr_res > prev_res and data.loc[k, 'Low'] < curr_support\
                        and data.loc[k, 'Close'] >= curr_support:
                    data.loc[k, 'Support'] = 'Price_Crs_Abv_Supp'
                    poc_bl += 1
                elif curr_support > prev_support and prev_res > curr_res > data.loc[k, 'Low'] <= data.loc[
                    k, 'Close'] > data.loc[k - 1, 'Close']:
                    data.loc[k, 'Support'] = 'Price_Crs_Abv_Cur_Res'
                    poc_bl += 1

                if prev_res > curr_res > data.loc[k, 'Close'] and data.loc[k, 'High'] > curr_res < data.loc[
                    k - 1, 'Close']:
                    data.loc[k, 'Resistance'] = 'Price_Crs_Blw_Res'
                    poc_br += 1
                elif curr_res > prev_res > data.loc[k, 'Close'] and data.loc[k, 'High'] > prev_res < data.loc[
                    k - 1, 'Close']:
                    data.loc[k, 'Resistance'] = 'Price_Crs_Blw_Prev_Res'
                    poc_br += .5

                if data.loc[k - 1, 'Close'] < data.loc[k - 1, 'High_20'] and data.loc[k, 'Close'] > data.loc[
                    k - 1, 'High_20'] and data.loc[k, 'Reg_6'] > data.loc[k, 'Reg_18']:
                    data.loc[k, 'Breakout_20'] = 'Breakout_20_Up'
                    poc_bl += 1
                elif data.loc[k - 1, 'Close'] > data.loc[k - 1, 'Low_20'] and data.loc[k, 'Close'] < data.loc[
                    k - 1, 'Low_20'] and data.loc[k, 'Reg_6'] < data.loc[k, 'Reg_18']:
                    data.loc[k, 'Breakout_20'] = 'Breakout_20_Down'
                    poc_br += 1

                res_break_chk = (data.loc[k, 'Low'] < curr_res
                                 and data.loc[k - 1, 'Close'] < curr_res
                                 and data.loc[k - 2, 'Close'] < curr_res
                                 and data.loc[k, 'Close'] >= curr_res)

                pres_break_chk = data.loc[k, 'Low'] < prev_res and data.loc[k - 1, 'Close'] < prev_res and data.loc[
                    k - 2, 'Close'] < prev_res and data.loc[k, 'Close'] >= prev_res
                if res_break_chk:
                    data.loc[k, 'Break_Sup_Res'] = 'Cur_Res_Broken_Up'
                    poc_bl += 1
                elif pres_break_chk:
                    data.loc[k, 'Break_Sup_Res'] = 'Prev_Res_Broken_Up'
                    poc_bl += 1
                elif res_break_chk and pres_break_chk:
                    data.loc[k, 'Break_Sup_Res'] = 'Both_Res_Broken_Up'
                    poc_bl += 1

                sup_break_chk = data.loc[k, 'High'] > curr_support and data.loc[k - 1, 'Close'] > curr_support and data.loc[
                    k - 2, 'Close'] > curr_support and data.loc[k, 'Close'] < curr_support
                psup_break_chk = data.loc[k, 'High'] > prev_support and data.loc[k - 1, 'Close'] > prev_support and \
                                 data.loc[k - 2, 'Close'] > prev_support and data.loc[k, 'Close'] < prev_support
                if sup_break_chk:
                    data.loc[k, 'Break_Sup_Res'] = 'Cur_Sup_Broken_Down'
                    poc_br += 1
                elif psup_break_chk:
                    data.loc[k, 'Break_Sup_Res'] = 'Prev_Sup_Broken_Down'
                    poc_br += 1
                elif sup_break_chk and psup_break_chk:
                    data.loc[k, 'Break_Sup_Res'] = 'Both_Sup_Broken_Down'
                    poc_br += 1

                if curr_res > prev_res and curr_support > prev_support:
                    diff_price = data.loc[k, 'High_20'] - data.loc[k, 'Low_20']
                    fib382 = data.loc[k, 'Fib_382'] = round(data.loc[k, 'High_20'] - diff_price * .382, 2)
                    fib50 = data.loc[k, 'Fib_50'] = round(data.loc[k, 'High_20'] - diff_price * .5, 2)
                    fib618 = data.loc[k, 'Fib_618'] = round(data.loc[k, 'High_20'] - diff_price * .618, 2)

                if curr_res > prev_res:
                    if data.loc[k, 'Low'] < fib382 < data.loc[k, 'Close']:
                        data.loc[k, 'Fib_Status'] = 'Cls_Crs_Abv_F382'
                        poc_bl += 1
                    if data.loc[k, 'Low'] < fib50 < data.loc[k, 'Close']:
                        data.loc[k, 'Fib_Status'] = 'Cls_Crs_Abv_F50'
                        poc_bl += 1
                    if data.loc[k, 'Low'] < fib618 < data.loc[k, 'Close']:
                        data.loc[k, 'Fib_Status'] = 'Cls_Crs_Abv_F618'
                        poc_bl += 1
                data.loc[k, 'Bull_Signal'] = poc_bl
                data.loc[k, 'Bear_Signal'] = poc_br

        # data.to_csv('data_'+stock+'.csv',index=False)
        load_msg = rd.load_sql_data(data_to_load=data, table_name='data_'+stock)
        print(load_msg)

        out_cols = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Pct_Chg', 'Pct_Chg_5D', 'Pct_Chg_20D',
                    'Pct_Chg_365D', 'ATR', 'Vol20_Sig', 'Range_ATR', 'Fib_Status', 'EMA20_Sig', 'EMA20_Cnt',
                    'High_Low', 'Support', 'Break_Sup_Res', 'Resistance', 'Breakout_20', 'Bull_Signal', 'Bear_Signal']
        summary_df = pd.concat([summary_df, data.tail(1)], axis=0, ignore_index=True)
        # summary_df = summary_df[out_cols]

    # Load Summary data
    # load_msg = rd.load_sql_data(summary_df, table_name='EOD_ANALYSIS')
    # return 'Success' if 'success' in load_msg else 'Failure'


def print_summary_data_analysis(summary_data):
    """
    Print summary data analysis for the output of EOD analysis
    :param summary_data: 
    :return: 
    """
    ema_20_abv = ['Close_GT_20EMA', 'Cross_Abv_20EMA']
    ema_20_blw = ['Close_LT_20EMA', 'Cross_Blw_20EMA']

    print("List of Bullish Stocks")
    print(summary_data[(summary_data['Bull_Signal'] > 2) & (summary_data['Bear_Signal'] <= 1) & (summary_data['EMA20_Sig'].isin(ema_20_abv))])
    print("========================")
    print("List of Bearish Stocks")
    print(summary_data[(summary_data['Bull_Signal'] <= 1) & (summary_data['Bear_Signal'] > 2) & (summary_data['EMA20_Sig'].isin(ema_20_blw))])
    print("========================")

    print('Report is generated successfully')
    
    fib_stocks = summary_data['Symbol'][summary_data['Fib_Status'] == 'Cls_Crs_Abv_F50'].to_list()
    
    ema_20_stocks = summary_data['Symbol'][summary_data['EMA20_Sig'].isin(ema_20_abv)].to_list()
    vol_check_list = summary_data['Symbol'][summary_data['Vol20_Sig'] == 'Vol_GT_Avg20'].to_list()
    supp_cond_list = ['Price_Abv_Supp', 'Price_Abv_Cur_Res', 'Price_Abv_Prev_Sup', 'Price_Crs_Abv_Supp',
                      'Price_Crs_Abv_Prev_Sup', 'Price_Crs_Abv_Cur_Res']
    supp_crs_stocks = summary_data['Symbol'][summary_data['Support'].isin(supp_cond_list)].to_list()
    
    print('Stocks which are at Support Levels')
    print(supp_crs_stocks)
    print('************************************')
    print('Stocks above 20 day EMA')
    print(ema_20_stocks)
    print('************************************')
    print('Stocks with greater Volume')
    print(vol_check_list)
    print('************************************')
    print('Stocks at Important Fibanocci levels')
    print(fib_stocks)
    print('************************************')


if __name__ == '__main__':
    stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
    stock_list = stock_list_df['SYMBOL'].values.tolist()
    stock_list = ['TATAMOTORS']

    summary_data_load_status = eod_data_analysis(stocks_list=stock_list, adhoc_date=dt.date(2024, 9, 9))
    print(summary_data_load_status)
    summary_data = rd.get_table_data(selected_table='EOD_ANALYSIS')
    print_summary_data_analysis(summary_data=summary_data)

