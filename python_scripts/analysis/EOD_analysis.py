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
        high, low, close = data_copy['high'], data_copy['low'], data_copy['close']
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
            slopes.append(results.params.iloc[-1])
            reg_prices.append(model.predict(results.params)[-1])
        return reg_prices

    def process_stock_data(self, stock):
        query = self.get_query(stock)
        data = rd.get_table_data(query=query)
        data = self.preprocess_data(data, stock)
        data = self.calculate_indicators(data)
        data = self.analyze_price_action(data)
        data = self.analyze_additional_signals(data)
        data = self.analyze_narrow_range(data)
        return data

    def get_query(self, stock):
        if self.analysis_period == 'by_date':
            return f"SELECT * from public.\"{stock}\" WHERE timestamp BETWEEN '{self.analysis_start_date}' AND '{self.analysis_end_date}' ORDER BY timestamp ASC"
        else:
            return f"SELECT * from public.\"{stock}\" ORDER BY timestamp DESC LIMIT {self.analysis_days}"

    def preprocess_data(self, data, stock):
        if self.analysis_period != 'by_date':
            data.sort_values(by=['timestamp'], ascending=False, inplace=True)
        data['Symbol'] = stock
        return data

    def calculate_indicators(self, data):
        data['Pct_Chg'] = round(data['close'].pct_change() * 100, 1)
        data['Pct_Chg_5D'] = round(data['close'].pct_change(5) * 100, 1)
        data['Pct_Chg_20D'] = round(data['close'].pct_change(20) * 100, 1)
        if self.analysis_days >= 365:
            data['Pct_Chg_365D'] = round(data['close'].pct_change(240) * 100, 1)
        data['Range'] = round(data['high'] - data['low'], 2)
        data['HH'] = round(data['high'] - data['high'].shift(), 2)
        data['LL'] = round(data['low'] - data['low'].shift(), 2)
        data['High_20'] = data['high'].rolling(20, min_periods=20).max()
        data['Low_20'] = data['low'].rolling(20, min_periods=20).min()
        data['ATR'] = round(ta.atr(data['high'], data['low'], data['close'], length=14), 2)
        data['Range_ATR'] = round(data['Range'] / data['ATR'], 1)
        data['Vol_Avg20'] = round(data['volume'].rolling(20, min_periods=20).mean(), 0)
        data['EMA_20'] = round(data['close'].ewm(span=20, min_periods=20).mean(), 2)
        if self.analysis_days >= 200:
            data['EMA_60'] = round(data['close'].ewm(span=60, min_periods=60).mean(), 2)
            data['EMA_200'] = round(data['close'].ewm(span=200).mean(), 2)
        data['Reg_5'] = self.slope(data['close'], n=5)
        data['Reg_5'] = round(data['Reg_5'], 2)
        data['Reg_18'] = round(ta.linreg(data['close'], length=18), 2)
        data['Reg_5_Chg'] = round(data['Reg_5'] - data['Reg_5'].shift(), 1)
        data['Reg_Cross'] = round(data['Reg_5'] - data['Reg_18'], 1)
        data['RSI_14'] = round(ta.rsi(data['close'], length=14), 1)
        data['Vol_Abv_Avg20'] = round(data['volume'] / data['Vol_Avg20'], 2)
        data['Cls_Abv_EMA20'] = round(data['close'] - data['EMA_20'], 2)
        if self.analysis_days >= 200:
            data['Cls_Abv_EMA60'] = round(data['close'] - data['EMA_60'], 2)
            data['Cls_Abv_EMA200'] = round(data['close'] - data['EMA_200'], 2)
        data['Cls_Abv_Reg5'] = round(data['close'] - data['Reg_5'], 2)
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
        data.loc[k, 'Reg5_Sig'] = 'Close_GT_Reg5' if data.loc[k, 'Cls_Abv_Reg5'] >= 0 else 'Close_LT_Reg5'

        reg_cross = data.loc[k, 'Reg_Cross']
        prev_reg_cross = data.loc[k - 1, 'Reg_Cross']
        if reg_cross >= 0 > prev_reg_cross:
            data.loc[k, 'Reg_Cross_Sig'] = 'Cross_Up'
            poc_bl += 1
        elif reg_cross < 0 <= prev_reg_cross:
            data.loc[k, 'Reg_Cross_Sig'] = 'Cross_Down'
            poc_br += 1
        elif reg_cross >= 0 and prev_reg_cross >= 0:
            data.loc[k, 'Reg_Cross_Sig'] = 'Reg5_Abv_Reg18'
        elif reg_cross < 0 and prev_reg_cross < 0:
            data.loc[k, 'Reg_Cross_Sig'] = 'Reg5_Blw_Reg18'

        return poc_bl, poc_br

    def analyze_additional_signals(self, data):
        data = self.identify_rsi_divergence(data)
        data = self.calculate_support_resistance_gap(data)
        data = self.calculate_support_resistance_strength(data)
        data = self.identify_stop_loss_hunt(data)
        data = self.identify_level_reversal_and_breakout(data)
        return data

    @staticmethod
    def identify_rsi_divergence(data, lookback=3):
        data['RSI_Divergence'] = ''
        if len(data) <= (2 * lookback):
            return data

        indices = data.index.tolist()
        last_high_idx = None
        last_low_idx = None

        for pos in range(lookback, len(data) - lookback):
            idx = indices[pos]
            signals = []

            current_high = data['high'].iloc[pos]
            window_highs = data['high'].iloc[pos - lookback: pos + lookback + 1]
            if current_high == window_highs.max():
                if last_high_idx is not None:
                    prev_idx = last_high_idx
                    prev_high = data.loc[prev_idx, 'high']
                    prev_rsi = data.loc[prev_idx, 'RSI_14']
                    curr_rsi = data.loc[idx, 'RSI_14']
                    if pd.notna(prev_rsi) and pd.notna(curr_rsi):
                        if current_high > prev_high and curr_rsi < prev_rsi:
                            signals.append('Bearish')
                last_high_idx = idx

            current_low = data['low'].iloc[pos]
            window_lows = data['low'].iloc[pos - lookback: pos + lookback + 1]
            if current_low == window_lows.min():
                if last_low_idx is not None:
                    prev_idx = last_low_idx
                    prev_low = data.loc[prev_idx, 'low']
                    prev_rsi = data.loc[prev_idx, 'RSI_14']
                    curr_rsi = data.loc[idx, 'RSI_14']
                    if pd.notna(prev_rsi) and pd.notna(curr_rsi):
                        if current_low < prev_low and curr_rsi > prev_rsi:
                            signals.append('Bullish')
                last_low_idx = idx

            if signals:
                existing = data.at[idx, 'RSI_Divergence']
                new_signal = ';'.join(signals) if len(signals) > 1 else signals[0]
                data.at[idx, 'RSI_Divergence'] = f"{existing};{new_signal}" if existing else new_signal

        return data

    @staticmethod
    def calculate_support_resistance_gap(data):
        support_gap = (data['Curr_Supp'] - data['Prev_Supp']).round(2)
        resistance_gap = (data['Curr_Res'] - data['Prev_Res']).round(2)

        data['Support_Gap'] = support_gap
        data['Resistance_Gap'] = resistance_gap

        valid_support = (data['Curr_Supp'] != 0) & (data['Prev_Supp'] != 0)
        valid_resistance = (data['Curr_Res'] != 0) & (data['Prev_Res'] != 0)

        support_gap_pct = np.zeros(len(data))
        resistance_gap_pct = np.zeros(len(data))

        np.divide(
            support_gap,
            data['Prev_Supp'],
            out=support_gap_pct,
            where=valid_support
        )
        np.divide(
            resistance_gap,
            data['Prev_Res'],
            out=resistance_gap_pct,
            where=valid_resistance
        )

        data['Support_Gap_Pct'] = np.round(support_gap_pct * 100, 2)
        data['Resistance_Gap_Pct'] = np.round(resistance_gap_pct * 100, 2)

        return data

    @staticmethod
    def calculate_support_resistance_strength(data, tolerance_pct=1.0):
        data['Support_Strength_Count'] = 0
        data['Support_Strength_Label'] = ''
        data['Resistance_Strength_Count'] = 0
        data['Resistance_Strength_Label'] = ''

        support_history = []
        resistance_history = []

        indices = data.index.tolist()

        for pos, idx in enumerate(indices):
            curr_support = data.loc[idx, 'Curr_Supp']
            if pd.notna(curr_support) and curr_support != 0:
                valid_history = [val for val in support_history if val != 0]
                matches = [
                    val for val in valid_history
                    if abs(curr_support - val) / abs(val) <= tolerance_pct / 100
                ]
                count = len(matches) + 1
                data.at[idx, 'Support_Strength_Count'] = count
                if count > 1:
                    data.at[idx, 'Support_Strength_Label'] = f"Strong_Support_x{count}"
                else:
                    data.at[idx, 'Support_Strength_Label'] = 'New_Support'
                support_history.append(curr_support)

            curr_resistance = data.loc[idx, 'Curr_Res']
            if pd.notna(curr_resistance) and curr_resistance != 0:
                valid_history = [val for val in resistance_history if val != 0]
                matches = [
                    val for val in valid_history
                    if abs(curr_resistance - val) / abs(val) <= tolerance_pct / 100
                ]
                count = len(matches) + 1
                data.at[idx, 'Resistance_Strength_Count'] = count
                if count > 1:
                    data.at[idx, 'Resistance_Strength_Label'] = f"Strong_Resistance_x{count}"
                else:
                    data.at[idx, 'Resistance_Strength_Label'] = 'New_Resistance'
                resistance_history.append(curr_resistance)

        data['Support_Strength_Count'] = data['Support_Strength_Count'].astype(int)
        data['Resistance_Strength_Count'] = data['Resistance_Strength_Count'].astype(int)
        return data

    @staticmethod
    def identify_stop_loss_hunt(data):
        data['Stop_Loss_Hunt'] = ''
        if not {'Range_ATR', 'open', 'low', 'high', 'Pct_Chg'}.issubset(data.columns):
            return data

        range_expansion = data['Range_ATR'] > 1.2
        open_low_equal = np.isclose(data['open'], data['low'], atol=0.01)
        high_less_prev_high = data['high'] < data['high'].shift(1)
        pct_change = data['Pct_Chg'].abs()
        pct_condition = (pct_change > 0) & (pct_change < 1)

        mask = range_expansion & open_low_equal & high_less_prev_high & pct_condition
        data.loc[mask, 'Stop_Loss_Hunt'] = 'Bullish_Stop_Loss_Hunt'
        return data

    def identify_level_reversal_and_breakout(self, data):
        data['Reversal_Signals'] = ''
        data['Failed_Breakout_Signals'] = ''

        if len(data) < 4 or 'Reg_5_Chg' not in data.columns:
            return data

        level_names = ['EMA_20', 'EMA_60', 'EMA_200', 'Curr_Supp', 'Prev_Supp', 'Curr_Res', 'Prev_Res']
        level_series_map = {
            level: data[level].replace(0, np.nan).ffill()
            for level in level_names
            if level in data.columns
        }

        bullish_levels = ['EMA_20', 'EMA_60', 'EMA_200', 'Curr_Supp', 'Prev_Supp']
        bearish_levels = ['EMA_20', 'EMA_60', 'EMA_200', 'Curr_Res', 'Prev_Res']

        indices = data.index.tolist()

        for pos in range(3, len(data)):
            idx = indices[pos]
            rev_signals = []
            failed_signals = []

            prev_vals = [data.loc[indices[pos - j], 'Reg_5_Chg'] for j in range(1, 4)]
            if any(pd.isna(val) for val in prev_vals) or pd.isna(data.loc[idx, 'Reg_5_Chg']):
                continue

            reg_prev_neg = all(val < 0 for val in prev_vals)
            reg_prev_pos = all(val > 0 for val in prev_vals)
            reg_curr_pos = data.loc[idx, 'Reg_5_Chg'] > 0
            reg_curr_neg = data.loc[idx, 'Reg_5_Chg'] < 0

            if reg_prev_neg and reg_curr_pos:
                for level_name in bullish_levels:
                    if level_name not in level_series_map:
                        continue
                    level_series = level_series_map[level_name]
                    level_current = level_series.iloc[pos]
                    if pd.isna(level_current):
                        continue

                    touched = False
                    crossed_below = False
                    for j in range(1, 4):
                        prev_pos = pos - j
                        prev_idx = indices[prev_pos]
                        level_prev = level_series.iloc[prev_pos]
                        if pd.isna(level_prev):
                            continue
                        low_prev = data.loc[prev_idx, 'low']
                        close_prev = data.loc[prev_idx, 'close']
                        if low_prev <= level_prev <= close_prev:
                            touched = True
                        if close_prev < level_prev:
                            crossed_below = True

                    if touched and not crossed_below and data.loc[idx, 'close'] >= level_current:
                        rev_signals.append(f"Bullish_{level_name}")
                    if crossed_below and data.loc[idx, 'close'] >= level_current:
                        failed_signals.append(f"Bullish_FailedBreakout_{level_name}")

            if reg_prev_pos and reg_curr_neg:
                for level_name in bearish_levels:
                    if level_name not in level_series_map:
                        continue
                    level_series = level_series_map[level_name]
                    level_current = level_series.iloc[pos]
                    if pd.isna(level_current):
                        continue

                    touched = False
                    crossed_above = False
                    for j in range(1, 4):
                        prev_pos = pos - j
                        prev_idx = indices[prev_pos]
                        level_prev = level_series.iloc[prev_pos]
                        if pd.isna(level_prev):
                            continue
                        high_prev = data.loc[prev_idx, 'high']
                        close_prev = data.loc[prev_idx, 'close']
                        if high_prev >= level_prev >= close_prev:
                            touched = True
                        if close_prev > level_prev:
                            crossed_above = True

                    if touched and not crossed_above and data.loc[idx, 'close'] <= level_current:
                        rev_signals.append(f"Bearish_{level_name}")
                    if crossed_above and data.loc[idx, 'close'] <= level_current:
                        failed_signals.append(f"Bearish_FailedBreakout_{level_name}")

            if rev_signals:
                data.at[idx, 'Reversal_Signals'] = ';'.join(rev_signals)
            if failed_signals:
                data.at[idx, 'Failed_Breakout_Signals'] = ';'.join(failed_signals)

        return data

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
        supp_cond = (data.loc[k, 'Reg_5'] > data.loc[k - 1, 'Reg_5']) and (
                data.loc[k - 1, 'Reg_5'] < data.loc[k - 2, 'Reg_5'] < data.loc[k - 3, 'Reg_5'] < data.loc[k - 4, 'Reg_5'])
        if supp_cond:
            prev_support = curr_support
            curr_support = data.loc[k - 1, 'Reg_5']
            data.loc[k, 'Prev_Supp'] = round(prev_support, 2)
            data.loc[k, 'Curr_Supp'] = round(curr_support, 2)

            # Pullback up at Support in the direction of the trend
            if prev_support < curr_support < data.loc[k, 'low'] and curr_res > prev_res:
                data.loc[k, 'Support'] = 'Price_Abv_Supp'
                poc_bl += 1
            if prev_support < curr_support and data.loc[k, 'low'] < curr_support and curr_res > prev_res:
                data.loc[k, 'Support'] = 'Price_Crs_Abv_Supp'
                poc_bl += 1
            elif curr_support > prev_support and curr_res < prev_res and data.loc[k, 'close'] > curr_res:
                data.loc[k, 'Support'] = 'Price_Abv_Cur_Res'
                poc_bl += 1

        sup_break_chk = (data.loc[k, 'high'] > curr_support
                         and data.loc[k - 1, 'close'] > curr_support > data.loc[k, 'close']
                         and data.loc[k - 2, 'close'] > curr_support)
        psup_break_chk = (data.loc[k, 'high'] > prev_support
                          and data.loc[k - 1, 'close'] > prev_support > data.loc[k, 'close']
                          and data.loc[k - 2, 'close'] > prev_support)

        if sup_break_chk and psup_break_chk:
            data.loc[k, 'Break_Sup_Res'] = 'Both_Sup_Broken_Down'
        elif sup_break_chk:
            data.loc[k, 'Break_Sup_Res'] = 'Cur_Sup_Broken_Down'

        return curr_support, prev_support, poc_bl

    @staticmethod
    def analyze_resistance(data, k, curr_res, prev_res, curr_support, prev_support, poc_br):
        res_cond = (data.loc[k, 'Reg_5'] < data.loc[k - 1, 'Reg_5']) and (
                data.loc[k - 1, 'Reg_5'] > data.loc[k - 2, 'Reg_5'] > data.loc[k - 3, 'Reg_5'] > data.loc[k - 4, 'Reg_5'])
        if res_cond:
            prev_res = curr_res
            curr_res = data.loc[k - 1, 'Reg_5']
            data.loc[k, 'Prev_Res'] = round(prev_res, 2)
            data.loc[k, 'Curr_Res'] = round(curr_res, 2)

            # Pullback down at Resistance in the direction of the trend
            if prev_res > curr_res > data.loc[k, 'high'] and curr_support < prev_support:
                data.loc[k, 'Resistance'] = 'Price_Blw_Res'
                poc_br += 1
            if prev_res > curr_res and prev_res > data.loc[k, 'close'] and data.loc[k, 'high'] > curr_res < data.loc[k - 1, 'close']:
                data.loc[k, 'Resistance'] = 'Price_Crs_Blw_Res'
                poc_br += 1
            elif curr_res < prev_res and prev_support > curr_support > data.loc[k, 'high'] <= data.loc[k - 1, 'close']:
                data.loc[k, 'Resistance'] = 'Price_Blw_Cur_Sup'
                poc_br += 1

        # Resistance break check
        res_break_chk = (data.loc[k, 'low'] < curr_res
                         and data.loc[k - 1, 'close'] < curr_res <= data.loc[k, 'close']
                         and data.loc[k - 2, 'close'] < curr_res)
        pres_break_chk = (data.loc[k, 'low'] < prev_res
                          and data.loc[k - 1, 'close'] < prev_res <= data.loc[k, 'close']
                          and data.loc[k - 2, 'close'] < prev_res)

        if res_break_chk and pres_break_chk:
            data.loc[k, 'Break_Sup_Res'] = 'Both_Res_Broken_Up'
        elif res_break_chk:
            data.loc[k, 'Break_Sup_Res'] = 'Cur_Res_Broken_Up'

        return curr_res, prev_res, poc_br

    @staticmethod
    def analyze_breakouts(data, k, poc_bl, poc_br):
        # Check for 20 days High/Low breakout. Ensure we have at least 20 days of data
        if k >= 20:
            high_20 = data.loc[k - 20:k - 1, 'high'].max()
            low_20 = data.loc[k - 20:k - 1, 'low'].min()
            high_date = data.loc[k - 20:k - 1, 'high'].idxmax()
            low_date = data.loc[k - 20:k - 1, 'low'].idxmin()

            days_since_high = k - high_date
            days_since_low = k - low_date

            if (data.loc[k, 'close'] > high_20 and days_since_high >= 10 and
                    data.loc[k, 'Reg_5'] > data.loc[k, 'Reg_18']):
                data.loc[k, 'Breakout_20'] = 'Breakout_20_Up'
                poc_bl += 1
            elif (data.loc[k, 'close'] < low_20 and days_since_low >= 10 and
                  data.loc[k, 'Reg_5'] < data.loc[k, 'Reg_18']):
                data.loc[k, 'Breakout_20'] = 'Breakout_20_Down'
                poc_br += 1

        return poc_bl, poc_br

    def analyze_narrow_range(self, data):
        # Calculate the Average True Range (ATR) for the last 20 days
        data['ATR_20'] = self.atr(data[['high', 'low', 'close']], 20)

        # Calculate the range as a percentage of the closing price
        data['Range_Pct'] = round(((data['high'] - data['low']) / data['close'])*100, 1)

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
            stock = stock.replace('-', '_')
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
