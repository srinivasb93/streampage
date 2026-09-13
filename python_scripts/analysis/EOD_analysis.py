import pandas as pd
import numpy as np
import datetime as dt
import statsmodels.api as sm
from common_utils import read_write_sql_data as rd
import pandas_ta as ta


class EODAnalysis:
    def __init__(self, stocks_list, adhoc_date=None, analysis_days=365, analysis_period='by_date', timeframe='D'):
        self.stocks_list = stocks_list
        self.adhoc_date = adhoc_date
        self.analysis_days = analysis_days
        self.analysis_period = analysis_period
        self.timeframe = (timeframe or 'D').upper()
        self.ema_cnt_chk = 0
        self.summary_df = pd.DataFrame()
        self.config = self.get_timeframe_config()
        self.set_analysis_dates()

    def get_timeframe_config(self):
        configs = {
            'D': {
                'table_suffix': '',
                'summary_table': 'EOD_Summary',
                'ema_mid': 60,
                'ema_long': 200,
                'ema_mid_label': 'EMA60',
                'ema_long_label': 'EMA200',
                'breakout_bars': 20,
                'breakout_label': '20',
                'high_low_window': 20,
                'high_low_label': '20',
                'high_low_long_window': None,
                'high_low_long_label': None,
                'sma_source': None,
                'run_extended_signals': True,
            },
            'W': {
                'table_suffix': '_W',
                'summary_table': 'EOW_Summary',
                'ema_mid': None,
                'ema_long': 52,
                'ema_mid_label': None,
                'ema_long_label': 'EMA52',
                'breakout_bars': 20,
                'breakout_label': '20W',
                'high_low_window': 20,
                'high_low_label': '20W',
                'high_low_long_window': 52,
                'high_low_long_label': '52W',
                'sma_source': 'EMA_20',
                'run_extended_signals': False,
            },
            'M': {
                'table_suffix': '_M',
                'summary_table': 'EOM_Summary',
                'ema_mid': None,
                'ema_long': 52,
                'ema_long_label': 'EMA52',
                'breakout_bars': 20,
                'breakout_label': '20M',
                'high_low_window': 20,
                'high_low_label': '20M',
                'high_low_long_window': 52,
                'high_low_long_label': '52M',
                'sma_source': 'EMA_20',
                'run_extended_signals': False,
            }
        }
        if self.timeframe not in configs:
            raise ValueError(f"Unsupported timeframe: {self.timeframe}")
        return configs[self.timeframe]

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
        if data.empty:
            return data
        data.attrs['Breakout_Window'] = self.config['breakout_bars']
        data.attrs['Breakout_Label'] = self.config['breakout_label']
        data = self.calculate_indicators(data)
        data = self.analyze_price_action(data)
        if self.config['run_extended_signals']:
            data = self.analyze_additional_signals(data)
            data = self.analyze_narrow_range(data)
        return data

    def get_query(self, stock):
        stock_table = f'{stock}{self.config["table_suffix"]}'
        if self.analysis_period == 'by_date':
            return (
                f"SELECT * from public.\"{stock_table}\" "
                f"WHERE timestamp BETWEEN '{self.analysis_start_date}' AND '{self.analysis_end_date}' "
                f"ORDER BY timestamp ASC"
            )
        else:
            return f"SELECT * from public.\"{stock_table}\" ORDER BY timestamp DESC LIMIT {self.analysis_days}"

    def preprocess_data(self, data, stock):
        if data.empty:
            return data
        data = data.copy()
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data.sort_values(by=['timestamp'], ascending=True, inplace=True)
        data.reset_index(drop=True, inplace=True)
        data['Symbol'] = stock
        return data

    def calculate_indicators(self, data):
        data['Pct_Chg'] = round(data['close'].pct_change() * 100, 1)
        data['Pct_Chg_5D'] = round(data['close'].pct_change(5) * 100, 1)
        data['Pct_Chg_20D'] = round(data['close'].pct_change(20) * 100, 1)
        data['Pct_Chg_60D'] = round(data['close'].pct_change(60) * 100, 1)
        data['Pct_Chg_120D'] = round(data['close'].pct_change(120) * 100, 1)
        if self.analysis_days >= 365:
            data['Pct_Chg_365D'] = round(data['close'].pct_change(240) * 100, 1)
        data['Range'] = round(data['high'] - data['low'], 2)
        data['HH'] = round(data['high'] - data['high'].shift(), 2)
        data['LL'] = round(data['low'] - data['low'].shift(), 2)
        high_low_window = self.config['high_low_window']
        high_low_label = self.config['high_low_label']
        data[f'High_{high_low_label}'] = data['high'].rolling(high_low_window, min_periods=high_low_window).max()
        data[f'Low_{high_low_label}'] = data['low'].rolling(high_low_window, min_periods=high_low_window).min()
        if self.config['high_low_long_window']:
            long_window = self.config['high_low_long_window']
            long_label = self.config['high_low_long_label']
            data[f'High_{long_label}'] = data['high'].rolling(long_window, min_periods=long_window).max()
            data[f'Low_{long_label}'] = data['low'].rolling(long_window, min_periods=long_window).min()
        data['ATR'] = round(ta.atr(data['high'], data['low'], data['close'], length=14), 2)
        data['Range_ATR'] = round(data['Range'] / data['ATR'], 1)
        data['Vol_Avg20'] = round(data['volume'].rolling(20, min_periods=20).mean(), 0)
        data['EMA_20'] = round(data['close'].ewm(span=20, min_periods=20).mean(), 2)
        if self.config.get('ema_mid'):
            span = self.config['ema_mid']
            data[f'EMA_{span}'] = round(data['close'].ewm(span=span, min_periods=span).mean(), 2)
        if self.config.get('ema_long'):
            span = self.config['ema_long']
            data[f'EMA_{span}'] = round(data['close'].ewm(span=span, min_periods=span).mean(), 2)
        if self.config.get('sma_source'):
            source_col = self.config['sma_source']
            data['SMA_9'] = round(data[source_col].rolling(9, min_periods=9).mean(), 2)
            data['EMA20_SMA9_Spread'] = round(data['EMA_20'] - data['SMA_9'], 2)
            data['Cls_Abv_SMA9'] = round(data['close'] - data['SMA_9'], 2)
        data['Reg_5'] = self.slope(data['close'], n=5)
        data['Reg_5'] = round(data['Reg_5'], 2)
        data['Reg_18'] = round(ta.linreg(data['close'], length=18), 2)
        data['Reg_5_Chg'] = round(data['Reg_5'] - data['Reg_5'].shift(), 1)
        data['Reg_Cross'] = round(data['Reg_5'] - data['Reg_18'], 1)
        data['RSI_14'] = round(ta.rsi(data['close'], length=14), 1)
        data['Vol_Abv_Avg20'] = round(data['volume'] / data['Vol_Avg20'], 2)
        data['Cls_Abv_EMA20'] = round(data['close'] - data['EMA_20'], 2)
        if self.config.get('ema_mid'):
            span = self.config['ema_mid']
            data[f'Cls_Abv_EMA{span}'] = round(data['close'] - data[f'EMA_{span}'], 2)
        if self.config.get('ema_long'):
            span = self.config['ema_long']
            data[f'Cls_Abv_EMA{span}'] = round(data['close'] - data[f'EMA_{span}'], 2)
        data['Cls_Abv_Reg5'] = round(data['close'] - data['Reg_5'], 2)
        return data

    def analyze_price_action(self, data):
        curr_support = prev_support = curr_res = prev_res = 0.0
        data['Curr_Supp'] = data['Prev_Supp'] = data['Curr_Res'] = data['Prev_Res'] = 0.0
        data['Resistance'] = data['Support'] = ''

        for k in range(4, len(data)):
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

        ema_mid = self.config.get('ema_mid')
        if ema_mid:
            cls_col = f'Cls_Abv_EMA{ema_mid}'
            sig_col = f'EMA{ema_mid}_Sig'
            if data.loc[k, cls_col] >= 0:
                data.loc[k, sig_col] = f'Close_GT_{ema_mid}EMA' if data.loc[k - 1, cls_col] >= 0 else f'Cross_Abv_{ema_mid}EMA'
            else:
                data.loc[k, sig_col] = f'Close_LT_{ema_mid}EMA' if data.loc[k - 1, cls_col] < 0 else f'Cross_Blw_{ema_mid}EMA'

        ema_long = self.config.get('ema_long')
        if ema_long:
            cls_col = f'Cls_Abv_EMA{ema_long}'
            sig_col = f'EMA{ema_long}_Sig'
            if data.loc[k, cls_col] >= 0:
                data.loc[k, sig_col] = f'Close_GT_{ema_long}EMA' if data.loc[k - 1, cls_col] >= 0 else f'Cross_Abv_{ema_long}EMA'
            else:
                data.loc[k, sig_col] = f'Close_LT_{ema_long}EMA' if data.loc[k - 1, cls_col] < 0 else f'Cross_Blw_{ema_long}EMA'

        if 'Cls_Abv_SMA9' in data.columns:
            if data.loc[k, 'Cls_Abv_SMA9'] >= 0:
                data.loc[k, 'SMA9_Sig'] = 'Close_GT_SMA9' if data.loc[k - 1, 'Cls_Abv_SMA9'] >= 0 else 'Cross_Abv_SMA9'
            else:
                data.loc[k, 'SMA9_Sig'] = 'Close_LT_SMA9' if data.loc[k - 1, 'Cls_Abv_SMA9'] < 0 else 'Cross_Blw_SMA9'

        if 'EMA20_SMA9_Spread' in data.columns:
            spread = data.loc[k, 'EMA20_SMA9_Spread']
            prev_spread = data.loc[k - 1, 'EMA20_SMA9_Spread']
            if spread >= 0 > prev_spread:
                data.loc[k, 'EMA20_SMA9_Cross_Sig'] = 'Cross_Up'
            elif spread < 0 <= prev_spread:
                data.loc[k, 'EMA20_SMA9_Cross_Sig'] = 'Cross_Down'
            elif spread >= 0:
                data.loc[k, 'EMA20_SMA9_Cross_Sig'] = 'EMA20_Abv_SMA9'
            else:
                data.loc[k, 'EMA20_SMA9_Cross_Sig'] = 'EMA20_Blw_SMA9'

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
        data = self.analyze_trend_and_regime(data)
        data = self.calculate_signal_confidence(data)
        return data

    @staticmethod
    def identify_rsi_divergence(data, lookback=3, min_price_change_pct=0.25, min_rsi_delta=1.0):
        data['RSI_Divergence'] = ''
        if len(data) <= (2 * lookback) or not {'high', 'low', 'RSI_14'}.issubset(data.columns):
            return data

        highs = data['high'].to_numpy()
        lows = data['low'].to_numpy()
        rsi = data['RSI_14'].to_numpy()
        out = [''] * len(data)

        last_high_pos = None
        last_low_pos = None

        for pos in range(lookback, len(data) - lookback):
            signals = []

            hi_window = highs[pos - lookback:pos + lookback + 1]
            current_high = highs[pos]
            is_pivot_high = (
                np.isfinite(current_high)
                and current_high == np.nanmax(hi_window)
                and np.sum(np.isclose(hi_window, current_high, equal_nan=False)) == 1
            )
            if is_pivot_high:
                if last_high_pos is not None and pd.notna(rsi[last_high_pos]) and pd.notna(rsi[pos]):
                    prev_high = highs[last_high_pos]
                    price_change_pct = ((current_high - prev_high) / prev_high) * 100 if prev_high else 0
                    rsi_delta = rsi[last_high_pos] - rsi[pos]
                    if price_change_pct >= min_price_change_pct and rsi_delta >= min_rsi_delta:
                        signals.append('Bearish')
                last_high_pos = pos

            lo_window = lows[pos - lookback:pos + lookback + 1]
            current_low = lows[pos]
            is_pivot_low = (
                np.isfinite(current_low)
                and current_low == np.nanmin(lo_window)
                and np.sum(np.isclose(lo_window, current_low, equal_nan=False)) == 1
            )
            if is_pivot_low:
                if last_low_pos is not None and pd.notna(rsi[last_low_pos]) and pd.notna(rsi[pos]):
                    prev_low = lows[last_low_pos]
                    price_change_pct = ((prev_low - current_low) / prev_low) * 100 if prev_low else 0
                    rsi_delta = rsi[pos] - rsi[last_low_pos]
                    if price_change_pct >= min_price_change_pct and rsi_delta >= min_rsi_delta:
                        signals.append('Bullish')
                last_low_pos = pos

            if signals:
                out[pos] = ';'.join(signals)

        data['RSI_Divergence'] = out
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
    def identify_stop_loss_hunt(data, min_range_atr=1.2, wick_ratio_min=0.35, max_abs_pct_chg=2.0):
        data['Stop_Loss_Hunt'] = ''
        req_cols = {'open', 'high', 'low', 'close', 'Range', 'Range_ATR', 'Pct_Chg'}
        if not req_cols.issubset(data.columns):
            return data

        prev_low = data['low'].shift(1)
        prev_high = data['high'].shift(1)
        day_range = data['Range'].replace(0, np.nan)
        upper_wick = data['high'] - data[['open', 'close']].max(axis=1)
        lower_wick = data[['open', 'close']].min(axis=1) - data['low']

        range_expansion = data['Range_ATR'] >= min_range_atr
        controlled_close = data['Pct_Chg'].abs().between(0.05, max_abs_pct_chg)

        bullish_hunt = (
            range_expansion
            & controlled_close
            & (data['low'] < prev_low)
            & (data['close'] > prev_low)
            & ((lower_wick / day_range) >= wick_ratio_min)
        )
        bearish_hunt = (
            range_expansion
            & controlled_close
            & (data['high'] > prev_high)
            & (data['close'] < prev_high)
            & ((upper_wick / day_range) >= wick_ratio_min)
        )

        data.loc[bullish_hunt, 'Stop_Loss_Hunt'] = 'Bullish_Stop_Loss_Hunt'
        data.loc[bearish_hunt, 'Stop_Loss_Hunt'] = np.where(
            data.loc[bearish_hunt, 'Stop_Loss_Hunt'].eq(''),
            'Bearish_Stop_Loss_Hunt',
            data.loc[bearish_hunt, 'Stop_Loss_Hunt'] + ';Bearish_Stop_Loss_Hunt'
        )
        return data

    def identify_level_reversal_and_breakout(self, data, lookback_bars=3, level_tolerance_pct=0.2):
        data['Reversal_Signals'] = ''
        data['Failed_Breakout_Signals'] = ''

        if len(data) <= lookback_bars or 'Reg_5_Chg' not in data.columns:
            return data

        level_names = ['EMA_20', 'Curr_Supp', 'Prev_Supp', 'Curr_Res', 'Prev_Res']
        if self.config.get('ema_mid'):
            level_names.append(f'EMA_{self.config["ema_mid"]}')
        if self.config.get('ema_long'):
            level_names.append(f'EMA_{self.config["ema_long"]}')
        level_series_map = {
            level: data[level].replace(0, np.nan).ffill()
            for level in level_names
            if level in data.columns
        }

        bullish_levels = ['EMA_20', 'Curr_Supp', 'Prev_Supp']
        bearish_levels = ['EMA_20', 'Curr_Res', 'Prev_Res']
        if self.config.get('ema_mid'):
            bullish_levels.append(f'EMA_{self.config["ema_mid"]}')
            bearish_levels.append(f'EMA_{self.config["ema_mid"]}')
        if self.config.get('ema_long'):
            bullish_levels.append(f'EMA_{self.config["ema_long"]}')
            bearish_levels.append(f'EMA_{self.config["ema_long"]}')

        indices = data.index.tolist()
        tol = level_tolerance_pct / 100

        for pos in range(lookback_bars, len(data)):
            idx = indices[pos]
            rev_signals = []
            failed_signals = []

            prev_vals = [data.loc[indices[pos - j], 'Reg_5_Chg'] for j in range(1, lookback_bars + 1)]
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
                    closed_below = False
                    for j in range(1, lookback_bars + 1):
                        prev_pos = pos - j
                        prev_idx = indices[prev_pos]
                        level_prev = level_series.iloc[prev_pos]
                        if pd.isna(level_prev):
                            continue
                        level_floor = level_prev * (1 - tol)
                        level_ceiling = level_prev * (1 + tol)
                        low_prev = data.loc[prev_idx, 'low']
                        close_prev = data.loc[prev_idx, 'close']
                        if low_prev <= level_ceiling and close_prev >= level_floor:
                            touched = True
                        if close_prev < level_floor:
                            closed_below = True

                    curr_level_floor = level_current * (1 - tol)
                    curr_level_ceiling = level_current * (1 + tol)
                    curr_close = data.loc[idx, 'close']
                    if touched and not closed_below and curr_close >= curr_level_floor:
                        rev_signals.append(f"Bullish_{level_name}")
                    if closed_below and curr_close >= curr_level_ceiling:
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
                    closed_above = False
                    for j in range(1, lookback_bars + 1):
                        prev_pos = pos - j
                        prev_idx = indices[prev_pos]
                        level_prev = level_series.iloc[prev_pos]
                        if pd.isna(level_prev):
                            continue
                        level_floor = level_prev * (1 - tol)
                        level_ceiling = level_prev * (1 + tol)
                        high_prev = data.loc[prev_idx, 'high']
                        close_prev = data.loc[prev_idx, 'close']
                        if high_prev >= level_floor and close_prev <= level_ceiling:
                            touched = True
                        if close_prev > level_ceiling:
                            closed_above = True

                    curr_level_floor = level_current * (1 - tol)
                    curr_level_ceiling = level_current * (1 + tol)
                    curr_close = data.loc[idx, 'close']
                    if touched and not closed_above and curr_close <= curr_level_ceiling:
                        rev_signals.append(f"Bearish_{level_name}")
                    if closed_above and curr_close <= curr_level_floor:
                        failed_signals.append(f"Bearish_FailedBreakout_{level_name}")

            if rev_signals:
                data.at[idx, 'Reversal_Signals'] = ';'.join(rev_signals)
            if failed_signals:
                data.at[idx, 'Failed_Breakout_Signals'] = ';'.join(failed_signals)

        return data

    def analyze_trend_and_regime(self, data):
        adx = ta.adx(data['high'], data['low'], data['close'], length=14)
        if isinstance(adx, pd.DataFrame):
            if 'ADX_14' in adx.columns:
                data['ADX_14'] = round(adx['ADX_14'], 2)
            if 'DMP_14' in adx.columns:
                data['DMP_14'] = round(adx['DMP_14'], 2)
            if 'DMN_14' in adx.columns:
                data['DMN_14'] = round(adx['DMN_14'], 2)

        data['ADX_Slope'] = round(data['ADX_14'].diff(), 2) if 'ADX_14' in data.columns else np.nan
        data['ATR_Pct'] = round((data['ATR'] / data['close']) * 100, 2)
        data['ATR_Pct_60Q'] = round(data['ATR_Pct'].rolling(60, min_periods=30).quantile(0.6), 2)

        ema_mid = f'EMA_{self.config["ema_mid"]}' if self.config.get('ema_mid') else None
        ema_long = f'EMA_{self.config["ema_long"]}' if self.config.get('ema_long') else None
        if ema_mid in data.columns and ema_long in data.columns:
            bullish_structure = (data['close'] > data['EMA_20']) & (data['EMA_20'] > data[ema_mid]) & (data[ema_mid] > data[ema_long])
            bearish_structure = (data['close'] < data['EMA_20']) & (data['EMA_20'] < data[ema_mid]) & (data[ema_mid] < data[ema_long])
        elif ema_long in data.columns:
            bullish_structure = (data['close'] > data['EMA_20']) & (data['EMA_20'] > data[ema_long])
            bearish_structure = (data['close'] < data['EMA_20']) & (data['EMA_20'] < data[ema_long])
        else:
            bullish_structure = data['close'] > data['EMA_20']
            bearish_structure = data['close'] < data['EMA_20']

        adx_strong = data['ADX_14'] >= 25 if 'ADX_14' in data.columns else pd.Series(False, index=data.index)
        adx_medium = data['ADX_14'].between(18, 24.99) if 'ADX_14' in data.columns else pd.Series(False, index=data.index)

        data['Trend_Direction'] = np.select(
            [bullish_structure, bearish_structure],
            ['Bullish', 'Bearish'],
            default='Sideways'
        )
        data['Trend_Strength'] = np.select(
            [adx_strong, adx_medium],
            ['Strong', 'Moderate'],
            default='Weak'
        )
        data['Trend_Label'] = data['Trend_Direction'] + '_' + data['Trend_Strength']

        high_vol = (data['Range_ATR'] >= 1.5) | (data['ATR_Pct'] > data['ATR_Pct_60Q'])
        low_vol = (data['Range_ATR'] <= 0.8) & (data['ATR_Pct'] < data['ATR_Pct_60Q'])
        data['Volatility_Regime'] = np.select(
            [high_vol, low_vol],
            ['High_Vol', 'Low_Vol'],
            default='Normal_Vol'
        )

        high_volm = data['Vol_Abv_Avg20'] >= 1.5
        low_volm = data['Vol_Abv_Avg20'] <= 0.8
        data['Volume_Regime'] = np.select(
            [high_volm, low_volm],
            ['High_Volume', 'Low_Volume'],
            default='Normal_Volume'
        )

        rs_components = {
            'Pct_Chg_20D': 0.4,
            'Pct_Chg_60D': 0.35,
            'Pct_Chg_120D': 0.25
        }
        rs_score = np.zeros(len(data))
        for col, wt in rs_components.items():
            if col in data.columns:
                rs_score += data[col].fillna(0).to_numpy() * wt
        data['Relative_Strength_Score'] = np.round(rs_score, 2)

        mom_score = np.zeros(len(data))
        if 'Reg_Cross' in data.columns:
            mom_score += np.tanh(data['Reg_Cross'].fillna(0).to_numpy()) * 8
        if 'RSI_14' in data.columns:
            mom_score += np.clip((data['RSI_14'].fillna(50).to_numpy() - 50) / 2, -12, 12)
        if 'Pct_Chg_20D' in data.columns:
            mom_score += np.clip(data['Pct_Chg_20D'].fillna(0).to_numpy() / 2, -10, 10)
        data['Momentum_Score'] = np.round(mom_score, 2)
        return data

    @staticmethod
    def calculate_signal_confidence(data):
        score = np.full(len(data), 50.0)

        if 'Trend_Direction' in data.columns:
            score += np.where(data['Trend_Direction'].eq('Bullish'), 8, 0)
            score -= np.where(data['Trend_Direction'].eq('Bearish'), 8, 0)
        if 'Trend_Strength' in data.columns:
            score += np.where(data['Trend_Strength'].eq('Strong'), 8, 0)
            score += np.where(data['Trend_Strength'].eq('Moderate'), 3, 0)
        if 'Relative_Strength_Score' in data.columns:
            score += np.clip(data['Relative_Strength_Score'].fillna(0).to_numpy() / 3, -12, 12)
        if 'Momentum_Score' in data.columns:
            score += np.clip(data['Momentum_Score'].fillna(0).to_numpy() / 2, -10, 10)

        if 'RSI_Divergence' in data.columns:
            has_bull_div = data['RSI_Divergence'].fillna('').str.contains('Bullish')
            has_bear_div = data['RSI_Divergence'].fillna('').str.contains('Bearish')
            score += np.where(has_bull_div, 7, 0)
            score -= np.where(has_bear_div, 7, 0)
        if 'Stop_Loss_Hunt' in data.columns:
            has_bull_hunt = data['Stop_Loss_Hunt'].fillna('').str.contains('Bullish')
            has_bear_hunt = data['Stop_Loss_Hunt'].fillna('').str.contains('Bearish')
            score += np.where(has_bull_hunt, 6, 0)
            score -= np.where(has_bear_hunt, 6, 0)
        if 'Reversal_Signals' in data.columns:
            has_bull_rev = data['Reversal_Signals'].fillna('').str.contains('Bullish')
            has_bear_rev = data['Reversal_Signals'].fillna('').str.contains('Bearish')
            score += np.where(has_bull_rev, 6, 0)
            score -= np.where(has_bear_rev, 6, 0)
        if 'Failed_Breakout_Signals' in data.columns:
            has_bull_fb = data['Failed_Breakout_Signals'].fillna('').str.contains('Bullish')
            has_bear_fb = data['Failed_Breakout_Signals'].fillna('').str.contains('Bearish')
            score += np.where(has_bull_fb, 5, 0)
            score -= np.where(has_bear_fb, 5, 0)
        if 'Breakout_20' in data.columns:
            score += np.where(data['Breakout_20'].eq('Breakout_20_Up'), 5, 0)
            score -= np.where(data['Breakout_20'].eq('Breakout_20_Down'), 5, 0)
        if 'Volume_Regime' in data.columns:
            score += np.where(data['Volume_Regime'].eq('High_Volume'), 3, 0)
        if 'Volatility_Regime' in data.columns:
            score -= np.where(data['Volatility_Regime'].eq('High_Vol'), 2, 0)

        score = np.clip(score, 0, 100)
        data['Signal_Confidence'] = np.round(score, 1)

        direction = np.where(
            data['Signal_Confidence'] >= 65,
            'Long',
            np.where(data['Signal_Confidence'] <= 35, 'Short', 'Neutral')
        )
        conviction = np.select(
            [data['Signal_Confidence'] >= 80, data['Signal_Confidence'] >= 65,
             data['Signal_Confidence'] <= 20, data['Signal_Confidence'] <= 35],
            ['Strong', 'Moderate', 'Strong', 'Moderate'],
            default='Low'
        )
        data['Trade_Bias'] = np.where(direction == 'Neutral', 'Neutral', conviction + '_' + direction)

        data['Signal_Confidence_Bucket'] = pd.cut(
            data['Signal_Confidence'],
            bins=[-0.1, 20, 35, 65, 80, 100],
            labels=['Very_Bearish', 'Bearish', 'Neutral', 'Bullish', 'Very_Bullish']
        ).astype(str)
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
        breakout_bars = 20
        breakout_label = '20'
        if 'Breakout_Window' in data.attrs:
            breakout_bars = data.attrs['Breakout_Window']
            breakout_label = data.attrs['Breakout_Label']

        if k >= breakout_bars:
            high_20 = data.loc[k - breakout_bars:k - 1, 'high'].max()
            low_20 = data.loc[k - breakout_bars:k - 1, 'low'].min()
            high_date = data.loc[k - breakout_bars:k - 1, 'high'].idxmax()
            low_date = data.loc[k - breakout_bars:k - 1, 'low'].idxmin()

            days_since_high = k - high_date
            days_since_low = k - low_date

            if (data.loc[k, 'close'] > high_20 and days_since_high >= 10 and
                    data.loc[k, 'Reg_5'] > data.loc[k, 'Reg_18']):
                data.loc[k, 'Breakout_20'] = f'Breakout_{breakout_label}_Up'
                poc_bl += 1
            elif (data.loc[k, 'close'] < low_20 and days_since_low >= 10 and
                  data.loc[k, 'Reg_5'] < data.loc[k, 'Reg_18']):
                data.loc[k, 'Breakout_20'] = f'Breakout_{breakout_label}_Down'
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
            if data.empty:
                continue
            # load_msg = rd.load_sql_data(data_to_load=data, table_name='data_' + stock)
            # print(load_msg)
            self.summary_df = pd.concat([self.summary_df, data.tail(1)], axis=0, ignore_index=True)
        if self.summary_df.empty:
            print(f'No rows available to load into {self.config["summary_table"]}')
            return 'Failed'
        summary_load_msg = rd.load_sql_data(data_to_load=self.summary_df, table_name=self.config['summary_table'])
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
