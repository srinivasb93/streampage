import pandas as pd
import numpy as np
import datetime as dt
import statsmodels.api as sm
from common_utils import read_write_sql_data as rd  # Assuming this is your SQL utility
import multiprocessing as mp
import configparser
import logging
from functools import partial

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')

# Default config if file not present
DEFAULT_CONFIG = {
    'Indicators': {
        'atr_period': '14',
        'rsi_period': '14',
        'ema_short': '20',
        'ema_mid': '60',
        'ema_long': '200',
        'slope_short': '6',
        'slope_long': '18'
    },
    'SwingTrading': {
        'atr_multiplier_stop': '1.5',
        'atr_multiplier_target': '2.0',
        'rsi_oversold': '30',
        'rsi_overbought': '70',
        'vol_threshold': '1.2',
        'atr_pct_min': '1.0'
    }
}

if not config.sections():
    config.read_dict(DEFAULT_CONFIG)


class TechnicalIndicators:
    """Class to calculate technical indicators."""

    @staticmethod
    def wwma(values, n_days):
        return values.ewm(alpha=1 / n_days, adjust=False).mean()

    @staticmethod
    def atr(df, n_days=int(config['Indicators']['atr_period'])):
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return round(TechnicalIndicators.wwma(tr, n_days), 2)

    @staticmethod
    def rsi(series, period=int(config['Indicators']['rsi_period'])):
        delta = series.diff()
        gain = delta.where(delta > 0, 0).rolling(window=period).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def slope(series, n):
        x = np.arange(n)
        slopes = pd.Series(index=series.index, dtype=float)
        reg_prices = pd.Series(index=series.index, dtype=float)
        for i in range(n - 1, len(series)):
            y = series.iloc[i - n + 1:i + 1]
            x_scaled = sm.add_constant(x)
            model = sm.OLS(y, x_scaled).fit()
            slopes.iloc[i] = model.params[-1]
            reg_prices.iloc[i] = model.predict(x_scaled)[-1]
        return reg_prices.fillna(0)


class StockAnalyzer:
    """Class to perform stock analysis and generate swing trading signals."""

    def __init__(self, stocks_list, adhoc_date=None, analysis_days=365, analysis_period='by_date'):
        self.stocks_list = stocks_list
        self.adhoc_date = adhoc_date
        self.analysis_days = analysis_days
        self.analysis_period = analysis_period
        self.start_date, self.end_date = self._set_date_range()

    def _set_date_range(self):
        today = dt.date.today()
        if not self.adhoc_date:
            start_date = today - dt.timedelta(days=self.analysis_days)
            end_date = today
        else:
            try:
                end_date = dt.date(*self.adhoc_date)
                start_date = end_date - dt.timedelta(days=self.analysis_days)
            except Exception as e:
                logging.error(f"Invalid adhoc_date: {e}")
                start_date = today - dt.timedelta(days=self.analysis_days)
                end_date = today
        return start_date, end_date

    def _fetch_data(self, stock):
        stock_clean = stock.replace('&', '').replace('-', '')
        try:
            if self.analysis_period == 'by_date':
                query = f"SELECT * from public.{stock_clean} WHERE DATE BETWEEN '{self.start_date}' AND '{self.end_date}' ORDER BY DATE ASC"
            else:
                query = f"SELECT TOP {self.analysis_days} * from public.{stock_clean} ORDER BY DATE DESC"
            data = rd.get_table_data(query=query)
            data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')
            if self.analysis_period != 'by_date':
                data.sort_values(by='Date', ascending=True, inplace=True)
            data['Symbol'] = stock
            return data
        except Exception as e:
            logging.error(f"Error fetching data for {stock}: {e}")
            return pd.DataFrame()

    def _calculate_indicators(self, data):
        if data.empty:
            return data

        # Price-based indicators
        data['Pct_Chg'] = round(data['Close'].pct_change() * 100, 1)
        data['Range'] = round(data['High'] - data['Low'], 2)
        data['HH'] = round(data['High'] - data['High'].shift(), 2)
        data['LL'] = round(data['Low'] - data['Low'].shift(), 2)

        # Volatility
        data['ATR'] = TechnicalIndicators.atr(data[['High', 'Low', 'Close']])
        data['ATR_Pct'] = data['ATR'] / data['Close'] * 100

        # Moving Averages and Trend
        data['EMA_20'] = round(data['Close'].ewm(span=int(config['Indicators']['ema_short']), min_periods=20).mean(), 2)
        if self.analysis_days >= 200:
            data['EMA_60'] = round(data['Close'].ewm(span=int(config['Indicators']['ema_mid']), min_periods=60).mean(),
                                   2)
            data['EMA_200'] = round(
                data['Close'].ewm(span=int(config['Indicators']['ema_long']), min_periods=200).mean(), 2)
            data['Trend'] = 'Range'
            data.loc[(data['Close'] > data['EMA_20']) & (data['EMA_20'] > data['EMA_60']) &
                     (data['EMA_60'] > data['EMA_200']), 'Trend'] = 'Uptrend'
            data.loc[(data['Close'] < data['EMA_20']) & (data['EMA_20'] < data['EMA_60']) &
                     (data['EMA_60'] < data['EMA_200']), 'Trend'] = 'Downtrend'

        # Volume
        data['Vol_Avg20'] = round(data['Volume'].rolling(20, min_periods=20).mean(), 0)
        data['Vol_Abv_Avg20'] = round(data['Volume'] / data['Vol_Avg20'], 2)

        # Regression
        data['Reg_6'] = TechnicalIndicators.slope(data['Close'], int(config['Indicators']['slope_short']))
        data['Reg_18'] = TechnicalIndicators.slope(data['Close'], int(config['Indicators']['slope_long']))
        data['Reg_6_Chg'] = round(data['Reg_6'].diff(), 1)

        # Momentum
        data['RSI_14'] = TechnicalIndicators.rsi(data['Close'])

        # Breakouts
        data['High_20'] = data['High'].rolling(20, min_periods=20).max()
        data['Low_20'] = data['Low'].rolling(20, min_periods=20).min()

        return data

    def _generate_signals(self, data):
        if data.empty:
            return data

        # Vectorized signal generation
        data['EMA20_Sig'] = np.where(data['Close'] >= data['EMA_20'], 'Close_GT_20EMA', 'Close_LT_20EMA')
        data['EMA20_Sig'] = np.where(
            (data['Close'] >= data['EMA_20']) & (data['Close'].shift() < data['EMA_20'].shift()),
            'Cross_Abv_20EMA', data['EMA20_Sig'])
        data['EMA20_Sig'] = np.where(
            (data['Close'] < data['EMA_20']) & (data['Close'].shift() >= data['EMA_20'].shift()),
            'Cross_Blw_20EMA', data['EMA20_Sig'])

        data['Vol20_Sig'] = np.select(
            [data['Vol_Abv_Avg20'] > 1.5, data['Vol_Abv_Avg20'] >= float(config['SwingTrading']['vol_threshold'])],
            ['Super Volume', 'Above Avg Volume'], default='Normal Volume'
        )

        # Support/Resistance
        data['Curr_Supp'] = data['Reg_6'].where(data['Reg_6_Chg'] > 0, 0).shift(1)
        data['Curr_Res'] = data['Reg_6'].where(data['Reg_6_Chg'] < 0, 0).shift(1)

        # Swing Trading Signals
        vol_thresh = float(config['SwingTrading']['vol_threshold'])
        data['Swing_Long'] = np.where(
            (data['Low'] <= data['Curr_Supp'] * 1.01) & (data['Close'] > data['Curr_Supp']) &
            (data['Vol_Abv_Avg20'] > vol_thresh), 'Support_Reversal', None
        )
        data['Swing_Long'] = np.where(
            (data['Close'] > data['High_20'].shift()) & (data['Vol_Abv_Avg20'] > 1.5), 'Breakout_Long',
            data['Swing_Long']
        )
        data['Swing_Short'] = np.where(
            (data['High'] >= data['Curr_Res'] * 0.99) & (data['Close'] < data['Curr_Res']) &
            (data['Vol_Abv_Avg20'] > vol_thresh), 'Resistance_Reversal', None
        )

        # RSI-based signals
        rsi_oversold = float(config['SwingTrading']['rsi_oversold'])
        rsi_overbought = float(config['SwingTrading']['rsi_overbought'])
        data['Swing_Long'] = np.where(
            (data['RSI_14'] < rsi_oversold) & (data['Close'] > data['EMA_20']), 'RSI_Oversold', data['Swing_Long']
        )
        data['Swing_Short'] = np.where(
            (data['RSI_14'] > rsi_overbought) & (data['Close'] < data['EMA_20']), 'RSI_Overbought', data['Swing_Short']
        )

        # Risk Management
        atr_stop = float(config['SwingTrading']['atr_multiplier_stop'])
        atr_target = float(config['SwingTrading']['atr_multiplier_target'])
        data['Stop_Loss_Long'] = data['Low'] - data['ATR'] * atr_stop
        data['Target_Long'] = data['Close'] + data['ATR'] * atr_target
        data['Stop_Loss_Short'] = data['High'] + data['ATR'] * atr_stop
        data['Target_Short'] = data['Close'] - data['ATR'] * atr_target

        return data

    def _process_stock(self, stock):
        logging.info(f"Processing data for {stock}")
        data = self._fetch_data(stock)
        if not data.empty:
            data = self._calculate_indicators(data)
            data = self._generate_signals(data)
            rd.load_sql_data(data_to_load=data, table_name=f'data_{stock}')
            return data.tail(1)
        return pd.DataFrame()

    def analyze(self):
        with mp.Pool(processes=mp.cpu_count()) as pool:
            results = pool.map(self._process_stock, self.stocks_list)
        summary_df = pd.concat(results, axis=0, ignore_index=True)
        return summary_df


class Backtester:
    """Class to backtest swing trading signals."""

    @staticmethod
    def backtest(data, signal_col, stop_col, target_col, direction='long'):
        trades = []
        position = None
        entry_price = 0

        for i, row in data.iterrows():
            if position is None and pd.notna(row[signal_col]):
                position = direction
                entry_price = row['Close']
                entry_date = row['Date']
            elif position == 'long':
                if row['Low'] <= row[stop_col]:
                    trades.append({'Entry': entry_price, 'Exit': row[stop_col], 'Profit': row[stop_col] - entry_price,
                                   'Date': entry_date})
                    position = None
                elif row['High'] >= row[target_col]:
                    trades.append(
                        {'Entry': entry_price, 'Exit': row[target_col], 'Profit': row[target_col] - entry_price,
                         'Date': entry_date})
                    position = None
            elif position == 'short':
                if row['High'] >= row[stop_col]:
                    trades.append({'Entry': entry_price, 'Exit': row[stop_col], 'Profit': entry_price - row[stop_col],
                                   'Date': entry_date})
                    position = None
                elif row['Low'] <= row[target_col]:
                    trades.append(
                        {'Entry': entry_price, 'Exit': row[target_col], 'Profit': entry_price - row[target_col],
                         'Date': entry_date})
                    position = None

        return pd.DataFrame(trades)

    @classmethod
    def run(cls, summary_data):
        results = {}
        for stock in summary_data['Symbol'].unique():
            stock_data = rd.get_table_data(query=f"SELECT * from public.data_{stock} ORDER BY Date ASC")
            if not stock_data.empty:
                long_trades = cls.backtest(stock_data, 'Swing_Long', 'Stop_Loss_Long', 'Target_Long', 'long')
                short_trades = cls.backtest(stock_data, 'Swing_Short', 'Stop_Loss_Short', 'Target_Short', 'short')
                results[stock] = {'Long': long_trades, 'Short': short_trades}
        return results


class ReportGenerator:
    """Class to generate and display analysis reports."""

    @staticmethod
    def print_summary(summary_data):
        atr_min = float(config['SwingTrading']['atr_pct_min'])
        print("Swing Trading Opportunities - Long")
        long_conditions = (summary_data['Swing_Long'].notna() &
                           (summary_data['ATR_Pct'] > atr_min) &
                           (summary_data['Trend'] != 'Downtrend'))
        print(summary_data[long_conditions][['Symbol', 'Date', 'Close', 'ATR_Pct', 'Swing_Long', 'Trend', 'RSI_14']])

        print("========================")
        print("Swing Trading Opportunities - Short")
        short_conditions = (summary_data['Swing_Short'].notna() &
                            (summary_data['ATR_Pct'] > atr_min) &
                            (summary_data['Trend'] != 'Uptrend'))
        print(summary_data[short_conditions][['Symbol', 'Date', 'Close', 'ATR_Pct', 'Swing_Short', 'Trend', 'RSI_14']])

        print("========================")
        print("Detailed Metrics for Swing Candidates")
        swing_candidates = summary_data[long_conditions | short_conditions]
        print(swing_candidates[['Symbol', 'Close', 'EMA_20', 'Curr_Supp', 'Curr_Res', 'RSI_14', 'Vol_Abv_Avg20',
                                'Stop_Loss_Long', 'Target_Long', 'Stop_Loss_Short', 'Target_Short']])

    @staticmethod
    def print_backtest_results(backtest_results):
        for stock, trades in backtest_results.items():
            print(f"\nBacktest Results for {stock}")
            print("Long Trades:")
            print(trades['Long'])
            print(f"Total Profit (Long): {trades['Long']['Profit'].sum():.2f}")
            print("Short Trades:")
            print(trades['Short'])
            print(f"Total Profit (Short): {trades['Short']['Profit'].sum():.2f}")


if __name__ == "__main__":
    # Example usage
    stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB', sample=True, sample_count=50)
    stock_list = stock_list_df['SYMBOL'].values.tolist() if not stock_list_df.empty else ['TATAMOTORS']

    analyzer = StockAnalyzer(stocks_list=stock_list, adhoc_date=(2024, 9, 9))
    summary_data = analyzer.analyze()

    reporter = ReportGenerator()
    reporter.print_summary(summary_data)

    backtester = Backtester()
    backtest_results = backtester.run(summary_data)
    reporter.print_backtest_results(backtest_results)