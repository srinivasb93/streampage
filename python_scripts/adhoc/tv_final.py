import time

import pandas as pd
import numpy as np
from lightweight_charts import Chart
from common_utils import read_write_sql_data as rd
import pandas_ta as ta
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta


def on_search(chart, searched_string):
    get_bar_data(chart, mode=searched_string)
    clear_indicators(chart)  # Clear existing indicators when searching for a new stock
    chart.watermark(searched_string)


def get_bar_data(chart, mode='selection', fetch_data=False, need_drawings=False):
    symbol = chart.topbar['symbol'].value if mode == 'selection' else mode
    timeframe = chart.topbar['timeframe'].value
    if timeframe == 'W':
        symbol = symbol + '_W'
    elif timeframe == 'M':
        symbol = symbol + '_M'

    query = f"SELECT * FROM nsedata.public.{symbol} ORDER BY Date"
    stock_data = rd.get_table_data(query=query)

    if fetch_data:
        return stock_data
    else:
        chart.set(stock_data, keep_drawings=need_drawings)
        chart.watermark(symbol)


def add_indicator(chart):
    stock_data = get_bar_data(chart, fetch_data=True)
    indicator = chart.topbar['indicators'].value
    period = int(chart.topbar['period'].value)
    ind_color = chart.topbar['ind_color'].value

    if indicator in ['RSI', 'MACD']:
        add_subchart_indicator(chart, stock_data, indicator, period, ind_color)
    else:
        ind_line = chart.create_line(name=f'{indicator} {period}', color=ind_color, width=2, price_label=True)
        ind_df = pd.DataFrame({'time': stock_data.Date})

        if indicator == 'EMA':
            ind_df[f'{indicator} {period}'] = stock_data.ta.ema(length=period)
        elif indicator == 'Bollinger Bands':
            bb = stock_data.ta.bbands(length=period)
            ind_df[f'{indicator} Upper'] = bb[f'BBU_{period}_2.0']
            ind_df[f'{indicator} Middle'] = bb[f'BBM_{period}_2.0']
            ind_df[f'{indicator} Lower'] = bb[f'BBL_{period}_2.0']

            upper_line = chart.create_line(name=f'BB Upper', color='red', width=1, price_label=True)
            middle_line = chart.create_line(name=f'BB Middle', color='blue', width=1, price_label=True)
            lower_line = chart.create_line(name=f'BB Lower', color='green', width=1, price_label=True)

            upper_line.set(ind_df[['time', f'{indicator} Upper']].dropna())
            middle_line.set(ind_df[['time', f'{indicator} Middle']].dropna())
            lower_line.set(ind_df[['time', f'{indicator} Lower']].dropna())
            return  # Early return as we've set all lines
        elif indicator == 'Linear Regression':
            X = np.arange(len(stock_data)).reshape(-1, 1)
            y = stock_data.Close.values
            reg = LinearRegression().fit(X, y)
            ind_df[f'{indicator} {period}'] = reg.predict(X)

        ind_line.set(ind_df.dropna())


def add_subchart_indicator(chart, stock_data, indicator, period, ind_color):
    if indicator == 'RSI':
        rsi = stock_data.ta.rsi(length=period)
        subchart = chart.create_subchart(height_ratio=20)
        rsi_line = subchart.create_line(name=f'RSI {period}', color=ind_color, width=2, price_label=True)
        rsi_df = pd.DataFrame({'time': stock_data.Date, f'RSI {period}': rsi})
        rsi_line.set(rsi_df.dropna())
        subchart.y_axis.set_range(0, 100)
    elif indicator == 'MACD':
        macd = stock_data.ta.macd()
        subchart = chart.create_subchart(position='bottom', height=0.2, sync=True)
        macd_line = subchart.create_line(name='MACD', color=ind_color, width=2, price_label=True)
        signal_line = subchart.create_line(name='Signal', color='orange', width=1, price_label=True)
        histogram = subchart.create_histogram(name='Histogram', color='blue')

        macd_df = pd.DataFrame({
            'time': stock_data.Date,
            'MACD': macd['MACD_12_26_9'],
            'Signal': macd['MACDs_12_26_9'],
            'Histogram': macd['MACDh_12_26_9']
        })
        macd_line.set(macd_df[['time', 'MACD']].dropna())
        signal_line.set(macd_df[['time', 'Signal']].dropna())
        histogram.set(macd_df[['time', 'Histogram']].dropna())


def clear_indicators(chart):
    for line in chart.lines():
        line.delete()


def apply_patterns(chart):
    stock_data = get_bar_data(chart, fetch_data=True)

    # Simple support and resistance calculation (you may want to use a more sophisticated method)
    window = 20
    support = stock_data.Low.rolling(window=window, center=True).min()
    resistance = stock_data.High.rolling(window=window, center=True).max()

    support_line = chart.create_line(name='Support', color='green', width=1, price_label=True)
    resistance_line = chart.create_line(name='Resistance', color='red', width=1, price_label=True)

    support_df = pd.DataFrame({'time': stock_data.Date, 'Support': support})
    resistance_df = pd.DataFrame({'time': stock_data.Date, 'Resistance': resistance})

    support_line.set(support_df.dropna())
    resistance_line.set(resistance_df.dropna())


class DataReplay:
    def __init__(self, chart, data):
        self.chart = chart
        self.full_data = data
        self.current_index = len(data) - 1
        self.max_index = len(data) - 1
        self.is_playing = False
        self.play_speed = 1  # seconds between updates

    def set_date(self, date_str):
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d').date()
            self.current_index = self.full_data.index[self.full_data['Date'].dt.date <= date][-1]
            self.update_chart()
        except ValueError:
            print(f"Invalid date format: {date_str}. Please use YYYY-MM-DD format.")

    def play_pause(self):
        self.is_playing = not self.is_playing
        if self.is_playing:
            self.play()

    def play(self):
        if self.is_playing and self.current_index < len(self.full_data) - 1:
            self.current_index += 1
            self.update_chart()

    def stop(self):
        self.is_playing = False
        self.current_index = self.max_index
        self.update_chart()

    def next(self):
        if self.current_index < len(self.full_data) - 1:
            self.current_index += 1
            self.update_chart()

    def prev(self):
        if self.current_index > 0:
            self.current_index -= 1
            self.update_chart()

    def update_chart(self):
        hist_data = self.full_data.iloc[:self.current_index + 1]
        current_data = self.full_data.iloc[self.current_index + 1:]
        self.chart.set(hist_data, keep_drawings=True)

        if self.is_playing:
            for row_no, data in current_data.iterrows():
                self.chart.update(data)
                time.sleep(.2)
            self.is_playing = False


def toggle_theme(chart):
    current_theme = chart
    new_theme = 'Light' if current_theme == 'Dark' else 'Dark'
    chart.options['theme'] = new_theme
    chart.apply_options()


def setup_chart():
    chart = Chart(toolbox=True)
    chart.legend(True, color_based_on_candle=True)

    chart.events.search += on_search

    stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
    stock_list = stock_list_df['SYMBOL'].values.tolist()

    chart.topbar.menu('symbol', options=stock_list, default='TATAMOTORS', func=get_bar_data)
    chart.topbar.switcher('timeframe', ('D', 'W', 'M'), default='D', func=on_timeframe_selection)
    chart.topbar.menu(name='indicators', options=('EMA', 'RSI', 'MACD', 'Bollinger Bands', 'Linear Regression'),
                      default='EMA', func=lambda x: None)
    chart.topbar.menu(name='period', options=(6, 10, 20, 50, 200), default=20, func=lambda x: None)
    chart.topbar.menu(name='ind_color', options=('red', 'blue', 'yellow', 'white', 'green', 'cyan'), default='yellow',
                      func=lambda x: None)
    chart.topbar.button(name='apply_indicator', button_text='Apply Indicator', func=lambda x: add_indicator(x))
    chart.topbar.button(name='clear_indicator', button_text='Clear Indicator(s)', func=clear_indicators)
    chart.topbar.button(name='apply_patterns', button_text='Apply Patterns', func=apply_patterns)

    # Data Replay Controls
    chart.topbar.textbox('replay_date', initial_text=datetime.now().strftime('%Y-%m-%d'),
                         func=lambda x: data_replay.set_date(x.topbar['replay_date'].value))
    chart.topbar.button(name='prev', button_text='⏮️', func=lambda x: data_replay.prev())
    chart.topbar.button(name='play_pause', button_text='⏯️', func=lambda x: data_replay.play_pause())
    chart.topbar.button(name='next', button_text='⏭️', func=lambda x: data_replay.next())
    chart.topbar.button(name='stop', button_text='⏹️', func=lambda x: data_replay.stop())

    # Theme Toggle
    chart.topbar.button(name='toggle_theme', button_text='🌓', func=toggle_theme)

    df = get_bar_data(chart, fetch_data=True)
    chart.set(df, keep_drawings=True)
    chart.watermark(chart.topbar['symbol'].value)

    global data_replay
    data_replay = DataReplay(chart, df)

    return chart


def on_timeframe_selection(chart):
    get_bar_data(chart, need_drawings=True)
    clear_indicators(chart)


if __name__ == '__main__':
    chart = setup_chart()
    chart.show(block=True)