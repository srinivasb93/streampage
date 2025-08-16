import time
import pandas as pd
import numpy as np
from lightweight_charts import Chart
from common_utils import read_write_sql_data as rd
import pandas_ta as ta
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta


class StockData:
    def __init__(self, symbol, timeframe='D'):
        self.symbol = symbol
        self.timeframe = timeframe
        self.data = self._fetch_data()

    def _fetch_data(self):
        symbol = self.symbol
        if self.timeframe == 'W':
            symbol += '_W'
        elif self.timeframe == 'M':
            symbol += '_M'
        query = f"SELECT * FROM nsedata.public.{symbol} ORDER BY Date"
        return rd.get_table_data(query=query)

    def get_data_until(self, date):
        return self.data[self.data['Date'].dt.date <= date]


class Indicator:
    def __init__(self, name, period, color):
        self.name = name
        self.period = period
        self.color = color
        self.line = None

    def calculate(self, data):
        if self.name == 'EMA':
            return data.ta.ema(length=self.period)
        elif self.name == 'RSI':
            return data.ta.rsi(length=self.period)
        elif self.name == 'MACD':
            return data.ta.macd()
        elif self.name == 'Bollinger Bands':
            return data.ta.bbands(length=self.period)
        elif self.name == 'Linear Regression':
            return data.ta.linreg(length=self.period)
            # X = np.arange(len(data)).reshape(-1, 1)
            # y = data.Close.values
            # reg = LinearRegression().fit(X, y)
            # return pd.Series(reg.predict(X), index=data.index)

    def update(self, chart, data):
        calculated_data = self.calculate(data)
        if self.name in ['RSI', 'MACD']:
            self._update_subchart(chart, data, calculated_data)
        else:
            if self.line is None:
                self.line = chart.create_line(name=f'{self.name} {self.period}', color=self.color, width=2,
                                              price_label=True)

            if self.name == 'Bollinger Bands':
                self._update_bollinger_bands(chart, data, calculated_data)
            else:
                ind_df = pd.DataFrame({'time': data.Date, f'{self.name} {self.period}': calculated_data})
                self.line.set(ind_df.dropna())

    def _update_subchart(self, chart, data, calculated_data):
        if self.name == 'RSI':
            if not hasattr(self, 'rsi_chart'):
                self.rsi_chart = chart.create_subchart(height=.2, width=1, sync=True)
                chart.resize(height=.8, width=1)
                self.line = self.rsi_chart.create_line(name=f'RSI {self.period}', color=self.color, width=2,
                                                      price_label=True)
            rsi_df = pd.DataFrame({'time': data.Date, f'RSI {self.period}': calculated_data})
            self.line.set(rsi_df.dropna())
        elif self.name == 'MACD':
            if not hasattr(self, 'macd_chart'):
                self.macd_chart = chart.create_subchart(height=0.2, width=1, sync=True)
                chart.resize(height=.8, width=1)
                self.macd_line = self.macd_chart.create_line(name='MACD', color=self.color, width=2, price_label=True)
                self.signal_line = self.macd_chart.create_line(name='Signal', color='orange', width=1, price_label=True)
                self.histogram = self.macd_chart.create_histogram(name='Histogram', color='blue')

            macd_df = pd.DataFrame({
                'time': data.Date,
                'MACD': calculated_data['MACD_12_26_9'],
                'Signal': calculated_data['MACDs_12_26_9'],
                'Histogram': calculated_data['MACDh_12_26_9']
            })
            self.macd_line.set(macd_df[['time', 'MACD']].dropna())
            self.signal_line.set(macd_df[['time', 'Signal']].dropna())
            self.histogram.set(macd_df[['time', 'Histogram']].dropna())

    def _update_bollinger_bands(self, chart, data, calculated_data):
        if not hasattr(self, 'upper_line'):
            self.upper_line = chart.create_line(name=f'BB Upper', color='red', width=1.5, price_label=True)
            self.middle_line = chart.create_line(name=f'BB Middle', color='blue', width=1.5, price_label=True)
            self.lower_line = chart.create_line(name=f'BB Lower', color='green', width=1.5, price_label=True)

        ind_df = pd.DataFrame({
            'time': data.Date,
            'BB Upper': calculated_data[f'BBU_{self.period}_2.0'],
            'BB Middle': calculated_data[f'BBM_{self.period}_2.0'],
            'BB Lower': calculated_data[f'BBL_{self.period}_2.0']
        })
        self.upper_line.set(ind_df[['time', 'BB Upper']].dropna())
        self.middle_line.set(ind_df[['time', 'BB Middle']].dropna())
        self.lower_line.set(ind_df[['time', 'BB Lower']].dropna())

    def clear(self):
        if self.line:
            self.line.delete()
        if hasattr(self, 'rsi_chart'):
            self.rsi_chart.resize(width=0, height=0)
        if hasattr(self, 'macd_chart'):
            self.macd_chart.resize(width=0, height=0)
        if hasattr(self, 'upper_line'):
            self.upper_line.delete()
            self.middle_line.delete()
            self.lower_line.delete()


class ChartManager:
    def __init__(self):
        self.chart = Chart(toolbox=True)
        self.stock_data = None
        self.indicators = []
        self.data_replay = None
        self.setup_chart()

    def setup_chart(self):
        self.chart.legend(True, color_based_on_candle=True)
        self.chart.events.search += self.on_search

        stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
        stock_list = stock_list_df['SYMBOL'].values.tolist()

        self.chart.topbar.menu('symbol', options=stock_list, default='TATAMOTORS', func=self.on_symbol_change)
        self.chart.topbar.switcher('timeframe', ('D', 'W', 'M'), default='D', func=self.on_timeframe_change)
        self.chart.topbar.menu(name='indicators',
                               options=('EMA', 'RSI', 'MACD', 'Bollinger Bands', 'Linear Regression'),
                               default='EMA', func=lambda x: None)
        self.chart.topbar.menu(name='period', options=(6, 10, 20, 50, 200), default=20, func=lambda x: None)
        self.chart.topbar.menu(name='ind_color', options=('red', 'blue', 'yellow', 'white', 'green', 'cyan'),
                               default='yellow',
                               func=lambda x: None)
        self.chart.topbar.button(name='apply_indicator', button_text='Apply Indicator', func=self.add_indicator)
        self.chart.topbar.button(name='clear_indicator', button_text='Clear Indicator(s)', func=self.clear_indicators)
        self.chart.topbar.button(name='apply_patterns', button_text='Apply Patterns', func=self.apply_patterns)

        # Data Replay Controls
        self.chart.topbar.textbox('replay_date', initial_text=datetime.now().strftime('%Y-%m-%d'),
                                  func=lambda x: self.set_replay_date(x.topbar['replay_date'].value))
        self.chart.topbar.button(name='prev', button_text='⏮️', func=lambda x: self.data_replay.prev())
        self.chart.topbar.button(name='play_pause', button_text='⏯️', func=lambda x: self.data_replay.play_pause())
        self.chart.topbar.button(name='next', button_text='⏭️', func=lambda x: self.data_replay.next())
        self.chart.topbar.button(name='stop', button_text='⏹️', func=lambda x: self.data_replay.stop())

        # Theme Toggle
        self.chart.topbar.button(name='toggle_theme', button_text='🌓', func=self.toggle_theme)

        self.on_symbol_change(self.chart)

    def on_search(self, chart, searched_string):
        self.on_symbol_change(chart, searched_string)

    def on_symbol_change(self, chart, symbol=None):
        if symbol is None:
            symbol = chart.topbar['symbol'].value
        timeframe = chart.topbar['timeframe'].value
        self.stock_data = StockData(symbol, timeframe)
        self.update_chart()
        self.clear_indicators()
        chart.watermark(symbol)

    def on_timeframe_change(self, chart):
        self.on_symbol_change(chart)

    def update_chart(self, data=None):
        if data is None:
            data = self.stock_data.data
        self.chart.set(data, keep_drawings=True)
        for indicator in self.indicators:
            indicator.update(self.chart, data)

    def add_indicator(self, chart):
        indicator = chart.topbar['indicators'].value
        period = int(chart.topbar['period'].value)
        ind_color = chart.topbar['ind_color'].value
        new_indicator = Indicator(indicator, period, ind_color)
        self.indicators.append(new_indicator)
        new_indicator.update(self.chart, self.stock_data.data)

    def clear_indicators(self, chart=None):
        for indicator in self.indicators:
            indicator.clear()
        self.indicators = []

    def apply_patterns(self, chart):
        # Implementation remains the same
        pass

    def set_replay_date(self, date_str):
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d').date()
            data_until_date = self.stock_data.get_data_until(date)
            self.update_chart(data_until_date)
            if self.data_replay is None:
                self.data_replay = DataReplay(self)
            self.data_replay.set_date(date)
        except ValueError:
            print(f"Invalid date format: {date_str}. Please use YYYY-MM-DD format.")

    def toggle_theme(self, chart):
        current_theme = chart.options['theme']
        new_theme = 'Light' if current_theme == 'Dark' else 'Dark'
        chart.options['theme'] = new_theme
        chart.apply_options()


class DataReplay:
    def __init__(self, chart_manager):
        self.chart_manager = chart_manager
        self.current_index = len(chart_manager.stock_data.data) - 1
        self.max_index = len(chart_manager.stock_data.data) - 1
        self.is_playing = False
        self.play_speed = .1  # seconds between updates

    def set_date(self, date):
        self.current_index = \
        self.chart_manager.stock_data.data.index[self.chart_manager.stock_data.data['Date'].dt.date <= date][-1]
        self.update_chart()

    def play_pause(self):
        if self.is_playing:
            self.is_playing = False  # Stop existing loop
        else:
            self.is_playing = True
            self.play()

    def play(self):
        while self.is_playing and self.current_index < self.max_index:
            self.next()
            time.sleep(self.play_speed)

    def stop(self):
        self.is_playing = False
        self.current_index = self.max_index
        self.update_chart()

    def next(self):
        if self.current_index < self.max_index:
            self.current_index += 1
            self.update_chart()

    def prev(self):
        if self.current_index > 0:
            self.current_index -= 1
            self.update_chart()

    def update_chart(self):
        current_data = self.chart_manager.stock_data.data.iloc[:self.current_index + 1]
        self.chart_manager.update_chart(current_data)


def main():
    chart_manager = ChartManager()
    chart_manager.chart.show(block=True)


if __name__ == '__main__':
    main()