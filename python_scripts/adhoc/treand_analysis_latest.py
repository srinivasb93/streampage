import pandas as pd
import pandas_ta as ta
import numpy as np
import backtrader as bt
from datetime import datetime, timedelta
import yfinance as yf


class NarrowBandStrategy:
    def __init__(self, lookback_period=20, band_threshold=0.02):
        """
        Initialize strategy parameters

        Parameters:
        lookback_period (int): Period for calculating the narrow band
        band_threshold (float): Maximum allowed price variation within the band (as percentage)
        """
        self.lookback_period = lookback_period
        self.band_threshold = band_threshold

    def identify_narrow_band(self, data):
        """
        Identify periods where price is trading within a narrow band
        """
        # Calculate rolling high and low
        rolling_high = data['High'].rolling(window=self.lookback_period).max()
        rolling_low = data['Low'].rolling(window=self.lookback_period).min()

        # Calculate band width as percentage
        band_width = (rolling_high - rolling_low) / rolling_low

        # Identify narrow band periods
        is_narrow_band = band_width <= self.band_threshold

        return is_narrow_band

    def add_technical_indicators(self, data):
        """
        Add technical indicators for trade confirmation
        """
        # Add RSI
        data['RSI'] = ta.rsi(data['Close'], length=14)

        # Add MACD
        macd = ta.macd(data['Close'])
        data['MACD'] = macd['MACD_12_26_9']
        data['MACD_Signal'] = macd['MACDs_12_26_9']

        # Add Bollinger Bands
        bb = ta.bbands(data['Close'])
        data['BB_Upper'] = bb['BBU_20_2.0']
        data['BB_Middle'] = bb['BBM_20_2.0']
        data['BB_Lower'] = bb['BBL_20_2.0']

        # Add Average True Range (ATR)
        data['ATR'] = ta.atr(data['High'], data['Low'], data['Close'])

        return data

    def generate_signals(self, data):
        """
        Generate entry and exit signals
        """
        # Initialize signal columns
        data['Signal'] = 0

        # Identify narrow band periods
        data['Is_Narrow_Band'] = self.identify_narrow_band(data)

        # Calculate price breakouts
        data['Upper_Band'] = data['High'].rolling(window=self.lookback_period).max()
        data['Lower_Band'] = data['Low'].rolling(window=self.lookback_period).min()

        # Generate entry signals
        for i in range(self.lookback_period, len(data)):
            if data['Is_Narrow_Band'].iloc[i - 1]:
                # Bullish breakout
                if (data['Close'].iloc[i] > data['Upper_Band'].iloc[i - 1] and
                        data['RSI'].iloc[i] > 50 and
                        data['MACD'].iloc[i] > data['MACD_Signal'].iloc[i]):
                    data.loc[data.index[i], 'Signal'] = 1

                # Bearish breakout
                elif (data['Close'].iloc[i] < data['Lower_Band'].iloc[i - 1] and
                      data['RSI'].iloc[i] < 50 and
                      data['MACD'].iloc[i] < data['MACD_Signal'].iloc[i]):
                    data.loc[data.index[i], 'Signal'] = -1

        return data


class NarrowBandStrategyBT(bt.Strategy):
    params = (
        ('lookback_period', 20),
        ('band_threshold', 0.02),
    )

    def __init__(self):
        # Initialize indicators
        self.rsi = bt.indicators.RSI(self.data.close, period=14)
        self.macd = bt.indicators.MACD(self.data.close)
        self.bbands = bt.indicators.BollingerBands(self.data.close)

        # Track rolling high and low
        self.rolling_high = bt.indicators.Highest(self.data.high, period=self.params.lookback_period)
        self.rolling_low = bt.indicators.Lowest(self.data.low, period=self.params.lookback_period)

    def next(self):
        # Calculate band width
        band_width = (self.rolling_high[0] - self.rolling_low[0]) / self.rolling_low[0]
        is_narrow_band = band_width <= self.params.band_threshold

        if not self.position and is_narrow_band:
            # Bullish breakout
            if (self.data.close[0] > self.rolling_high[-1] and
                    self.rsi[0] > 50 and
                    self.macd.macd[0] > self.macd.signal[0]):
                self.buy()

            # Bearish breakout
            elif (self.data.close[0] < self.rolling_low[-1] and
                  self.rsi[0] < 50 and
                  self.macd.macd[0] < self.macd.signal[0]):
                self.sell()

        # Exit rules
        elif self.position:
            # Exit long position
            if self.position.size > 0:
                if (self.data.close[0] < self.bbands.lines.mid[0] or
                        self.rsi[0] > 70):
                    self.close()

            # Exit short position
            elif self.position.size < 0:
                if (self.data.close[0] > self.bbands.lines.mid[0] or
                        self.rsi[0] < 30):
                    self.close()


def run_backtest(symbol, start_date, end_date):
    """
    Run backtest for the strategy
    """
    # Create Backtrader cerebro
    cerebro = bt.Cerebro()

    # Add strategy
    cerebro.addstrategy(NarrowBandStrategyBT)

    # Download data using yfinance
    data = yf.download(symbol, start=start_date, end=end_date)

    # Create Backtrader data feed
    feed = bt.feeds.PandasData(dataname=data)
    cerebro.adddata(feed)

    # Set initial capital
    cerebro.broker.setcash(100000.0)

    # Add analyzers
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')

    # Run backtest
    results = cerebro.run()

    # Get analysis results
    strat = results[0]

    # Print results
    print(f"Final Portfolio Value: ${cerebro.broker.getvalue():,.2f}")
    # print(f"Sharpe Ratio: {strat.analyzers.sharpe.get_analysis()['sharperatio']:.2f}")
    # print(f"Max Drawdown: {strat.analyzers.drawdown.get_analysis()['max']['drawdown']:.2f}%")
    print(f"Total Return: {strat.analyzers.returns.get_analysis()['rtot']:.2f}%")

    # Plot results
    cerebro.plot()


# Example usage
if __name__ == "__main__":
    # Initialize strategy
    strategy = NarrowBandStrategy(lookback_period=20, band_threshold=0.02)

    # Download sample data
    symbol = "SBIN.NS"
    start_date = "2023-01-01"
    end_date = "2024-01-01"

    # Run backtest
    run_backtest(symbol, start_date, end_date)