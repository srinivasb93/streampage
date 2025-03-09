import pandas as pd
import numpy as np
from typing import Union, List, Tuple
import pandas_ta as ta
from datetime import datetime
import matplotlib.pyplot as plt


class LinearRegressionIndicator:
    def __init__(
            self,
            length: int = 100,
            deviation: float = 2.0,
            offset: int = 0,
            smoothing: int = 1,
            show_last: bool = True
    ):
        """
        Initialize the Linear Regression ++ [Dev Lucem] indicator

        Parameters:
        -----------
        length : int
            The lookback period for calculations
        deviation : float
            The number of standard deviations for the channels
        offset : int
            The offset for the regression calculation
        smoothing : int
            The smoothing period for the SMA
        show_last : bool
            Whether to show only the last signals
        """
        self.length = length
        self.deviation = deviation
        self.offset = offset
        self.smoothing = smoothing
        self.show_last = show_last

    def calculate_linreg(self, data: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Calculate linear regression and deviation channels

        Parameters:
        -----------
        data : pd.Series
            Price series (typically close prices)

        Returns:
        --------
        Tuple containing:
            - Linear regression line
            - Upper deviation channel
            - Lower deviation channel
        """
        # Calculate linear regression
        y = data.values
        x = np.arange(len(y))

        # Calculate slope and intercept
        slope, intercept = np.polyfit(x, y, 1)

        # Calculate regression line
        linreg = pd.Series(slope * x + intercept, index=data.index)

        # Calculate standard deviation
        deviation = np.sqrt(np.sum((y - (slope * x + intercept)) ** 2) / len(y))

        # Calculate channels
        upper_channel = linreg + (deviation * self.deviation)
        lower_channel = linreg - (deviation * self.deviation)

        # Apply smoothing if needed
        if self.smoothing > 1:
            linreg = linreg.rolling(window=self.smoothing).mean()
            upper_channel = upper_channel.rolling(window=self.smoothing).mean()
            lower_channel = lower_channel.rolling(window=self.smoothing).mean()

        return linreg, upper_channel, lower_channel

    def generate_signals(
            self,
            data: pd.Series,
            upper_channel: pd.Series,
            lower_channel: pd.Series
    ) -> Tuple[pd.Series, pd.Series]:
        """
        Generate buy and sell signals

        Parameters:
        -----------
        data : pd.Series
            Price series
        upper_channel : pd.Series
            Upper deviation channel
        lower_channel : pd.Series
            Lower deviation channel

        Returns:
        --------
        Tuple containing:
            - Buy signals series
            - Sell signals series
        """
        # Initialize signal series
        buy_signals = pd.Series(False, index=data.index)
        sell_signals = pd.Series(False, index=data.index)

        # Generate signals
        for i in range(1, len(data)):
            # Buy signal: price crosses below lower channel
            if (data.iloc[i - 1] >= lower_channel.iloc[i - 1] and
                    data.iloc[i] < lower_channel.iloc[i]):
                buy_signals.iloc[i] = True

            # Sell signal: price crosses above upper channel
            if (data.iloc[i - 1] <= upper_channel.iloc[i - 1] and
                    data.iloc[i] > upper_channel.iloc[i]):
                sell_signals.iloc[i] = True

        return buy_signals, sell_signals

    def plot_indicator(
            self,
            df: pd.DataFrame,
            price_col: str = 'close',
            up_color: str = 'lime',
            down_color: str = 'red',
            line_color: str = 'aqua'
    ) -> None:
        """
        Plot the indicator with matplotlib

        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame containing price data
        price_col : str
            Column name for price data
        up_color : str
            Color for bullish elements
        down_color : str
            Color for bearish elements
        line_color : str
            Color for regression line
        """
        # Calculate indicator values
        linreg, upper, lower = self.calculate_linreg(df[price_col])
        buy_signals, sell_signals = self.generate_signals(df[price_col], upper, lower)

        # Create plot
        plt.figure(figsize=(15, 7))

        # Plot price and regression lines
        plt.plot(df.index, df[price_col], label='Price', color='gray', alpha=0.6)
        plt.plot(df.index, linreg, label='Regression', color=line_color)
        plt.plot(df.index, upper, label='Upper Channel', color=down_color)
        plt.plot(df.index, lower, label='Lower Channel', color=up_color)

        # Plot signals
        buy_points = df[price_col][buy_signals]
        sell_points = df[price_col][sell_signals]

        plt.scatter(buy_points.index, buy_points, color=up_color, marker='^',
                    label='Buy Signal', s=100)
        plt.scatter(sell_points.index, sell_points, color=down_color, marker='v',
                    label='Sell Signal', s=100)

        plt.title('Linear Regression ++ [Dev Lucem]')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.show()

    def apply(self, df: pd.DataFrame, price_col: str = 'close') -> pd.DataFrame:
        """
        Apply the indicator to a DataFrame

        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame containing price data
        price_col : str
            Column name for price data

        Returns:
        --------
        pd.DataFrame with added indicator columns
        """
        result_df = df.copy()

        # Calculate main indicator values
        linreg, upper, lower = self.calculate_linreg(df[price_col])
        buy_signals, sell_signals = self.generate_signals(df[price_col], upper, lower)

        # Add columns to DataFrame
        result_df['linreg'] = linreg
        result_df['upper_channel'] = upper
        result_df['lower_channel'] = lower
        result_df['buy_signal'] = buy_signals
        result_df['sell_signal'] = sell_signals

        return result_df


# Example usage:

# Create indicator instance
indicator = LinearRegressionIndicator(
    length=100,
    deviation=2.0,
    offset=0,
    smoothing=1,
    show_last=True
)

# Assuming you have a DataFrame 'df' with OHLCV data:
from common_utils import read_write_sql_data as rd
df = rd.get_table_data(selected_table='UNIONBANK', sort=True)
df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']

# Apply indicator
results = indicator.apply(df)

# Plot indicator
indicator.plot_indicator(df)
