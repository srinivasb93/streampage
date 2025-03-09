import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime


def calculate_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """Calculate Average True Range (ATR) indicator."""
    high = df['High']
    low = df['Low']
    close = df['Close']

    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=window).mean()

    return atr


def calculate_technical_indicators(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Calculate various technical indicators for breakout confirmation.

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with OHLCV data
    window : int
        Lookback period for calculations

    Returns:
    --------
    pandas.DataFrame
        DataFrame with additional technical indicator columns
    """
    df = df.copy()

    # 1. Bollinger Bands
    df['SMA'] = df['Close'].rolling(window=window).mean()
    rolling_std = df['Close'].rolling(window=window).std()
    df['BB_Upper'] = df['SMA'] + (rolling_std * 2)
    df['BB_Lower'] = df['SMA'] - (rolling_std * 2)
    df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['SMA']

    # 2. RSI (Relative Strength Index)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # 3. Keltner Channels
    df['EMA'] = df['Close'].ewm(span=window).mean()
    df['ATR'] = calculate_atr(df, window)
    df['KC_Upper'] = df['EMA'] + (df['ATR'] * 2)
    df['KC_Lower'] = df['EMA'] - (df['ATR'] * 2)
    df['KC_Width'] = (df['KC_Upper'] - df['KC_Lower']) / df['EMA']

    # 4. Volume indicators
    df['Volume_SMA'] = df['Volume'].rolling(window=window).mean()
    df['Volume_Ratio'] = df['Volume'] / df['Volume_SMA']
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).cumsum()

    # 5. MACD (Moving Average Convergence Divergence)
    exp1 = df['Close'].ewm(span=12, adjust=False).mean()
    exp2 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = exp1 - exp2
    df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']

    return df


def detect_consolidation_breakout(
        df: pd.DataFrame,
        window: int = 20,
        band_threshold: float = 0.05,
        breakout_threshold: float = 0.02
) -> pd.DataFrame:
    """
    Detect periods of price consolidation and subsequent breakouts with enhanced confirmation.

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with columns 'Date', 'Open', 'High', 'Low', 'Close', 'Volume'
    window : int
        Number of periods to look back for consolidation (default: 20)
    band_threshold : float
        Maximum allowed range as a percentage for consolidation (default: 5%)
    breakout_threshold : float
        Minimum movement required for breakout confirmation (default: 2%)

    Returns:
    --------
    pandas.DataFrame
        Original DataFrame with additional columns for analysis
    """

    def is_consolidating(data: pd.DataFrame, idx: int) -> bool:
        """
        Enhanced consolidation detection using multiple indicators.
        """
        # Price-based consolidation
        high_low_range = (data['High'].iloc[idx - window:idx].max() -
                          data['Low'].iloc[idx - window:idx].min()) / data['Close'].iloc[idx]
        price_consolidation = high_low_range <= band_threshold

        # Bollinger Band squeeze
        bb_squeeze = data['BB_Width'].iloc[idx] < data['BB_Width'].iloc[idx - window:idx].mean()

        # Keltner Channel squeeze
        kc_squeeze = data['KC_Width'].iloc[idx] < data['KC_Width'].iloc[idx - window:idx].mean()

        # Volume decline (typical in consolidation)
        volume_decline = data['Volume_Ratio'].iloc[idx] < 1.0

        # RSI range-bound (between 45 and 55 indicates consolidation)
        rsi_consolidated = 45 <= data['RSI'].iloc[idx] <= 55

        # Require majority of conditions to be true
        conditions_met = sum([price_consolidation, bb_squeeze, kc_squeeze,
                              volume_decline, rsi_consolidated])
        return conditions_met >= 3

    def get_breakout_direction(data: pd.DataFrame, idx: int) -> Optional[str]:
        """
        Enhanced breakout detection using multiple confirmation signals.
        """
        current_close = data['Close'].iloc[idx]
        recent_high = data['High'].iloc[idx - window:idx].max()
        recent_low = data['Low'].iloc[idx - window:idx].min()

        # Price-based breakout thresholds
        upward_threshold = recent_high * (1 + breakout_threshold)
        downward_threshold = recent_low * (1 - breakout_threshold)

        # Initialize breakout signals
        upward_signals = 0
        downward_signals = 0

        # 1. Price breakout
        if current_close > upward_threshold:
            upward_signals += 1
        elif current_close < downward_threshold:
            downward_signals += 1

        # 2. Volume confirmation
        if data['Volume_Ratio'].iloc[idx] > 1.5:  # 50% above average volume
            if current_close > data['Close'].iloc[idx - 1]:
                upward_signals += 1
            else:
                downward_signals += 1

        # 3. RSI confirmation
        if data['RSI'].iloc[idx] > 70:
            upward_signals += 1
        elif data['RSI'].iloc[idx] < 30:
            downward_signals += 1

        # 4. MACD confirmation
        if data['MACD_Hist'].iloc[idx] > 0 and data['MACD_Hist'].iloc[idx] > data['MACD_Hist'].iloc[idx - 1]:
            upward_signals += 1
        elif data['MACD_Hist'].iloc[idx] < 0 and data['MACD_Hist'].iloc[idx] < data['MACD_Hist'].iloc[idx - 1]:
            downward_signals += 1

        # 5. Bollinger Band confirmation
        if current_close > data['BB_Upper'].iloc[idx]:
            upward_signals += 1
        elif current_close < data['BB_Lower'].iloc[idx]:
            downward_signals += 1

        # Require at least 3 confirmation signals for a breakout
        if upward_signals >= 3:
            return 'upward'
        elif downward_signals >= 3:
            return 'downward'
        return None

    # Calculate technical indicators
    result_df = calculate_technical_indicators(df, window)

    # Initialize analysis columns
    result_df['is_consolidating'] = False
    result_df['breakout_direction'] = None
    result_df['consolidation_high'] = np.nan
    result_df['consolidation_low'] = np.nan
    result_df['breakout_strength'] = 0  # New column for breakout strength

    # Analyze each period
    for i in range(window, len(df)):
        # Check for consolidation
        if is_consolidating(result_df, i):
            result_df.iloc[i, result_df.columns.get_loc('is_consolidating')] = True
            result_df.iloc[i, result_df.columns.get_loc('consolidation_high')] = \
                df['High'].iloc[i - window:i].max()
            result_df.iloc[i, result_df.columns.get_loc('consolidation_low')] = \
                df['Low'].iloc[i - window:i].min()

            # Check for breakout
            breakout = get_breakout_direction(result_df, i)
            if breakout:
                result_df.iloc[i, result_df.columns.get_loc('breakout_direction')] = breakout

                # Calculate breakout strength score (0-100)
                volume_score = min(100, (result_df['Volume_Ratio'].iloc[i] - 1) * 50)
                price_score = min(100, abs(result_df['Close'].iloc[i] /
                                           result_df['Close'].iloc[i - 1] - 1) * 1000)
                rsi_score = min(100, abs(result_df['RSI'].iloc[i] - 50))

                strength_score = (volume_score + price_score + rsi_score) / 3
                result_df.iloc[i, result_df.columns.get_loc('breakout_strength')] = strength_score

    return result_df


@dataclass
class TradeSignal:
    """Class for storing trade signal information"""
    date: datetime
    type: str  # 'entry' or 'exit'
    direction: str  # 'long' or 'short'
    price: float
    strength: float
    stop_loss: float
    take_profit: float


@dataclass
class Trade:
    """Class for storing trade information"""
    entry_date: datetime
    exit_date: datetime
    entry_price: float
    exit_price: float
    direction: str
    profit_loss: float
    profit_loss_pct: float
    trade_duration: int
    exit_reason: str


class BreakoutStrategy:
    def __init__(
            self,
            window: int = 20,
            band_threshold: float = 0.05,
            breakout_threshold: float = 0.02,
            stop_loss_pct: float = 0.02,
            take_profit_pct: float = 0.04,
            min_strength_score: float = 70,
            position_size_pct: float = 0.02  # 2% of portfolio per trade
    ):
        self.window = window
        self.band_threshold = band_threshold
        self.breakout_threshold = breakout_threshold
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.min_strength_score = min_strength_score
        self.position_size_pct = position_size_pct

    def generate_signals(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[TradeSignal]]:
        """
        Generate entry and exit signals based on breakout analysis and technical indicators.
        """
        # Get base analysis
        result_df = detect_consolidation_breakout(
            df,
            self.window,
            self.band_threshold,
            self.breakout_threshold
        )

        signals: List[TradeSignal] = []

        for i in range(self.window, len(result_df)):
            row = result_df.iloc[i]

            # Skip if we're not in a breakout or strength is too low
            if pd.isna(row['breakout_direction']) or row['breakout_strength'] < self.min_strength_score:
                continue

            # Additional confirmation checks
            volume_confirmed = row['Volume_Ratio'] > 1.5
            rsi_confirmed = (row['RSI'] > 70) if row['breakout_direction'] == 'upward' else (row['RSI'] < 30)
            macd_confirmed = (row['MACD_Hist'] > 0) if row['breakout_direction'] == 'upward' else (row['MACD_Hist'] < 0)

            if volume_confirmed and rsi_confirmed and macd_confirmed:
                # Calculate stop loss and take profit levels
                entry_price = row['Close']
                if row['breakout_direction'] == 'upward':
                    stop_loss = entry_price * (1 - self.stop_loss_pct)
                    take_profit = entry_price * (1 + self.take_profit_pct)
                    direction = 'long'
                else:
                    stop_loss = entry_price * (1 + self.stop_loss_pct)
                    take_profit = entry_price * (1 - self.take_profit_pct)
                    direction = 'short'

                signal = TradeSignal(
                    date=row.name,
                    type='entry',
                    direction=direction,
                    price=entry_price,
                    strength=row['breakout_strength'],
                    stop_loss=stop_loss,
                    take_profit=take_profit
                )
                signals.append(signal)

        return result_df, signals


class BreakoutBacktester:
    def __init__(
            self,
            strategy: BreakoutStrategy,
            initial_capital: float = 100000,
            commission_pct: float = 0.001  # 0.1% commission per trade
    ):
        self.strategy = strategy
        self.initial_capital = initial_capital
        self.commission_pct = commission_pct
        self.trades: List[Trade] = []
        self.equity_curve: List[float] = []

    def run_backtest(self, df: pd.DataFrame) -> Dict:
        """
        Run backtest on historical data and return performance metrics.
        """
        # Generate signals
        analysis_df, signals = self.strategy.generate_signals(df)

        current_capital = self.initial_capital
        open_position = None

        for i in range(len(signals)):
            signal = signals[i]

            if signal.type == 'entry' and not open_position:
                # Calculate position size
                position_size = current_capital * self.strategy.position_size_pct
                shares = position_size / signal.price

                # Track open position
                open_position = {
                    'entry_date': signal.date,
                    'entry_price': signal.price,
                    'direction': signal.direction,
                    'shares': shares,
                    'stop_loss': signal.stop_loss,
                    'take_profit': signal.take_profit
                }

                # Deduct commission
                current_capital -= position_size * self.commission_pct

            elif open_position:
                # Check for exit conditions
                exit_price = signal.price
                exit_reason = 'signal'

                # Calculate P&L
                if open_position['direction'] == 'long':
                    profit_loss = (exit_price - open_position['entry_price']) * open_position['shares']
                else:
                    profit_loss = (open_position['entry_price'] - exit_price) * open_position['shares']

                # Add commission
                profit_loss -= (open_position['shares'] * exit_price * self.commission_pct)

                # Update capital
                current_capital += profit_loss

                # Record trade
                trade = Trade(
                    entry_date=open_position['entry_date'],
                    exit_date=signal.date,
                    entry_price=open_position['entry_price'],
                    exit_price=exit_price,
                    direction=open_position['direction'],
                    profit_loss=profit_loss,
                    profit_loss_pct=(profit_loss / self.initial_capital) * 100,
                    trade_duration=(signal.date - open_position['entry_date']).days,
                    exit_reason=exit_reason
                )
                self.trades.append(trade)

                # Reset position
                open_position = None

            self.equity_curve.append(current_capital)

        return self._calculate_performance_metrics()

    def _calculate_performance_metrics(self) -> Dict:
        """
        Calculate various performance metrics from backtest results.
        """
        if not self.trades:
            return {}

        profits = [t.profit_loss for t in self.trades]
        win_trades = [t for t in self.trades if t.profit_loss > 0]
        loss_trades = [t for t in self.trades if t.profit_loss <= 0]

        equity_series = pd.Series(self.equity_curve)
        returns = equity_series.pct_change().dropna()

        metrics = {
            'total_trades': len(self.trades),
            'winning_trades': len(win_trades),
            'losing_trades': len(loss_trades),
            'win_rate': len(win_trades) / len(self.trades) if self.trades else 0,
            'avg_profit_per_trade': np.mean(profits),
            'max_drawdown': self._calculate_max_drawdown(equity_series),
            'sharpe_ratio': self._calculate_sharpe_ratio(returns),
            'profit_factor': abs(sum(t.profit_loss for t in win_trades) /
                                 sum(t.profit_loss for t in loss_trades)) if loss_trades else float('inf'),
            'total_return': (self.equity_curve[-1] - self.initial_capital) / self.initial_capital * 100,
            'avg_trade_duration': np.mean([t.trade_duration for t in self.trades])
        }

        return metrics

    def _calculate_max_drawdown(self, equity_series: pd.Series) -> float:
        """Calculate maximum drawdown from peak."""
        rolling_max = equity_series.expanding().max()
        drawdowns = (equity_series - rolling_max) / rolling_max
        return abs(drawdowns.min()) * 100

    def _calculate_sharpe_ratio(self, returns: pd.Series) -> float:
        """Calculate Sharpe ratio (assuming risk-free rate of 0)."""
        if len(returns) < 2:
            return 0
        return np.sqrt(252) * (returns.mean() / returns.std())


def run_strategy_optimization(
        df: pd.DataFrame,
        window_range: range = range(10, 31, 5),
        band_threshold_range: List[float] = [0.03, 0.05, 0.07],
        breakout_threshold_range: List[float] = [0.01, 0.02, 0.03],
        stop_loss_range: List[float] = [0.01, 0.02, 0.03],
        take_profit_range: List[float] = [0.02, 0.04, 0.06]
) -> Dict:
    """
    Optimize strategy parameters using grid search.
    """
    best_metrics = None
    best_params = None
    best_sharpe = float('-inf')

    for window in window_range:
        for band_threshold in band_threshold_range:
            for breakout_threshold in breakout_threshold_range:
                for stop_loss in stop_loss_range:
                    for take_profit in take_profit_range:
                        # Create and run strategy with current parameters
                        strategy = BreakoutStrategy(
                            window=window,
                            band_threshold=band_threshold,
                            breakout_threshold=breakout_threshold,
                            stop_loss_pct=stop_loss,
                            take_profit_pct=take_profit
                        )

                        backtester = BreakoutBacktester(strategy)
                        metrics = backtester.run_backtest(df)

                        if metrics['sharpe_ratio'] > best_sharpe:
                            best_sharpe = metrics['sharpe_ratio']
                            best_metrics = metrics
                            best_params = {
                                'window': window,
                                'band_threshold': band_threshold,
                                'breakout_threshold': breakout_threshold,
                                'stop_loss_pct': stop_loss,
                                'take_profit_pct': take_profit
                            }

    return {
        'best_params': best_params,
        'best_metrics': best_metrics
    }


# Example usage
def analyze_and_backtest(symbol: str, data: pd.DataFrame):
    """
    Run complete analysis including optimization and backtesting.
    """
    print(f"\nRunning analysis for {symbol}")
    print("-" * 50)

    # First, optimize strategy parameters
    print("Optimizing strategy parameters...")
    optimization_results = run_strategy_optimization(data)

    print("\nBest Parameters Found:")
    for param, value in optimization_results['best_params'].items():
        print(f"{param}: {value}")

    # Run backtest with optimized parameters
    print("\nRunning backtest with optimized parameters...")
    strategy = BreakoutStrategy(**optimization_results['best_params'])
    backtester = BreakoutBacktester(strategy)
    metrics = backtester.run_backtest(data)

    print("\nBacktest Results:")
    print("-" * 50)
    print(f"Total Return: {metrics['total_return']:.2f}%")
    print(f"Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
    print(f"Max Drawdown: {metrics['max_drawdown']:.2f}%")
    print(f"Win Rate: {metrics['win_rate'] * 100:.2f}%")
    print(f"Profit Factor: {metrics['profit_factor']:.2f}")
    print(f"Total Trades: {metrics['total_trades']}")
    print(f"Average Trade Duration: {metrics['avg_trade_duration']:.1f} days")

    return {
        'optimization_results': optimization_results,
        'backtest_metrics': metrics,
        'trades': backtester.trades,
        'equity_curve': backtester.equity_curve
    }


if __name__ == '__main__':
    from common_utils import read_write_sql_data as rd

    data = rd.get_table_data(selected_table='TATAMOTORS', sort=True)

    # Run complete analysis with optimization and backtesting
    results = analyze_and_backtest('TATAMOTORS', data)

    # Access specific components
    optimization_results = results['optimization_results']
    backtest_metrics = results['backtest_metrics']
    trades = results['trades']
    equity_curve = results['equity_curve']

    # Create custom strategy instance with specific parameters
    custom_strategy = BreakoutStrategy(
        window=20,
        band_threshold=0.05,
        breakout_threshold=0.02,
        stop_loss_pct=0.02,
        take_profit_pct=0.04,
        min_strength_score=70,
        position_size_pct=0.02
    )

    # Run backtest with custom parameters
    custom_backtester = BreakoutBacktester(
        strategy=custom_strategy,
        initial_capital=100000,
        commission_pct=0.001
    )
    custom_metrics = custom_backtester.run_backtest(df=data)