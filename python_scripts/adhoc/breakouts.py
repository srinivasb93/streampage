import pandas as pd
import numpy as np
from itertools import product
from typing import List, Dict, Tuple, Optional
import warnings

warnings.filterwarnings('ignore')


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

def optimize_parameters(df: pd.DataFrame) -> Tuple[Dict, pd.DataFrame]:
    """
    Test different parameter combinations to find optimal values that maximize breakout strength.

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with OHLCV data

    Returns:
    --------
    tuple
        Best parameters and DataFrame with all results
    """
    # Define parameter ranges to test
    windows = [10, 15, 20, 25, 30]
    band_thresholds = [0.03, 0.05, 0.07, 0.09]
    breakout_thresholds = [0.01, 0.02, 0.03, 0.04]

    results = []

    # Test all combinations
    for window, band_thresh, break_thresh in product(windows, band_thresholds, breakout_thresholds):
        try:
            # Run detection with current parameters
            result_df = detect_consolidation_breakout(
                df,
                window=window,
                band_threshold=band_thresh,
                breakout_threshold=break_thresh
            )

            # Calculate metrics
            avg_strength = result_df['breakout_strength'].mean()
            max_strength = result_df['breakout_strength'].max()
            breakout_count = (result_df['breakout_direction'].notna()).sum()

            # Store results
            results.append({
                'window': window,
                'band_threshold': band_thresh,
                'breakout_threshold': break_thresh,
                'avg_strength': avg_strength,
                'max_strength': max_strength,
                'breakout_count': breakout_count,
                'score': avg_strength * 0.3 + max_strength * 0.4 + (breakout_count / len(df)) * 0.3
            })

        except Exception as e:
            print(f"Error with parameters: window={window}, band={band_thresh}, breakout={break_thresh}")
            print(f"Error message: {str(e)}")
            continue

    # Convert results to DataFrame
    results_df = pd.DataFrame(results)

    # Find best parameters based on composite score
    best_params = results_df.loc[results_df['score'].idxmax()]

    best_parameters = {
        'window': int(best_params['window']),
        'band_threshold': float(best_params['band_threshold']),
        'breakout_threshold': float(best_params['breakout_threshold'])
    }

    return best_parameters, results_df


def analyze_results(results_df: pd.DataFrame) -> None:
    """
    Print detailed analysis of parameter optimization results.
    """
    print("\nParameter Optimization Results:")
    print("-" * 50)

    # Best overall combination
    best_row = results_df.loc[results_df['score'].idxmax()]
    print("\nBest Parameter Combination:")
    print(f"Window: {int(best_row['window'])}")
    print(f"Band Threshold: {best_row['band_threshold']:.3f}")
    print(f"Breakout Threshold: {best_row['breakout_threshold']:.3f}")
    print(f"Average Strength: {best_row['avg_strength']:.2f}")
    print(f"Max Strength: {best_row['max_strength']:.2f}")
    print(f"Breakout Count: {int(best_row['breakout_count'])}")
    print(f"Overall Score: {best_row['score']:.2f}")

    # Analysis by window size
    print("\nAnalysis by Window Size:")
    window_analysis = results_df.groupby('window')['score'].mean()
    print(window_analysis)

    # Analysis by band threshold
    print("\nAnalysis by Band Threshold:")
    band_analysis = results_df.groupby('band_threshold')['score'].mean()
    print(band_analysis)

    # Analysis by breakout threshold
    print("\nAnalysis by Breakout Threshold:")
    breakout_analysis = results_df.groupby('breakout_threshold')['score'].mean()
    print(breakout_analysis)


# Example usage:
if __name__ == "__main__":
    # Assuming you have your data in a DataFrame called 'df'
    from common_utils import read_write_sql_data as rd

    df = rd.get_table_data(selected_table='UNIONBANK', sort=True)

    # Find optimal parameters
    best_params, results_df = optimize_parameters(df)

    # Print detailed analysis
    analyze_results(results_df)

    # Use best parameters for final analysis
    final_results = detect_consolidation_breakout(
        df,
        window=best_params['window'],
        band_threshold=best_params['band_threshold'],
        breakout_threshold=best_params['breakout_threshold']
    )