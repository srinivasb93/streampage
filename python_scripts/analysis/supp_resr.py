import pandas as pd
import numpy as np


def identify_key_levels(df, threshold_pct=5.0):
    """
    Identify stocks that are within a certain percentage of their key levels
    (all-time high/low, 52-week high/low)

    Parameters:
    df: DataFrame with columns ['symbol', 'Date', 'Close']
    threshold_pct: percentage threshold to consider (default 5%)

    Returns:
    DataFrame with stocks at key levels and their distances from these levels
    """
    results = []

    # for symbol in df['symbol'].unique():
    stock_data = df.copy()
    current_price = stock_data['Close'].iloc[-1]
    current_date = stock_data['Date'].iloc[-1]

    # Calculate all-time levels
    all_time_high = stock_data['Close'].max()
    all_time_low = stock_data['Close'].min()

    # Calculate 52-week levels
    year_ago = current_date - pd.Timedelta(days=365)
    year_data = stock_data[stock_data['Date'] >= year_ago]
    week52_high = year_data['Close'].max()
    week52_low = year_data['Close'].min()

    # Calculate distances from levels
    dist_ath = ((all_time_high - current_price) / current_price) * 100
    dist_atl = ((current_price - all_time_low) / current_price) * 100
    dist_52h = ((week52_high - current_price) / current_price) * 100
    dist_52l = ((current_price - week52_low) / current_price) * 100

    # Check if within threshold of any level
    if any(abs(dist) <= threshold_pct for dist in [dist_ath, dist_atl, dist_52h, dist_52l]):
        results.append({
            'current_price': current_price,
            'Date': current_date,
            'dist_to_ath': round(dist_ath, 2),
            'dist_to_atl': round(dist_atl, 2),
            'dist_to_52w_high': round(dist_52h, 2),
            'dist_to_52w_low': round(dist_52l, 2)
        })

    return pd.DataFrame(results)


def identify_bounces(df, lookback_period=20, bounce_threshold=2.0, support_resistance_window=5):
    """
    Identify stocks that have bounced from support or resistance levels

    Parameters:
    df: DataFrame with columns ['symbol', 'Date', 'High', 'Low', 'Close']
    lookback_period: number of days to look back for establishing support/resistance
    bounce_threshold: minimum percentage move to consider as a bounce
    support_resistance_window: number of days to confirm support/resistance level

    Returns:
    DataFrame with identified bounce patterns
    """
    results = []

    # for symbol in df['symbol'].unique():
    stock_data = df.copy()

    # Calculate rolling min/max for support/resistance
    stock_data['rolling_min'] = stock_data['Low'].rolling(window=lookback_period).min()
    stock_data['rolling_max'] = stock_data['High'].rolling(window=lookback_period).max()

    for i in range(support_resistance_window, len(stock_data) - support_resistance_window):
        current_window = stock_data.iloc[i - support_resistance_window:i + support_resistance_window]
        current_price = current_window['Close'].iloc[-1]

        # Check for support bounce
        if (abs(current_window['Low'].min() - current_window['rolling_min'].iloc[-1]) < 0.01 * current_price and
                ((current_window['Close'].iloc[-1] - current_window['Low'].min()) / current_window[
                    'Low'].min()) * 100 >= bounce_threshold):
            results.append({
                'Date': current_window.index[-1],
                'pattern': 'Support Bounce',
                'bounce_level': round(current_window['Low'].min(), 2),
                'bounce_percentage': round(((current_window['Close'].iloc[-1] - current_window['Low'].min()) /
                                            current_window['Low'].min()) * 100, 2),
                'current_price': current_price
            })

        # Check for resistance bounce
        if (abs(current_window['High'].max() - current_window['rolling_max'].iloc[-1]) < 0.01 * current_price and
                ((current_window['High'].max() - current_window['Close'].iloc[-1]) / current_window[
                    'High'].max()) * 100 >= bounce_threshold):
            results.append({
                'Date': current_window.index[-1],
                'pattern': 'Resistance Bounce',
                'bounce_level': round(current_window['High'].max(), 2),
                'bounce_percentage': round(((current_window['High'].max() - current_window['Close'].iloc[-1]) /
                                            current_window['High'].max()) * 100, 2),
                'current_price': current_price
            })

    return pd.DataFrame(results)


# Example usage:
def main():
    # Sample data structure
    from common_utils import read_write_sql_data as rd
    df = rd.get_table_data(selected_table='TATAMOTORS', sort=True)
    # Find stocks near key levels
    key_levels = identify_key_levels(df, threshold_pct=5.0)
    print("\nStocks near key levels:")
    print(key_levels)

    # Find bounce patterns
    bounces = identify_bounces(df, lookback_period=20, bounce_threshold=2.0)
    print("\nStocks with bounce patterns:")
    print(bounces)


if __name__ == "__main__":
    main()