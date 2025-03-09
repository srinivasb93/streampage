def analyze_narrow_band_breakout(data, lookback_period=20, band_threshold=0.05):
    """
    Identify stocks that stay within a narrow price band and then break out.

    Parameters:
    - data: DataFrame with 'High', 'Low', 'Close' columns
    - lookback_period: Number of days to look back for narrow band (default: 20)
    - band_threshold: Maximum allowed band width as % of price (default: 2%)

    Returns:
    - DataFrame with narrow band and breakout signals
    """
    # Calculate daily price range as percentage of closing price
    data['Daily_Range_Pct'] = (data['High'] - data['Low']) / data['Close']

    # Calculate highest high and lowest low over lookback period
    data['Highest_High'] = data['High'].rolling(window=lookback_period).max()
    data['Lowest_Low'] = data['Low'].rolling(window=lookback_period).min()

    # Calculate band width as percentage of average price
    data['Band_Width_Pct'] = (data['Highest_High'] - data['Lowest_Low']) / data['Close']

    # Identify periods where price stays within narrow band
    data['In_Narrow_Band'] = data['Band_Width_Pct'] <= band_threshold

    # Count consecutive days in narrow band
    data['Narrow_Band_Days'] = data['In_Narrow_Band'].groupby(
        (data['In_Narrow_Band'] != data['In_Narrow_Band'].shift()).cumsum()
    ).cumcount() + 1

    # Reset counter when not in narrow band
    data.loc[~data['In_Narrow_Band'], 'Narrow_Band_Days'] = 0

    # Identify breakouts
    data['Breakout'] = 'None'

    # Look for breakouts after minimum period in narrow band
    breakout_condition = (data['Narrow_Band_Days'].shift(1) >= lookback_period) & \
                         (~data['In_Narrow_Band'])

    # Upward breakouts
    data.loc[breakout_condition &
             (data['High'] > data['Highest_High'].shift(1)), 'Breakout'] = 'Up'

    # Downward breakouts
    data.loc[breakout_condition &
             (data['Low'] < data['Lowest_Low'].shift(1)), 'Breakout'] = 'Down'

    # Calculate breakout strength
    data['Breakout_Range_Pct'] = data['Daily_Range_Pct'] / \
                                 data['Daily_Range_Pct'].rolling(window=lookback_period).mean()

    return data


# Example usage:
from common_utils import read_write_sql_data as rd
stock_data = rd.get_table_data(selected_table='TATAMOTORS', sort=True)
# For a single stock
results = analyze_narrow_band_breakout(
    stock_data,
    lookback_period=20,
    band_threshold=0.06
)

print(results)
# Find recent breakouts
recent_breakouts = results[results['Breakout'] != 'None'].tail()

print(recent_breakouts)

# For multiple stocks
def scan_stocks_for_breakouts(stock_dict, lookback_period=20, band_threshold=0.02):
    breakouts = {}

    for symbol, data in stock_dict.items():
        analysis = analyze_narrow_band_breakout(
            data,
            lookback_period=lookback_period,
            band_threshold=band_threshold
        )

        # Check if latest day shows breakout
        if analysis['Breakout'].iloc[-1] != 'None':
            breakouts[symbol] = {
                'breakout_direction': analysis['Breakout'].iloc[-1],
                'days_in_band': analysis['Narrow_Band_Days'].iloc[-2],  # previous day
                'breakout_strength': analysis['Breakout_Range_Pct'].iloc[-1]
            }

    return breakouts

# Usage for multiple stocks
stock_dict = {
    'TATAMOTORS': stock_data,
}

breakout_stocks = scan_stocks_for_breakouts(stock_dict)

print(breakout_stocks)