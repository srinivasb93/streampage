import pandas as pd
import numpy as np


def analyze_stocks(df):
    """
    Analyze stocks for trends, swing trading opportunities, and support/resistance levels

    Parameters:
    df (pandas.DataFrame): DataFrame containing stock data

    Returns:
    dict: Dictionary containing analysis results
    """

    def identify_trends(data):
        """Identify stocks in uptrend and downtrend"""
        trends = []
        for _, row in data.iterrows():
            is_above_13_ema = row['Close'] > row['Wk_EMA_13']
            is_above_52_ema = row['Close'] > row['Wk_EMA_52']
            weekly_trend = row['Percent_Chg_W']
            monthly_trend = row['Percent_Chg_M']

            trend_data = {
                'Symbol': row['Symbol'],
                'Close': row['Close'],
                'Weekly_Change': row['Percent_Chg_W'],
                'Monthly_Change': row['Percent_Chg_M'],
                'Trend': 'Uptrend' if (is_above_13_ema and is_above_52_ema and weekly_trend > 0)
                else 'Downtrend' if (not is_above_13_ema and not is_above_52_ema and weekly_trend < 0)
                else 'Sideways'
            }
            trends.append(trend_data)
        return pd.DataFrame(trends)

    def identify_swing_opportunities(data):
        """Identify potential swing trading opportunities"""
        swing_opportunities = []
        for _, row in data.iterrows():
            # Look for stocks with recent price movement but not overextended
            range_weekly = (row['High_W'] - row['Low_W']) / row['Low_W'] * 100
            range_daily = (row['High'] - row['Low']) / row['Low'] * 100

            # Calculate distance from 52-week high and low
            dist_from_high = (row['High_52W'] - row['Close']) / row['High_52W'] * 100
            dist_from_low = (row['Close'] - row['Low_52W']) / row['Low_52W'] * 100

            # Define swing trading criteria
            good_volume = row['Volume_W'] > row['Volume'] * 5  # Higher than average volume
            good_range = range_weekly > 2 and range_weekly < 10  # Decent but not excessive range
            not_overextended = dist_from_high > 5 and dist_from_low > 5  # Not at extremes

            if good_volume and good_range and not_overextended:
                swing_opportunities.append({
                    'Symbol': row['Symbol'],
                    'Close': row['Close'],
                    'Weekly_Range': range_weekly,
                    'Volume_Multiple': row['Volume_W'] / row['Volume'],
                    'Distance_From_High': dist_from_high,
                    'Distance_From_Low': dist_from_low
                })
        return pd.DataFrame(swing_opportunities)

    def identify_support_resistance(data):
        """Identify stocks near support or resistance levels"""
        support_resistance = []
        for _, row in data.iterrows():
            # Calculate key levels
            recent_low = min(row['Low_6W'], row['Low_6M'])
            recent_high = max(row['High_6W'], row['High_6M'])

            # Calculate distances from current price
            dist_from_support = (row['Close'] - recent_low) / recent_low * 100
            dist_from_resistance = (recent_high - row['Close']) / row['Close'] * 100

            # Define criteria for being near support/resistance
            near_support = dist_from_support < 3
            near_resistance = dist_from_resistance < 3

            if near_support or near_resistance:
                support_resistance.append({
                    'Symbol': row['Symbol'],
                    'Close': row['Close'],
                    'Level_Type': 'Support' if near_support else 'Resistance',
                    'Distance': dist_from_support if near_support else dist_from_resistance,
                    'Level_Price': recent_low if near_support else recent_high
                })
        return pd.DataFrame(support_resistance)

    # Perform analysis
    results = {
        'trends': identify_trends(df),
        'swing_opportunities': identify_swing_opportunities(df),
        'support_resistance': identify_support_resistance(df)
    }

    return results


# Example usage:
if __name__ == "__main__":
    # Read the CSV file
    from common_utils import read_write_sql_data as rd
    # df = pd.read_csv('stock_agg_data.csv')
    df = rd.get_table_data(selected_table='AGG_DATA')
    # Run analysis
    analysis_results = analyze_stocks(df)

    # Print results
    print("\n=== Stocks Trending Up/Down ===")
    print(analysis_results['trends'])

    print("\n=== Potential Swing Trading Opportunities ===")
    print(analysis_results['swing_opportunities'])

    print("\n=== Stocks Near Support/Resistance ===")
    print(analysis_results['support_resistance'])