import pandas as pd
import numpy as np
from common_utils import read_write_sql_data as rd, utils as utils

# The data should have columns: 'Date', 'Open', 'High', 'Low', 'Close'
data = rd.get_table_data(selected_table='GOLDBEES', sort=True)

df = pd.DataFrame(data)


def backtest_strategy(data, units, stop_loss, target):
    """
    Backtest a daily trading strategy with fixed units traded, stop loss, and target.

    :param data: pandas DataFrame with 'Close' price column
    :param units: Number of shares to trade each day
    :param stop_loss: Fixed stop loss in currency units per share
    :param target: Fixed target profit in currency units per share
    :return: Dictionary with performance metrics
    """
    results = {
        'total_profit': 0,
        'wins': 0,
        'losses': 0,
        'total_trades': 0
    }

    for i in range(len(data) - 1):  # Loop through each day except the last
        entry_price = data.iloc[i]['Close']
        exit_price = data.iloc[i + 1]['Close']

        # Calculate potential stop loss price and target price
        stop_loss_price = entry_price - stop_loss
        target_price = entry_price + target

        # Check if stop loss or target was hit within the day
        if exit_price <= stop_loss_price:
            profit = -units * stop_loss
            results['losses'] += 1
        elif exit_price >= target_price:
            profit = units * target
            results['wins'] += 1
        else:  # If neither stop loss nor target is hit, sell at close
            profit = units * (exit_price - entry_price)

        results['total_profit'] += profit
        results['total_trades'] += 1

    # Calculate win rate
    results['win_rate'] = results['wins'] / results['total_trades'] if results['total_trades'] > 0 else 0

    return results


units = 1000  # Fixed number of shares to trade
stop_loss = 500  # Rs. 500 per share
target = 1000  # Rs. 1000 per share

backtest_results = backtest_strategy(df, units, stop_loss, target)
print(backtest_results)