import pandas as pd
import matplotlib.pyplot as plt
from itertools import product
from common_utils import read_write_sql_data as rd, utils as utils
import pandas_ta as ta
import pandas as pd
import numpy as np


def calculate_atr(df, period=14):
    """Calculate Average True Range (ATR) for the DataFrame."""
    high_low = df['High'] - df['Low']
    high_close = np.abs(df['High'] - df['Close'].shift())
    low_close = np.abs(df['Low'] - df['Close'].shift())
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()
    return atr


def backtest_stock(data, stock_name, initial_investment=100000, stop_loss_atr_mult=2.0, target_atr_mult=5.0,
                   atr_period=14):
    df = pd.DataFrame(data)

    # Calculate ATR
    df['ATR'] = calculate_atr(df, atr_period)

    # Initialize variables
    investment_amount = initial_investment
    total_profit = 0
    total_trades = 0
    total_charges = 0
    winning_trades = 0
    losing_trades = 0

    tradebook = []
    # Initialize portfolio_value with initial investment for all days
    portfolio_value = [initial_investment] * len(df)

    # Backtesting loop (start after ATR period to avoid NaN)
    for i in range(atr_period, len(df)):
        entry_price = df['Open'][i]
        high_price = df['High'][i]
        low_price = df['Low'][i]
        close_price = df['Close'][i]
        previous_close = df['Close'][i - 1]
        atr = df['ATR'][i]

        shares_sold = int(investment_amount / entry_price)

        if entry_price > previous_close and shares_sold > 0:
            stop_loss_price = entry_price + (atr * stop_loss_atr_mult)
            target_price = entry_price - (atr * target_atr_mult)

            if target_price < 0:
                target_price = entry_price * 0.01

            if high_price >= stop_loss_price:
                exit_price = stop_loss_price
                profit = (entry_price - exit_price) * shares_sold
                trade_status = "Loss"
                losing_trades += 1
            elif low_price <= target_price:
                exit_price = target_price
                profit = (entry_price - exit_price) * shares_sold
                trade_status = "Win"
                winning_trades += 1
            else:
                exit_price = df['Close'][i]
                profit = (entry_price - exit_price) * shares_sold
                trade_status = "No Target/Stop Loss Hit" if profit > 0 else "Loss"
                if profit > 0:
                    winning_trades += 1
                else:
                    losing_trades += 1

            charges = .0005 * investment_amount

            tradebook.append({
                'Date': df['Date'][i],
                'Entry Price': entry_price,
                'Close Price': close_price,
                'Prev Close': previous_close,
                'Stop Loss': stop_loss_price,
                'Target Price': target_price,
                'Exit Price': exit_price,
                'Shares Sold': shares_sold,
                'Charges': charges,
                'Investment_amount': investment_amount,
                'Total_Profit': total_profit,
                'Total Charges': total_charges,
                'Profit/Loss': profit,
                'Status': trade_status,
                'ATR': atr
            })

            investment_amount += profit - charges
            total_profit += profit - charges
            total_trades += 1
            total_charges += charges

        # Update portfolio value for current day and onward
        portfolio_value[i] = investment_amount

    tradebook_df = pd.DataFrame(tradebook)
    if not tradebook_df.empty:
        tradebook_df['Year'] = tradebook_df['Date'].dt.year
        yearly_summary = tradebook_df.groupby('Year').apply(
            lambda x: pd.Series({
                'Total Trades': len(x),
                'Winning Trades': len(x[x['Status'] == 'Win']),
                'Losing Trades': len(x[x['Status'] == 'Loss']),
                'No Target/Stop Loss Hit': len(x[x['Status'] == 'No Target/Stop Loss Hit']),
                'Profit from Winning Trades': x[x['Status'] == 'Win']['Profit/Loss'].sum(),
                'Loss from Losing Trades': x[x['Status'] == 'Loss']['Profit/Loss'].sum(),
                'Profit from No Target/Stop Loss Hit': x[x['Status'] == 'No Target/Stop Loss Hit']['Profit/Loss'].sum()
            })
        ).reset_index()
    else:
        yearly_summary = pd.DataFrame()

    win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
    loss_rate = (losing_trades / total_trades) * 100 if total_trades > 0 else 0
    net_profit = total_profit

    return {
        'Stock': stock_name,
        "stop_loss_atr_mult": stop_loss_atr_mult,
        "target_atr_mult": target_atr_mult,
        'Initial Investment': initial_investment,
        'Final Portfolio Value': investment_amount,
        'Total Profit': net_profit,
        'Win Rate': win_rate,
        'Loss Rate': loss_rate,
        'Total Trades': total_trades,
        'Winning Trades': winning_trades,
        'Losing Trades': losing_trades,
        'Tradebook': tradebook_df,
        'Yearly Summary': yearly_summary,
        'Portfolio Value': portfolio_value
    }

from itertools import product

def optimize_parameters(data, stock_name, initial_investment_range, stop_loss_atr_mult_range, target_atr_mult_range):
    best_result = None
    best_final_value = -float('inf')

    for initial_investment, stop_loss_mult, target_mult in product(initial_investment_range, stop_loss_atr_mult_range, target_atr_mult_range):
        result = backtest_stock(data, stock_name, initial_investment, stop_loss_mult, target_mult)
        if result['Final Portfolio Value'] > best_final_value:
            best_final_value = result['Final Portfolio Value']
            best_result = result

    return best_result

# # List of stocks to backtest
# stocks = ['GOLDBEES', 'JUNIORBEES', 'ICICIB22', 'CPSEETF']  # Add more stocks as needed
#
# # Define parameter ranges for optimization
# initial_investment_range = [50000]  # Fixed initial investment
# stop_loss_range = [1000, 1100, 1200, 1300, 1400, 1500]  # Example range for stop_loss
# target_range = [2000, 2200, 2500, 2800, 3000, 3300, 3600, 4600, 5600, 6600]     # Example range for target

# # Dictionary to store optimized results for each stock
# optimized_results = {}
#
# # Optimize parameters for each stock
# for stock in stocks:
#     query = f"Select * from dbo.{stock} where date between '2020-01-01 00:00:00.000' and '2026-01-01 00:00:00.000' order by Date ASC"
#     data = rd.get_table_data(query=query)
#     optimized_results[stock] = optimize_parameters(data, stock, initial_investment_range, stop_loss_range, target_range)
#
# # Print optimized results
# for stock, result in optimized_results.items():
#     print(f"\nOptimized Parameters for {stock}:")
#     print(f"Initial Investment: Rs. {result['Initial Investment']}")
#     print(f"Stop Loss: {result['stop_loss']}")
#     print(f"Target: {result['target']}")
#     print(f"Final Portfolio Value: Rs. {result['Final Portfolio Value']}")
#     print(f"Total Profit: Rs. {result['Total Profit']}")
#     print(f"Win Rate: {result['Win Rate']:.2f}%")
#     print(f"Loss Rate: {result['Loss Rate']:.2f}%")
#     print(f"Total Trades: {result['Total Trades']}")
#     print(f"Winning Trades: {result['Winning Trades']}")
#     print(f"Losing Trades: {result['Losing Trades']}")
#     print(f"Yearly Summary: {result['Yearly Summary']}")
