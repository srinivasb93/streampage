import pandas as pd
from common_utils import read_write_sql_data as rd, utils as utils

# The data should have columns: 'timestamp', 'Open', 'High', 'Low', 'Close'
data = rd.get_table_data(selected_table='SBIN', sort=True)

df = pd.DataFrame(data)

# Fixed parameters
shares_sold = 1000  # Fixed number of shares to sell every day
stop_loss = 1100  # Fixed stop loss in Rs.
target = 2500  # Fixed target in Rs.

# Initialize variables to track performance
total_investment = 0
total_profit = 0
total_trades = 0
winning_trades = 0
losing_trades = 0

# Tradebook to log all trades
tradebook = []

# Backtesting loop
for i in range(1, len(df)):  # Start from 1 to compare with previous day's close
    entry_price = df['open'][i]
    high_price = df['high'][i]
    low_price = df['low'][i]
    previous_close = df['close'][i - 1]  # Yesterday's close price

    # Check if current open price is above yesterday's close price
    if entry_price > previous_close:
        # Calculate the stop loss and target prices
        stop_loss_price = entry_price + (stop_loss / shares_sold)  # Price increases for a short position
        target_price = entry_price - (target / shares_sold)  # Price decreases for a short position

        # Determine exit price and profit/loss
        if high_price >= stop_loss_price:
            exit_price = stop_loss_price
            profit = (entry_price - exit_price) * shares_sold  # Profit = (Entry - Exit) * Shares
            trade_status = "Loss"
            losing_trades += 1
        elif low_price <= target_price:
            exit_price = target_price
            profit = (entry_price - exit_price) * shares_sold
            trade_status = "Win"
            winning_trades += 1
        else:
            exit_price = df['close'][i]
            profit = (entry_price - exit_price) * shares_sold
            trade_status = "No Target/Stop Loss Hit"

        # Log the trade in the tradebook
        tradebook.append({
            'timestamp': df['timestamp'][i],
            'Entry Price': entry_price,
            'Exit Price': exit_price,
            'Shares Sold': shares_sold,
            'Profit/Loss': profit,
            'Status': trade_status
        })

        # Update totals
        total_investment += entry_price * shares_sold
        total_profit += profit - 60
        total_trades += 1

# Convert tradebook to a DataFrame for better visualization
tradebook_df = pd.DataFrame(tradebook)

# Add a 'Year' column to the tradebook for grouping
tradebook_df['Year'] = tradebook_df['timestamp'].dt.year

# Yearly summary
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

# Print tradebook and yearly summary
print("Tradebook:")
print(tradebook_df)
print("\nYearly Summary:")
print(yearly_summary)

# Calculate overall performance metrics
win_rate = (winning_trades / total_trades) * 100
loss_rate = (losing_trades / total_trades) * 100
net_profit = total_profit

# Print overall performance metrics
print("\nOverall Performance Metrics:")
print(f"Total Investment: Rs. {total_investment}")
print(f"Total Profit: Rs. {net_profit}")
print(f"Win Rate: {win_rate:.2f}%")
print(f"Loss Rate: {loss_rate:.2f}%")
print(f"Total Trades: {total_trades}")
print(f"Winning Trades: {winning_trades}")
print(f"Losing Trades: {losing_trades}")