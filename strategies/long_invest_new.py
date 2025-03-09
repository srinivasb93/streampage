import pandas as pd
import numpy as np
from datetime import datetime
from common_utils import read_write_sql_data as rd

# Fetch data from the database
def fetch_data_from_db(table_name, start_date, end_date):
    query = f"SELECT * FROM {table_name} WHERE date BETWEEN '{start_date}' AND '{end_date}' ORDER BY DATE ASC"
    data = rd.get_table_data(query=query)
    return data

# Calculate percentage drawdown from recent high
def calculate_drawdown(data):
    data['RecentHigh'] = data['Close'].rolling(window=100).max()
    data['Drawdown'] = (data['Close'] - data['RecentHigh']) / data['RecentHigh'] * 100
    return data

def backtest_dynamic_strategy(data, fixed_investment, drawdown_threshold=-5):
    """
    Backtest the enhanced dynamic investment strategy.
    - Initial buy when drawdown exceeds 5%.
    - Subsequent buys with increasing amounts when drawdown exceeds 5% from the previous noted drawdown value.
    - Fallback buy on the last working day of the month if no drawdown conditions are met.
    """
    # Calculate drawdown
    data = calculate_drawdown(data)

    # Initialize portfolio
    portfolio = {
        'units': 0,
        'total_investment': 0,
        'portfolio_value': 0,
        'trades': []
    }

    # Extract unique year-month combinations from the data
    data['YearMonth'] = data['Date'].dt.to_period('M')
    unique_year_months = data['YearMonth'].unique()

    # Monthly investment process
    for year_month in unique_year_months:
        # Filter data for the current month
        month_data = data[data['YearMonth'] == year_month]

        # Initialize variables for the month
        first_drawdown = second_drawdown = third_drawdown = None
        investments_made = 0

        # Check for drawdown conditions
        for date, row in month_data.iterrows():
            if first_drawdown is None and row['Drawdown'] < drawdown_threshold*2:
                # Initial buy: Invest the fixed amount
                units_bought = (2 * fixed_investment) / row['Close']
                portfolio['units'] += units_bought
                portfolio['total_investment'] += fixed_investment * 2
                first_drawdown = row['Drawdown']
                investments_made += 1

                # Record the trade
                portfolio['trades'].append({
                    'Date': row['Date'],
                    'Price': row['Close'],
                    'Units': units_bought,
                    'Drawdown': first_drawdown,
                    'total_investment': portfolio['total_investment'],
                    'portfolio_value': int(portfolio['units'] * row['Close']),
                    'Type': 'Dynamic (2x)'
                })
            elif first_drawdown is not None and second_drawdown is None and row['Drawdown'] < first_drawdown-4:
                # Subsequent buy: Invest twice the fixed amount
                units_bought = (3 * fixed_investment) / row['Close']
                portfolio['units'] += units_bought
                portfolio['total_investment'] += 3 * fixed_investment
                second_drawdown = row['Drawdown']
                investments_made += 1

                # Record the trade
                portfolio['trades'].append({
                    'Date': row['Date'],
                    'Price': row['Close'],
                    'Units': units_bought,
                    'Drawdown': second_drawdown,
                    'total_investment': portfolio['total_investment'],
                    'portfolio_value': int(portfolio['units'] * row['Close']),
                    'Type': 'Dynamic (3x)'
                })
            elif second_drawdown is not None and third_drawdown is None and row['Drawdown'] < second_drawdown - 4:
                # Subsequent buy: Invest thrice the fixed amount
                units_bought = 5 * fixed_investment / row['Close']
                portfolio['units'] += units_bought
                portfolio['total_investment'] += fixed_investment * 5
                third_drawdown = row['Drawdown']
                investments_made += 1

                # Record the trade
                portfolio['trades'].append({
                    'Date': row['Date'],
                    'Price': row['Close'],
                    'Units': units_bought,
                    'Drawdown': third_drawdown,
                    'total_investment': portfolio['total_investment'],
                    'portfolio_value': int(portfolio['units'] * row['Close']),
                    'Type': 'Dynamic (5x)'
                })

        # Fallback buy: Invest the fixed amount on the last working day of the month if no investments were made
        if investments_made == 0:
            last_working_day = month_data['Date'].max()
            last_working_row = month_data[month_data['Date'] == last_working_day].iloc[0]

            units_bought = fixed_investment / last_working_row['Close']
            portfolio['units'] += units_bought
            portfolio['total_investment'] += fixed_investment

            # Record the trade
            portfolio['trades'].append({
                'Date': last_working_row['Date'],
                'Price': last_working_row['Close'],
                'Units': units_bought,
                'total_investment': portfolio['total_investment'],
                'portfolio_value': int(portfolio['units'] * last_working_row['Close']),
                'Type': 'Fallback (1x)'
            })

    # Calculate final portfolio value
    portfolio['portfolio_value'] = portfolio['units'] * data['Close'].iloc[-1]

    # Calculate CAGR
    start_date = data['Date'].min()
    end_date = data['Date'].max()
    num_years = (end_date - start_date).days / 365.25
    portfolio['cagr'] = (portfolio['portfolio_value'] / portfolio['total_investment']) ** (1 / num_years) - 1

    # Calculate average buy price
    portfolio['average_buy_price'] = portfolio['total_investment'] / portfolio['units']

    return portfolio

# Backtest the fixed investment benchmark
def backtest_fixed_investment(data, fixed_investment, investment_day=15):
    """
    Backtest the fixed investment strategy (invest on a chosen day every month).
    If the chosen day is missing, invest on the next available trading day.
    """
    # Initialize portfolio
    portfolio = {
        'units': 0,
        'total_investment': 0,
        'portfolio_value': 0,
        'trades': []
    }

    # Extract unique year-month combinations from the data
    data['YearMonth'] = data['Date'].dt.to_period('M')
    unique_year_months = data['YearMonth'].unique()

    # Monthly investment process
    for year_month in unique_year_months:
        # Convert YearMonth back to datetime
        month_start = year_month.start_time

        # Find the investment day for the current month
        investment_date = month_start.replace(day=investment_day)

        # If the investment date is missing, find the next available trading day
        if investment_date not in data['Date'].values:
            # Find the next available trading day after the investment date
            next_available_dates = data[data['Date'] > investment_date]
            if len(next_available_dates) == 0:
                # If no next available date, skip the month
                continue
            investment_date = next_available_dates['Date'].iloc[0]

        # Get the row for the investment date
        investment_row = data[data['Date'] == investment_date]
        if len(investment_row) == 0:
            continue

        # Invest the fixed amount
        units_bought = fixed_investment / investment_row['Close'].iloc[0]
        portfolio['units'] += units_bought
        portfolio['total_investment'] += fixed_investment

        # Record the trade
        portfolio['trades'].append({
            'Date': investment_row['Date'].iloc[0],
            'Price': investment_row['Close'].iloc[0],
            'Units': units_bought
        })

    # Calculate final portfolio value
    portfolio['portfolio_value'] = portfolio['units'] * data['Close'].iloc[-1]

    # Calculate CAGR
    start_date = data['Date'].min()
    end_date = data['Date'].max()
    num_years = (end_date - start_date).days / 365.25
    portfolio['cagr'] = (portfolio['portfolio_value'] / portfolio['total_investment']) ** (1 / num_years) - 1

    # Calculate average buy price
    portfolio['average_buy_price'] = portfolio['total_investment'] / portfolio['units']

    return portfolio

# Calculate Buy & Hold Return and CAGR
def calculate_buy_hold_returns(data):
    """
    Calculate Buy & Hold returns.
    """
    buy_hold_return = (data['Close'].iloc[-1] / data['Close'].iloc[0]) - 1
    num_years = (data['Date'].iloc[-1] - data['Date'].iloc[0]).days / 365.25
    buy_hold_cagr = (data['Close'].iloc[-1] / data['Close'].iloc[0]) ** (1 / num_years) - 1
    return buy_hold_return, buy_hold_cagr

def main():
    # Example parameters
    start_date = '2015-02-09'
    end_date = '2025-2-6'
    fixed_investment = 5000  # Fixed amount to invest
    drawdown_threshold = -5  # Drawdown threshold for dynamic strategy
    investment_day = 22  # Chosen day for fixed investment benchmark

    # Fetch ETF data from the database
    etf_data = fetch_data_from_db('juniorbees', start_date, end_date)

    # Convert date columns to datetime
    etf_data['Date'] = pd.to_datetime(etf_data['Date'])

    # Backtest the dynamic investment strategy
    dynamic_portfolio = backtest_dynamic_strategy(etf_data, fixed_investment, drawdown_threshold)

    # Backtest the fixed investment benchmark
    fixed_portfolio = backtest_fixed_investment(etf_data, fixed_investment, investment_day)

    # Calculate Buy & Hold returns
    buy_hold_return, buy_hold_cagr = calculate_buy_hold_returns(etf_data)

    # Print results
    print("Dynamic Investment Strategy:")
    print(f"Total Investment: {dynamic_portfolio['total_investment']}")
    print(f"Final Portfolio Value: {dynamic_portfolio['portfolio_value']}")
    print(f"Strategy CAGR: {dynamic_portfolio['cagr'] * 100:.2f}%")
    print(f"Number of Trades: {len(dynamic_portfolio['trades'])}")
    print(f"Total Shares: {dynamic_portfolio['units']}")
    print(f"Average Buy Price: {dynamic_portfolio['average_buy_price']}")
    print("\nTradebook:")
    print(pd.DataFrame(dynamic_portfolio['trades']))

    print("\nFixed Investment Benchmark:")
    print(f"Total Investment: {fixed_portfolio['total_investment']}")
    print(f"Final Portfolio Value: {fixed_portfolio['portfolio_value']}")
    print(f"Strategy CAGR: {fixed_portfolio['cagr'] * 100:.2f}%")
    print(f"Number of Trades: {len(fixed_portfolio['trades'])}")
    print(f"Total Shares: {fixed_portfolio['units']}")
    print(f"Average Buy Price: {fixed_portfolio['average_buy_price']}")
    print("\nTradebook:")
    print(pd.DataFrame(fixed_portfolio['trades']))

    print("\nBuy & Hold Strategy:")
    print(f"Buy & Hold Return: {buy_hold_return * 100:.2f}%")
    print(f"Buy & Hold CAGR: {buy_hold_cagr * 100:.2f}%")

if __name__ == "__main__":
    main()