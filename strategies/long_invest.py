import pandas as pd
import numpy as np
from joblib import Parallel, delayed
from datetime import datetime
from common_utils import read_write_sql_data as rd


# Assuming you have a function to fetch data from the database
def fetch_data_from_db(table_name, start_date, end_date):
    """
    Placeholder function to fetch data from the database.
    Replace this with your actual function to fetch data.
    """
    query = f"SELECT * FROM {table_name} WHERE date BETWEEN '{start_date}' AND '{end_date}'"
    data = rd.get_table_data(query=query)
    return data


def calculate_recent_high(data, window):
    """
    Calculate the recent high for the given window.
    """
    data['recent_high'] = data['Close'].rolling(window=window).max()
    return data


def generate_signals(data, down_percent):
    """
    Generate buy signals based on the strategy.
    """
    data['signal'] = 0
    data.loc[data['Close'] <= (1 - down_percent / 100) * data['recent_high'], 'signal'] = 1
    return data


def limit_signals_per_month(data, max_signals_per_month=2):
    """
    Limit the number of buy signals to a maximum of `max_signals_per_month` per month.
    """
    data['year'] = data['Date'].dt.year
    data['month'] = data['Date'].dt.month

    # Group by year and month, and limit the number of signals
    def limit_group(group):
        # Keep only the first `max_signals_per_month` signals in the month
        buy_signals = group[group['signal'] == 1]
        if len(buy_signals) > max_signals_per_month:
            # Keep only the first `max_signals_per_month` signals
            buy_signals = buy_signals.iloc[:max_signals_per_month]
            # Mark the rest as 0
            group.loc[~group.index.isin(buy_signals.index), 'signal'] = 0
        return group

    data = data.groupby(['year', 'month']).apply(limit_group).reset_index(drop=True)
    return data


def backtest_strategy(index_data, etf_data, window, down_percent, fixed_investment):
    """
    Backtest the strategy with the given parameters.
    """
    # Calculate recent high for the index
    index_data = calculate_recent_high(index_data, window)

    # Generate buy signals
    index_data = generate_signals(index_data, down_percent)

    # Limit buy signals to a maximum of 2 per month
    index_data = limit_signals_per_month(index_data, max_signals_per_month=2)

    # Merge index signals with ETF data
    merged_data = pd.merge(index_data[['Date', 'signal']], etf_data, on='Date', how='left')

    # Calculate returns based on signals
    merged_data['etf_return'] = merged_data['Close'].pct_change()
    merged_data['strategy_return'] = merged_data['signal'].shift(1) * merged_data['etf_return']

    # Calculate cumulative returns with fixed investment
    merged_data['investment'] = merged_data['signal'].shift(1) * fixed_investment
    merged_data['cumulative_investment'] = merged_data['investment'].cumsum()
    merged_data['portfolio_value'] = (1 + merged_data['strategy_return']).cumprod() * merged_data[
        'cumulative_investment']

    # Calculate CAGR for the strategy
    start_date = merged_data['Date'].min()
    end_date = merged_data['Date'].max()
    num_years = (end_date - start_date).days / 365.25
    strategy_cagr = (merged_data['portfolio_value'].iloc[-1] / merged_data['cumulative_investment'].iloc[-1]) ** (
                1 / num_years) - 1

    # Calculate Buy & Hold Return and CAGR
    buy_hold_return = (merged_data['Close'].iloc[-1] / merged_data['Close'].iloc[0]) - 1
    buy_hold_cagr = (merged_data['Close'].iloc[-1] / merged_data['Close'].iloc[0]) ** (1 / num_years) - 1

    # Summary of buy signals, monthly and yearly investments
    merged_data['year'] = merged_data['Date'].dt.year
    merged_data['month'] = merged_data['Date'].dt.month
    monthly_investment = merged_data.groupby(['year', 'month'])['investment'].sum().reset_index()
    yearly_investment = merged_data.groupby('year')['investment'].sum().reset_index()
    total_buy_signals = merged_data['signal'].sum()

    return {
        'window': window,
        'down_percent': down_percent,
        'total_investment': merged_data['cumulative_investment'].iloc[-1],
        'final_portfolio_value': merged_data['portfolio_value'].iloc[-1],
        'strategy_cagr': strategy_cagr,
        'buy_hold_return': buy_hold_return,
        'buy_hold_cagr': buy_hold_cagr,
        'total_buy_signals': total_buy_signals,
        'monthly_investment': monthly_investment,
        'yearly_investment': yearly_investment
    }


def find_best_strategy(index_data, etf_data, window_range, down_percent_range, fixed_investment):
    """
    Perform a grid search to find the best combination of window and down_percent.
    """
    # Use parallel processing to speed up the grid search
    results = Parallel(n_jobs=-1)(
        delayed(backtest_strategy)(index_data, etf_data, window, down_percent, fixed_investment)
        for window in window_range
        for down_percent in down_percent_range
    )

    # Convert results to a DataFrame
    results_df = pd.DataFrame(results)

    # Identify strategies that outperform buy-and-hold CAGR
    results_df['outperforms_buy_hold'] = results_df['strategy_cagr'] > results_df['buy_hold_cagr']

    # Find the best strategy based on CAGR
    best_by_cagr = results_df.loc[results_df['strategy_cagr'].idxmax()]

    return results_df, best_by_cagr


def main():
    # Example parameters
    start_date = '2020-01-01'
    end_date = '2023-01-01'
    fixed_investment = 5000  # Fixed amount to invest on each buy signal

    # Define ranges for window and down_percent
    window_range = range(10, 51, 5)  # e.g., 10 to 50 days, in steps of 5
    down_percent_range = np.arange(2, 10.5, 0.5)  # e.g., 2% to 10%, in steps of 0.5

    # Fetch index and ETF data from the database
    benchmark_index = 'NIFTY_50'
    stock_name = 'SBIN'
    index_data = fetch_data_from_db(benchmark_index, start_date, end_date)
    etf_data = fetch_data_from_db(stock_name, start_date, end_date)

    # Convert date columns to datetime
    index_data['Date'] = pd.to_datetime(index_data['Date'])
    etf_data['Date'] = pd.to_datetime(etf_data['Date'])

    # Find the best strategy
    results_df, best_by_cagr = find_best_strategy(index_data, etf_data, window_range, down_percent_range,
                                                  fixed_investment)

    # Print results
    print("All Results:")
    print(results_df)

    result_cols = ['window', 'down_percent', 'total_investment', 'final_portfolio_value', 'strategy_cagr',
                   'buy_hold_return', 'buy_hold_cagr', 'total_buy_signals', 'outperforms_buy_hold']
    results_df = results_df[result_cols]
    results_df['total_investment'] = round(results_df['total_investment'], 2)
    results_df['final_portfolio_value'] = round(results_df['final_portfolio_value'], 2)
    results_df['strategy_cagr'] = round(results_df['strategy_cagr']*100, 2)
    results_df['buy_hold_return'] = round(results_df['buy_hold_return']*100, 2)
    results_df['buy_hold_cagr'] = round(results_df['buy_hold_cagr']*100, 2)

    msg = rd.load_sql_data(data_to_load=results_df, table_name=f'DATA_{stock_name}', database='STRATEGY')
    print(msg)

    print("\nBest Strategy by CAGR:")
    print(best_by_cagr)

    # Print summary of buy signals and investments for the best strategy
    print("\nSummary of Buy Signals and Investments for Best Strategy:")
    print(f"Total Buy Signals: {best_by_cagr['total_buy_signals']}")
    print("\nMonthly Investments:")
    print(best_by_cagr['monthly_investment'])
    print("\nYearly Investments:")
    print(best_by_cagr['yearly_investment'])


if __name__ == "__main__":
    main()