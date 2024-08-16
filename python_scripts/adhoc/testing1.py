import pandas as pd
import matplotlib.pyplot as plt
from common_utils import read_write_sql_data as rd

connection_string = rd.create_connection()


# Function to fetch historical stock data from SQL Server
def fetch_stock_data(symbol, start_date):
    # Replace 'your_connection_string' with your actual SQL Server connection string
    query = f"SELECT Date, [Close] FROM dbo.{symbol} WHERE Date >= '{start_date}' ORDER BY Date"

    # Fetch data from SQL Server
    stock_data = pd.read_sql(query, connection_string, parse_dates=['Date'], index_col='Date')

    return stock_data


# Function to calculate daily returns
def calculate_returns(data):
    data['Returns'] = data['Close'].pct_change()
    data['Cumulative Returns'] = (1 + data['Returns']).cumprod() - 1
    data['Profit/Loss'] = (1 + data['Returns']).cumprod() * data['Close'].iloc[0] - data['Close']
    return data


# Function to fetch benchmark index data
def fetch_benchmark_data(index_symbol, start_date):
    # Replace 'your_connection_string' with your actual SQL Server connection string
    query = f"SELECT Date, [Close] FROM dbo.{index_symbol} WHERE Date >= '{start_date}' ORDER BY Date"

    # Fetch data from SQL Server
    index_data = pd.read_sql(query, connection_string, parse_dates=['Date'], index_col='Date')

    return index_data


# Function to plot portfolio and benchmark returns
def plot_returns(portfolio_returns, benchmark_returns, symbols):
    plt.figure(figsize=(12, 6))

    for symbol in symbols:
        plt.plot(portfolio_returns.index, portfolio_returns[f'{symbol}_Cumulative_Returns'],
                 label=f'{symbol} Portfolio')

    plt.plot(benchmark_returns.index, benchmark_returns['Cumulative Returns'], label='Benchmark Index', linestyle='--',
             color='black')

    plt.title('Portfolio Returns vs Benchmark Index')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Returns')
    plt.legend()
    plt.show()


# List of stocks in your portfolio
portfolio_symbols = ['SBIN', 'TATAMOTORS', 'INFY', 'ITC', 'SONATSOFTW']


def fetch_portfolio_data(symbols, start_date):
    portfolio_data = pd.concat([fetch_stock_data(symbol, start_date) for symbol in symbols], axis=1)
    portfolio_data.columns = symbols
    return portfolio_data


print(fetch_portfolio_data(portfolio_symbols, '2022-01-01'))


# Benchmark index symbol
benchmark_index_symbol = 'NIFTY_50'  # Replace with your actual benchmark index symbol

# Date when you purchased the stocks
purchase_start_date = '2016-09-15'  # Replace with your actual purchase start date

# Fetch and calculate returns for each stock in the portfolio
portfolio_returns = pd.DataFrame()
for symbol in portfolio_symbols:
    print(symbol)
    stock_data = fetch_stock_data(symbol, purchase_start_date)
    stock_returns = calculate_returns(stock_data)
    portfolio_returns[f'{symbol}_Cumulative_Returns'] = stock_returns['Cumulative Returns']
    portfolio_returns[f'{symbol}_Profit/Loss'] = stock_returns['Profit/Loss']

# Fetch and calculate returns for the benchmark index
benchmark_returns = fetch_benchmark_data(benchmark_index_symbol, purchase_start_date)
benchmark_returns = calculate_returns(benchmark_returns)

print(portfolio_returns)

# Plot portfolio and benchmark returns
# plot_returns(portfolio_returns, benchmark_returns, portfolio_symbols)
