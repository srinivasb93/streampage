import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

# Constants
START_DATE = "2010-01-01"
END_DATE = datetime.today().strftime('%Y-%m-%d')
INITIAL_INVESTMENT = 100000  # Initial investment amount
MONTHLY_SIP = 10000  # Monthly SIP amount
REBALANCE_FREQ = 12  # Rebalance portfolio every 12 months
PE_RATIO_THRESHOLD = 20  # Buy if P/E ratio is below this threshold
MOMENTUM_WINDOW = 6  # Months to calculate momentum (e.g., 6-month momentum)

# Define your stock universe (tickers)
STOCK_UNIVERSE = {
    "Large-Cap": ["INFY.NS", "SBIN.NS", "TCS.NS", "ITC.NS"],
    "Mid-Cap": ["UBER", "LYFT", "ZM", "PTON"],
    "Small-Cap": ["FUV", "BLNK", "WKHS", "NIO"]
}

# Fetch historical stock data
def fetch_stock_data(tickers, start_date, end_date):
    data = yf.download(tickers, start=start_date, end=end_date)['Adj Close']
    return data

# Fetch P/E ratios (example: using yfinance for simplicity)
def fetch_pe_ratios(tickers):
    pe_ratios = {}
    for ticker in tickers:
        stock = yf.Ticker(ticker)
        pe_ratio = stock.info.get('trailingPE', None)
        pe_ratios[ticker] = pe_ratio
    return pe_ratios

# Calculate monthly returns
def calculate_monthly_returns(data):
    monthly_returns = data.resample('M').last().pct_change().dropna()
    return monthly_returns

# Calculate momentum (e.g., 6-month price momentum)
def calculate_momentum(data, window):
    momentum = data.pct_change(window).dropna()
    return momentum

# Determine buy signals based on P/E ratio and momentum
def generate_buy_signals(data, pe_ratios, momentum, pe_threshold):
    buy_signals = pd.DataFrame(index=data.index, columns=data.columns, data=False)
    for ticker in data.columns:
        pe_ratio = pe_ratios.get(ticker, None)
        if pe_ratio and pe_ratio < pe_threshold:  # Check if P/E ratio is below threshold
            buy_signals[ticker] = momentum[ticker] > 0  # Buy if momentum is positive
    return buy_signals

# Portfolio allocation with buy signals
def allocate_portfolio_with_signals(returns, allocation, buy_signals):
    weighted_returns = returns * allocation
    weighted_returns[~buy_signals] = 0  # Set returns to 0 for stocks not meeting buy criteria
    portfolio_returns = weighted_returns.sum(axis=1)
    return portfolio_returns

# Rebalance portfolio with buy signals
def rebalance_portfolio_with_signals(returns, allocation, buy_signals, rebalance_freq):
    rebalanced_returns = pd.Series(index=returns.index, dtype=float)
    for i in range(0, len(returns), rebalance_freq):
        start = i
        end = min(i + rebalance_freq, len(returns))
        rebalanced_returns[start:end] = allocate_portfolio_with_signals(returns[start:end], allocation, buy_signals[start:end])
    return rebalanced_returns

# Simulate SIP investment
def simulate_sip(portfolio_returns, initial_investment, monthly_sip):
    investment = initial_investment
    portfolio_value = []
    for i, ret in enumerate(portfolio_returns):
        if i > 0 and i % 12 == 0:  # Add SIP annually for simplicity
            investment += monthly_sip * 12
        investment *= (1 + ret)
        portfolio_value.append(investment)
    return pd.Series(portfolio_value, index=portfolio_returns.index)

# Main function
def main():
    # Fetch historical data
    all_tickers = [ticker for cap in STOCK_UNIVERSE.values() for ticker in cap]
    stock_data = fetch_stock_data(all_tickers, START_DATE, END_DATE)

    # Fetch P/E ratios
    pe_ratios = fetch_pe_ratios(all_tickers)

    # Calculate monthly returns
    monthly_returns = calculate_monthly_returns(stock_data)

    # Calculate momentum
    momentum = calculate_momentum(stock_data, MOMENTUM_WINDOW)

    # Generate buy signals
    buy_signals = generate_buy_signals(stock_data, pe_ratios, momentum, PE_RATIO_THRESHOLD)

    # Define portfolio allocation (e.g., 50% Large-Cap, 30% Mid-Cap, 20% Small-Cap)
    allocation = {
        "AAPL": 0.125, "MSFT": 0.125, "GOOGL": 0.125, "AMZN": 0.125,  # Large-Cap (50%)
        "UBER": 0.075, "LYFT": 0.075, "ZM": 0.075, "PTON": 0.075,     # Mid-Cap (30%)
        "FUV": 0.05, "BLNK": 0.05, "WKHS": 0.05, "NIO": 0.05          # Small-Cap (20%)
    }

    # Rebalance portfolio with buy signals
    portfolio_returns = rebalance_portfolio_with_signals(monthly_returns, allocation, buy_signals, REBALANCE_FREQ)

    # Simulate SIP investment
    portfolio_value = simulate_sip(portfolio_returns, INITIAL_INVESTMENT, MONTHLY_SIP)

    # Print results
    print("Final Portfolio Value:", portfolio_value.iloc[-1])
    print("CAGR:", ((portfolio_value.iloc[-1] / INITIAL_INVESTMENT) ** (1 / (len(portfolio_value) / 12)) - 1) * 100)

    # Plot portfolio value over time
    portfolio_value.plot(title="Portfolio Value Over Time", xlabel="Date", ylabel="Portfolio Value (USD)")

if __name__ == "__main__":
    main()