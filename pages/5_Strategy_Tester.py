import streamlit as st
import pandas as pd
import numpy as np
import datetime as dt
from common_utils import read_write_sql_data as rd  # Replace with your actual module
from common_utils.auth import require_authentication

st.set_page_config(layout="wide")

# Require authentication for this page
require_authentication()

# Function to calculate drawdown
def calculate_drawdown(data):
    data['RecentHigh'] = data['close'].rolling(window=100).max()
    data['Drawdown'] = (data['close'] - data['RecentHigh']) / data['RecentHigh'] * 100
    return data

# Function to backtest the enhanced dynamic investment strategy
def backtest_dynamic_strategy(data, fixed_investment, drawdown_threshold=-5):
    """
    Backtest the enhanced dynamic investment strategy.
    - Initial buy when drawdown exceeds 5%.
    - Subsequent buys with increasing amounts when drawdown exceeds 5% from the previous noted drawdown value.
    - Fallback buy on the last working day of the month if no drawdown conditions are met.
    """
    # Calculate drawdown
    data = calculate_drawdown(data)
    # data.dropna(inplace=True)

    # Initialize portfolio
    portfolio = {
        'units': 0,
        'total_investment': 0,
        'portfolio_value': 0,
        'trades': []
    }

    # Extract unique year-month combinations from the data
    data['YearMonth'] = data['timestamp'].dt.to_period('M')
    unique_year_months = data['YearMonth'].unique()

    # Monthly investment process
    for year_month in unique_year_months:
        # Filter data for the current month
        month_data = data[data['YearMonth'] == year_month]

        # Initialize variables for the month
        first_drawdown = second_drawdown = third_drawdown = fourth_drawdown =None
        investments_made = 0

        # Check for drawdown conditions
        for date, row in month_data.iterrows():
            if first_drawdown is None and row['Drawdown'] < drawdown_threshold*2:
                # Initial buy: Invest the fixed amount
                units_bought = (2 * fixed_investment) / row['close']
                portfolio['units'] += units_bought
                portfolio['total_investment'] += fixed_investment * 2
                first_drawdown = row['Drawdown']
                investments_made += 1

                # Record the trade
                portfolio['trades'].append({
                    'Date': row['timestamp'],
                    'Price': row['close'],
                    'Units': units_bought,
                    'Drawdown': first_drawdown,
                    'total_investment': portfolio['total_investment'],
                    'portfolio_value': int(portfolio['units'] * row['close']),
                    'Type': 'Dynamic (2x)'
                })
            elif first_drawdown is not None and second_drawdown is None and row['Drawdown'] < first_drawdown-4:
                # Subsequent buy: Invest twice the fixed amount
                units_bought = (3 * fixed_investment) / row['close']
                portfolio['units'] += units_bought
                portfolio['total_investment'] += 3 * fixed_investment
                second_drawdown = row['Drawdown']
                investments_made += 1

                # Record the trade
                portfolio['trades'].append({
                    'Date': row['timestamp'],
                    'Price': row['close'],
                    'Units': units_bought,
                    'Drawdown': second_drawdown,
                    'total_investment': portfolio['total_investment'],
                    'portfolio_value': int(portfolio['units'] * row['close']),
                    'Type': 'Dynamic (3x)'
                })
            elif second_drawdown is not None and third_drawdown is None and row['Drawdown'] < second_drawdown - 4:
                # Subsequent buy: Invest thrice the fixed amount
                units_bought = 5 * fixed_investment / row['close']
                portfolio['units'] += units_bought
                portfolio['total_investment'] += fixed_investment * 5
                third_drawdown = row['Drawdown']
                investments_made += 1

                # Record the trade
                portfolio['trades'].append({
                    'Date': row['timestamp'],
                    'Price': row['close'],
                    'Units': units_bought,
                    'Drawdown': third_drawdown,
                    'total_investment': portfolio['total_investment'],
                    'portfolio_value': int(portfolio['units'] * row['close']),
                    'Type': 'Dynamic (5x)'
                })
            elif third_drawdown is not None and fourth_drawdown is None and row['Drawdown'] < third_drawdown - 4:
                # Subsequent buy: Invest thrice the fixed amount
                units_bought = 7 * fixed_investment / row['close']
                portfolio['units'] += units_bought
                portfolio['total_investment'] += fixed_investment * 7
                fourth_drawdown = row['Drawdown']
                investments_made += 1

                # Record the trade
                portfolio['trades'].append({
                    'Date': row['timestamp'],
                    'Price': row['close'],
                    'Units': units_bought,
                    'Drawdown': fourth_drawdown,
                    'total_investment': portfolio['total_investment'],
                    'portfolio_value': int(portfolio['units'] * row['close']),
                    'Type': 'Dynamic (7x)'
                })

        # Fallback buy: Invest the fixed amount on the last working day of the month if no investments were made
        if investments_made == 0:
            last_working_day = month_data['timestamp'].max()
            last_working_row = month_data[month_data['timestamp'] == last_working_day].iloc[0]

            units_bought = fixed_investment / last_working_row['close']
            portfolio['units'] += units_bought
            portfolio['total_investment'] += fixed_investment

            # Record the trade
            portfolio['trades'].append({
                'Date': last_working_row['timestamp'],
                'Price': last_working_row['close'],
                'Units': units_bought,
                'total_investment': portfolio['total_investment'],
                'portfolio_value': int(portfolio['units'] * last_working_row['close']),
                'Type': 'Fallback (1x)'
            })

    # Calculate final portfolio value
    portfolio['portfolio_value'] = portfolio['units'] * data['close'].iloc[-1]

    # Calculate CAGR
    start_date = data['timestamp'].min()
    end_date = data['timestamp'].max()
    num_years = (end_date - start_date).days / 365.25
    portfolio['cagr'] = (portfolio['portfolio_value'] / portfolio['total_investment']) ** (1 / num_years) - 1

    # Calculate average buy price
    portfolio['average_buy_price'] = portfolio['total_investment'] / portfolio['units']

    return portfolio

# Function to backtest the fixed investment strategy
def backtest_fixed_strategy(data, fixed_investment, investment_day=15):
    """
    Backtest the fixed investment strategy.
    - Invest a fixed amount on the specified investment day (or next available trading day).
    """
    # Initialize portfolio
    portfolio = {
        'units': 0,
        'total_investment': 0,
        'portfolio_value': 0,
        'trades': []
    }

    # Extract unique year-month combinations from the data
    data['YearMonth'] = data['timestamp'].dt.to_period('M')
    unique_year_months = data['YearMonth'].unique()

    # Monthly investment process
    for year_month in unique_year_months:
        # Convert YearMonth back to datetime
        month_start = year_month.start_time

        # Find the investment day for the current month (keep as tz-naive to match data)
        investment_date = pd.Timestamp(month_start.replace(day=investment_day))
        if investment_date not in data['timestamp'].values:
            # Find the next available trading day after the investment date
            next_available_dates = data[data['timestamp'] > investment_date]
            if len(next_available_dates) == 0:
                # If no next available date, skip the month (should not happen)
                continue
            investment_date = next_available_dates['timestamp'].iloc[0]

        # Get the row for the investment date
        investment_row = data[data['timestamp'] == investment_date]
        if len(investment_row) == 0:
            continue

        # Invest the fixed amount
        units_bought = fixed_investment / investment_row['close'].iloc[0]
        portfolio['units'] += units_bought
        portfolio['total_investment'] += fixed_investment

        # Record the trade
        portfolio['trades'].append({
            'Date': investment_row['timestamp'].iloc[0],
            'Price': investment_row['close'].iloc[0],
            'Units': units_bought,
            'total_investment': portfolio['total_investment'],
            'portfolio_value': int(portfolio['units'] * investment_row['close'].iloc[0]),
            'Type': 'Fixed'
        })

    # Calculate final portfolio value
    portfolio['portfolio_value'] = portfolio['units'] * data['close'].iloc[-1]

    # Calculate CAGR
    start_date = data['timestamp'].min()
    end_date = data['timestamp'].max()
    num_years = (end_date - start_date).days / 365.25
    portfolio['cagr'] = (portfolio['portfolio_value'] / portfolio['total_investment']) ** (1 / num_years) - 1

    # Calculate average buy price
    portfolio['average_buy_price'] = portfolio['total_investment'] / portfolio['units']

    return portfolio

# Function to fetch data from SQL Server
def fetch_data_from_db(table_name, start_date, end_date):
    """
    Fetch data from the SQL Server database.
    Replace this with your actual function to fetch data.
    """
    query = f"SELECT * FROM public.\"{table_name}\" WHERE timestamp BETWEEN '{start_date}' AND '{end_date}' ORDER BY timestamp ASC"
    data = rd.get_table_data(query=query)
    # Ensure timestamp is datetime (tz-naive, consistent with stored data)
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    return data

# Streamlit app
def strategy_tester():
    # Set page title and description
    st.title("📈 Systematic Investment Plan (SIP) Strategy Backtester")
    st.write("""
    This app backtests a SIP strategy with fixed and dynamic investments. 
    You can fetch data from your SQL Server database, set strategy parameters, and visualize the results.
    """)

    # Sidebar for inputs
    st.sidebar.header("⚙️ Input Parameters")

    # Input parameters for fetching data
    with st.sidebar.expander("📂 Database Parameters"):
        all_stocks_df = rd.get_table_data(selected_table='ALL_STOCKS', selected_database='nsedata')
        table_name = st.selectbox("Table Name", options=all_stocks_df['SYMBOL'].unique(), index=0)
        start_date = st.date_input("Start Date (YYYY-MM-DD)", value=dt.date.today() - dt.timedelta(days=3650))
        end_date = st.date_input("End Date (YYYY-MM-DD)", max_value=dt.date.today())

    # Fetch data from the database
    if st.sidebar.button("📥 Fetch Data"):
        with st.spinner("Fetching data from the database..."):
            data = fetch_data_from_db(table_name, start_date, end_date)
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            st.session_state.data = data  # Store data in session state
        st.success("Data fetched successfully!")
        st.write("Data Preview:")
        st.write(data.head())

    # Check if data is loaded
    if 'data' not in st.session_state:
        st.warning("Please fetch data from the database.")
        return

    data = st.session_state.data

    # Input parameters for the strategy
    with st.sidebar.expander("📊 Strategy Parameters"):
        fixed_investment = st.number_input("Fixed Investment Amount", value=5000, min_value=1)
        investment_day = st.number_input("Investment Day of the Month", value=15, min_value=1, max_value=31)
        drawdown_threshold = st.number_input("Drawdown Threshold (%)", value=-5.0, max_value=0.0)

    # Run strategies
    if st.sidebar.button("🚀 Run Strategies"):
        with st.spinner("Running strategies..."):
            # Backtest dynamic strategy
            dynamic_portfolio = backtest_dynamic_strategy(data, fixed_investment, drawdown_threshold)

            # Backtest fixed strategy
            fixed_portfolio = backtest_fixed_strategy(data, fixed_investment, investment_day)

            # Calculate benchmark index performance
            benchmark_return = (data['close'].iloc[-1] / data['close'].iloc[0]) - 1
            benchmark_cagr = (data['close'].iloc[-1] / data['close'].iloc[0]) ** (1 / ((data['timestamp'].iloc[-1] - data['timestamp'].iloc[0]).days / 365.25)) - 1

        # Display results
        st.header("📊 Strategy Results")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Investment (Dynamic)", f"₹{dynamic_portfolio['total_investment']:,.2f}")
        with col2:
            st.metric("Final Portfolio Value (Dynamic)", f"₹{dynamic_portfolio['portfolio_value']:,.2f}")
        with col3:
            st.metric("Strategy CAGR (Dynamic)", f"{dynamic_portfolio['cagr'] * 100:.2f}%")

        col4, col5, col6 = st.columns(3)
        with col4:
            st.metric("Total Investment (Fixed)", f"₹{fixed_portfolio['total_investment']:,.2f}")
        with col5:
            st.metric("Final Portfolio Value (Fixed)", f"₹{fixed_portfolio['portfolio_value']:,.2f}")
        with col6:
            st.metric("Strategy CAGR (Fixed)", f"{fixed_portfolio['cagr'] * 100:.2f}%")

        col7, col8, col9 = st.columns(3)
        with col7:
            st.metric("Buy & Hold Return", f"{benchmark_return * 100:.2f}%")
        with col8:
            st.metric("Buy & Hold CAGR", f"{benchmark_cagr * 100:.2f}%")

        # Display tradebooks
        st.header("📝 Tradebooks")
        st.subheader("Dynamic Strategy Tradebook")
        dynamic_tradebook = pd.DataFrame(dynamic_portfolio['trades'])
        st.dataframe(dynamic_tradebook)

        st.subheader("Fixed Strategy Tradebook")
        fixed_tradebook = pd.DataFrame(fixed_portfolio['trades'])
        st.dataframe(fixed_tradebook)

        # Plot performance comparison
        st.header("📈 Performance Comparison")
        data['DynamicPortfolioValue'] = data['close'] * dynamic_portfolio['units']
        data['FixedPortfolioValue'] = data['close'] * fixed_portfolio['units']
        data['BenchmarkValue'] = data['close'] / data['close'].iloc[0] * fixed_investment * len(data['YearMonth'].unique())

        st.line_chart(data.set_index('timestamp')[['DynamicPortfolioValue', 'FixedPortfolioValue', 'BenchmarkValue']])


if __name__ == "__main__":
    strategy_tester()
