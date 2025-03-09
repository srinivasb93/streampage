import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import plotly.graph_objects as go
from datetime import datetime, timedelta


def calculate_drawdown(data):
    """Calculate drawdown from peak for the series."""
    rolling_max = data.cummax()
    drawdown = (data - rolling_max) / rolling_max * 100
    return drawdown


def implement_dynamic_strategy(prices, monthly_investment, start_date, end_date):
    """Implement the dynamic investment strategy."""
    try:
        # Initialize variables
        investments = []
        total_investment = 0
        total_units = 0

        # Ensure prices index is datetime
        prices.index = pd.to_datetime(prices.index)

        # Get monthly prices
        monthly_prices = prices.resample('M').last()

        # Convert input dates to datetime
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        # Filter data for date range
        monthly_prices = monthly_prices[
            (monthly_prices.index >= start_dt) &
            (monthly_prices.index <= end_dt)
            ]

        last_drawdown = 0

        for date, price in monthly_prices.items():
            # Calculate current drawdown using all available data up to this point
            price_series = prices[prices.index <= date]
            peak = price_series.max()
            current_drawdown = ((price - peak) / peak) * 100

            # Initialize month's investment
            month_investment = monthly_investment
            month_units = 0

            # Check drawdown conditions
            if current_drawdown <= -5:
                # First investment
                month_units += month_investment / price

                # Check for second investment
                if current_drawdown <= (last_drawdown - 5):
                    additional_investment = 2 * monthly_investment
                    month_investment += additional_investment
                    month_units += additional_investment / price

                    # Check for third investment
                    if current_drawdown <= (last_drawdown - 5):
                        additional_investment = 3 * monthly_investment
                        month_investment += additional_investment
                        month_units += additional_investment / price

            # If no drawdown conditions met, invest fixed amount
            if month_units == 0:
                month_units = monthly_investment / price
                month_investment = monthly_investment

            # Update running totals
            total_investment += month_investment
            total_units += month_units

            # Store last drawdown for next iteration
            last_drawdown = current_drawdown

            # Record investment details
            investments.append({
                'Date': date,
                'Price': price,
                'Investment': month_investment,
                'Units': month_units,
                'Total_Investment': total_investment,
                'Total_Units': total_units,
                'Portfolio_Value': total_units * price,
                'Drawdown': current_drawdown
            })

        return pd.DataFrame(investments)

    except Exception as e:
        st.error(f"Error in dynamic strategy implementation: {str(e)}")
        return pd.DataFrame()


def implement_fixed_strategy(prices, monthly_investment, start_date, end_date):
    """Implement fixed monthly investment strategy."""
    try:
        investments = []
        total_investment = 0
        total_units = 0

        # Ensure prices index is datetime
        prices.index = pd.to_datetime(prices.index)

        # Get monthly prices
        monthly_prices = prices.resample('M').last()

        # Convert input dates to datetime
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        # Filter data for date range
        monthly_prices = monthly_prices[
            (monthly_prices.index >= start_dt) &
            (monthly_prices.index <= end_dt)
            ]

        for date, price in monthly_prices.items():
            units = monthly_investment / price
            total_investment += monthly_investment
            total_units += units

            investments.append({
                'Date': date,
                'Price': price,
                'Investment': monthly_investment,
                'Units': units,
                'Total_Investment': total_investment,
                'Total_Units': total_units,
                'Portfolio_Value': total_units * price
            })

        return pd.DataFrame(investments)

    except Exception as e:
        st.error(f"Error in fixed strategy implementation: {str(e)}")
        return pd.DataFrame()


# Streamlit app
st.title('Investment Strategy Comparison')

# Sidebar inputs
st.sidebar.header('Input Parameters')
ticker = st.sidebar.text_input('Enter Stock/Index Symbol (e.g., ^NSEI for Nifty):', '^NSEI')

# Date inputs
min_date = datetime(2000, 1, 1)
max_date = datetime.now()

start_date = st.sidebar.date_input(
    'Start Date:',
    value=datetime.now() - timedelta(days=365 * 5),
    min_value=min_date,
    max_value=max_date
)

end_date = st.sidebar.date_input(
    'End Date:',
    value=datetime.now(),
    min_value=start_date,
    max_value=max_date
)

monthly_investment = st.sidebar.number_input(
    'Monthly Investment Amount:',
    min_value=1000,
    value=10000,
    step=1000
)

# Fetch and process data
if ticker:
    try:
        # Download data
        data = yf.download(ticker, start=start_date, end=end_date)

        if not data.empty:
            # Calculate strategies
            dynamic_results = implement_dynamic_strategy(data['Close'], monthly_investment, start_date, end_date)
            fixed_results = implement_fixed_strategy(data['Close'], monthly_investment, start_date, end_date)

            if not dynamic_results.empty and not fixed_results.empty:
                # Create comparison plot
                fig = go.Figure()

                # Dynamic strategy
                fig.add_trace(go.Scatter(
                    x=dynamic_results['Date'],
                    y=dynamic_results['Portfolio_Value'],
                    name='Dynamic Strategy',
                    line=dict(color='blue')
                ))

                # Fixed strategy
                fig.add_trace(go.Scatter(
                    x=fixed_results['Date'],
                    y=fixed_results['Portfolio_Value'],
                    name='Fixed Strategy',
                    line=dict(color='green')
                ))

                # Index performance
                normalized_index = data['Close'] * (monthly_investment / data['Close'].iloc[0])
                fig.add_trace(go.Scatter(
                    x=data.index,
                    y=normalized_index,
                    name='Index Performance',
                    line=dict(color='red')
                ))

                fig.update_layout(
                    title='Portfolio Value Comparison',
                    xaxis_title='Date',
                    yaxis_title='Portfolio Value',
                    hovermode='x unified'
                )

                st.plotly_chart(fig)

                # Display statistics
                st.header('Performance Statistics')

                col1, col2, col3 = st.columns(3)

                with col1:
                    st.subheader('Dynamic Strategy')
                    st.write(f"Final Value: ₹{dynamic_results['Portfolio_Value'].iloc[-1]:,.2f}")
                    st.write(f"Total Investment: ₹{dynamic_results['Total_Investment'].iloc[-1]:,.2f}")
                    roi = ((dynamic_results['Portfolio_Value'].iloc[-1] / dynamic_results['Total_Investment'].iloc[
                        -1]) - 1) * 100
                    st.write(f"ROI: {roi:.2f}%")

                with col2:
                    st.subheader('Fixed Strategy')
                    st.write(f"Final Value: ₹{fixed_results['Portfolio_Value'].iloc[-1]:,.2f}")
                    st.write(f"Total Investment: ₹{fixed_results['Total_Investment'].iloc[-1]:,.2f}")
                    roi = ((fixed_results['Portfolio_Value'].iloc[-1] / fixed_results['Total_Investment'].iloc[
                        -1]) - 1) * 100
                    st.write(f"ROI: {roi:.2f}%")

                with col3:
                    st.subheader('Index Performance')
                    st.write(f"Final Value: ₹{normalized_index.iloc[-1]:,.2f}")
                    st.write(f"Total Investment: ₹{monthly_investment * len(fixed_results):,.2f}")
                    roi = ((normalized_index.iloc[-1] / (monthly_investment * len(fixed_results))) - 1) * 100
                    st.write(f"ROI: {roi:.2f}%")

                # Display investment details
                st.header('Monthly Investment Details')
                st.dataframe(dynamic_results)

        else:
            st.error('No data available for the selected ticker symbol.')

    except Exception as e:
        st.error(f'Error occurred: {str(e)}')
else:
    st.warning('Please enter a valid ticker symbol.')