import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import sqlalchemy
import plotly.graph_objs as go
from nsepython import *
import datetime
import pytz

# Configuration and Setup
st.set_page_config(page_title="Market Summary Dashboard", layout="wide")


# Database Connection (Replace with your actual SQL Server connection details)
def get_db_connection():
    # Example connection string - modify according to your SQL Server setup
    connection_string = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
    return sqlalchemy.create_engine(connection_string)


# 1. Fetch NIFTY Indices Live Data
def get_nifty_indices():
    try:
        # NSE Python module to fetch live indices
        nifty50 = nse_get_index_quote('NIFTY 50')
        nifty500 = nse_get_index_quote('NIFTY 500')
        niftymidcap = nse_get_index_quote('NIFTY MIDCAP 150')
        niftysmallcap = nse_get_index_quote('NIFTY SMALLCAP 250')

        return {
            'NIFTY 50': nifty50['lastPrice'],
            'NIFTY 500': nifty500['lastPrice'],
            'NIFTY MIDCAP': niftymidcap['lastPrice'],
            'NIFTY SMALLCAP': niftysmallcap['lastPrice']
        }
    except Exception as e:
        st.error(f"Error fetching Nifty indices: {e}")
        return {}


# 2. Fetch Portfolio Holdings
def get_portfolio_holdings():
    try:
        # Connect to database and fetch portfolio
        engine = get_db_connection()
        query = """
        SELECT 
            Symbol, 
            Quantity, 
            Avg_Cost_Price, 
            (SELECT LastPrice FROM LiveStockData WHERE Symbol = Portfolio.Symbol) AS CurrentPrice,
            Quantity * (SELECT LastPrice FROM LiveStockData WHERE Symbol = Portfolio.Symbol) AS CurrentValue,
            Quantity * Avg_Cost_Price AS InvestedValue,
            (Quantity * (SELECT LastPrice FROM LiveStockData WHERE Symbol = Portfolio.Symbol) - 
             Quantity * Avg_Cost_Price) AS ProfitLoss
        FROM Portfolio
        """
        portfolio_df = pd.read_sql(query, engine)
        return portfolio_df
    except Exception as e:
        st.error(f"Error fetching portfolio: {e}")
        return pd.DataFrame()


# 3. Gold Price and Chart
def get_gold_data():
    try:
        # Fetch Gold ETF data (using GOLDBEES as a proxy)
        gold_data = yf.download('GOLDBEES.NS', period='1y')

        # Create plotly chart
        fig = go.Figure(data=[go.Candlestick(
            x=gold_data.index,
            open=gold_data['Open'],
            high=gold_data['High'],
            low=gold_data['Low'],
            close=gold_data['Close']
        )])
        fig.update_layout(
            title='Gold Price Trend (Last 365 Days)',
            xaxis_title='Date',
            yaxis_title='Price',
            height=300,
            width=400
        )

        return gold_data['Close'][-1], fig
    except Exception as e:
        st.error(f"Error fetching gold data: {e}")
        return None, None


# 4. NSE Indices and Sectors
def get_nse_indices_and_sectors():
    try:
        # Fetch all NSE indices
        indices = nse_get_all_indices()
        return indices
    except Exception as e:
        st.error(f"Error fetching indices: {e}")
        return {}


# 5. FII/DII Data
def get_fii_dii_data():
    try:
        # This would typically involve fetching from SEBI or a financial API
        # Placeholder implementation
        return {
            'FII Buy': 'Data Not Available',
            'FII Sell': 'Data Not Available',
            'DII Buy': 'Data Not Available',
            'DII Sell': 'Data Not Available'
        }
    except Exception as e:
        st.error(f"Error fetching FII/DII data: {e}")
        return {}


# 6. Market Breadth
def get_market_breadth():
    try:
        # This requires real-time data from NSE or a market data provider
        # Placeholder implementation
        return {
            'Advances': 'Data Not Available',
            'Declines': 'Data Not Available',
            'Unchanged': 'Data Not Available'
        }
    except Exception as e:
        st.error(f"Error fetching market breadth: {e}")
        return {}


# Main Streamlit App
def main():
    st.title("Market Summary Dashboard")

    # Create columns for main sections
    col1, col2 = st.columns([3, 1])

    with col1:
        # 1. NIFTY Indices Metrics
        st.subheader("Nifty Indices")
        indices_data = get_nifty_indices()

        indices_cols = st.columns(4)
        for i, (name, value) in enumerate(indices_data.items()):
            indices_cols[i].metric(name, f"₹{value:,.2f}")

        # 2. Portfolio Holdings
        st.subheader("Portfolio Holdings")
        portfolio_df = get_portfolio_holdings()
        if not portfolio_df.empty:
            st.dataframe(portfolio_df, use_container_width=True)

    with col2:
        # 3. Gold Price and Chart
        st.subheader("Gold Price")
        gold_price, gold_chart = get_gold_data()
        if gold_price:
            st.metric("Gold Price", f"₹{gold_price:,.2f}")
        if gold_chart:
            st.plotly_chart(gold_chart, use_container_width=True)

    # Create another row of columns
    col3, col4, col5 = st.columns(3)

    with col3:
        # 4. NSE Indices
        st.subheader("NSE Indices")
        indices = get_nse_indices_and_sectors()
        for index, details in indices.items():
            st.text(f"{index}: {details.get('lastPrice', 'N/A')}")

    with col4:
        # 5. FII/DII Data
        st.subheader("FII/DII Data")
        fii_dii_data = get_fii_dii_data()
        for key, value in fii_dii_data.items():
            st.metric(key, value)

    with col5:
        # 6. Market Breadth
        st.subheader("Market Breadth")
        market_breadth = get_market_breadth()
        for key, value in market_breadth.items():
            st.metric(key, value)


# Run the app
if __name__ == "__main__":
    main()

# Additional Requirements (to be installed):
# pip install streamlit nsepython sqlalchemy yfinance plotly pyodbc

"""
Note: This script requires several configurations:
1. Install required libraries
2. Configure SQL Server connection string
3. Ensure you have the necessary data tables in your database
4. Replace placeholder implementations for FII/DII and Market Breadth

Recommended database table structures:
- Portfolio Table:
    Symbol (VARCHAR)
    Quantity (INT)
    Avg_Cost_Price (DECIMAL)

- LiveStockData Table:
    Symbol (VARCHAR)
    LastPrice (DECIMAL)
"""