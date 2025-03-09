import streamlit as st
import yfinance as yf
import plotly.graph_objs as go
import pandas_ta as ta  # Import pandas_ta for technical indicators
from datetime import datetime
from plotly.subplots import make_subplots  # Import for creating subplots

# Set the title of the app
st.title('yfinance Candlestick Charts')

# Input fields in the sidebar
symbol = st.sidebar.text_input("Enter Stock Symbol (e.g., RELIANCE.NS)", "RELIANCE.NS")
start_date = st.sidebar.date_input("Start Date", datetime(2023, 1, 1))
end_date = st.sidebar.date_input("End Date", datetime.today())
timeframe = st.sidebar.selectbox("Select Timeframe", ["1m", "5m", "15m", "60m", "1d", "1wk", "1mo"], index=4)

# Additional inputs for indicators
show_rsi = st.sidebar.checkbox("Show RSI", value=True)
rsi_period = st.sidebar.number_input("RSI Lookback Period", min_value=1, max_value=100, value=14) if show_rsi else None

show_ema10 = st.sidebar.checkbox("Show EMA-10", value=True)
ema10_period = st.sidebar.number_input("EMA-10 Lookback Period", min_value=1, max_value=100,
                                       value=10) if show_ema10 else None

show_ema20 = st.sidebar.checkbox("Show EMA-20", value=True)
ema20_period = st.sidebar.number_input("EMA-20 Lookback Period", min_value=1, max_value=100,
                                       value=20) if show_ema20 else None

# Inputs for Supertrend indicator
show_supertrend = st.sidebar.checkbox("Show Supertrend", value=True)
supertrend_factor = st.sidebar.number_input("Supertrend Factor/Multiplier", min_value=1, max_value=10,
                                            value=3) if show_supertrend else None
supertrend_atr_period = st.sidebar.number_input("Supertrend ATR Period", min_value=1, max_value=100,
                                                value=10) if show_supertrend else None

generate_chart = st.sidebar.button("Get Charts")

# Chart display in the main area
if generate_chart:
    # Fetch the stock data
    data = yf.download(symbol, start=start_date, end=end_date, interval=timeframe)

    # Check if the data is not empty
    if not data.empty:
        # Calculate indicators using pandas_ta
        if show_rsi:
            data['RSI'] = ta.rsi(data['Close'], length=rsi_period)
        if show_ema10:
            data['EMA-10'] = ta.ema(data['Close'], length=ema10_period)
        if show_ema20:
            data['EMA-20'] = ta.ema(data['Close'], length=ema20_period)
        if show_supertrend:
            supertrend = ta.supertrend(data['High'], data['Low'], data['Close'], length=supertrend_atr_period,
                                       multiplier=supertrend_factor)
            data['Supertrend'] = supertrend['SUPERT_10_3.0']
            data['Supertrend Direction'] = supertrend['SUPERTd_10_3.0']

        # Get the latest values of indicators
        latest_rsi = data['RSI'].iloc[-1] if show_rsi else None
        latest_ema10 = data['EMA-10'].iloc[-1] if show_ema10 else None
        latest_ema20 = data['EMA-20'].iloc[-1] if show_ema20 else None
        latest_supertrend = data['Supertrend'].iloc[-1] if show_supertrend else None

        # Display metrics in a card format
        with st.container():
            cols = st.columns(4)
            if show_rsi:
                cols[0].metric(label=f"RSI-{rsi_period}", value=f"{latest_rsi:.2f}")
            if show_ema10:
                cols[1].metric(label=f"EMA-{ema10_period}", value=f"{latest_ema10:.2f}")
            if show_ema20:
                cols[2].metric(label=f"EMA-{ema20_period}", value=f"{latest_ema20:.2f}")
            if show_supertrend:
                cols[3].metric(label="Supertrend", value=f"{latest_supertrend:.2f}")

        # Create subplots: rows=2, cols=1
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            vertical_spacing=0.1, subplot_titles=('Candlestick', 'RSI'))

        # Candlestick chart in the first row
        fig.add_trace(go.Candlestick(x=data.index,
                                     open=data['Open'],
                                     high=data['High'],
                                     low=data['Low'],
                                     close=data['Close'],
                                     name='Candlestick'), row=1, col=1)

        # Add EMA-10 to the candlestick chart if selected
        if show_ema10:
            fig.add_trace(go.Scatter(x=data.index, y=data['EMA-10'],
                                     mode='lines', name=f'EMA-{ema10_period}',
                                     line=dict(color='orange')), row=1, col=1)

        # Add EMA-20 to the candlestick chart if selected
        if show_ema20:
            fig.add_trace(go.Scatter(x=data.index, y=data['EMA-20'],
                                     mode='lines', name=f'EMA-{ema20_period}',
                                     line=dict(color='blue')), row=1, col=1)

        # Add Supertrend to the candlestick chart if selected
        if show_supertrend:
            fig.add_trace(go.Scatter(x=data.index, y=data['Supertrend'],
                                     mode='lines', name='Supertrend',
                                     line=dict(color='green')), row=1, col=1)

        # Add RSI to the second row
        if show_rsi:
            fig.add_trace(go.Scatter(x=data.index, y=data['RSI'],
                                     mode='lines', name=f'RSI-{rsi_period}',
                                     line=dict(color='red')), row=2, col=1)

        # Update layout for the chart
        fig.update_layout(
            title=f'Candlestick chart for {symbol}',
            xaxis_title='Date',
            yaxis_title='Price',
            template='plotly_dark',
            width=1200,  # Set chart width
            height=800,  # Set chart height
            xaxis2=dict(nticks=10),  # Reduce the number of x-axis labels to 10 ticks in the RSI chart
            yaxis2=dict(title='RSI', range=[0, 100]),  # Set RSI range to 0-100
            xaxis_rangeslider_visible=False,  # Remove range slider for the first x-axis
            xaxis2_rangeslider_visible=False  # Remove range slider for the second x-axis
        )

        # Display the chart
        st.plotly_chart(fig, use_container_width=False)  # Enforce the exact chart size
    else:
        st.write("No data available for the selected dates.")