import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from common_utils import read_write_sql_data as rd  # Assuming this is your SQL utility

# Set page config
st.set_page_config(
    page_title="Enhanced Stock Analysis Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS styling (unchanged from your code)
st.markdown("""
<style>
    .main-header { font-size: 36px; font-weight: bold; color: #1E88E5; text-align: center; margin-bottom: 20px; }
    .section-header { font-size: 24px; font-weight: bold; color: #0D47A1; margin-top: 20px; margin-bottom: 10px; }
    .insight-box { background-color: #E3F2FD; padding: 15px; border-radius: 5px; margin-bottom: 10px; }
    .positive { color: #4CAF50; font-weight: bold; }
    .negative { color: #F44336; font-weight: bold; }
    .neutral { color: #FF9800; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<div class="main-header">Enhanced Stock Analysis Dashboard</div>', unsafe_allow_html=True)


# Load and cache data
@st.cache_data
def load_data():
    df = rd.get_table_data(selected_table='AGG_DATA')
    # Ensure numeric columns are properly typed
    numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume', 'Pct_Chg_D', 'Percent_Chg_W',
                    'Percent_Chg_M', 'Percent_Chg_Y', 'Wk_EMA_13', 'Wk_EMA_52', 'Mth_EMA_20',
                    'High_52W', 'Low_52W', '3_Year_Returns', '5_Year_Returns', 'Max_Returns']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


# Load data
df = load_data()

# Sidebar for filtering
st.sidebar.markdown("## Filter Options")
view_mode = st.sidebar.radio("View Mode", ["Single Stock", "Portfolio Overview"])
if view_mode == "Single Stock":
    selected_symbol = st.sidebar.selectbox("Select Stock/Index", df['Symbol'].unique())
    filtered_df = df[df['Symbol'] == selected_symbol]
else:
    filtered_df = df  # Use full dataset for portfolio view

# Tabs for navigation
tab1, tab2, tab3, tab4 = st.tabs(
    ["Market Overview", "Technical Analysis", "Investment Opportunities", "Portfolio Insights"])

# --- Tab 1: Market Overview ---
with tab1:
    st.markdown('<div class="section-header">Market Summary</div>', unsafe_allow_html=True)

    if view_mode == "Single Stock":
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Current Price", f"₹{filtered_df['Close'].values[0]:.2f}",
                      f"{filtered_df['Pct_Chg_D'].values[0]:.2f}%")
        with col2:
            st.metric("Weekly Change", f"{filtered_df['Percent_Chg_W'].values[0]:.2f}%")
        with col3:
            st.metric("Monthly Change", f"{filtered_df['Percent_Chg_M'].values[0]:.2f}%")

        st.markdown(
            f"**52 Week Range:** ₹{filtered_df['Low_52W'].values[0]:.2f} - ₹{filtered_df['High_52W'].values[0]:.2f}")

        # Enhanced OHLC Chart
        st.markdown('<div class="section-header">Price Performance</div>', unsafe_allow_html=True)
        time_period = st.radio("Select Time Period", ["Daily", "Weekly", "Monthly", "Yearly"], horizontal=True)
        price_cols = {'Daily': ['Open', 'High', 'Low', 'Close'],
                      'Weekly': ['Open_W', 'High_W', 'Low_W', 'Close_W'],
                      'Monthly': ['Open_M', 'High_M', 'Low_M', 'Close_M'],
                      'Yearly': ['Open_Y', 'High_Y', 'Low_Y', 'Close_Y']}
        fig = go.Figure(data=[go.Candlestick(x=['Open', 'High', 'Low', 'Close'],
                                             open=[filtered_df[price_cols[time_period][0]].values[0]],
                                             high=[filtered_df[price_cols[time_period][1]].values[0]],
                                             low=[filtered_df[price_cols[time_period][2]].values[0]],
                                             close=[filtered_df[price_cols[time_period][3]].values[0]])])
        fig.update_layout(title=f"{selected_symbol} - {time_period} OHLC", height=400)
        st.plotly_chart(fig, use_container_width=True)

    else:  # Portfolio Overview
        st.write("Top Performers (Weekly)")
        top_weekly = df.nlargest(5, 'Percent_Chg_W')[['Symbol', 'Close', 'Percent_Chg_W']]
        st.dataframe(top_weekly)
        fig = px.bar(top_weekly, x='Symbol', y='Percent_Chg_W', title="Top 5 Weekly Performers")
        st.plotly_chart(fig)

# --- Tab 2: Technical Analysis ---
with tab2:
    st.markdown('<div class="section-header">Technical Analysis</div>', unsafe_allow_html=True)

    if view_mode == "Single Stock":
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Moving Averages")
            current_price = filtered_df['Close'].values[0]
            wk_ema_13, wk_ema_52 = filtered_df['Wk_EMA_13'].values[0], filtered_df['Wk_EMA_52'].values[0]
            st.markdown(f"Weekly EMA (13): **₹{wk_ema_13:.2f}**")
            st.markdown(f"Weekly EMA (52): **₹{wk_ema_52:.2f}**")
            if current_price > wk_ema_13 > wk_ema_52:
                st.markdown('<div class="insight-box"><span class="positive">BULLISH</span>: Golden Cross</div>',
                            unsafe_allow_html=True)
            elif current_price < wk_ema_13 < wk_ema_52:
                st.markdown('<div class="insight-box"><span class="negative">BEARISH</span>: Death Cross</div>',
                            unsafe_allow_html=True)
            else:
                st.markdown('<div class="insight-box"><span class="neutral">NEUTRAL</span>: Mixed Signals</div>',
                            unsafe_allow_html=True)

        with col2:
            st.markdown("#### RSI Approximation (Using 6W Range)")
            high_6w, low_6w = filtered_df['High_6W'].values[0], filtered_df['Low_6W'].values[0]
            rsi_approx = 100 - (100 / (1 + ((current_price - low_6w) / max(high_6w - current_price, 0.01))))
            st.markdown(f"RSI (Approx): **{rsi_approx:.2f}**")
            if rsi_approx > 70:
                st.markdown('<div class="insight-box"><span class="negative">OVERBOUGHT</span></div>',
                            unsafe_allow_html=True)
            elif rsi_approx < 30:
                st.markdown('<div class="insight-box"><span class="positive">OVERSOLD</span></div>',
                            unsafe_allow_html=True)
            else:
                st.markdown('<div class="insight-box"><span class="neutral">NEUTRAL</span></div>',
                            unsafe_allow_html=True)

        # Enhanced Trading Signals
        st.markdown('<div class="section-header">Trading Signals</div>', unsafe_allow_html=True)
        signals = []
        if wk_ema_13 > wk_ema_52 and current_price > wk_ema_13:
            signals.append({"signal": "BUY", "desc": "Golden Cross with price confirmation", "strength": "High"})
        if rsi_approx < 30 and current_price > filtered_df['Low_6W'].values[0]:
            signals.append({"signal": "BUY", "desc": "Oversold with potential bounce", "strength": "Medium"})
        if rsi_approx > 70 and current_price < filtered_df['High_6W'].values[0]:
            signals.append({"signal": "SELL", "desc": "Overbought with pullback risk", "strength": "Medium"})
        for signal in signals:
            st.markdown(
                f'<div class="insight-box"><span class="{"positive" if signal["signal"] == "BUY" else "negative"}">{signal["signal"]}</span>: {signal["desc"]} (Strength: {signal["strength"]})</div>',
                unsafe_allow_html=True)

    else:  # Portfolio Overview
        st.write("Stocks with Bullish Signals")
        bullish = df[(df['Wk_EMA_13'] > df['Wk_EMA_52']) & (df['Close'] > df['Wk_EMA_13'])][
            ['Symbol', 'Close', 'Wk_EMA_13', 'Wk_EMA_52']]
        st.dataframe(bullish)

# --- Tab 3: Investment Opportunities ---
with tab3:
    st.markdown('<div class="section-header">Investment Opportunities</div>', unsafe_allow_html=True)

    if view_mode == "Single Stock":
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Performance Metrics")
            metrics = pd.DataFrame({
                'Period': ['1 Year', '3 Years', '5 Years'],
                'Return (%)': [filtered_df['Percent_Chg_Y'].values[0], filtered_df['3_Year_Returns'].values[0],
                               filtered_df['5_Year_Returns'].values[0]]
            })
            st.dataframe(metrics.style.apply(lambda x: ['color: green' if v > 0 else 'color: red' for v in x],
                                             subset=['Return (%)']))

        with col2:
            st.markdown("#### Investment Score")
            score = sum([filtered_df['Percent_Chg_Y'].values[0] > 0, filtered_df['5_Year_Returns'].values[0] > 50,
                         filtered_df['Wk_EMA_13'].values[0] > filtered_df['Wk_EMA_52'].values[0]])
            rating = "BUY" if score >= 2 else ("HOLD" if score == 1 else "SELL")
            st.markdown(
                f'<div class="insight-box"><span class="{"positive" if rating == "BUY" else "negative" if rating == "SELL" else "neutral"}">{rating}</span> (Score: {score}/3)</div>',
                unsafe_allow_html=True)

    else:  # Portfolio Overview
        st.write("Top Investment Picks")
        invest_picks = df[(df['Percent_Chg_Y'] > 0) & (df['5_Year_Returns'] > 50)][
            ['Symbol', 'Close', 'Percent_Chg_Y', '5_Year_Returns']]
        st.dataframe(invest_picks)

# --- Tab 4: Portfolio Insights ---
with tab4:
    st.markdown('<div class="section-header">Portfolio Insights</div>', unsafe_allow_html=True)
    if view_mode == "Single Stock":
        st.write("Switch to Portfolio Overview for aggregated insights.")
    else:
        # Risk-Return Scatter
        fig = px.scatter(df, x='Range_M', y='Percent_Chg_Y', text='Symbol', size='Volume_M',
                         title="Risk (Monthly Range) vs Return (Yearly)", hover_data=['Close'])
        fig.update_traces(textposition='top center')
        st.plotly_chart(fig)

        # Correlation Heatmap (simplified)
        corr_cols = ['Close', 'Percent_Chg_W', 'Percent_Chg_M', 'Percent_Chg_Y']
        corr_matrix = df[corr_cols].corr()
        fig = px.imshow(corr_matrix, text_auto=True, title="Correlation Across Stocks")
        st.plotly_chart(fig)
