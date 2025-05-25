import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from common_utils import read_write_sql_data as rd

# Set page config
st.set_page_config(
    page_title="Stock Analysis Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS styling
st.markdown("""
<style>
    .main-header {
        font-size: 36px;
        font-weight: bold;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 20px;
    }
    .section-header {
        font-size: 24px;
        font-weight: bold;
        color: #0D47A1;
        margin-top: 20px;
        margin-bottom: 10px;
    }
    .insight-box {
        background-color: #E3F2FD;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 10px;
    }
    .positive {
        color: #4CAF50;
        font-weight: bold;
    }
    .negative {
        color: #F44336;
        font-weight: bold;
    }
    .neutral {
        color: #FF9800;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<div class="main-header">Stock Analysis Dashboard</div>', unsafe_allow_html=True)


# Load data
@st.cache_data
def load_data():
    df = rd.get_table_data(selected_table='AGG_DATA')

    # Convert date columns if needed (dates seem to be in a non-standard format)
    # This may need adjustment based on actual data format
    date_columns = ['Date', 'ATH_Date', 'ATL_Date', 'High_52W_Date', 'Low_52W_Date']
    # for col in date_columns:
    #     try:
    #         df[col] = pd.to_datetime(df[col])
    #     except:
    #         pass  # Handle non-standard date formats

    return df


# Load the data
df = load_data()

# Sidebar for filtering
st.sidebar.markdown("## Filter Options")
selected_symbol = st.sidebar.selectbox("Select Stock/Index", df['Symbol'].unique())

# Filter data based on selection
filtered_df = df[df['Symbol'] == selected_symbol]

# Main dashboard layout with tabs
tab1, tab2, tab3, tab4 = st.tabs(
    ["Market Overview", "Technical Analysis", "Investment Opportunities", "Interpretation"])

with tab1:
    st.markdown('<div class="section-header">Market Summary</div>', unsafe_allow_html=True)

    # Stock overview card
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="Current Price",
            value=f"₹{filtered_df['Close'].values[0]:.2f}",
            delta=f"{filtered_df['Pct_Chg_D'].values[0]:.2f}%"
        )

    with col2:
        weekly_change = filtered_df['Percent_Chg_W'].values[0]
        st.metric(
            label="Weekly Change",
            value=f"{weekly_change:.2f}%",
            delta=f"{weekly_change:.2f}%"
        )

    with col3:
        monthly_change = filtered_df['Percent_Chg_M'].values[0]
        st.metric(
            label="Monthly Change",
            value=f"{monthly_change:.2f}%",
            delta=f"{monthly_change:.2f}%"
        )

    # 52 Week Range
    st.markdown(
        f"**52 Week Range:** ₹{filtered_df['Low_52W'].values[0]:.2f} - ₹{filtered_df['High_52W'].values[0]:.2f}")

    # Price chart with time frames
    st.markdown('<div class="section-header">Price Performance</div>', unsafe_allow_html=True)

    # Create sample time series data for visualization
    # This would be replaced with actual historical data in a real application
    time_periods = ['Daily', 'Weekly', 'Monthly', 'Yearly']
    selected_period = st.radio("Select Time Period", time_periods, horizontal=True)

    if selected_period == 'Daily':
        price_data = {
            'Price': [filtered_df['Open'].values[0], filtered_df['High'].values[0],
                      filtered_df['Low'].values[0], filtered_df['Close'].values[0]],
            'Volume': filtered_df['Volume'].values[0]
        }
    elif selected_period == 'Weekly':
        price_data = {
            'Price': [filtered_df['Open_W'].values[0], filtered_df['High_W'].values[0],
                      filtered_df['Low_W'].values[0], filtered_df['Close_W'].values[0]],
            'Volume': filtered_df['Volume_W'].values[0]
        }
    elif selected_period == 'Monthly':
        price_data = {
            'Price': [filtered_df['Open_M'].values[0], filtered_df['High_M'].values[0],
                      filtered_df['Low_M'].values[0], filtered_df['Close_M'].values[0]],
            'Volume': filtered_df['Volume_M'].values[0]
        }
    else:  # Yearly
        price_data = {
            'Price': [filtered_df['Open_Y'].values[0], filtered_df['High_Y'].values[0],
                      filtered_df['Low_Y'].values[0], filtered_df['Close_Y'].values[0]],
            'Volume': filtered_df['Volume_Y'].values[0]
        }

    # Create a simplified OHLC chart
    fig = go.Figure(data=[go.Candlestick(
        x=['Open', 'High', 'Low', 'Close'],
        open=[price_data['Price'][0]],
        high=[price_data['Price'][1]],
        low=[price_data['Price'][2]],
        close=[price_data['Price'][3]],
        increasing_line_color='green',
        decreasing_line_color='red'
    )])

    fig.update_layout(
        title=f"{selected_symbol} - {selected_period} OHLC",
        xaxis_title="Price Points",
        yaxis_title="Price (₹)",
        height=400
    )

    st.plotly_chart(fig, use_container_width=True)

    # Comparative overview
    st.markdown('<div class="section-header">Performance Comparison</div>', unsafe_allow_html=True)

    # Compare Weekly, Monthly, Yearly performance
    fig = go.Figure()

    performance_data = {
        'Weekly': filtered_df['Percent_Chg_W'].values[0],
        'Monthly': filtered_df['Percent_Chg_M'].values[0],
        'Yearly': filtered_df['Percent_Chg_Y'].values[0],
        '3 Year': filtered_df['3_Year_Returns'].values[0],
        '5 Year': filtered_df['5_Year_Returns'].values[0]
    }

    colors = ['green' if x >= 0 else 'red' for x in performance_data.values()]

    fig.add_trace(go.Bar(
        x=list(performance_data.keys()),
        y=list(performance_data.values()),
        marker_color=colors,
        text=[f"{x:.2f}%" for x in performance_data.values()],
        textposition='auto'
    ))

    fig.update_layout(
        title=f"{selected_symbol} - Performance Comparison",
        xaxis_title="Time Period",
        yaxis_title="Percentage Change (%)",
        height=400
    )

    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.markdown('<div class="section-header">Technical Analysis</div>', unsafe_allow_html=True)

    # Technical indicators and patterns
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Moving Averages")

        # Weekly EMAs
        wk_ema_13 = filtered_df['Wk_EMA_13'].values[0]
        wk_ema_52 = filtered_df['Wk_EMA_52'].values[0]
        current_price = filtered_df['Close'].values[0]

        st.markdown(f"Weekly EMA (13): **₹{wk_ema_13:.2f}**")
        st.markdown(f"Weekly EMA (52): **₹{wk_ema_52:.2f}**")

        # EMA Crossover signal
        if current_price > wk_ema_13 and wk_ema_13 > wk_ema_52:
            st.markdown(
                '<div class="insight-box"><span class="positive">BULLISH:</span> Price above both EMAs with 13-week EMA above 52-week EMA</div>',
                unsafe_allow_html=True)
        elif current_price < wk_ema_13 and wk_ema_13 < wk_ema_52:
            st.markdown(
                '<div class="insight-box"><span class="negative">BEARISH:</span> Price below both EMAs with 13-week EMA below 52-week EMA</div>',
                unsafe_allow_html=True)
        else:
            st.markdown(
                '<div class="insight-box"><span class="neutral">NEUTRAL:</span> Mixed EMA signals, monitor for clearer trend development</div>',
                unsafe_allow_html=True)

        # Monthly EMA
        st.markdown(f"Monthly EMA (20): **₹{filtered_df['Mth_EMA_20'].values[0]:.2f}**")
        if current_price > filtered_df['Mth_EMA_20'].values[0]:
            st.markdown(
                '<div class="insight-box"><span class="positive">BULLISH:</span> Price above 20-month EMA, indicating uptrend</div>',
                unsafe_allow_html=True)
        else:
            st.markdown(
                '<div class="insight-box"><span class="negative">BEARISH:</span> Price below 20-month EMA, indicating downtrend</div>',
                unsafe_allow_html=True)

    with col2:
        st.markdown("#### Trend Analysis")

        # Linear regression analysis
        lr_6w = filtered_df['LR_6_W'].values[0]
        lr_low_6w = filtered_df['Low_LR_6_W'].values[0]
        lr_high_6w = filtered_df['High_LR_6_W'].values[0]

        st.markdown(f"6-Week Linear Regression: **₹{lr_6w:.2f}**")
        st.markdown(f"LR Channel: **₹{lr_low_6w:.2f} - ₹{lr_high_6w:.2f}**")

        # LR Channel position
        if current_price > lr_high_6w:
            st.markdown(
                '<div class="insight-box"><span class="positive">OVERBOUGHT:</span> Price above upper LR channel, potential reversal or continuation</div>',
                unsafe_allow_html=True)
        elif current_price < lr_low_6w:
            st.markdown(
                '<div class="insight-box"><span class="negative">OVERSOLD:</span> Price below lower LR channel, potential reversal or breakdown</div>',
                unsafe_allow_html=True)
        else:
            st.markdown(
                '<div class="insight-box"><span class="neutral">WITHIN CHANNEL:</span> Price within LR channel, follow the trend direction</div>',
                unsafe_allow_html=True)

        # 6-Month LR Analysis
        lr_6m = filtered_df['LR_6_M'].values[0]
        lr_low_6m = filtered_df['Low_LR_6_M'].values[0]
        lr_high_6m = filtered_df['High_LR_6_M'].values[0]

        st.markdown(f"6-Month Linear Regression: **₹{lr_6m:.2f}**")
        if lr_6m > filtered_df['Close_M'].values[0]:
            slope_desc = "bearish"
        else:
            slope_desc = "bullish"

        st.markdown(f"Long-term trend is **{slope_desc}** with LR channel: **₹{lr_low_6m:.2f} - ₹{lr_high_6m:.2f}**")

    # Support and Resistance Levels
    st.markdown('<div class="section-header">Support & Resistance Levels</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Support Levels")
        supports = [
            filtered_df['Low'].values[0],
            filtered_df['Low_W'].values[0],
            filtered_df['Low_6W'].values[0],
            filtered_df['Low_52W'].values[0]
        ]
        supports = sorted([s for s in supports if s < current_price])

        if supports:
            for i, level in enumerate(supports[-3:], 1):
                st.markdown(f"S{i}: **₹{level:.2f}**")
        else:
            st.markdown("No support levels identified below current price")

    with col2:
        st.markdown("#### Resistance Levels")
        resistances = [
            filtered_df['High'].values[0],
            filtered_df['High_W'].values[0],
            filtered_df['High_6W'].values[0],
            filtered_df['High_52W'].values[0]
        ]
        resistances = sorted([r for r in resistances if r > current_price])

        if resistances:
            for i, level in enumerate(resistances[:3], 1):
                st.markdown(f"R{i}: **₹{level:.2f}**")
        else:
            st.markdown("No resistance levels identified above current price")

    # Trading Signals
    st.markdown('<div class="section-header">Trading Signals</div>', unsafe_allow_html=True)

    # Generate trading signals based on technical indicators
    signals = []

    # EMA Crossover
    if wk_ema_13 > wk_ema_52 and (wk_ema_13 / wk_ema_52 - 1) < 0.03:
        signals.append({
            "signal": "POTENTIAL GOLDEN CROSS",
            "description": "13-week EMA crossing above 52-week EMA",
            "action": "Consider adding to long positions",
            "strength": "Strong",
            "type": "positive"
        })
    elif wk_ema_13 < wk_ema_52 and (wk_ema_52 / wk_ema_13 - 1) < 0.03:
        signals.append({
            "signal": "POTENTIAL DEATH CROSS",
            "description": "13-week EMA crossing below 52-week EMA",
            "action": "Consider reducing exposure",
            "strength": "Strong",
            "type": "negative"
        })

    # Price vs LR Channel
    if current_price < lr_low_6w and current_price > filtered_df['Low_6W'].values[0]:
        signals.append({
            "signal": "OVERSOLD BOUNCE POTENTIAL",
            "description": "Price below lower LR channel but above recent lows",
            "action": "Watch for reversal patterns",
            "strength": "Moderate",
            "type": "positive"
        })
    elif current_price > lr_high_6w and current_price < filtered_df['High_6W'].values[0]:
        signals.append({
            "signal": "OVERBOUGHT PULLBACK RISK",
            "description": "Price above upper LR channel but below recent highs",
            "action": "Consider taking partial profits",
            "strength": "Moderate",
            "type": "negative"
        })

    # Trend strength
    if filtered_df['High_6W'].values[0] > filtered_df['High_6M'].values[0] and filtered_df['Low_6W'].values[0] > \
            filtered_df['Low_6M'].values[0]:
        signals.append({
            "signal": "STRONG UPTREND",
            "description": "Higher highs and higher lows forming",
            "action": "Look for pullbacks to add positions",
            "strength": "Strong",
            "type": "positive"
        })
    elif filtered_df['High_6W'].values[0] < filtered_df['High_6M'].values[0] and filtered_df['Low_6W'].values[0] < \
            filtered_df['Low_6M'].values[0]:
        signals.append({
            "signal": "STRONG DOWNTREND",
            "description": "Lower highs and lower lows forming",
            "action": "Avoid catching falling knives",
            "strength": "Strong",
            "type": "negative"
        })

    # Display signals
    if signals:
        for signal in signals:
            signal_type = signal["type"]
            st.markdown(f'''
            <div class="insight-box">
                <span class="{signal_type}">{signal["signal"]}</span><br>
                <strong>Description:</strong> {signal["description"]}<br>
                <strong>Suggested Action:</strong> {signal["action"]}<br>
                <strong>Signal Strength:</strong> {signal["strength"]}
            </div>
            ''', unsafe_allow_html=True)
    else:
        st.markdown(
            '<div class="insight-box"><span class="neutral">NO CLEAR SIGNALS</span><br>Current technical indicators do not suggest strong trading signals. Monitor for developing patterns.</div>',
            unsafe_allow_html=True)

with tab3:
    st.markdown('<div class="section-header">Investment Opportunities</div>', unsafe_allow_html=True)

    # Long-term investment analysis
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Performance Metrics")

        yearly_return = filtered_df['Percent_Chg_Y'].values[0]
        three_yr_return = filtered_df['3_Year_Returns'].values[0]
        five_yr_return = filtered_df['5_Year_Returns'].values[0]
        max_return = filtered_df['Max_Returns'].values[0]

        metrics = pd.DataFrame({
            'Time Period': ['1 Year', '3 Years', '5 Years', 'Maximum'],
            'Return (%)': [yearly_return, three_yr_return, five_yr_return, max_return]
        })


        # Color formatting
        def color_negative_red(val):
            color = 'red' if val < 0 else 'green'
            return f'color: {color}'


        st.dataframe(metrics.style.applymap(color_negative_red, subset=['Return (%)']))

    with col2:
        st.markdown("#### Investment Rating")

        # Simple investment rating algorithm
        score = 0

        # Trend scores
        if wk_ema_13 > wk_ema_52:
            score += 1
        if current_price > filtered_df['Mth_EMA_20'].values[0]:
            score += 1

        # Performance scores
        if yearly_return > 0:
            score += 1
        if three_yr_return > 15:  # Assuming 5% annual is good for 3 years
            score += 1
        if five_yr_return > 25:  # Assuming 5% annual is good for 5 years
            score += 1

        # Current position versus historical
        current_vs_high = (filtered_df['High_52W'].values[0] - current_price) / filtered_df['High_52W'].values[0] * 100
        if current_vs_high > 30:  # More than 30% below 52-week high
            score += 1

        # Rating scale
        ratings = {
            0: {"rating": "STRONG SELL", "color": "red",
                "description": "Technical and fundamental indicators overwhelmingly negative"},
            1: {"rating": "SELL", "color": "red", "description": "Multiple negative indicators suggest caution"},
            2: {"rating": "NEUTRAL - LEANING BEARISH", "color": "orange",
                "description": "Mixed signals with bearish bias"},
            3: {"rating": "NEUTRAL", "color": "orange",
                "description": "Balanced indicators, no strong directional bias"},
            4: {"rating": "NEUTRAL - LEANING BULLISH", "color": "lightgreen",
                "description": "Mixed signals with bullish bias"},
            5: {"rating": "BUY", "color": "green", "description": "Multiple positive indicators suggest opportunity"},
            6: {"rating": "STRONG BUY", "color": "green",
                "description": "Technical and fundamental indicators overwhelmingly positive"}
        }

        rating = ratings.get(score, ratings[3])  # Default to neutral if score is outside range

        st.markdown(f'''
        <div style="background-color: {rating["color"]}; padding: 20px; border-radius: 5px; text-align: center; color: white;">
            <h1>{rating["rating"]}</h1>
            <p>{rating["description"]}</p>
            <h3>Score: {score}/6</h3>
        </div>
        ''', unsafe_allow_html=True)

    # Investment horizon recommendations
    st.markdown('<div class="section-header">Investment Horizon Recommendations</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("#### Short Term (< 3 months)")

        short_bias = "NEUTRAL"
        short_desc = "Mixed signals in the short term"

        if filtered_df['Percent_Chg_W'].values[0] > 0 and filtered_df['Percent_Chg_M'].values[0] > 0:
            short_bias = "BULLISH"
            short_desc = "Positive weekly and monthly momentum"
        elif filtered_df['Percent_Chg_W'].values[0] < 0 and filtered_df['Percent_Chg_M'].values[0] < 0:
            short_bias = "BEARISH"
            short_desc = "Negative weekly and monthly momentum"

        st.markdown(f'''
        <div class="insight-box">
            <h4>{short_bias}</h4>
            <p>{short_desc}</p>
            <p><strong>Key Level to Watch:</strong> ₹{max(filtered_df['Low_W'].values[0], filtered_df['Low_M'].values[0]):.2f} (Support)</p>
        </div>
        ''', unsafe_allow_html=True)

    with col2:
        st.markdown("#### Medium Term (3-12 months)")

        medium_bias = "NEUTRAL"
        medium_desc = "No clear directional bias for medium term"

        if filtered_df['Percent_Chg_M'].values[0] > 0 and current_price > filtered_df['Mth_EMA_20'].values[0]:
            medium_bias = "BULLISH"
            medium_desc = "Price above 20-month EMA with positive momentum"
        elif filtered_df['Percent_Chg_M'].values[0] < 0 and current_price < filtered_df['Mth_EMA_20'].values[0]:
            medium_bias = "BEARISH"
            medium_desc = "Price below 20-month EMA with negative momentum"

        st.markdown(f'''
        <div class="insight-box">
            <h4>{medium_bias}</h4>
            <p>{medium_desc}</p>
            <p><strong>Target Range:</strong> ₹{filtered_df['Low_6M'].values[0]:.2f} - ₹{filtered_df['High_6M'].values[0]:.2f}</p>
        </div>
        ''', unsafe_allow_html=True)

    with col3:
        st.markdown("#### Long Term (> 1 year)")

        long_bias = "NEUTRAL"
        long_desc = "Insufficient evidence for strong long-term bias"

        if three_yr_return > 0 and five_yr_return > 0:
            long_bias = "BULLISH"
            long_desc = "Positive long-term returns with established trend"
        elif three_yr_return < 0 and five_yr_return < 0:
            long_bias = "BEARISH"
            long_desc = "Negative long-term returns with established trend"

        st.markdown(f'''
        <div class="insight-box">
            <h4>{long_bias}</h4>
            <p>{long_desc}</p>
            <p><strong>Historical Max Return:</strong> {max_return:.2f}%</p>
        </div>
        ''', unsafe_allow_html=True)

    # Risk assessment
    st.markdown('<div class="section-header">Risk Assessment</div>', unsafe_allow_html=True)

    # Calculate volatility metrics
    daily_range = filtered_df['Range_D'].values[0] / filtered_df['Close'].values[0] * 100
    weekly_range = filtered_df['Range_W'].values[0] / filtered_df['Close_W'].values[0] * 100
    monthly_range = filtered_df['Range_M'].values[0] / filtered_df['Close_M'].values[0] * 100

    max_weekly_chg = filtered_df['Max_Chg_W'].values[0]
    max_monthly_chg = filtered_df['Max_Chg_M'].values[0]

    # Risk levels
    risk_levels = {
        "daily": "Low" if daily_range < 2 else ("Medium" if daily_range < 4 else "High"),
        "weekly": "Low" if weekly_range < 5 else ("Medium" if weekly_range < 10 else "High"),
        "monthly": "Low" if monthly_range < 10 else ("Medium" if monthly_range < 20 else "High"),
    }

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Volatility Metrics")

        vol_df = pd.DataFrame({
            'Time Period': ['Daily', 'Weekly', 'Monthly'],
            'Range (%)': [daily_range, weekly_range, monthly_range],
            'Risk Level': [risk_levels["daily"], risk_levels["weekly"], risk_levels["monthly"]]
        })


        # Color formatting based on risk level
        def color_risk(val):
            colors = {"Low": "green", "Medium": "orange", "High": "red"}
            return f'color: {colors.get(val, "black")}'


        st.dataframe(vol_df.style.applymap(color_risk, subset=['Risk Level']))

    with col2:
        st.markdown("#### Maximum Drawdowns")

        st.markdown(f'''
        <div class="insight-box">
            <p><strong>Maximum Weekly Change:</strong> {max_weekly_chg:.2f}%</p>
            <p><strong>Maximum Monthly Change:</strong> {max_monthly_chg:.2f}%</p>
            <p><strong>Suggested Position Size:</strong> {'Small' if risk_levels["monthly"] == "High" else ('Medium' if risk_levels["monthly"] == "Medium" else 'Standard')}</p>
        </div>
        ''', unsafe_allow_html=True)

        # Stop loss guidance
        suggested_stop = current_price * 0.95  # 5% below current price as a simple example

        # Find nearest support for better stop placement
        supports = [
            filtered_df['Low'].values[0],
            filtered_df['Low_W'].values[0],
            filtered_df['Low_6W'].values[0]
        ]

        # Get supports below current price
        valid_supports = [s for s in supports if s < current_price]
        if valid_supports:
            # Find closest support below current price
            best_stop = max(valid_supports)
            stop_pct = (current_price - best_stop) / current_price * 100

            if stop_pct > 15:  # If nearest support is too far
                best_stop = current_price * 0.92  # Use 8% as maximum stop distance
                stop_pct = 8
        else:
            best_stop = suggested_stop
            stop_pct = 5

        st.markdown(f'''
        <div class="insight-box">
            <p><strong>Suggested Stop Loss Level:</strong> ₹{best_stop:.2f} ({stop_pct:.1f}% below current price)</p>
        </div>
        ''', unsafe_allow_html=True)

with tab4:
    st.markdown('<div class="section-header">Market Interpretation & Insights</div>', unsafe_allow_html=True)

    # Overall market sentiment
    st.markdown("### Overall Market Sentiment")

    # Determine sentiment based on multiple factors
    sentiment_score = 0

    # Price vs EMAs
    if current_price > wk_ema_13 and current_price > wk_ema_52:
        sentiment_score += 1
    if current_price > filtered_df['Mth_EMA_20'].values[0]:
        sentiment_score += 1

    # Momentum
    if filtered_df['Percent_Chg_W'].values[0] > 0:
        sentiment_score += 0.5
    if filtered_df['Percent_Chg_M'].values[0] > 0:
        sentiment_score += 0.5
    if filtered_df['Percent_Chg_Y'].values[0] > 0:
        sentiment_score += 1

    # Volume analysis (if available)
    try:
        if filtered_df['Volume'].values[0] > filtered_df['Volume_W'].values[0]:
            sentiment_score += 0.5
    except:
        pass

    # Sentiment classification
    if sentiment_score >= 3.5:
        sentiment = "Strong Bullish"
        sentiment_color = "positive"
        sentiment_desc = "Market shows strong bullish characteristics across multiple timeframes"
    elif sentiment_score >= 2:
        sentiment = "Moderately Bullish"
        sentiment_color = "positive"
        sentiment_desc = "Market shows more bullish than bearish characteristics"
    elif sentiment_score >= 1:
        sentiment = "Neutral to Slightly Bullish"
        sentiment_color = "neutral"
        sentiment_desc = "Market shows mixed signals with slight bullish bias"
    elif sentiment_score >= 0:
        sentiment = "Neutral to Slightly Bearish"
        sentiment_color = "neutral"
        sentiment_desc = "Market shows mixed signals with slight bearish bias"
    else:
        sentiment = "Bearish"
        sentiment_color = "negative"
        sentiment_desc = "Market shows more bearish than bullish characteristics"

    st.markdown(f'''
    <div class="insight-box">
        <span class="{sentiment_color}"><strong>Current Sentiment:</strong> {sentiment}</span><br>
        {sentiment_desc}<br>
        <strong>Sentiment Score:</strong> {sentiment_score}/4.5
    </div>
    ''', unsafe_allow_html=True)

    # Key Technical Observations
    st.markdown("### Key Technical Observations")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Trend Analysis")

        # Weekly trend
        weekly_trend = "Up" if filtered_df['Percent_Chg_W'].values[0] > 0 else "Down"
        weekly_trend_color = "positive" if weekly_trend == "Up" else "negative"

        # Monthly trend
        monthly_trend = "Up" if filtered_df['Percent_Chg_M'].values[0] > 0 else "Down"
        monthly_trend_color = "positive" if monthly_trend == "Up" else "negative"

        st.markdown(f'''
        <div class="insight-box">
            <p><span class="{weekly_trend_color}"><strong>Weekly Trend:</strong> {weekly_trend}</span> ({filtered_df['Percent_Chg_W'].values[0]:.2f}%)</p>
            <p><span class="{monthly_trend_color}"><strong>Monthly Trend:</strong> {monthly_trend}</span> ({filtered_df['Percent_Chg_M'].values[0]:.2f}%)</p>
            <p><strong>Dominant Trend:</strong> {"Bullish" if weekly_trend == "Up" and monthly_trend == "Up" else "Bearish" if weekly_trend == "Down" and monthly_trend == "Down" else "Mixed"}</p>
        </div>
        ''', unsafe_allow_html=True)

    with col2:
        st.markdown("#### Price Position Analysis")

        # Price vs 52-week range
        pct_from_52w_high = (filtered_df['High_52W'].values[0] - current_price) / filtered_df['High_52W'].values[
            0] * 100
        pct_from_52w_low = (current_price - filtered_df['Low_52W'].values[0]) / filtered_df['Low_52W'].values[0] * 100

        position_52w = ""
        if pct_from_52w_high < 10:
            position_52w = "Near 52-week High"
            position_color = "positive"
        elif pct_from_52w_low < 10:
            position_52w = "Near 52-week Low"
            position_color = "negative"
        else:
            position_52w = "Mid-Range"
            position_color = "neutral"

        st.markdown(f'''
        <div class="insight-box">
            <p><span class="{position_color}"><strong>52-Week Position:</strong> {position_52w}</span></p>
            <p><strong>From High:</strong> {pct_from_52w_high:.2f}% below 52-week high</p>
            <p><strong>From Low:</strong> {pct_from_52w_low:.2f}% above 52-week low</p>
        </div>
        ''', unsafe_allow_html=True)

    # Market Psychology Interpretation
    st.markdown("### Market Psychology Interpretation")

    # Determine market phase
    if (current_price > wk_ema_13 > wk_ema_52 and
            current_price > filtered_df['Mth_EMA_20'].values[0] and
            filtered_df['Percent_Chg_M'].values[0] > 0):
        market_phase = "Bull Market"
        phase_desc = "Investors are confident, buying on dips, and pushing prices higher"
        phase_color = "positive"
    elif (current_price < wk_ema_13 < wk_ema_52 and
          current_price < filtered_df['Mth_EMA_20'].values[0] and
          filtered_df['Percent_Chg_M'].values[0] < 0):
        market_phase = "Bear Market"
        phase_desc = "Investors are fearful, selling rallies, and pushing prices lower"
        phase_color = "negative"
    else:
        market_phase = "Transition Phase"
        phase_desc = "Market is consolidating or transitioning between trends"
        phase_color = "neutral"

    st.markdown(f'''
    <div class="insight-box">
        <span class="{phase_color}"><strong>Current Market Phase:</strong> {market_phase}</span><br>
        {phase_desc}
    </div>
    ''', unsafe_allow_html=True)

    # Strategic Recommendations
    st.markdown("### Strategic Recommendations")

    # Generate recommendations based on analysis
    recommendations = []

    # Trend-following recommendation
    if market_phase == "Bull Market":
        recommendations.append({
            "type": "positive",
            "title": "Trend Following Strategy",
            "description": "Consider buying pullbacks to key support levels as the trend is clearly up",
            "timeframe": "Medium to Long Term"
        })
    elif market_phase == "Bear Market":
        recommendations.append({
            "type": "negative",
            "title": "Defensive Positioning",
            "description": "Consider reducing exposure or using rallies to exit positions",
            "timeframe": "Until trend reversal signals appear"
        })
    else:
        recommendations.append({
            "type": "neutral",
            "title": "Range-bound Strategy",
            "description": "Consider buying near support and selling near resistance until clear trend emerges",
            "timeframe": "Short to Medium Term"
        })

    # Mean-reversion opportunity
    if position_52w == "Near 52-week High" and market_phase != "Bull Market":
        recommendations.append({
            "type": "negative",
            "title": "Potential Mean Reversion",
            "description": "Prices near highs without strong bull market signals may present shorting opportunities",
            "timeframe": "Short to Medium Term"
        })
    elif position_52w == "Near 52-week Low" and market_phase != "Bear Market":
        recommendations.append({
            "type": "positive",
            "title": "Potential Rebound Opportunity",
            "description": "Prices near lows without strong bear market signals may present buying opportunities",
            "timeframe": "Medium Term"
        })

    # Display recommendations
    for rec in recommendations:
        st.markdown(f'''
        <div class="insight-box">
            <span class="{rec['type']}"><strong>{rec['title']}</strong></span><br>
            <strong>Description:</strong> {rec['description']}<br>
            <strong>Timeframe:</strong> {rec['timeframe']}
        </div>
        ''', unsafe_allow_html=True)

    # Risk Management Considerations
    st.markdown("### Risk Management Considerations")

    # Determine appropriate risk management approach
    if risk_levels["daily"] == "High" or risk_levels["weekly"] == "High":
        risk_approach = "Conservative"
        risk_desc = "High volatility suggests using smaller position sizes and wider stops"
    elif risk_levels["daily"] == "Low" and risk_levels["weekly"] == "Low":
        risk_approach = "Standard"
        risk_desc = "Low volatility allows for normal position sizing and tighter stops"
    else:
        risk_approach = "Moderate"
        risk_desc = "Moderate volatility suggests careful position sizing and medium stops"

    st.markdown(f'''
    <div class="insight-box">
        <strong>Suggested Risk Approach:</strong> {risk_approach}<br>
        {risk_desc}<br>
        <strong>Key Levels:</strong> Support at ₹{filtered_df['Low_W'].values[0]:.2f}, Resistance at ₹{filtered_df['High_W'].values[0]:.2f}
    </div>
    ''', unsafe_allow_html=True)

