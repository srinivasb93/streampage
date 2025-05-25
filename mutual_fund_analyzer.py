import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from mftool import Mftool
import warnings
import time
import numpy_financial as npf

warnings.filterwarnings('ignore')


# Initialize the mftool with retry mechanism
class RobustMftool(Mftool):
    def __init__(self, max_retries=3):
        super().__init__()
        self.max_retries = max_retries

    def get_scheme_historical_nav_with_retry(self, scheme_code, start_date, end_date):
        for attempt in range(self.max_retries):
            try:
                return self.get_scheme_historical_nav(scheme_code, start_date, end_date)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise e
                time.sleep(1)  # Wait before retrying


# Initialize the robust mftool
mf = RobustMftool()

# Set page configuration
st.set_page_config(
    page_title="Enhanced Indian Mutual Fund SIP Analyzer",
    page_icon="📈",
    layout="wide"
)

# App title and description
st.title("Enhanced Indian Mutual Fund SIP Analyzer")
st.markdown("""
This app helps you analyze various Indian mutual funds and identify the best ones for Systematic Investment Plans (SIPs).
Use the sidebar to select different analysis options and filters.
""")

# Sidebar for user inputs
st.sidebar.header("Analysis Settings")

# Predefined fund categories with better mapping
FUND_CATEGORIES = {
    "Equity": ["equity", "growth", "dividend", "large cap", "mid cap", "small cap",
               "flexi cap", "focused", "sectoral", "thematic", "value", "contra"],
    "Debt": ["debt", "income", "bond", "gilt", "liquid", "overnight",
             "money market", "corporate", "credit", "banking", "psu"],
    "Hybrid": ["hybrid", "balanced", "aggressive", "conservative",
               "asset allocation", "multi asset", "arbitrage"],
    "Solution Oriented": ["retirement", "children", "pension", "tax saver", "elss"],
    "Index/ETF": ["index", "etf", "exchange traded"],
    "International": ["international", "global", "foreign", "overseas"],
    "Other": ["gold", "commodity", "fof", "fund of funds"]
}


# Function to search mutual funds by name with caching
@st.cache_data(ttl=3600)
def search_mutual_funds(query):
    all_funds = mf.get_scheme_codes()
    if not query:
        return all_funds
    return {code: name for code, name in all_funds.items() if query.lower() in name.lower()}


# Enhanced fund category loading with better caching
@st.cache_data(ttl=86400)
def load_category_funds(category):
    if category == "All":
        return mf.get_scheme_codes()

    all_funds = mf.get_scheme_codes()
    keywords = FUND_CATEGORIES.get(category, [])

    # First pass: filter by keywords in fund name
    filtered_funds = {}
    for code, name in all_funds.items():
        if any(keyword.lower() in name.lower() for keyword in keywords):
            filtered_funds[code] = name

    # If we have enough funds, return them
    if len(filtered_funds) >= 10 or category in ["Index/ETF", "International", "Other"]:
        return filtered_funds

    # Second pass: check scheme details for better accuracy
    detailed_funds = {}
    sample_size = min(200, len(all_funds))  # Limit to 200 funds for performance

    progress_bar = st.sidebar.progress(0)
    status_text = st.sidebar.empty()

    for i, (code, name) in enumerate(list(all_funds.items())[:sample_size]):
        try:
            details = mf.get_scheme_details(code)
            if details and 'scheme_category' in details:
                scheme_category = details['scheme_category'].lower()
                if any(keyword.lower() in scheme_category for keyword in keywords):
                    detailed_funds[code] = name
        except:
            continue

        # Update progress
        progress = (i + 1) / sample_size
        progress_bar.progress(progress)
        status_text.text(f"Scanning funds... {int(progress * 100)}%")

    progress_bar.empty()
    status_text.empty()

    # Combine both sets of funds
    filtered_funds.update(detailed_funds)
    return filtered_funds if filtered_funds else all_funds


# Fund search and selection
search_query = st.sidebar.text_input("Search Mutual Funds", "")
fund_category = st.sidebar.selectbox(
    "Fund Category",
    ["All"] + list(FUND_CATEGORIES.keys())
)

# Get filtered funds
if fund_category == "All" and not search_query:
    funds = mf.get_scheme_codes()
elif fund_category != "All":
    funds = load_category_funds(fund_category)
else:
    funds = search_mutual_funds(search_query)

# Show number of funds found
st.sidebar.write(f"Found {len(funds)} mutual funds")

# Fund selection with limit
selected_funds = st.sidebar.multiselect(
    "Select Mutual Funds to Compare (Max 5)",
    options=list(funds.values()),
    max_selections=5
)

# Time period selection with better date handling
time_periods = {
    "1 Month": 30,
    "3 Months": 90,
    "6 Months": 180,
    "1 Year": 365,
    "3 Years": 1095,
    "5 Years": 1825,
    "10 Years": 3650
}

time_period = st.sidebar.selectbox(
    "Analysis Time Period",
    list(time_periods.keys()),
    index=3  # Default to 1 Year
)

# Enhanced analysis parameters
min_investment = st.sidebar.number_input(
    "Monthly SIP Amount (₹)",
    min_value=500,
    value=5000,
    step=500,
    help="Minimum ₹500 is required for most SIPs"
)

risk_appetite = st.sidebar.slider(
    "Risk Appetite (1-10)",
    1, 10, 5,
    help="1: Very Low Risk (Debt Funds), 10: Very High Risk (Small Cap/ Sectoral Funds)"
)


# Enhanced NAV data fetching with date validation
def get_historical_nav_enhanced(scheme_code, days):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    # Adjust start date to ensure we get enough data points
    if days <= 90:  # For short durations, get daily data
        nav_data = mf.get_scheme_historical_nav_with_retry(
            scheme_code,
            start_date.strftime('%d-%m-%Y'),
            end_date.strftime('%d-%m-%Y')
        )
    else:  # For longer durations, get monthly data for better performance
        nav_data = mf.get_scheme_historical_nav_with_retry(
            scheme_code,
            start_date.strftime('%d-%m-%Y'),
            end_date.strftime('%d-%m-%Y')
        )

    if not nav_data or 'data' not in nav_data:
        return pd.DataFrame()

    nav_df = pd.DataFrame(nav_data['data'])
    nav_df['date'] = pd.to_datetime(nav_df['date'], format='%d-%m-%Y')
    nav_df['nav'] = pd.to_numeric(nav_df['nav'])
    nav_df = nav_df.sort_values('date')

    # Ensure we have continuous dates (fill weekends/holidays with last NAV)
    date_range = pd.date_range(start=nav_df['date'].min(), end=nav_df['date'].max())
    nav_df = nav_df.set_index('date').reindex(date_range).ffill().reset_index()
    nav_df = nav_df.rename(columns={'index': 'date'})

    return nav_df


# Enhanced SIP calculation with exact dates
def calculate_sip_returns_enhanced(nav_df, monthly_investment, start_date=None):
    if nav_df.empty:
        return 0, 0, pd.DataFrame()

    if start_date is None:
        start_date = nav_df['date'].min()

    # Find all investment dates (1st of each month)
    investment_dates = pd.date_range(
        start=start_date,
        end=nav_df['date'].max(),
        freq='MS'  # Month Start
    )

    # Filter to dates where NAV data exists
    valid_dates = []
    for date in investment_dates:
        # Find the next available NAV date if exact date not available
        nav_date = nav_df[nav_df['date'] >= date]['date'].min()
        if pd.notna(nav_date):
            valid_dates.append(nav_date)

    if not valid_dates:
        return 0, 0, pd.DataFrame()

    # Calculate SIP
    sip_data = []
    total_units = 0
    total_invested = 0

    for inv_date in valid_dates:
        nav_row = nav_df[nav_df['date'] == inv_date].iloc[0]
        nav = nav_row['nav']
        units = monthly_investment / nav
        total_units += units
        total_invested += monthly_investment

        current_value = total_units * nav
        profit_loss = current_value - total_invested

        sip_data.append({
            'date': inv_date,
            'nav': nav,
            'investment': monthly_investment,
            'units_purchased': units,
            'total_units': total_units,
            'total_investment': total_invested,
            'current_value': current_value,
            'profit_loss': profit_loss
        })

    sip_df = pd.DataFrame(sip_data)

    if sip_df.empty:
        return 0, 0, pd.DataFrame()

    # Calculate returns
    final_value = sip_df.iloc[-1]['current_value']
    total_invested = sip_df.iloc[-1]['total_investment']
    absolute_return = ((final_value - total_invested) / total_invested) * 100

    # Calculate XIRR (approximation)
    duration_days = (sip_df['date'].iloc[-1] - sip_df['date'].iloc[0]).days
    if duration_days > 365:
        duration_years = duration_days / 365
        cagr = ((final_value / total_invested) ** (1 / duration_years)) - 1
        annualized_return = cagr * 100
    else:
        annualized_return = absolute_return * (365 / duration_days) if duration_days > 0 else 0

    return absolute_return, annualized_return, sip_df


# Enhanced risk metrics calculation
def calculate_risk_metrics_enhanced(nav_df):
    if nav_df.empty or len(nav_df) < 5:
        return 0.0, 0.0, 0.0, 0.0
    try:
        # Calculate daily returns
        nav_df['daily_return'] = nav_df['nav'].pct_change()
        nav_df = nav_df.dropna()

        if len(nav_df) < 5:
            return 0.0, 0.0, 0.0, 0.0

        # Annualized volatility
        volatility = nav_df['daily_return'].std() * np.sqrt(252) * 100

        # Maximum drawdown
        nav_df['cummax'] = nav_df['nav'].cummax()
        nav_df['drawdown'] = (nav_df['nav'] - nav_df['cummax']) / nav_df['cummax']
        max_drawdown = nav_df['drawdown'].min() * 100

        # Sortino ratio (only considers downside deviation)
        risk_free_rate = 0.05  # Assuming 5% risk-free rate
        downside_returns = nav_df[nav_df['daily_return'] < 0]['daily_return']
        downside_deviation = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 0 else 0
        annualized_return = nav_df['daily_return'].mean() * 252
        sortino_ratio = (annualized_return - risk_free_rate) / downside_deviation if downside_deviation > 0 else 0

        # Beta calculation (would need benchmark data for proper calculation)
        beta = 1.0  # Placeholder

        return volatility, abs(max_drawdown), sortino_ratio, beta
    except Exception as e:
        st.error(f"Error calculating risk metrics: {str(e)}")
        return 0.0, 0.0, 0.0, 0.0


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


# Main analysis function
def analyze_funds(selected_fund_codes, funds, time_period_key, monthly_investment, risk_appetite):
    if not selected_fund_codes:
        st.warning("Please select at least one mutual fund to analyze.")
        return

    days = time_periods[time_period_key]
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()

    for i, code in enumerate(selected_fund_codes):
        fund_name = funds[code]
        status_text.text(f"Analyzing {i + 1}/{len(selected_fund_codes)}: {fund_name}")
        progress_bar.progress((i + 0.5) / len(selected_fund_codes))

        try:
            # Get fund details
            fund_details = mf.get_scheme_details(code)

            # Get historical NAV data
            nav_df = get_historical_nav_enhanced(code, days)

            if nav_df.empty:
                st.warning(f"Could not fetch NAV data for {fund_name}")
                continue

            # Plot NAV trend
            fig = px.line(nav_df, x='date', y='nav', title=f"NAV Trend: {fund_name}")
            st.plotly_chart(fig, use_container_width=True)

            # Calculate SIP returns
            absolute_return, annualized_return, sip_df = calculate_sip_returns_enhanced(
                nav_df, monthly_investment
            )

            # Calculate risk metrics
            volatility, max_drawdown, sortino_ratio, beta = calculate_risk_metrics_enhanced(nav_df)

            fund_category = fund_details.get('scheme_category', 'N/A')
            fund_type = fund_details.get('scheme_type', 'N/A')
            latest_nav = safe_float(nav_df.iloc[-1]['nav']) if not nav_df.empty else 0.0
            aum = safe_float(fund_details.get('aum', '0').replace(' Cr.', '')) * 100  # Convert to lakhs if needed
            expense_ratio = safe_float(fund_details.get('expense_ratio', '0').replace('%', ''))
            min_sip = safe_float(fund_details.get('minimum_sip_investment', '0'))

            # Store results
            results.append({
                'Fund Name': fund_name,
                'Fund Code': code,
                'Category': fund_category,
                'Type': fund_type,
                'Latest NAV': latest_nav,
                'Absolute Return (%)': absolute_return,
                'Annualized Return (%)': annualized_return,
                'Volatility (%)': volatility,
                'Max Drawdown (%)': max_drawdown,
                'Sortino Ratio': sortino_ratio,
                'Beta': beta,
                'AUM (Cr)': aum,
                'Expense Ratio (%)': expense_ratio,
                'Min SIP Amount': min_sip
            })

            # Plot SIP growth if data available
            if not sip_df.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=sip_df['date'], y=sip_df['total_investment'],
                    name='Total Invested', line=dict(color='blue')))
                fig.add_trace(go.Scatter(
                    x=sip_df['date'], y=sip_df['current_value'],
                    name='Current Value', line=dict(color='green')))
                fig.update_layout(
                    title=f"SIP Growth: {fund_name}",
                    xaxis_title='Date',
                    yaxis_title='Amount (₹)',
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)

                # Show SIP details in expander
                with st.expander(f"SIP Details for {fund_name}"):
                    # Calculate correct returns
                    total_invested = sip_df['total_investment'].iloc[-1]
                    current_value = sip_df['current_value'].iloc[-1]
                    absolute_return_pct = ((current_value - total_invested) / total_invested) * 100

                    # Calculate XIRR properly
                    cash_flows = [-monthly_investment] * (len(sip_df) - 1) + [current_value]
                    dates = sip_df['date'].tolist()
                    try:
                        xirr = npf.irr(cash_flows)
                        annualized_return = (1 + xirr) ** 12 - 1 if xirr >= -1 else 0
                        annualized_return_pct = annualized_return * 100
                    except:
                        annualized_return_pct = absolute_return_pct

                    # Display key metrics
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Invested", f"₹{total_invested:,.2f}")
                    col2.metric("Current Value", f"₹{current_value:,.2f}")
                    col3.metric("Absolute Return", f"{absolute_return_pct:.2f}%")

                    col1, col2 = st.columns(2)
                    col1.metric("Annualized Return (XIRR)", f"{annualized_return_pct:.2f}%")
                    col2.metric("Investment Period", f"{len(sip_df)} months")

                    # Enhanced SIP transactions table
                    st.write("**Monthly Transactions:**")
                    display_df = sip_df.copy()
                    display_df['Return (%)'] = display_df['current_value'].pct_change() * 100
                    display_df['Cumulative Return (%)'] = ((display_df['current_value'] - display_df[
                        'total_investment']) /
                                                           display_df['total_investment']) * 100
                    st.dataframe(
                        display_df[['date', 'nav', 'investment', 'total_investment',
                                    'current_value', 'Return (%)', 'Cumulative Return (%)']]
                        .rename(columns={
                            'date': 'Date',
                            'nav': 'NAV',
                            'investment': 'Monthly Investment',
                            'total_investment': 'Total Invested',
                            'current_value': 'Current Value'
                        })
                        .style.format({
                            'Date': lambda x: x.strftime('%b %Y'),
                            'NAV': '{:,.2f}',
                            'Monthly Investment': '₹{:,.2f}',
                            'Total Invested': '₹{:,.2f}',
                            'Current Value': '₹{:,.2f}',
                            'Return (%)': '{:.2f}%',
                            'Cumulative Return (%)': '{:.2f}%'
                        })
                    )

            # Show fund details
            with st.expander(f"Fund Details: {fund_name}"):
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Basic Information**")
                    st.write(f"**Category:** {fund_details.get('scheme_category', 'N/A')}")
                    st.write(f"**Type:** {fund_details.get('scheme_type', 'N/A')}")
                    st.write(f"**Launch Date:** {fund_details.get('launch_date', 'N/A')}")
                    st.write(f"**Fund Manager:** {fund_details.get('fund_manager', 'N/A')}")

                with col2:
                    st.write("**Performance Metrics**")
                    st.write(f"**Volatility:** {volatility:.2f}%")
                    st.write(f"**Max Drawdown:** {max_drawdown:.2f}%")
                    st.write(f"**Sortino Ratio:** {sortino_ratio:.2f}")
                    st.write(f"**Beta:** {beta:.2f}")

                col3, col4 = st.columns(2)
                with col3:
                    st.write("**Investment Details**")
                    st.write(f"**AUM:** {fund_details.get('aum', 'N/A')}")
                    st.write(f"**Expense Ratio:** {fund_details.get('expense_ratio', 'N/A')}%")
                    st.write(f"**Min SIP:** ₹{fund_details.get('minimum_sip_investment', 'N/A')}")
                    st.write(f"**Exit Load:** {fund_details.get('exit_load', 'N/A')}")

                with col4:
                    st.write("**Risk Profile**")
                    risk_level = "Low"
                    if volatility > 20:
                        risk_level = "High"
                    elif volatility > 12:
                        risk_level = "Medium"
                    st.write(f"**Risk Level:** {risk_level}")

                    # Risk meter visualization
                    risk_meter = go.Figure(go.Indicator(
                        mode="gauge+number",
                        value=volatility,
                        domain={'x': [0, 1], 'y': [0, 1]},
                        title={'text': "Risk Meter (Volatility)"},
                        gauge={
                            'axis': {'range': [0, 40]},
                            'steps': [
                                {'range': [0, 12], 'color': "lightgreen"},
                                {'range': [12, 20], 'color': "orange"},
                                {'range': [20, 40], 'color': "red"}],
                            'threshold': {
                                'line': {'color': "black", 'width': 4},
                                'thickness': 0.75,
                                'value': volatility}
                        }
                    ))
                    st.plotly_chart(risk_meter, use_container_width=True)

        except Exception as e:
            st.error(f"Error analyzing {fund_name}: {str(e)}")
            continue

        progress_bar.progress((i + 1) / len(selected_fund_codes))

    progress_bar.empty()
    status_text.empty()

    if not results:
        st.error("No valid analysis results to display.")
        return

    # Display comparison table
    result_df = pd.DataFrame(results)

    # Calculate investment metrics
    months = days / 30
    result_df['Total Invested'] = monthly_investment * months
    result_df['Current Value'] = result_df['Total Invested'] * (1 + result_df['Absolute Return (%)'] / 100)
    result_df['Profit/Loss'] = result_df['Current Value'] - result_df['Total Invested']

    # Display comparison table
    st.header("Fund Comparison")
    display_cols = [
        'Fund Name', 'Category', 'Latest NAV', 'Total Invested',
        'Current Value', 'Profit/Loss', 'Annualized Return (%)',
        'Volatility (%)', 'Max Drawdown (%)', 'Sortino Ratio', 'Expense Ratio (%)'
    ]

    # Create styled DataFrame
    comparison_df = result_df[display_cols].set_index('Fund Name').round(2)

    # Apply styling only to numeric columns
    styled_df = comparison_df.style.format({
        'Latest NAV': '{:,.2f}',
        'Total Invested': '₹{:,.2f}',
        'Current Value': '₹{:,.2f}',
        'Profit/Loss': '₹{:,.2f}',
        'Annualized Return (%)': '{:.2f}%',
        'Volatility (%)': '{:.2f}%',
        'Max Drawdown (%)': '{:.2f}%',
        'Expense Ratio (%)': '{:.2f}%'
    })

    # Apply color gradients
    styled_df = styled_df.background_gradient(
        cmap='RdYlGn',
        subset=['Annualized Return (%)', 'Sortino Ratio']
    ).background_gradient(
        cmap='RdYlGn_r',
        subset=['Volatility (%)', 'Max Drawdown (%)', 'Expense Ratio (%)']
    )

    st.dataframe(styled_df)

    # Generate recommendations
    generate_recommendations(result_df, risk_appetite)


# Recommendation engine
# Update the recommendation generation with clear criteria explanation
def generate_recommendations(result_df, risk_appetite):
    st.header("SIP Recommendations")

    # Explanation of recommendation criteria
    with st.expander("How recommendations are calculated"):
        st.markdown("""
        Our recommendation algorithm considers multiple factors weighted according to your risk appetite:

        **1. Return Metrics (50% weight)**
        - Annualized Return (30%)
        - Absolute Return (20%)

        **2. Risk Metrics (30% weight)**
        - Volatility (adjusted by your risk appetite)
        - Maximum Drawdown (adjusted by your risk appetite)
        - Sortino Ratio (measures downside risk)

        **3. Cost Efficiency (20% weight)**
        - Expense Ratio
        - Minimum SIP Amount

        Your selected risk appetite (currently: {risk_appetite}/10) adjusts how much we penalize volatile funds.
        Higher risk appetite means we tolerate more volatility for potentially higher returns.
        """)

    # Normalize metrics for scoring
    metrics = {
        'Annualized Return (%)': 0.30,
        'Absolute Return (%)': 0.20,
        'Sortino Ratio': 0.15,
        'Volatility (%)': -0.15 * (1 - risk_appetite / 10),
        'Max Drawdown (%)': -0.10 * (1 - risk_appetite / 10),
        'Expense Ratio (%)': -0.05,
        'Min SIP Amount': -0.05
    }

    # Calculate scores
    scores = pd.DataFrame()
    for metric, weight in metrics.items():
        if metric in result_df.columns:
            if weight > 0:  # Higher is better
                normalized = (result_df[metric] - result_df[metric].min()) / \
                             (result_df[metric].max() - result_df[metric].min() + 1e-10)
            else:  # Lower is better
                normalized = 1 - ((result_df[metric] - result_df[metric].min()) / \
                                  (result_df[metric].max() - result_df[metric].min() + 1e-10))
            scores[metric] = normalized * abs(weight)

    result_df['Score'] = scores.sum(axis=1)
    result_df['Rank'] = result_df['Score'].rank(ascending=False)
    result_df = result_df.sort_values('Score', ascending=False)

    # Display top recommendation with criteria breakdown
    top_fund = result_df.iloc[0]
    st.subheader(f"Top Recommendation: {top_fund['Fund Name']}")

    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["Performance", "Risk Profile", "Cost Structure"])

    with tab1:
        col1, col2 = st.columns(2)
        col1.metric("Annualized Return", f"{top_fund['Annualized Return (%)']:.2f}%")
        col2.metric("Absolute Return", f"{top_fund['Absolute Return (%)']:.2f}%")

    with tab2:
        col1, col2, col3 = st.columns(3)
        col1.metric("Volatility", f"{top_fund['Volatility (%)']:.2f}%")
        col2.metric("Max Drawdown", f"{top_fund['Max Drawdown (%)']:.2f}%")
        col3.metric("Sortino Ratio", f"{top_fund['Sortino Ratio']:.2f}")

    with tab3:
        col1, col2 = st.columns(2)
        col1.metric("Expense Ratio", f"{top_fund['Expense Ratio (%)']:.2f}%")
        col2.metric("Min SIP Amount", f"₹{top_fund['Min SIP Amount']:,.2f}")

    # Show score breakdown
    with st.expander("See scoring breakdown"):
        score_df = pd.DataFrame({
            'Factor': list(metrics.keys()),
            'Weight': [f"{abs(w * 100):.1f}%" for w in metrics.values()],
            'Normalized Score': [scores[col].iloc[0] if col in scores.columns else 0 for col in metrics.keys()],
            'Contribution': [
                f"{(scores[col].iloc[0] if col in scores.columns else 0) / result_df['Score'].iloc[0] * 100:.1f}%"
                for col in metrics.keys()]
        })
        st.dataframe(score_df)

    # Single Annualized Returns Comparison chart
    if len(result_df) > 1:
        st.subheader("Performance Comparison")
        fig = px.bar(
            result_df.sort_values('Annualized Return (%)', ascending=True),
            x='Annualized Return (%)',
            y='Fund Name',
            orientation='h',
            title='Annualized Returns Comparison',
            color='Annualized Return (%)',
            color_continuous_scale='RdYlGn'
        )
        st.plotly_chart(fig, use_container_width=True)

# Get fund codes from names
selected_fund_codes = [code for code, name in funds.items() if name in selected_funds]

# Main app flow
if st.sidebar.button("Analyze Selected Funds"):
    analyze_funds(selected_fund_codes, funds, time_period, min_investment, risk_appetite)
else:
# Default view with educational content
    st.write(
        "👈 Please select funds from the sidebar and click 'Analyze Selected Funds' to see the analysis.")

# Enhanced educational content
st.header("How to Choose the Best SIP for Your Goals")

st.markdown("""
### Understanding Your Investment Needs

1. **Investment Horizon**:
- Short-term (1-3 years): Consider debt funds or arbitrage funds
- Medium-term (3-5 years): Balanced or hybrid funds work well
- Long-term (5+ years): Equity funds provide best growth potential

2. **Risk Capacity**:
- Conservative: Stick to large cap or multi cap funds
- Moderate: Consider flexi cap or value funds
- Aggressive: Small cap or sectoral funds may be appropriate

### Key Metrics to Evaluate

- **Returns**: Look for consistent performance across market cycles
- **Risk Metrics**: Volatility and drawdown indicate fund stability
- **Expense Ratio**: Lower costs mean more money stays invested
- **Fund Manager Tenure**: Experienced managers often deliver better results
- **AUM Size**: Very small or very large AUMs may have challenges

### SIP Best Practices

- Start early to benefit from compounding
- Increase your SIP amount annually with income growth
- Stay invested through market cycles
- Review your portfolio annually
- Diversify across 3-5 funds for better risk management
""")

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center">
<p>Data provided by mftool | Past performance is not indicative of future returns</p>
<p>For educational purposes only | Consult a financial advisor before investing</p>
</div>
""", unsafe_allow_html=True)