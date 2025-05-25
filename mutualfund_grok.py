import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from mftool import Mftool
import warnings
import time
import pyodbc
from typing import Dict, List, Tuple
from common_utils import read_write_sql_data as rd

warnings.filterwarnings('ignore')

# Initialize robust mftool
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
                time.sleep(1)

mf = RobustMftool()

# Page config
st.set_page_config(page_title="Enhanced Indian Mutual Fund SIP Analyzer", layout="wide")

# Fund categories
FUND_CATEGORIES = {
    "Equity": ["equity", "growth", "dividend", "large cap", "mid cap", "small cap", "flexi cap"],
    "Debt": ["debt", "income", "bond", "gilt", "liquid", "overnight", "money market"],
    "Hybrid": ["hybrid", "balanced", "aggressive", "conservative", "arbitrage"],
    "Solution Oriented": ["retirement", "children", "pension", "elss"],
    "Index/ETF": ["index", "etf"],
    "International": ["international", "global", "foreign"],
    "Other": ["gold", "commodity", "fof"]
}

# Cached fund loading
@st.cache_data(ttl=86400)
def load_all_funds() -> Dict[str, str]:
    return mf.get_scheme_codes()

@st.cache_data(ttl=3600)
def filter_funds(query: str, category: str) -> Dict[str, str]:
    all_funds = load_all_funds()
    filtered = all_funds
    if query:
        filtered = {code: name for code, name in filtered.items() if query.lower() in name.lower()}
    if category != "All":
        keywords = FUND_CATEGORIES.get(category, [])
        filtered = {code: name for code, name in filtered.items()
                    if any(keyword.lower() in name.lower() for keyword in keywords)}
    return filtered

# Fetch benchmark data from SQL Server
@st.cache_data(ttl=86400)
def fetch_benchmark_data(index_name: str, days: int) -> pd.DataFrame:
    try:
        end_date = datetime.today()
        start_date = end_date - timedelta(days=days)
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        query = f"""
            SELECT [Date], [Close]
            FROM {index_name}
            WHERE [Date] BETWEEN '{start_date_str}' AND '{end_date_str}'
            ORDER BY [Date]
        """
        df = rd.get_table_data(query=query)
        df['Date'] = pd.to_datetime(df['Date'])
        date_range = pd.date_range(start=df['Date'].min(), end=df['Date'].max())
        return df.set_index('Date').reindex(date_range).ffill().reset_index().rename(columns={'index': 'date', 'Close': 'value'}).dropna()
    except Exception as e:
        st.error(f"Failed to fetch benchmark data: {str(e)}")
        return pd.DataFrame()

# Custom XIRR implementation
def xirr(cash_flows: List[float], dates: List[datetime]) -> float:
    """
    Calculate XIRR using Newton-Raphson method.
    cash_flows: List of cash flows (negative for investments, positive for returns)
    dates: List of corresponding dates
    """
    if len(cash_flows) != len(dates):
        raise ValueError("Cash flows and dates must have the same length")

    # Convert dates to days since the first date
    start_date = dates[0]
    days = [(d - start_date).days for d in dates]

    def npv(rate: float) -> float:
        return sum(cf / (1 + rate) ** (day / 365.0) for cf, day in zip(cash_flows, days))

    def npv_derivative(rate: float) -> float:
        return sum(-day / 365.0 * cf / (1 + rate) ** (day / 365.0 + 1) for cf, day in zip(cash_flows, days))

    # Newton-Raphson method to find the rate
    rate = 0.1  # Initial guess
    max_iterations = 1000
    tolerance = 1e-6

    for _ in range(max_iterations):
        npv_value = npv(rate)
        if abs(npv_value) < tolerance:
            return rate
        derivative = npv_derivative(rate)
        if derivative == 0:
            raise ValueError("XIRR calculation failed: derivative is zero")
        rate -= npv_value / derivative

    raise ValueError("XIRR calculation failed: did not converge")

# NAV and SIP calculations
def get_historical_nav(scheme_code: str, days: int) -> pd.DataFrame:
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    nav_data = mf.get_scheme_historical_nav_with_retry(
        scheme_code, start_date.strftime('%d-%m-%Y'), end_date.strftime('%d-%m-%Y')
    )
    if not nav_data or 'data' not in nav_data:
        return pd.DataFrame()
    df = pd.DataFrame(nav_data['data'])
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
    df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
    df = df.dropna().sort_values('date')
    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max())
    return df.set_index('date').reindex(date_range).ffill().reset_index().rename(columns={'index': 'date'})

def calculate_sip_returns(nav_df: pd.DataFrame, monthly_investment: float) -> Tuple[float, float, pd.DataFrame]:
    if nav_df.empty:
        return 0.0, 0.0, pd.DataFrame()

    investment_dates = pd.date_range(start=nav_df['date'].min(), end=nav_df['date'].max(), freq='MS')
    sip_data = []
    total_units = 0
    total_invested = 0

    for date in investment_dates:
        nav_row = nav_df[nav_df['date'] >= date].iloc[0]
        nav = nav_row['nav']
        units = monthly_investment / nav
        total_units += units
        total_invested += monthly_investment
        current_value = total_units * nav
        profit_loss = current_value - total_invested
        sip_data.append({
            'date': date, 'nav': nav, 'investment': monthly_investment, 'units_purchased': units,
            'total_units': total_units, 'total_invested': total_invested, 'current_value': current_value,
            'profit_loss': profit_loss
        })

    sip_df = pd.DataFrame(sip_data)
    if sip_df.empty:
        return 0.0, 0.0, pd.DataFrame()

    final_value = sip_df['current_value'].iloc[-1]
    total_invested = sip_df['total_invested'].iloc[-1]
    absolute_return = (final_value - total_invested) / total_invested * 100

    # Correct XIRR calculation using custom function
    cash_flows = [-monthly_investment] * len(sip_df) + [final_value]
    dates = sip_df['date'].tolist() + [sip_df['date'].iloc[-1]]
    try:
        xirr_rate = xirr(cash_flows, dates)
        annualized_return = xirr_rate * 100
    except Exception as e:
        st.warning(f"XIRR calculation failed: {str(e)}. Falling back to absolute return.")
        annualized_return = absolute_return

    return absolute_return, annualized_return, sip_df

def calculate_risk_metrics(nav_df: pd.DataFrame, benchmark_df: pd.DataFrame) -> Tuple[float, float, float, float, int, int, pd.DataFrame, pd.DataFrame]:
    if nav_df.empty or len(nav_df) < 5 or benchmark_df.empty:
        return 0.0, 0.0, 0.0, 0.0, 0, 0, pd.DataFrame(), pd.DataFrame()

    merged_df = pd.merge(nav_df, benchmark_df, on='date', how='inner', suffixes=('_fund', '_bench'))
    if len(merged_df) < 5:
        return 0.0, 0.0, 0.0, 0.0, 0, 0, pd.DataFrame(), pd.DataFrame()

    fund_returns = merged_df['nav'].pct_change().dropna()
    bench_returns = merged_df['value'].pct_change().dropna()

    volatility = fund_returns.std() * np.sqrt(252) * 100
    max_drawdown = ((merged_df['nav'] / merged_df['nav'].cummax()) - 1).min() * 100
    risk_free_rate = 0.05
    downside_returns = fund_returns[fund_returns < 0]
    downside_deviation = downside_returns.std() * np.sqrt(252) if not downside_returns.empty else 0
    annualized_return = fund_returns.mean() * 252
    sortino_ratio = (annualized_return - risk_free_rate) / downside_deviation if downside_deviation > 0 else 0
    covariance = np.cov(fund_returns, bench_returns)[0, 1]
    bench_variance = bench_returns.var()
    beta = covariance / bench_variance if bench_variance > 0 else 1.0

    # Beating benchmark summary and returns for heatmap
    monthly_returns = merged_df.resample('M', on='date').last().pct_change().dropna()
    quarterly_returns = merged_df.resample('Q', on='date').last().pct_change().dropna()
    months_beating = (monthly_returns['nav'] > monthly_returns['value']).sum()
    quarters_beating = (quarterly_returns['nav'] > quarterly_returns['value']).sum()

    # Prepare data for heatmap
    monthly_returns['date'] = monthly_returns.index.strftime('%Y-%m')
    quarterly_returns['date'] = quarterly_returns.index.strftime('%Y-%Q')

    return volatility, abs(max_drawdown), sortino_ratio, beta, months_beating, quarters_beating, monthly_returns, quarterly_returns

def safe_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

# Main analysis function
def analyze_funds(selected_codes: List[str], funds: Dict[str, str], days: int, monthly_investment: float,
                  risk_appetite: float, benchmark_index: str):
    if not selected_codes:
        st.warning("Please select at least one mutual fund to analyze.")
        return

    benchmark_df = fetch_benchmark_data(benchmark_index, days)
    if benchmark_df.empty:
        st.warning("Benchmark data unavailable; Beta will be set to 1.0.")

    results = []
    fund_data = {}
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Consolidated data for charts
    cum_returns_fig = go.Figure()
    sip_growth_fig = go.Figure()
    monthly_returns_all = []
    quarterly_returns_all = []

    for i, code in enumerate(selected_codes):
        fund_name = funds[code]
        status_text.text(f"Analyzing {i + 1}/{len(selected_codes)}: {fund_name}")
        progress_bar.progress((i + 0.5) / len(selected_codes))

        try:
            nav_df = get_historical_nav(code, days)
            if nav_df.empty:
                st.warning(f"Could not fetch NAV data for {fund_name}")
                continue

            details = mf.get_scheme_details(code)
            abs_return, ann_return, sip_df = calculate_sip_returns(nav_df, monthly_investment)
            volatility, max_drawdown, sortino, beta, months_beating, quarters_beating, monthly_returns, quarterly_returns = calculate_risk_metrics(nav_df, benchmark_df)

            # Store results
            results.append({
                'Fund Name': fund_name, 'Code': code, 'Category': details.get('scheme_category', 'N/A'),
                'Type': details.get('scheme_type', 'N/A'), 'Latest NAV': nav_df['nav'].iloc[-1],
                'Absolute Return (%)': abs_return, 'Annualized Return (%)': ann_return,
                'Volatility (%)': volatility, 'Max Drawdown (%)': max_drawdown, 'Sortino Ratio': sortino,
                'Beta': beta, 'AUM (Cr)': safe_float(details.get('aum', '0').replace(' Cr.', '')),
                'Expense Ratio (%)': safe_float(details.get('expense_ratio', '0').replace('%', '')),
                'Min SIP Amount': safe_float(details.get('minimum_sip_investment', '0')),
                'Months Beating Benchmark': months_beating, 'Quarters Beating Benchmark': quarters_beating,
                'Total Invested': sip_df['total_invested'].iloc[-1] if not sip_df.empty else 0.0,
                'Current Value': sip_df['current_value'].iloc[-1] if not sip_df.empty else 0.0,
                'Profit/Loss': sip_df['profit_loss'].iloc[-1] if not sip_df.empty else 0.0
            })

            # Store fund data for later display
            fund_data[fund_name] = {
                'nav_df': nav_df, 'sip_df': sip_df, 'details': details,
                'volatility': volatility, 'max_drawdown': max_drawdown, 'sortino': sortino, 'beta': beta,
                'months_beating': months_beating, 'quarters_beating': quarters_beating,
                'total_months': len(pd.merge(nav_df, benchmark_df, on='date', how='inner').resample('M', on='date').last()),
                'total_quarters': len(pd.merge(nav_df, benchmark_df, on='date', how='inner').resample('Q', on='date').last())
            }

            # Add to consolidated cumulative returns chart
            merged_df = pd.merge(nav_df, benchmark_df, on='date', how='inner')
            fund_cum_returns = (1 + merged_df['nav'].pct_change()).cumprod() * 100 - 100
            cum_returns_fig.add_trace(go.Scatter(x=merged_df['date'], y=fund_cum_returns, name=fund_name))

            # Add to consolidated SIP growth chart
            if not sip_df.empty:
                sip_growth_fig.add_trace(go.Scatter(x=sip_df['date'], y=sip_df['current_value'], name=fund_name))

            # Collect returns for heatmap
            monthly_returns['Fund'] = fund_name
            quarterly_returns['Fund'] = fund_name
            monthly_returns_all.append(monthly_returns)
            quarterly_returns_all.append(quarterly_returns)

        except Exception as e:
            st.error(f"Error analyzing {fund_name}: {str(e)}")
            continue

        progress_bar.progress((i + 1) / len(selected_codes))

    progress_bar.empty()
    status_text.empty()

    if not results:
        st.error("No valid analysis results to display.")
        return

    # Add benchmark to cumulative returns chart
    if not benchmark_df.empty:
        bench_cum_returns = (1 + benchmark_df['value'].pct_change()).cumprod() * 100 - 100
        cum_returns_fig.add_trace(go.Scatter(x=benchmark_df['date'], y=bench_cum_returns, name=benchmark_index, line=dict(dash='dash')))
    cum_returns_fig.update_layout(title="Cumulative Returns Comparison", yaxis_title="Cumulative Return (%)")
    st.plotly_chart(cum_returns_fig, use_container_width=True)

    # Display consolidated SIP growth chart
    sip_growth_fig.update_layout(title="SIP Growth Comparison", xaxis_title='Date', yaxis_title='Amount (₹)')
    st.plotly_chart(sip_growth_fig, use_container_width=True)

    # Display fund details in columns with expanders
    st.header("Fund Details")
    num_cols = min(len(selected_codes), 3)  # Max 3 columns for readability
    cols = st.columns(num_cols)
    for idx, (fund_name, data) in enumerate(fund_data.items()):
        col = cols[idx % num_cols]
        with col:
            with st.expander(fund_name):
                details = data['details']
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Basic Information**")
                    st.write(f"**Category:** {details.get('scheme_category', 'N/A')}")
                    st.write(f"**Type:** {details.get('scheme_type', 'N/A')}")
                    st.write(f"**Launch Date:** {details.get('launch_date', 'N/A')}")
                    st.write(f"**Fund Manager:** {details.get('fund_manager', 'N/A')}")
                with col2:
                    st.write("**Performance Metrics**")
                    st.write(f"**Volatility:** {data['volatility']:.2f}%")
                    st.write(f"**Max Drawdown:** {data['max_drawdown']:.2f}%")
                    st.write(f"**Sortino Ratio:** {data['sortino']:.2f}")
                    st.write(f"**Beta:** {data['beta']:.2f}")

                col3, col4 = st.columns(2)
                with col3:
                    st.write("**Investment Details**")
                    st.write(f"**AUM:** {details.get('aum', 'N/A')}")
                    st.write(f"**Expense Ratio:** {details.get('expense_ratio', 'N/A')}")
                    st.write(f"**Min SIP:** ₹{details.get('minimum_sip_investment', 'N/A')}")
                with col4:
                    st.write("**Risk Profile**")
                    risk_level = "Low" if data['volatility'] <= 12 else "Medium" if data['volatility'] <= 20 else "High"
                    st.write(f"**Risk Level:** {risk_level}")
                    risk_meter = go.Figure(go.Indicator(
                        mode="gauge+number", value=data['volatility'], domain={'x': [0, 1], 'y': [0, 1]},
                        title={'text': "Risk Meter (Volatility)"},
                        gauge={'axis': {'range': [0, 40]}, 'steps': [
                            {'range': [0, 12], 'color': "lightgreen"},
                            {'range': [12, 20], 'color': "orange"},
                            {'range': [20, 40], 'color': "red"}],
                            'threshold': {'line': {'color': "black", 'width': 4}, 'value': data['volatility']}}
                    ))
                    st.plotly_chart(risk_meter, use_container_width=True)

                # SIP Details
                sip_df = data['sip_df']
                if not sip_df.empty:
                    total_invested = sip_df['total_invested'].iloc[-1]
                    current_value = sip_df['current_value'].iloc[-1]
                    abs_return = (current_value - total_invested) / total_invested * 100
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Invested", f"₹{total_invested:,.2f}")
                    col2.metric("Current Value", f"₹{current_value:,.2f}")
                    col3.metric("Absolute Return", f"{abs_return:.2f}%")
                    col1, col2 = st.columns(2)
                    col1.metric("Annualized Return (XIRR)", f"{results[idx]['Annualized Return (%)']:.2f}%")
                    col2.metric("Investment Period", f"{len(sip_df)} months")

                    st.write("**Monthly Transactions:**")
                    display_df = sip_df.copy()
                    display_df['Return (%)'] = display_df['current_value'].pct_change() * 100
                    display_df['Cumulative Return (%)'] = ((display_df['current_value'] - display_df['total_invested']) /
                                                          display_df['total_invested']) * 100
                    st.dataframe(display_df[['date', 'nav', 'investment', 'total_invested', 'current_value',
                                            'Return (%)', 'Cumulative Return (%)']].style.format({
                        'date': lambda x: x.strftime('%b %Y'), 'nav': '{:.2f}', 'investment': '₹{:.2f}',
                        'total_invested': '₹{:.2f}', 'current_value': '₹{:.2f}', 'Return (%)': '{:.2f}%',
                        'Cumulative Return (%)': '{:.2f}%'
                    }))

                # Beating benchmark summary
                st.write(f"**Performance vs {benchmark_index}:**")
                st.write(f"- Months Beating Benchmark: {data['months_beating']} out of {data['total_months']}")
                st.write(f"- Quarters Beating Benchmark: {data['quarters_beating']} out of {data['total_quarters']}")

    # Heatmaps for monthly and quarterly returns
    if monthly_returns_all and quarterly_returns_all:
        st.header("Returns Heatmaps")
        monthly_df = pd.concat(monthly_returns_all)
        quarterly_df = pd.concat(quarterly_returns_all)

        # Add benchmark returns to heatmaps
        bench_monthly = benchmark_df.resample('M', on='date').last().pct_change().dropna()
        bench_monthly['date'] = bench_monthly.index.strftime('%Y-%m')
        bench_monthly['Fund'] = benchmark_index
        bench_monthly = bench_monthly[['date', 'value', 'Fund']].rename(columns={'value': 'nav'})
        monthly_df = pd.concat([monthly_df, bench_monthly])

        bench_quarterly = benchmark_df.resample('Q', on='date').last().pct_change().dropna()
        bench_quarterly['date'] = bench_quarterly.index.strftime('%Y-%Q')
        bench_quarterly['Fund'] = benchmark_index
        bench_quarterly = bench_quarterly[['date', 'value', 'Fund']].rename(columns={'value': 'nav'})
        quarterly_df = pd.concat([quarterly_df, bench_quarterly])

        # Monthly heatmap
        monthly_pivot = monthly_df.pivot(index='date', columns='Fund', values='nav') * 100
        fig = go.Figure(data=go.Heatmap(
            z=monthly_pivot.values,
            x=monthly_pivot.columns,
            y=monthly_pivot.index,
            colorscale='RdYlGn',
            colorbar=dict(title="Return (%)"),
            text=monthly_pivot.values,
            texttemplate="%{text:.2f}%",
            textfont={"size": 10}
        ))
        fig.update_layout(title="Monthly Returns Heatmap", xaxis_title="Fund", yaxis_title="Date")
        st.plotly_chart(fig, use_container_width=True)

        # Quarterly heatmap
        quarterly_pivot = quarterly_df.pivot(index='date', columns='Fund', values='nav') * 100
        fig = go.Figure(data=go.Heatmap(
            z=quarterly_pivot.values,
            x=quarterly_pivot.columns,
            y=quarterly_pivot.index,
            colorscale='RdYlGn',
            colorbar=dict(title="Return (%)"),
            text=quarterly_pivot.values,
            texttemplate="%{text:.2f}%",
            textfont={"size": 10}
        ))
        fig.update_layout(title="Quarterly Returns Heatmap", xaxis_title="Fund", yaxis_title="Date")
        st.plotly_chart(fig, use_container_width=True)

    # Fund comparison table
    result_df = pd.DataFrame(results)
    st.header("Fund Comparison")
    display_cols = ['Fund Name', 'Category', 'Latest NAV', 'Total Invested', 'Current Value', 'Profit/Loss',
                    'Absolute Return (%)', 'Annualized Return (%)', 'Volatility (%)', 'Max Drawdown (%)',
                    'Sortino Ratio', 'Beta', 'Expense Ratio (%)', 'Months Beating Benchmark', 'Quarters Beating Benchmark']
    styled_df = result_df[display_cols].set_index('Fund Name').style.format({
        'Latest NAV': '{:.2f}', 'Total Invested': '₹{:.2f}', 'Current Value': '₹{:.2f}', 'Profit/Loss': '₹{:.2f}',
        'Absolute Return (%)': '{:.2f}%', 'Annualized Return (%)': '{:.2f}%', 'Volatility (%)': '{:.2f}%',
        'Max Drawdown (%)': '{:.2f}%', 'Sortino Ratio': '{:.2f}', 'Beta': '{:.2f}', 'Expense Ratio (%)': '{:.2f}%',
        'Months Beating Benchmark': '{:.0f}', 'Quarters Beating Benchmark': '{:.0f}'
    }).background_gradient(cmap='RdYlGn', subset=['Absolute Return (%)', 'Annualized Return (%)', 'Sortino Ratio']).background_gradient(
        cmap='RdYlGn_r', subset=['Volatility (%)', 'Max Drawdown (%)', 'Beta', 'Expense Ratio (%)'])
    st.dataframe(styled_df)

    # Recommendation
    weights = {
        'Annualized Return (%)': 0.4, 'Sortino Ratio': 0.2,
        'Volatility (%)': -0.2 * (1 - risk_appetite), 'Max Drawdown (%)': -0.1 * (1 - risk_appetite),
        'Beta': -0.1 * (1 - risk_appetite), 'Expense Ratio (%)': -0.1
    }
    scores = pd.DataFrame()
    for metric, weight in weights.items():
        norm = (result_df[metric] - result_df[metric].min()) / \
               (result_df[metric].max() - result_df[metric].min() + 1e-10) if weight > 0 else \
               1 - (result_df[metric] - result_df[metric].min()) / \
               (result_df[metric].max() - result_df[metric].min() + 1e-10)
        scores[metric] = norm * abs(weight)

    result_df['Score'] = scores.sum(axis=1)
    top_fund = result_df.loc[result_df['Score'].idxmax()]
    st.subheader(f"Recommended Fund: {top_fund['Fund Name']}")
    st.write(f"Reason: High annualized return ({top_fund['Annualized Return (%)']:.2f}%) and Beta ({top_fund['Beta']:.2f}) suitable for your risk appetite ({risk_appetite * 10}/10).")

# Sidebar
st.sidebar.header("Settings")
time_periods = {"1 Month": 30, "3 Months": 90, "6 Months": 180, "1 Year": 365, "3 Years": 1095, "5 Years": 1825}
time_period = st.sidebar.selectbox("Time Period", list(time_periods.keys()), index=3)
sip_amount = st.sidebar.number_input("Monthly SIP (₹)", min_value=500, value=5000, step=500)
risk_appetite = st.sidebar.slider("Risk Appetite (1-10)", 1, 10, 5) / 10
benchmark_options = ["NIFTY_50", "NIFTY_500", "NIFTY_MIDCAP_100", "NIFTY_SMLCAP_250"]
benchmark_index = st.sidebar.selectbox("Benchmark Index", benchmark_options, index=0)
category = st.sidebar.selectbox("Category", ["All"] + list(FUND_CATEGORIES.keys()))
query = st.sidebar.text_input("Search Funds")
funds = filter_funds(query, category)
selected_funds = st.sidebar.multiselect("Select Funds (Max 5)", list(funds.values()), max_selections=5)
selected_codes = [code for code, name in funds.items() if name in selected_funds]

# Main flow
st.title("Enhanced Indian Mutual Fund SIP Analyzer")
if st.sidebar.button("Analyze"):
    analyze_funds(selected_codes, funds, time_periods[time_period], sip_amount, risk_appetite, benchmark_index)
else:
    st.write("👈 Select funds, benchmark index, and click 'Analyze' to begin.")
    st.header("How to Choose the Best SIP")
    st.markdown("""
    ### Key Metrics
    - **Absolute Return**: Total return over the period.
    - **Annualized Return**: Compounded annual growth rate (XIRR).
    - **Risk Metrics**: Volatility, Drawdown, Beta (relative to benchmark).
    - **Expense Ratio**: Lower costs improve net returns.
    """)