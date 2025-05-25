import streamlit as st
import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import yfinance as yf
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
import pandas_ta as ta
from lightweight_charts.widgets import StreamlitChart
import itertools
from plotly.subplots import make_subplots
from typing import Optional, Dict, Any
import logging
from common_utils import read_write_sql_data as rd

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set page configuration
st.set_page_config(
    layout="wide",
    page_title="📈 Advanced Trading Strategy Backtester",
    page_icon="📈",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .sidebar .sidebar-content { background-color: #ffffff; }
    .streamlit-expanderHeader { background-color: #ffffff; border-radius: 5px; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    </style>
    """, unsafe_allow_html=True)


class EMACrossover(Strategy):
    n1: int = 20
    n2: int = 50
    sl_pct: float = 2.0
    trail_sl: bool = False
    size: float = 1.0

    def init(self):
        try:
            df = pd.DataFrame(self.data.df)
            df.index = pd.to_datetime(df.index)  # Ensure index is datetime
            df['EMA1'] = ta.ema(df['Close'], length=self.n1)
            df['EMA2'] = ta.ema(df['Close'], length=self.n2)
            df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)

            if df[['EMA1', 'EMA2', 'ATR']].isnull().all().any():
                raise ValueError("Indicator calculation failed - insufficient data")

            self.ema1 = self.I(lambda: df['EMA1'])
            self.ema2 = self.I(lambda: df['EMA2'])
            self.atr = self.I(lambda: df['ATR'])

            self.sl_price = None
            self.trail_sl_price = None
            self.entry_price = None
            self.trail_multiplier = 2
        except Exception as e:
            logger.error(f"Strategy initialization failed: {str(e)}")
            raise

    def next(self):
        try:
            price = self.data.Close[-1]
            atr = self.atr[-1]

            if self.position:
                if self.entry_price is None:
                    self.entry_price = self.position.entry_price

                if self.trail_sl:
                    if self.trail_sl_price is None:
                        self.trail_sl_price = self.entry_price * (1 - self.sl_pct / 100)

                    if price >= self.entry_price * 1.03:
                        new_trail_sl = price - self.trail_multiplier * atr
                        self.trail_sl_price = max(self.trail_sl_price, new_trail_sl)

                    if price <= self.trail_sl_price:
                        self.position.close()
                        self.reset_trade_state()
                else:
                    if price <= self.sl_price:
                        self.position.close()
                        self.reset_trade_state()

            if crossover(self.ema1, self.ema2) and not self.position:
                self.buy(size=self.size)
                self.entry_price = price
                self.sl_price = price * (1 - self.sl_pct / 100) if not self.trail_sl else None
                self.trail_sl_price = None

            elif crossover(self.ema2, self.ema1) and self.position:
                self.position.close()
                self.reset_trade_state()
        except Exception as e:
            logger.error(f"Error in next step: {str(e)}")

    def reset_trade_state(self):
        self.sl_price = None
        self.trail_sl_price = None
        self.entry_price = None

def create_trading_chart(data: pd.DataFrame, signals: Optional[pd.DataFrame] = None) -> StreamlitChart:
    # Prepare data in the format expected by lightweight-charts
    chart_data = data.reset_index().rename(columns={
        'Date': 'time',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume'
    })

    # Create chart
    chart = StreamlitChart(height=800)

    # Set data
    chart.set(chart_data[['time', 'open', 'high', 'low', 'close']])

    # Add buy/sell markers if signals exist
    if signals is not None and not signals.empty:
        buy_signals = signals[signals['Signal'] == 'Buy']
        sell_signals = signals[signals['Signal'] == 'Sell']

        # Add buy markers
        for time in buy_signals.index:
            chart.marker(
                time=time,
                position='below',
                shape='arrow_up',
                color='#00FF00',
                text='Buy'
            )

        # Add sell markers
        for time in sell_signals.index:
            chart.marker(
                time=time,
                position='above',
                shape='arrow_down',
                color='#FF0000',
                text='Sell'
            )

    # Customize appearance
    chart.layout(background_color='#ffffff', text_color='#333333')
    chart.watermark(f"Price Action", color='rgba(0, 0, 0, 0.3)')
    chart.legend(True)

    return chart


def calculate_additional_stats(stats: Dict[str, Any], trades: pd.DataFrame) -> Dict[str, float]:
    """Calculate additional statistical measures"""
    additional_stats = {
        'Skewness': 0.0,
        'Kurtosis': 0.0,
        'Profit Factor': 0.0,
        'Avg Trade Duration (days)': 0.0,
        'Kelly Criterion': 0.0
    }

    try:
        if not trades.empty:
            returns = trades['ReturnPct'].to_numpy() / 100  # Convert to numpy array and to decimal

            # Skewness and Kurtosis
            additional_stats['Skewness'] = stats.skew(returns) if len(returns) > 1 else 0.0
            additional_stats['Kurtosis'] = stats.kurtosis(returns) if len(returns) > 1 else 0.0

            # Profit Factor
            profits = trades[trades['PnL'] > 0]['PnL'].sum()
            losses = abs(trades[trades['PnL'] < 0]['PnL'].sum())
            additional_stats['Profit Factor'] = profits / losses if losses > 0 else float('inf')

            # Average Trade Duration
            trade_durations = (trades['ExitTime'] - trades['EntryTime']).dt.total_seconds() / 86400
            additional_stats['Avg Trade Duration (days)'] = trade_durations.mean() if not trade_durations.empty else 0.0

            # Kelly Criterion
            win_rate = stats['Win Rate [%]'] / 100
            avg_win = trades[trades['PnL'] > 0]['ReturnPct'].mean() / 100
            avg_loss = abs(trades[trades['PnL'] < 0]['ReturnPct'].mean() / 100)
            if avg_loss > 0 and not pd.isna(avg_win) and not pd.isna(avg_loss):
                additional_stats['Kelly Criterion'] = (win_rate * avg_win - (1 - win_rate) * avg_loss) / (
                            avg_win * avg_loss)
    except Exception as e:
        logger.error(f"Error calculating additional stats: {str(e)}")

    return additional_stats


def display_strategy_metrics(stats: Dict[str, Any], trades: pd.DataFrame):
    st.markdown("### 📊 Performance Metrics")
    col1, col2, col3 = st.columns(3)

    return_color = 'green' if stats['Return [%]'] > 0 else 'red'
    col1.metric("Total Return", f"{stats['Return [%]']:.2f}%",
                f"vs Buy & Hold: {stats['Buy & Hold Return [%]']:.2f}%",
                delta_color='normal')
    col2.metric("Sharpe Ratio", f"{stats['Sharpe Ratio']:.2f}", "Risk-adjusted return")
    col3.metric("Max Drawdown", f"{stats['Max. Drawdown [%]']:.2f}%",
                f"Duration: {stats['Max. Drawdown Duration']}")

    with st.expander("📈 Detailed Statistics"):
        detailed_cols = st.columns(3)
        metrics = {
            "Trading Metrics": {
                "Number of Trades": stats['# Trades'],
                "Win Rate": f"{stats['Win Rate [%]']:.2f}%",
                "Best Trade": f"{stats['Best Trade [%]']:.2f}%",
                "Worst Trade": f"{stats['Worst Trade [%]']:.2f}%"
            },
            "Risk Metrics": {
                "Volatility (Ann.)": f"{stats['Volatility (Ann.) [%]']:.2f}%",
                "Calmar Ratio": f"{stats['Calmar Ratio']:.2f}",
                "Sortino Ratio": f"{stats['Sortino Ratio']:.2f}",
                "SQN": f"{stats['SQN']:.2f}"
            },
            "Time Metrics": {
                "Exposure Time": f"{stats['Exposure Time [%]']:.2f}%",
                "Start": stats['Start'],
                "End": stats['End'],
                "Duration": stats['Duration']
            }
        }

        for i, (category, category_metrics) in enumerate(metrics.items()):
            with detailed_cols[i]:
                st.markdown(f"**{category}**")
                for name, value in category_metrics.items():
                    st.markdown(f"- {name}: {value}")

    # Additional Statistical Analysis
    with st.expander("🔍 Advanced Statistical Analysis"):
        additional_stats = calculate_additional_stats(stats, trades)
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Distribution Metrics**")
            st.write(f"Skewness: {additional_stats.get('Skewness', 0):.2f}")
            st.write(f"Kurtosis: {additional_stats.get('Kurtosis', 0):.2f}")

        with col2:
            st.markdown("**Performance Metrics**")
            st.write(f"Profit Factor: {additional_stats.get('Profit Factor', 0):.2f}")
            st.write(f"Avg Trade Duration: {additional_stats.get('Avg Trade Duration (days)', 0):.2f} days")
            st.write(f"Kelly Criterion: {additional_stats.get('Kelly Criterion', 0):.2f}")


def fetch_data(symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Fetch data from yfinance as fallback if database fails"""
    try:
        # First try database
        query = f"SELECT * FROM {symbol} WHERE date BETWEEN '{start_date}' AND '{end_date}' ORDER BY DATE ASC"
        data = rd.get_table_data(query=query)  # Replace with your actual function

        if data.empty:
            raise ValueError("No data retrieved")

        data.index = pd.to_datetime(data['Date'])
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
        return data
    except Exception as e:
        logger.error(f"Data fetch failed: {str(e)}")
        raise


def run_backtest(data: pd.DataFrame, params: Dict[str, Any], initial_equity: float,
                 commission: float) -> Dict[str, Any]:
    """Run backtest with given parameters"""
    bt = Backtest(data, EMACrossover, cash=initial_equity, commission=commission)
    return bt.run(**params)


def main():
    st.subheader("Trading Strategy Backtester")

    with st.sidebar:
        st.markdown("## 🎯 Strategy Setup")
        strategy_name = st.selectbox("Select Strategy", ["📊 EMA Crossover"])
        symbol = st.text_input("📌 Stock Symbol", "AAPL").upper()

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("📅 Start Date", datetime.now() - timedelta(days=365))
        with col2:
            end_date = st.date_input("📅 End Date", datetime.now())

        with st.expander("💰 Trading Parameters"):
            initial_equity = st.number_input("Initial Capital ($)", value=100000, min_value=1000)
            position_size = st.slider("Position Size (%)", min_value=1, max_value=100, value=100)
            commission = st.number_input("Commission (%)", value=0.12, format="%.4f")

        st.markdown("### 📈 EMA Parameters")
        backtest_mode = st.radio("🔄 Backtest Mode", ["Standard", "Optimization"])

        param_configs = {}
        if backtest_mode == "Standard":
            col1, col2 = st.columns(2)
            with col1:
                param_configs['n1'] = st.number_input("Fast EMA", value=20, min_value=1)
            with col2:
                param_configs['n2'] = st.number_input("Slow EMA", value=50, min_value=1)
            param_configs['sl_pct'] = st.slider("Stop Loss %", 0.5, 10.0, 2.0, 0.1)
            param_configs['trail_sl'] = st.checkbox("Trail Stop Loss", value=False)
        else:
            optimize_params = st.multiselect("Select Parameters to Optimize",
                                             ["EMA1 Period", "EMA2 Period", "Stop Loss %", "Trail Stop Loss"])
            param_configs['n1'] = st.slider("Fast EMA Range", 5, 200, (10, 50),
                                            5) if "EMA1 Period" in optimize_params else st.number_input("Fast EMA",
                                                                                                        value=20)
            param_configs['n2'] = st.slider("Slow EMA Range", 10, 300, (30, 100),
                                            10) if "EMA2 Period" in optimize_params else st.number_input("Slow EMA",
                                                                                                         value=50)
            param_configs['sl_pct'] = st.slider("Stop Loss % Range", 0.5, 10.0, (1.0, 3.0),
                                                0.5) if "Stop Loss %" in optimize_params else st.number_input(
                "Stop Loss %", value=2.0)
            param_configs['trail_sl'] = [True, False] if "Trail Stop Loss" in optimize_params else st.checkbox(
                "Trail Stop Loss")

        run_backtest_btn = st.button("🚀 Run Backtest", use_container_width=True)

    if run_backtest_btn:
        with st.spinner('Running backtest...'):
            try:
                if start_date >= end_date:
                    st.error("Start date must be before end date")
                    return

                data = fetch_data(symbol, start_date, end_date)

                if len(data) < max(param_configs.get('n1', 20), param_configs.get('n2', 50)):
                    st.error("Insufficient data for the selected EMA periods")
                    return

                tabs = st.tabs(["Backtest Stats", "Trade Analysis", "Equity Curve", "Drawdown"])

                if backtest_mode == "Standard":
                    params = {**param_configs, 'size': position_size / 100}
                    stats = run_backtest(data, params, initial_equity, commission / 100)

                    # Convert trades to DataFrame with proper datetime handling
                    trades = pd.DataFrame(stats['_trades'])

                    if not trades.empty:
                        trades['EntryTime'] = pd.to_datetime(trades['EntryTime'])
                        trades['ExitTime'] = pd.to_datetime(trades['ExitTime'])

                    with tabs[0]:
                        display_strategy_metrics(stats, trades)
                        signals = pd.DataFrame(index=data.index)
                        # print(signals)
                        signals['Price'] = data['Close']
                        signals['Signal'] = None
                        if not trades.empty:
                            for _, trade in trades.iterrows():
                                # print(trade)
                                signals.loc[trade['EntryTime'], 'Signal'] = 'Buy'
                                signals.loc[trade['ExitTime'], 'Signal'] = 'Sell'

                        # st.plotly_chart(create_candlestick_chart(data, signals), use_container_width=True)
                        chart = create_trading_chart(data, signals)
                        chart.load()

                    with tabs[1]:
                        if not trades.empty:
                            trade_df = trades.copy()
                            trade_df['PnL'] = trade_df['PnL'].round(2)
                            trade_df['ReturnPct'] = trade_df['ReturnPct'].round(2)
                            st.dataframe(trade_df.style.format({'PnL': '{:.2f}', 'ReturnPct': '{:.2f}'}), height=400)
                        else:
                            st.info("No trades executed")

                    with tabs[2]:
                        equity_data = stats['_equity_curve']['Equity']
                        st.plotly_chart(go.Figure(
                            data=[go.Scatter(x=equity_data.index, y=equity_data, mode='lines', name='Equity')]),
                                        use_container_width=True)

                    with tabs[3]:
                        drawdown_data = stats['_equity_curve']['DrawdownPct'] * 100
                        st.plotly_chart(go.Figure(data=[
                            go.Scatter(x=drawdown_data.index, y=drawdown_data, mode='lines', name='Drawdown',
                                       fill='tozeroy')]),
                                        use_container_width=True)
                else:
                    # Optimization code would go here
                    pass

            except Exception as e:
                st.error(f"Backtest failed: {str(e)}")
                logger.error(f"Backtest error: {str(e)}")


if __name__ == "__main__":
    main()