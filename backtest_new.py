import streamlit as st
import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import yfinance as yf
from datetime import datetime, timedelta
import pytz
from typing import Dict, Any
import logging
import time
import pandas_ta as ta
import plotly.express as px
from common_utils import read_write_sql_data as rd
import itertools
import warnings
warnings.filterwarnings("ignore")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    layout="wide",
    page_title="📈 Advanced Trading Strategy Backtester",
    page_icon="📈",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .sidebar .sidebar-content { background-color: #ffffff; }
    .streamlit-expanderHeader { background-color: #ffffff; border-radius: 5px; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    </style>
    """, unsafe_allow_html=True)


def convert_to_timezone_aware(date_obj: datetime) -> datetime:
    if date_obj.tzinfo is None:
        return date_obj.replace(tzinfo=pytz.UTC)
    return date_obj


class EMACrossover(Strategy):
    n1: int = 20
    n2: int = 50
    sl_pct: float = 2.0
    trail_sl: bool = False
    size: float = 1.0

    def init(self):
        logger.info("Initializing strategy...")
        try:
            df = pd.DataFrame(self.data.df)
            df.index = pd.to_datetime(df.index)
            df['EMA1'] = df['Close'].ewm(span=self.n1, adjust=False).mean()
            df['EMA2'] = df['Close'].ewm(span=self.n2, adjust=False).mean()
            df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)

            if df[['EMA1', 'EMA2', 'ATR']].isnull().all().any():
                raise ValueError("Indicator calculation failed - insufficient data")

            self.ema1 = self.I(lambda: df['EMA1'], name='EMA1')
            self.ema2 = self.I(lambda: df['EMA2'], name='EMA2')
            self.ema3 = self.I(lambda: df['EMA1'] - (df['EMA2'] - df['EMA1']) / 2, name='Avg_EMA')
            self.atr = self.I(lambda: df['ATR'])

            self.sl_price = None
            self.trail_sl_price = None
            self.entry_price = None
            self.trail_multiplier = 2
            self.trades_log = []  # Initialize trades_log
        except Exception as e:
            logger.error(f"Strategy initialization failed: {str(e)}")
            raise

    def next(self):
        try:
            price = self.data.Close[-1]
            low_price = self.data.Low[-1]
            atr = self.atr[-1]

            if self.position and self.entry_price is not None:
                # Update trailing stop loss
                if self.trail_sl:
                    if self.trail_sl_price is None:
                        self.trail_sl_price = self.entry_price * (1 - self.sl_pct / 100)

                    if price >= self.entry_price * 1.03:
                        new_trail_sl = price - self.trail_multiplier * atr
                        self.trail_sl_price = max(self.trail_sl_price, new_trail_sl)

                    if self.trail_sl_price is not None and low_price <= self.trail_sl_price:
                        size = self.position.size
                        exit_price = self.trail_sl_price
                        if exit_price is not None and self.entry_price is not None:
                            pnl = (exit_price - self.entry_price) * size
                            return_pct = ((exit_price - self.entry_price) / self.entry_price) * 100
                            self.trades_log[-1].update({
                                'ExitTime': self.data.index[-1],
                                'ExitPrice': exit_price,
                                'PnL': pnl,
                                'ReturnPct': return_pct,
                                'StopLossPrice': self.trail_sl_price,
                                'ExitReason': 'Trailing Stop Loss'
                            })
                            logger.info(f"Trailing stop loss exit: {self.trades_log[-1]}")
                            self.position.close()
                            self.reset_trade_state()
                else:
                    if self.sl_price is not None and low_price <= self.sl_price:
                        size = self.position.size
                        exit_price = self.sl_price
                        if exit_price is not None and self.entry_price is not None:
                            pnl = (exit_price - self.entry_price) * size
                            return_pct = ((exit_price - self.entry_price) / self.entry_price) * 100
                            self.trades_log[-1].update({
                                'ExitTime': self.data.index[-1],
                                'ExitPrice': exit_price,
                                'PnL': pnl,
                                'ReturnPct': return_pct,
                                'StopLossPrice': self.sl_price,
                                'ExitReason': 'Stop Loss'
                            })
                            logger.info(f"Stop loss exit: {self.trades_log[-1]}")
                            self.position.close()
                            self.reset_trade_state()

            # Entry logic
            if crossover(self.ema1, self.ema2) and not self.position:
                self.buy(size=self.size)
                self.entry_price = price
                self.sl_price = price * (1 - self.sl_pct / 100) if not self.trail_sl else None
                self.trail_sl_price = None
                self.trades_log.append({
                    'EntryTime': self.data.index[-1],
                    'EntryPrice': price,
                    'StopLossPrice': self.sl_price,
                    'Size': self.size
                })
                logger.info(f"Trade entry: {self.trades_log[-1]}")

            # Exit on EMA crossover
            elif crossover(self.ema2, self.ema1) and self.position and self.entry_price is not None:
                size = self.position.size
                exit_price = price
                pnl = (exit_price - self.entry_price) * size
                return_pct = ((exit_price - self.entry_price) / self.entry_price) * 100
                self.trades_log[-1].update({
                    'ExitTime': self.data.index[-1],
                    'ExitPrice': exit_price,
                    'PnL': pnl,
                    'ReturnPct': return_pct,
                    'ExitReason': 'EMA Crossover'
                })
                logger.info(f"EMA crossover exit: {self.trades_log[-1]}")
                self.position.close()
                self.reset_trade_state()
        except Exception as e:
            logger.error(f"Error in next step: {str(e)}")
            raise

    def reset_trade_state(self):
        self.sl_price = None
        self.trail_sl_price = None
        self.entry_price = None


def display_strategy_metrics(stats: Dict[str, Any], trades: pd.DataFrame):
    st.markdown("### 📊 Performance Metrics")
    col1, col2, col3, col4 = st.columns(4)

    return_color = 'green' if stats['Return [%]'] > 0 else 'red'
    col1.metric("Total Return", f"{stats['Return [%]']:.2f}%",
                f"vs Buy & Hold: {stats['Buy & Hold Return [%]']:.2f}%",
                delta_color='normal')
    col2.metric("Sharpe Ratio", f"{stats['Sharpe Ratio']:.2f}", "Risk-adjusted return")
    col3.metric("Max Drawdown", f"{stats['Max. Drawdown [%]']:.2f}%",
                f"Duration: {stats['Max. Drawdown Duration']}")
    col4.metric("Portfolio Value", f"${stats['Equity Final [$]']:.2f}",
                f"Profit/Loss: ${stats['Equity Final [$]'] - 100000}",
                delta_color='normal')

    with st.expander("📈 Detailed Statistics", expanded=True):
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


def fetch_data(symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Fetch data from database with yfinance fallback"""
    logger.info(f"Fetching data for {symbol} from {start_date} to {end_date}")
    try:
        with st.spinner("Fetching data from database..."):
            query = f"SELECT * FROM {symbol} WHERE date BETWEEN '{start_date}' AND '{end_date}' ORDER BY DATE ASC"
            data = rd.get_table_data(query=query)
            if data.empty:
                raise ValueError("No data retrieved from database")

            data.index = pd.to_datetime(data['Date'])
            data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
            logger.info(f"Database fetch successful: {len(data)} rows")
            return data

    except Exception as e:
        logger.warning(f"Database fetch failed: {str(e)}. Falling back to yfinance...")
        try:
            with st.spinner("Fetching data from yfinance..."):
                data = yf.download(symbol, start=start_date, end=end_date, timeout=10)
                if data.empty:
                    raise ValueError("No data retrieved from yfinance")

                data.index = pd.to_datetime(data.index).tz_localize(None)
                data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
                logger.info(f"yfinance fetch successful: {len(data)} rows")
                return data
        except Exception as e:
            logger.error(f"yfinance fetch failed: {str(e)}")
            raise Exception(f"Data fetch failed: {str(e)}")


def run_backtest(data: pd.DataFrame, params: Dict[str, Any], initial_equity: float,
                 commission: float) -> Backtest:
    logger.info("Setting up backtest...")
    try:
        bt = Backtest(data, EMACrossover, cash=initial_equity, commission=commission, trade_on_close=True)
        return bt
    except Exception as e:
        logger.error(f"Backtest setup failed: {str(e)}")
        raise


def main():
    st.subheader("Trading Strategy Backtester")

    with st.sidebar:
        st.markdown("## 🎯 Strategy Setup")
        strategy_name = st.selectbox("Select Strategy", ["📊 EMA Crossover"])
        symbol = st.text_input("📌 Stock Symbol", "SBIN").upper()

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("📅 Start Date", datetime.now() - timedelta(days=365 * 10))
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
                # start_date = convert_to_timezone_aware(datetime.combine(start_date, datetime.min.time()))
                # end_date = convert_to_timezone_aware(datetime.combine(end_date, datetime.min.time()))

                if start_date >= end_date:
                    st.error("Start date must be before end date")
                    return

                data = fetch_data(symbol, start_date, end_date)

                if len(data) < max(param_configs.get('n1', 20), param_configs.get('n2', 50)):
                    st.error("Insufficient data for the selected EMA periods")
                    return

                tabs = st.tabs(["Backtest Stats", "Trade Analysis", "Strategy Plot"])

                if backtest_mode == "Standard":
                    params = {**param_configs, 'size': position_size / 100}
                    bt = run_backtest(data, params, initial_equity, commission / 100)

                    with st.spinner("Executing backtest..."):
                        start_time = time.time()
                        stats = bt.run(**params)
                        logger.info(f"Backtest completed in {time.time() - start_time:.2f} seconds")

                    trades = pd.DataFrame(stats['_trades'])
                    trades_log = getattr(bt._strategy, 'trades_log', [])

                    if not trades.empty:
                        trades['EntryTime'] = pd.to_datetime(trades['EntryTime'])
                        trades['ExitTime'] = pd.to_datetime(trades['ExitTime'])

                        logger.info(f"Trades columns: {trades.columns.tolist()}")
                        logger.info(f"Trades log: {trades_log}")

                        if trades_log:
                            trades_log_df = pd.DataFrame(trades_log)
                            trades_log_df['EntryTime'] = pd.to_datetime(trades_log_df['EntryTime'])
                            trades = trades.merge(
                                trades_log_df[
                                    ['EntryTime', 'EntryPrice', 'ExitPrice', 'PnL', 'ReturnPct', 'StopLossPrice',
                                     'ExitReason']],
                                on='EntryTime',
                                how='left'
                            )
                            trades['PnL'] = trades['PnL_y'].combine_first(trades['PnL_x'])
                            trades['ReturnPct'] = trades['ReturnPct_y'].combine_first(trades['ReturnPct_x'])
                            trades['ExitPrice'] = trades['ExitPrice'].combine_first(trades['ExitPrice_x'])
                            trades['EntryPrice'] = trades['EntryPrice'].combine_first(trades['EntryPrice_x'])
                            trades.drop(
                                columns=['PnL_x', 'PnL_y', 'ReturnPct_x', 'ReturnPct_y', 'EntryPrice_x', 'ExitPrice_x'],
                                errors='ignore', inplace=True)
                        else:
                            # Use existing columns from trades
                            trades['EntryPrice'] = trades.get('EntryPrice', np.nan)
                            trades['ExitPrice'] = trades.get('ExitPrice', np.nan)
                            trades['StopLossPrice'] = np.nan
                            trades['ExitReason'] = 'Unknown'

                        trades['Cumulative PnL'] = trades['PnL'].cumsum()
                        trades['Cumulative Capital'] = initial_equity + trades['Cumulative PnL']

                    with tabs[0]:
                        display_strategy_metrics(stats, trades)

                    with tabs[1]:
                        if not trades.empty:
                            trade_df = trades.copy()
                            trade_df['PnL'] = trade_df['PnL'].round(2)
                            trade_df['ReturnPct'] = trade_df['ReturnPct'].round(2)*100
                            trade_df['Cumulative PnL'] = trade_df['Cumulative PnL'].round(2)
                            trade_df['Cumulative Capital'] = trade_df['Cumulative Capital'].round(2)
                            trade_df['StopLossPrice'] = trade_df['StopLossPrice'].round(2)
                            trade_df['EntryPrice'] = trade_df['EntryPrice'].round(2)
                            trade_df['ExitPrice'] = trade_df['ExitPrice'].round(2)

                            # Color coding for trades
                            def color_trades(val):
                                color = 'green' if val > 0 else 'red' if val < 0 else 'black'
                                return f'color: {color}'

                            styled_trades = trade_df.style.applymap(color_trades, subset=['PnL', 'ReturnPct'])
                            st.dataframe(styled_trades)
                        else:
                            st.info("No trades executed")

                    with tabs[2]:
                        with st.spinner("Generating plot..."):
                            bt.plot(open_browser=False, filename="backtest_plot.html")
                            with open("backtest_plot.html", "r", encoding="utf-8") as f:
                                html_plot = f.read()
                            st.components.v1.html(html_plot, height=800, scrolling=True)

                else:

                    # Prepare parameter combinations for optimization
                    strategy = EMACrossover

                    # Generate parameter combinations
                    def generate_param_combinations(param_configs):
                        # Convert single values to lists
                        opt_params = {}
                        for param, value in param_configs.items():
                            if not isinstance(value, (list, tuple)):
                                opt_params[param] = [value]
                            else:
                                # For range-based parameters
                                if len(value) == 2:  # Slider input
                                    if param in ['n1', 'n2']:
                                        opt_params[param] = list(range(value[0], value[1] + 1, 5))
                                    elif param == 'sl_pct':
                                        opt_params[param] = list(np.arange(value[0], value[1] + 0.5, 0.5))
                                else:
                                    opt_params[param] = value

                        # Generate all combinations
                        keys, values = zip(*opt_params.items())
                        param_combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]
                        return param_combinations

                    # Generate parameter combinations
                    param_combinations = generate_param_combinations(param_configs)

                    # Run optimization
                    bt = Backtest(data, strategy, cash=100000, commission=.002)

                    # Collect optimization results
                    optimization_results = []

                    # Progress bar
                    progress_text = "Backtest in progress. Please wait.."
                    progress_bar = st.progress(0, text=progress_text)

                    # Run backtests for all parameter combinations
                    for i, params_dict in enumerate(param_combinations):
                        # Update progress bar
                        progress_bar.progress((i + 1) / len(param_combinations), text=progress_text)

                        # Run backtest for this specific parameter combination
                        try:
                            run_stats = bt.run(**params_dict)

                            # Collect results
                            result = params_dict.copy()
                            result.update({
                                'Return [%]': run_stats['Return [%]'],
                                'CAGR [%]': run_stats['Return (Ann.) [%]'],
                                'Max Drawdown [%]': run_stats['Max. Drawdown [%]'],
                                'Sharpe Ratio': run_stats['Sharpe Ratio'],
                                'Total Trades': run_stats['# Trades']
                            })
                            optimization_results.append(result)
                        except Exception as e:
                            st.warning(f"Skipping combination {params_dict} due to error: {e}")

                    # Convert results to DataFrame
                    results_df = pd.DataFrame(optimization_results)

                    # Close progress bar
                    progress_bar.empty()

                    # Display optimization results
                    st.subheader("Optimization Results")
                    st.dataframe(results_df)

                    # Find and highlight best parameters
                    best_return = results_df.loc[results_df['Return [%]'].idxmax()]
                    best_sharpe = results_df.loc[results_df['Sharpe Ratio'].idxmax()]

                    st.subheader("Best Performing Parameters")

                    # Best by Total Return
                    st.markdown("**Best by Total Return:**")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Parameters", str(dict(best_return[list(param_configs.keys())])))
                    col2.metric("Return (%)", f"{best_return['Return [%]']:.2f}%")
                    col3.metric("CAGR (%)", f"{best_return['CAGR [%]']:.2f}%")

                    # Best by Sharpe Ratio
                    st.markdown("**Best by Sharpe Ratio:**")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Parameters", str(dict(best_sharpe[list(param_configs.keys())])))
                    col2.metric("Sharpe Ratio", f"{best_sharpe['Sharpe Ratio']:.2f}")
                    col3.metric("Return (%)", f"{best_sharpe['Return [%]']:.2f}%")

                    # Visualize optimization results
                    st.subheader("Optimization Visualization")

                    # Scatter plot of key metrics
                    fig = px.scatter(
                        results_df,
                        x='Return [%]',
                        y='Sharpe Ratio',
                        color='Max Drawdown [%]',
                        hover_data=list(param_configs.keys()) + ['Return [%]', 'Sharpe Ratio', 'Max Drawdown [%]'],
                        title='Optimization Results: Return vs Sharpe Ratio'
                    )
                    st.plotly_chart(fig)

            except Exception as e:
                st.error(f"Backtest failed: {str(e)}")
                logger.error(f"Backtest error: {str(e)}")


if __name__ == "__main__":
    main()