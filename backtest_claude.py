import streamlit as st
import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
from backtesting.lib import crossover, barssince, TrailingStrategy
import yfinance as yf
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
import pandas_ta as ta
import itertools
import logging
import warnings
from common_utils import read_write_sql_data as rd
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


def fetch_stock_data(stock_symbol, start_dt, end_dt):
    query = f"select * from nsedata.public.{stock_symbol} where Date between '{start_dt}' and '{end_dt}' order by Date"
    df = rd.get_table_data(query=query)
    df.set_index("Date", inplace=True)
    # df.sort_index(inplace=True)
    return df


class LRMACrossOver(TrailingStrategy):
    lin_reg_days_fast = 5
    # lin_reg_days_slow = 24
    ma_days = 20

    def init(self):
        super().init()
        # self.lin_reg_slow = self.I(ta.linreg, self.data.Close, self.lin_reg_days_slow)
        self.lin_reg_fast = self.I(ta.linreg, pd.Series(self.data.Close), self.lin_reg_days_fast)
        self.lin_reg_chg = np.diff(self.lin_reg_fast)
        self.ema = self.I(ta.ema, pd.Series(self.data.Close), self.ma_days)
        # self.stop_price = 0
        self.curr_support = self.prev_support = self.curr_res = self.prev_res = self.stop_price= 0.0
        self.supp_cond = self.res_cond = False
        self.set_trailing_sl(2)

    def next(self):
        super().next()
        # print(self.data)
        # self.supp_cond = (self.lin_reg_chg[-1] >=0 and self.lin_reg_chg[-3] < 0
        #              and self.lin_reg_chg[-4] < 0 and self.lin_reg_chg[-2] < 0)
        self.supp_cond = (self.lin_reg_chg[-2] >= 0 > self.lin_reg_chg[-3] and self.lin_reg_chg[-4] < 0
                          and self.lin_reg_chg[-5] < 0
                          )
        if self.supp_cond:
            self.prev_support = self.curr_support
            self.curr_support = self.lin_reg_fast[-1]

        # self.res_cond = (self.lin_reg_chg[-1] < 0 and self.lin_reg_chg[-2] >= 0
        #             and self.lin_reg_chg[-3] >= 0 and self.lin_reg_chg[-4] >= 0)

        self.res_cond = (self.lin_reg_chg[-2] < 0 <= self.lin_reg_chg[-5]
                         and self.lin_reg_chg[-3] >= 0 and self.lin_reg_chg[-4] >= 0)
        if self.res_cond:
            self.prev_res = self.curr_res
            self.curr_res = self.lin_reg_fast[-1]

        if (self.prev_support < self.curr_support < self.data.Close[-1]
            and self.data.Close[-1] > self.ema) and self.supp_cond:
            if not self.position:
                self.buy()
                self.supp_cond = False
                # self.stop_price = self.data.Close[-1]*.95

        # print(self.data.index[-1], self.data.Close[-1], self.stop_price)
        if self.prev_res > self.curr_res > self.data.Close[-1] and self.res_cond:
            if self.position:
                self.position.close()
                self.res_cond = False


# Define strategies
class EMACrossover(Strategy):
    n1 = 20  # Fast EMA period
    n2 = 50  # Slow EMA period
    sl_pct = 2.0  # Stop loss percentage
    trail_sl = False  # Trailing stop loss flag

    def init(self):
        # Convert the Backtesting.py data to a DataFrame
        df = pd.DataFrame(self.data.df)

        # Calculate indicators using pandas_ta
        df['EMA1'] = df['Close'].ewm(span=self.n1, adjust=False).mean()
        df['EMA2'] = df['Close'].ewm(span=self.n2, adjust=False).mean()
        df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)

        # Register the indicators with Backtesting.py
        self.ema1 = self.I(lambda: df['EMA1'])
        self.ema2 = self.I(lambda: df['EMA2'])
        self.atr = self.I(lambda: df['ATR'])

        self.sl_price = None
        self.trail_sl_price = None
        self.entry_price = None
        self.trail_multiplier = 2  # ATR multiplier for trailing stop loss

    def next(self):
        price = self.data.Close[-1]
        atr = self.atr[-1]

        # Check if we have a position
        if self.position:
            # First time setting up stop loss or trailing stop loss
            if self.entry_price is None:
                self.entry_price = self.position.entry_price

            # Trailing Stop Loss Logic
            if self.trail_sl:
                # Initial stop loss at entry
                if self.trail_sl_price is None:
                    self.trail_sl_price = self.entry_price * (1 - self.sl_pct / 100)

                # Update trailing stop loss when price moves in our favor
                if price >= self.entry_price * 1.03:  # 3% profit threshold
                    new_trail_sl = price - self.trail_multiplier * atr
                    # Only move stop loss up (for long positions)
                    self.trail_sl_price = max(self.trail_sl_price, new_trail_sl)

                # Stop out if price drops below trailing stop loss
                if price <= self.trail_sl_price:
                    self.position.close()
                    self.reset_trade_state()
            else:
                # Fixed stop loss logic
                if price <= self.sl_price:
                    self.position.close()
                    self.reset_trade_state()

        # Entry signals
        if crossover(self.ema1, self.ema2):
            if not self.position:
                self.buy()
                self.entry_price = price

                # Set stop loss
                if not self.trail_sl:
                    self.sl_price = price * (1 - self.sl_pct / 100)
                else:
                    self.trail_sl_price = None  # Will be set in next method call

        # Exit signals
        elif crossover(self.ema2, self.ema1):
            if self.position:
                self.position.close()
                self.reset_trade_state()

    def reset_trade_state(self):
        """Reset trade-related state variables."""
        self.sl_price = None
        self.trail_sl_price = None
        self.entry_price = None


# Streamlit UI
st.set_page_config(layout="wide", page_title="Advanced Trading Strategy Backtester")

st.subheader(":rainbow[Advanced Trading Strategy Backtester]")

# Sidebar for inputs
with st.sidebar:
    st.header("Strategy Parameters")

    # Strategy selection
    strategy_name = st.selectbox(
        "Select Strategy",
        ["EMA Crossover", "LRMACrossOver"]
    )

    # Stock symbol and date range
    symbol = st.text_input("Stock Symbol", "SBIN")
    start_date = st.date_input("Start Date", datetime.now() - timedelta(days=365 * 3))
    end_date = st.date_input("End Date", datetime.now())

    # Backtest mode selection
    backtest_mode = st.radio(
        "Backtest Mode",
        ["Standard Backtest", "Parameter Optimization"],
        index=0
    )

    # Parameter ranges and inputs
    param_configs = {}

    if backtest_mode == "Parameter Optimization":
        # Strategy-specific parameters
        if strategy_name == "EMA Crossover":
            # Optimization parameters
            st.subheader("Optimization Parameters")

            # Multi-select for parameters to optimize
            optimize_params = st.multiselect(
                "Select Parameters to Optimize",
                ["EMA1 Period", "EMA2 Period", "Stop Loss %", "Trail Stop Loss"],
                default=[]
            )

            # EMA1 Period
            if "EMA1 Period" in optimize_params:
                param_configs['n1'] = st.slider(
                    "EMA1 Period Range", min_value=5, max_value=200, value=(10, 50), step=5)
            else:
                param_configs['n1'] = st.number_input("EMA1 Period", min_value=1, value=20)

            # EMA2 Period
            if "EMA2 Period" in optimize_params:
                param_configs['n2'] = st.slider(
                    "EMA2 Period Range", min_value=10, max_value=300, value=(30, 100), step=10)
            else:
                param_configs['n2'] = st.number_input("EMA2 Period", min_value=1, value=50)

            # Stop Loss %
            if "Stop Loss %" in optimize_params:
                param_configs['sl_pct'] = st.slider(
                    "Stop Loss % Range", min_value=0.5, max_value=10.0, value=(1.0, 3.0), step=0.5)
            else:
                param_configs['sl_pct'] = st.number_input("Stop Loss %", min_value=0.1, value=2.0, step=0.1)

            # Trail Stop Loss
            if "Trail Stop Loss" in optimize_params:
                param_configs['trail_sl'] = [True, False]
            else:
                param_configs['trail_sl'] = st.checkbox("Trail Stop Loss", value=False)
    else:
        if strategy_name == "EMA Crossover":
            param_configs['n1'] = st.number_input("EMA1 Period", min_value=1, value=20)
            param_configs['n2'] = st.number_input("EMA2 Period", min_value=1, value=50)
            param_configs['sl_pct'] = st.number_input("Stop Loss %", min_value=0.1, value=2.0, step=0.1)
            param_configs['trail_sl'] = st.checkbox("Trail Stop Loss", value=False)

    # Run backtest button
    run_backtest = st.button("Run Backtest")

# Main content
if run_backtest:
    try:
        # Fetch data
        # data = yf.download(symbol, start=start_date, end=end_date, multi_level_index=False)
        data = fetch_stock_data(symbol, start_dt=start_date, end_dt=end_date)
        print(data.head())

        if data.empty:
            st.error(f"No data found for symbol {symbol}. Please check the symbol and date range.")
        else:
            # Standard Backtest
            if backtest_mode == "Standard Backtest":
                # Initialize strategy
                strategy = EMACrossover
                params = {
                    'n1': param_configs['n1'],
                    'n2': param_configs['n2'],
                    'sl_pct': param_configs['sl_pct'],
                    'trail_sl': param_configs['trail_sl'] if isinstance(param_configs['trail_sl'], bool) else False
                }

                # Run backtest
                bt = Backtest(data, strategy, cash=100000, commission=.002, trade_on_close=True)
                stats = bt.run(**params)

                # Display metrics in a more organized way
                st.subheader("Backtest Performance Metrics")
                metrics_cols = st.columns(3)

                performance_metrics = {
                    'Return (%)': stats['Return [%]'],
                    'Buy & Hold Return (%)': stats['Buy & Hold Return [%]'],
                    'CAGR (%)': stats['Return (Ann.) [%]'],
                    'Win Rate (%)': stats['Win Rate [%]'],
                    'Sharpe Ratio': stats['Sharpe Ratio'],
                    'Max Drawdown (%)': stats['Max. Drawdown [%]']
                }

                for i, (metric, value) in enumerate(performance_metrics.items()):
                    metrics_cols[i % 3].metric(metric, f"{value:.2f}")

                # Trades analysis
                st.subheader("Trade Analysis")
                if any(stats._trades):
                    # Enhanced trade log with color coding
                    trade_df = stats._trades.copy()
                    trade_df['PnL'] = trade_df['PnL'].round(2)
                    trade_df['ReturnPct'] = trade_df['ReturnPct'].round(2)

                    # Color coding for trades
                    def color_trades(val):
                        color = 'green' if val > 0 else 'red' if val < 0 else 'black'
                        return f'color: {color}'


                    styled_trades = trade_df.style.applymap(color_trades, subset=['PnL', 'ReturnPct'])
                    st.dataframe(styled_trades)
                else:
                    st.info("No trades were executed during the backtest period.")

                # Backtest plot
                st.subheader("Equity Curve")
                bt.plot(filename='backtest_plot.html', open_browser=False)

                # Read and display the generated HTML plot
                with open('backtest_plot.html', 'r') as f:
                    plot_html = f.read()
                st.components.v1.html(plot_html, height=800)

            # Parameter Optimization
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
        st.error(f"An error occurred: {str(e)}")