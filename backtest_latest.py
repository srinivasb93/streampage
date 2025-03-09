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
import itertools
import pytz
from plotly.subplots import make_subplots


# Set page configuration with custom theme and favicon
st.set_page_config(
    layout="wide",
    page_title="📈 Advanced Trading Strategy Backtester",
    page_icon="📈",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .sidebar .sidebar-content {
        background-color: #ffffff;
    }
    .streamlit-expanderHeader {
        background-color: #ffffff;
        border-radius: 5px;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
    """, unsafe_allow_html=True)


def convert_to_timezone_aware(date_obj):
    return datetime.combine(date_obj, datetime.min.time()).replace(tzinfo=pytz.UTC)


class EMACrossover(Strategy):
    n1 = 20  # Fast EMA period
    n2 = 50  # Slow EMA period
    sl_pct = 2.0  # Stop loss percentage
    trail_sl = False  # Trailing stop loss flag
    size = 1.0  # Position size

    def init(self):
        # Convert the Backtesting.py data to a DataFrame
        df = pd.DataFrame(self.data.df)

        # Calculate indicators using pandas_ta
        df['EMA1'] = ta.ema(df['Close'], length=self.n1)
        df['EMA2'] = ta.ema(df['Close'], length=self.n2)
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
                self.buy(size=self.size)
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


def create_candlestick_chart(data, signals=None):
    """Create an interactive candlestick chart with signals"""
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.03, subplot_titles=('Price', 'Volume'),
                        row_width=[0.7, 0.3])

    # Candlestick chart
    fig.add_trace(go.Candlestick(x=data.index,
                                 open=data['Open'],
                                 high=data['High'],
                                 low=data['Low'],
                                 close=data['Close'],
                                 name='OHLC'), row=1, col=1)

    # Volume bars
    colors = ['red' if close < open else 'green'
              for close, open in zip(data['Close'], data['Open'])]

    fig.add_trace(go.Bar(x=data.index, y=data['Volume'],
                         marker_color=colors,
                         name='Volume'), row=2, col=1)

    # Add signals if provided
    if signals is not None:
        buy_signals = signals[signals['Signal'] == 'Buy']
        sell_signals = signals[signals['Signal'] == 'Sell']

        fig.add_trace(go.Scatter(x=buy_signals.index, y=buy_signals['Price'],
                                 mode='markers',
                                 marker=dict(symbol='triangle-up', size=15, color='green'),
                                 name='Buy Signal'), row=1, col=1)

        fig.add_trace(go.Scatter(x=sell_signals.index, y=sell_signals['Price'],
                                 mode='markers',
                                 marker=dict(symbol='triangle-down', size=15, color='red'),
                                 name='Sell Signal'), row=1, col=1)

    # Update layout
    fig.update_layout(
        title_text="Price Action & Volume Analysis",
        xaxis_rangeslider_visible=False,
        height=800,
        template='plotly_white',
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )

    return fig


def display_strategy_metrics(stats):
    """Display strategy metrics in an organized layout"""
    st.markdown("### 📊 Performance Metrics")

    col1, col2, col3 = st.columns(3)

    # Key metrics with colorful indicators
    return_color = 'green' if stats['Return [%]'] > 0 else 'red'
    col1.metric(
        "Total Return",
        f"{stats['Return [%]']:.2f}%",
        f"vs Buy & Hold: {stats['Buy & Hold Return [%]']:.2f}%",
        delta_color=return_color
    )

    col2.metric(
        "Sharpe Ratio",
        f"{stats['Sharpe Ratio']:.2f}",
        f"Risk-adjusted return"
    )

    col3.metric(
        "Max Drawdown",
        f"{stats['Max. Drawdown [%]']:.2f}%",
        f"Duration: {stats['Max. Drawdown Duration']}"
    )

    # Additional metrics in expandable section
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


def main():
    # Header with logo and title
    st.subheader("Trading Strategy backtester")

    # Sidebar organization
    with st.sidebar:
        st.markdown("## 🎯 Strategy Setup")

        # Strategy selection with icons
        strategy_name = st.selectbox(
            "Select Strategy",
            ["📊 EMA Crossover", "🔄 RSI Strategy", "🎯 MACD Strategy"]
        )

        # Asset selection with autocomplete
        symbol = st.text_input("📌 Stock Symbol", "AAPL")

        # Date range with calendar
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "📅 Start Date",
                datetime.now() - timedelta(days=365)
            )
        with col2:
            end_date = st.date_input(
                "📅 End Date",
                datetime.now()
            )

        # Trading parameters in an expander
        with st.expander("💰 Trading Parameters"):
            initial_equity = st.number_input(
                "Initial Capital ($)",
                value=100000,
                min_value=1000,
                help="Starting capital for the backtest"
            )

            position_size = st.slider(
                "Position Size (%)",
                min_value=1,
                max_value=100,
                value=100,
                help="Percentage of capital to invest per trade"
            )

            commission = st.number_input(
                "Commission (%)",
                value=0.12,
                format="%.4f",
                help="Trading commission percentage"
            )

        # Strategy parameters based on selection
        if "EMA Crossover" in strategy_name:
            st.markdown("### 📈 EMA Parameters")

            param_configs = {}
            backtest_mode = st.radio(
                "🔄 Backtest Mode",
                ["Standard", "Optimization"],
                help="Choose between single backtest or parameter optimization"
            )

            if backtest_mode == "Standard":
                col1, col2 = st.columns(2)
                with col1:
                    param_configs['n1'] = st.number_input("Fast EMA", 20, help="Fast EMA period")
                with col2:
                    param_configs['n2'] = st.number_input("Slow EMA", 50, help="Slow EMA period")

                param_configs['sl_pct'] = st.slider(
                    "Stop Loss %",
                    min_value=0.5,
                    max_value=10.0,
                    value=2.0,
                    step=0.1,
                    help="Stop loss percentage"
                )

                param_configs['trail_sl'] = st.checkbox(
                    "Trail Stop Loss",
                    value=False,
                    help="Enable trailing stop loss"
                )
            else:
                # Optimization parameters
                optimize_params = st.multiselect(
                    "Select Parameters to Optimize",
                    ["EMA1 Period", "EMA2 Period", "Stop Loss %", "Trail Stop Loss"],
                    default=[]
                )

                if "EMA1 Period" in optimize_params:
                    param_configs['n1'] = st.slider("Fast EMA Period Range", 5, 200, (10, 50), 5)
                else:
                    param_configs['n1'] = st.number_input("Fast EMA Period", value=20, min_value=1)

                if "EMA2 Period" in optimize_params:
                    param_configs['n2'] = st.slider("Slow EMA Period Range", 10, 300, (30, 100), 10)
                else:
                    param_configs['n2'] = st.number_input("Slow EMA Period", value=50, min_value=1)

                if "Stop Loss %" in optimize_params:
                    param_configs['sl_pct'] = st.slider("Stop Loss % Range", 0.5, 10.0, (1.0, 3.0), 0.5)
                else:
                    param_configs['sl_pct'] = st.number_input("Stop Loss %", value=2.0, min_value=0.1, step=0.1)

                if "Trail Stop Loss" in optimize_params:
                    param_configs['trail_sl'] = [True, False]
                else:
                    param_configs['trail_sl'] = st.checkbox("Trail Stop Loss", value=False)

        # Run backtest button
        run_backtest = st.button("🚀 Run Backtest", use_container_width=True)
    # Run backtest button with loading animation
    if run_backtest:
        with st.spinner('Running backtest... Please wait.'):
            try:
                # Convert dates to timezone aware
                start_date_tz = convert_to_timezone_aware(start_date)
                end_date_tz = convert_to_timezone_aware(end_date)

                # Fetch data
                data = yf.download(symbol, start=start_date_tz, end=end_date_tz, multi_level_index=False)

                if data.empty:
                    st.error(f"No data found for symbol {symbol}. Please check the symbol and date range.")
                else:
                    # Create tabs for results
                    tabs = st.tabs(["Backtest Stats", "Trade Analysis", "Equity Curve", "Drawdown"])

                    # Standard Backtest
                    if backtest_mode == "Standard Backtest":
                        # Initialize strategy with position size
                        strategy = EMACrossover
                        params = {
                            'n1': param_configs['n1'],
                            'n2': param_configs['n2'],
                            'sl_pct': param_configs['sl_pct'],
                            'trail_sl': param_configs['trail_sl'],
                            'size': position_size / 100  # Convert percentage to decimal
                        }

                        # Run backtest
                        bt = Backtest(data, strategy, cash=initial_equity, commission=commission / 100)
                        stats = bt.run(**params)

                        # Tab 1: Backtest Stats
                        with tabs[0]:
                            # Display comprehensive strategy metrics
                            display_strategy_metrics(stats)

                            # Create and display candlestick chart with signals
                            st.subheader("Price Action Analysis")

                            # Extract trade signals from backtest results
                            signals = pd.DataFrame(index=data.index)
                            signals['Price'] = data['Close']
                            signals['Signal'] = None

                            for trade in stats._trades:
                                signals.loc[trade.EntryTime, 'Signal'] = 'Buy'
                                signals.loc[trade.ExitTime, 'Signal'] = 'Sell'

                            # Create and display the candlestick chart
                            fig = create_candlestick_chart(data, signals)
                            st.plotly_chart(fig, use_container_width=True)

                        # Tab 2: Trade Analysis
                        with tabs[1]:
                            if any(stats._trades):
                                trade_df = stats._trades.copy()
                                trade_df['PnL'] = trade_df['PnL'].round(2)
                                trade_df['ReturnPct'] = trade_df['ReturnPct'].round(2)

                                def color_trades(val):
                                    color = 'green' if val > 0 else 'red' if val < 0 else 'black'
                                    return f'color: {color}'

                                styled_trades = trade_df.style.applymap(color_trades, subset=['PnL', 'ReturnPct'])
                                st.dataframe(styled_trades, height=400)
                            else:
                                st.info("No trades were executed during the backtest period.")

                        # Tab 3: Equity Curve
                        with tabs[2]:
                            equity_data = pd.Series(stats['_equity_curve']['Equity'])
                            fig = go.Figure(data=[
                                go.Scatter(x=equity_data.index, y=equity_data,
                                           mode='lines', name='Equity', line=dict(color='green'))
                            ])
                            fig.update_layout(title='Equity Curve', xaxis_title='Date',
                                              yaxis_title='Equity', height=600)
                            st.plotly_chart(fig, use_container_width=True)

                        # Tab 4: Drawdown
                        with tabs[3]:
                            drawdown_data = pd.Series(stats['_equity_curve']['DrawdownPct']) * 100
                            fig = go.Figure(data=[
                                go.Scatter(x=drawdown_data.index, y=drawdown_data,
                                           mode='lines', name='Drawdown', fill='tozeroy',
                                           line=dict(color='red'))
                            ])
                            fig.update_layout(title='Drawdown Curve', xaxis_title='Date',
                                              yaxis_title='Drawdown %', height=600)
                            st.plotly_chart(fig, use_container_width=True)

                    # Parameter Optimization
                    else:
                        # Prepare parameter combinations for optimization
                        strategy = EMACrossover

                        def generate_param_combinations(param_configs):
                            opt_params = {}
                            for param, value in param_configs.items():
                                if not isinstance(value, (list, tuple)):
                                    opt_params[param] = [value]
                                else:
                                    if len(value) == 2:  # Slider input
                                        if param in ['n1', 'n2']:
                                            opt_params[param] = list(range(value[0], value[1] + 1, 5))
                                        elif param == 'sl_pct':
                                            opt_params[param] = list(np.arange(value[0], value[1] + 0.5, 0.5))
                                    else:
                                        opt_params[param] = value

                            keys, values = zip(*opt_params.items())
                            return [dict(zip(keys, v)) for v in itertools.product(*values)]

                        # Generate parameter combinations
                        param_combinations = generate_param_combinations(param_configs)
                        bt = Backtest(data, strategy, cash=initial_equity, commission=commission / 100)

                        # Run optimization with progress bar
                        optimization_results = []
                        progress_bar = st.progress(0, text="Optimization in progress...")

                        for i, params_dict in enumerate(param_combinations):
                            params_dict['size'] = position_size / 100
                            progress_bar.progress((i + 1) / len(param_combinations))

                            try:
                                stats = bt.run(**params_dict)
                                result = params_dict.copy()
                                result.update({
                                    'Return [%]': stats['Return [%]'],
                                    'CAGR [%]': stats['Return (Ann.) [%]'],
                                    'Max Drawdown [%]': stats['Max. Drawdown [%]'],
                                    'Sharpe Ratio': stats['Sharpe Ratio'],
                                    'Total Trades': stats['# Trades']
                                })
                                optimization_results.append(result)
                            except Exception as e:
                                st.warning(f"Skipped combination {params_dict} due to error: {e}")

                        progress_bar.empty()

                        # Display optimization results in tabs
                        with tabs[0]:
                            st.subheader("Optimization Results")
                            results_df = pd.DataFrame(optimization_results)
                            st.dataframe(results_df)

                            # Find and display best parameters
                            best_return = results_df.loc[results_df['Return [%]'].idxmax()]
                            best_sharpe = results_df.loc[results_df['Sharpe Ratio'].idxmax()]

                            st.subheader("Best Parameters")
                            col1, col2 = st.columns(2)

                            with col1:
                                st.markdown("**Best by Return:**")
                                for param, value in dict(best_return[list(param_configs.keys())]).items():
                                    st.write(f"{param}: {value}")
                                st.metric("Return (%)", f"{best_return['Return [%]']:.2f}")

                            with col2:
                                st.markdown("**Best by Sharpe:**")
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
                                hover_data=list(param_configs.keys()) + ['Return [%]', 'Sharpe Ratio',
                                                                         'Max Drawdown [%]'],
                                title='Optimization Results: Return vs Sharpe Ratio'
                            )
                            st.plotly_chart(fig)
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()