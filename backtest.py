import streamlit as st
import pandas as pd
import sqlite3  # Replace with `pyodbc` for SQL Server
import backtrader as bt
from datetime import datetime
from common_utils import read_write_sql_data as rd


@st.cache_data
def extract_stock_data(stock_name, start_date, end_date):
    query = f'Select * from dbo.{stock_name} order by Date ASC'
    df = rd.get_table_data(query=query)
    df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)
    return df


class Strategy(bt.Strategy):
    params = (
        ('atr_period', 14),  # ATR period for trailing stop loss
        ('trail_multiplier', 2.0),  # ATR multiplier for trailing stop
    )

    def __init__(self):
        self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)
        self.trail_stop_price = None
        self.buy_price = None

        # Metrics
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_profit = 0
        self.trade_log = []  # Store trade details for summary

    def log(self, message, dt=None):
        """Log messages with an optional timestamp."""
        dt = dt or self.datas[0].datetime.datetime(0)
        st.write(f"{dt}: {message}")  # Writes to Streamlit UI

    def log_trade(self, trade_type, price, pnl):
        """Log details of a trade."""
        self.trade_log.append({"type": trade_type, "price": price, "pnl": pnl})
        self.log(f"{trade_type} at {price} with PnL: {pnl}")

    def next(self):
        if self.position:
            self.manage_trailing_stop()
        else:
            self.check_entry_conditions()

    def manage_trailing_stop(self):
        if self.trail_stop_price is None:
            self.trail_stop_price = self.data.close[0] - self.params.trail_multiplier * self.atr[0]
        else:
            new_trail_stop = self.data.close[0] - self.params.trail_multiplier * self.atr[0]
            self.trail_stop_price = max(self.trail_stop_price, new_trail_stop)

        if self.data.close[0] <= self.trail_stop_price:
            self.sell()
            pnl = self.data.close[0] - self.buy_price
            self.total_trades += 1
            if pnl > 0:
                self.winning_trades += 1
            else:
                self.losing_trades += 1
            self.total_profit += pnl
            self.log_trade("SELL", self.data.close[0], pnl)
            self.trail_stop_price = None

    def check_entry_conditions(self):
        raise NotImplementedError("Child strategy must implement entry conditions.")

    def stop(self):
        """Display final performance metrics."""
        self.log(f"Backtest completed: Total Trades: {self.total_trades}, "
                 f"Wins: {self.winning_trades}, Losses: {self.losing_trades}, "
                 f"Net Profit: {self.total_profit}")



class GoldenCrossoverStrategy(Strategy):
    params = (
        ('fast_period', 50),  # Fast moving average period
        ('slow_period', 200),  # Slow moving average period
    )

    def __init__(self):
        # Initialize parent class
        super().__init__()

        # Indicators for Golden Crossover
        self.fast_ma = bt.indicators.SMA(self.data.close, period=self.params.fast_period)
        self.slow_ma = bt.indicators.SMA(self.data.close, period=self.params.slow_period)

    def check_entry_conditions(self):
        """Entry logic for Golden Crossover."""
        if self.fast_ma[0] > self.slow_ma[0] and self.fast_ma[-1] <= self.slow_ma[-1]:  # Cross up
            self.buy()
            self.buy_price = self.data.close[0]
            self.trail_stop_price = None  # Reset the trailing stop
            self.log(f"Golden crossover: Bought at {self.buy_price}")


def to_dict(input_obj):
    """
    Recursively converts Backtrader's AutoOrderedDict or other nested structures
    into standard Python dicts for easier handling.
    """
    if isinstance(input_obj, bt.utils.AutoOrderedDict):
        return {k: to_dict(v) for k, v in input_obj.items()}
    elif isinstance(input_obj, list):
        return [to_dict(v) for v in input_obj]
    elif isinstance(input_obj, dict):
        return {k: to_dict(v) for k, v in input_obj.items()}
    return input_obj


def display_trades(trade_log):
    """
    Displays trades in a table format with color-coded profit/loss.
    """
    if not trade_log:
        st.warning("No trades executed during the backtest.")
        return

    # Convert trade log to DataFrame
    trades_df = pd.DataFrame(trade_log)

    # Color formatting for PnL
    def highlight_pnl(val):
        color = "green" if val > 0 else "red"
        return f"color: {color}"

    st.write("### Trades")
    styled_df = trades_df.style.applymap(highlight_pnl, subset=["PnL"])
    st.dataframe(styled_df)


# Streamlit Page
st.title("Refactored Backtester with Strategy Base Class")

# Sidebar for general settings
st.sidebar.header("General Settings")
strategy_name = st.sidebar.text_input("Strategy Name", value="Golden Crossover Strategy", max_chars=50)
exchange = st.sidebar.radio("Exchange", options=["NSE", "NFO", "MCX"], index=0)

# Main form for strategy details
with st.form("strategy_form"):
    st.header("Trading Strategy Inputs")

    # Select Stock Symbol
    st.subheader("Stock Selection")
    symbols = ["RELIANCE", "TCS", "INFY", "HDFC"]  # Replace with dynamic fetch if needed
    stock_symbol = st.selectbox("Select Stock Symbol", options=symbols)

    # Golden Crossover Parameters
    st.subheader("Golden Crossover Parameters")
    fast_period = st.number_input("Fast Moving Average Period", min_value=1, value=50, step=1)
    slow_period = st.number_input("Slow Moving Average Period", min_value=1, value=200, step=1)

    # ATR Settings for Trailing Stop Loss
    st.subheader("Trailing Stop Loss")
    atr_period = st.number_input("ATR Period", min_value=1, value=14, step=1)
    trail_multiplier = st.number_input("ATR Multiplier", min_value=0.1, value=2.0, step=0.1)

    # Date Selection
    st.subheader("Strategy Timing")
    start_date = st.date_input("Strategy Start Date", value=datetime.today())
    end_date = st.date_input("Strategy End Date", value=datetime.today())

    # Form submit button
    submit_button = st.form_submit_button("Run Backtest")

if submit_button:
    # Fetch stock data
    stock_data = extract_stock_data(stock_symbol, start_date, end_date)
    if stock_data is not None and not stock_data.empty:
        st.write("### Stock Data")
        st.dataframe(stock_data.head())

        # Backtrader setup
        cerebro = bt.Cerebro()
        cerebro.addstrategy(
            GoldenCrossoverStrategy,
            fast_period=fast_period,
            slow_period=slow_period,
            atr_period=atr_period,
            trail_multiplier=trail_multiplier,
        )

        # Convert stock data to Backtrader format
        data_feed = bt.feeds.PandasData(dataname=stock_data)
        cerebro.adddata(data_feed)

        # Add analyzers
        cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade_analyzer')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

        # Run backtest
        st.write("### Running Backtest...")
        cerebro_results = cerebro.run()
        strategy_instance = cerebro_results[0]

        # Assuming `strategy_instance` is available after backtest
        trade_analyzer = to_dict(strategy_instance.analyzers.trade_analyzer.get_analysis())
        sharpe_ratio = to_dict(strategy_instance.analyzers.sharpe_ratio.get_analysis())
        drawdown = to_dict(strategy_instance.analyzers.drawdown.get_analysis())

        # Trade Metrics Table
        st.write("### Trade Metrics")
        trades = []
        if 'trades' in trade_analyzer:
            for trade in trade_analyzer['trades']:
                pnl = trade['pnl']
                trades.append({
                    "Trade Type": "Buy" if trade['isbuy'] else "Sell",
                    "Price": trade['price'],
                    "PnL": pnl,
                    "Date": trade['datetime']
                })

        # Create DataFrame for Trades
        trades_df = pd.DataFrame(trades)
        if not trades_df.empty:
            def highlight_pnl(val):
                """Highlight PnL: Green for profit, Red for loss."""
                return f"color: {'green' if val > 0 else 'red'}"


            st.write("#### Trades Summary")
            st.dataframe(trades_df.style.applymap(highlight_pnl, subset=["PnL"]))
        else:
            st.warning("No trades executed during backtest.")

        # Summary Metrics Table
        summary_data = {
            "Metric": ["Total Closed Trades", "Winning Trades", "Losing Trades", "Net PnL", "Sharpe Ratio",
                       "Max Drawdown"],
            "Value": [
                trade_analyzer.get("total", {}).get("closed", 0),
                trade_analyzer.get("won", {}).get("total", 0),
                trade_analyzer.get("lost", {}).get("total", 0),
                trade_analyzer.get("pnl", {}).get("net", {}).get("total", "N/A"),
                sharpe_ratio.get("sharperatio", "N/A"),
                drawdown.get("max", "N/A"),
            ]
        }

        summary_df = pd.DataFrame(summary_data)
        st.write("### Backtest Summary")
        st.table(summary_df)

        # Enhanced Visualization Suggestions
        st.write("### Performance Chart")
        st.pyplot(cerebro.plot()[0][0])
    else:
        st.error("No stock data found for the selected symbol and date range.")


