import streamlit as st
import pandas as pd
import yfinance as yf
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import plotly.graph_objects as go
import pandas_ta as ta
import numpy as np
from datetime import datetime
import io
import logging
from common_utils import read_write_sql_data as rd

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Define strategies (unchanged from previous)
class SmaCross(Strategy):
    n1 = 10
    n2 = 30
    stop_loss = 0.05
    take_profit = 0.1

    def init(self):
        self.sma1 = self.I(pd.Series.rolling, pd.Series(self.data.Close), window=self.n1).mean()
        self.sma2 = self.I(pd.Series.rolling, pd.Series(self.data.Close), window=self.n2).mean()

    def next(self):
        if self.position:
            if self.position.pl_pct <= -self.stop_loss:
                self.position.close()
            elif self.position.pl_pct >= self.take_profit:
                self.position.close()
        if crossover(self.sma1, self.sma2):
            self.buy()
        elif crossover(self.sma2, self.sma1):
            self.sell()


class RsiStrategy(Strategy):
    rsi_period = 14
    rsi_upper = 70
    rsi_lower = 30
    stop_loss = 0.05
    take_profit = 0.1

    def init(self):
        self.rsi = self.I(ta.rsi, pd.Series(self.data.Close),
                          window=self.rsi_period)

    def next(self):
        if self.position:
            if self.position.pl_pct <= -self.stop_loss:
                self.position.close()
            elif self.position.pl_pct >= self.take_profit:
                self.position.close()
        if self.rsi[-1] < self.rsi_lower and self.rsi[-2] >= self.rsi_lower:
            self.buy()
        elif self.rsi[-1] > self.rsi_upper and self.rsi[-2] <= self.rsi_upper:
            self.sell()


# Enhanced portfolio backtesting function
def run_portfolio_backtest(tickers, strategy_class, params):
    results = {}
    for ticker in tickers:
        try:
            logger.info(f"Fetching data for {ticker}")
            data = fetch_data_from_db(ticker, start_date=params['start_date'], end_date=params['end_date'])
            data.set_index('Date', drop=True, inplace=True)
            data.index = pd.to_datetime(data.index)

            if data.empty:
                st.warning(f"No data returned for {ticker}")
                logger.warning(f"No data returned for {ticker}")
                continue
            else:
                print(data.head())

            bt = Backtest(data, strategy_class, cash=params['cash'] / len(tickers),
                          commission=params['commission'])
            stats = bt.run(
                **{k: v for k, v in params.items() if k not in ['start_date', 'end_date', 'cash', 'commission']})
            results[ticker] = stats
        except Exception as e:
            st.error(f"Error fetching data for {ticker}: {str(e)}")
            logger.error(f"Error fetching data for {ticker}: {str(e)}")
    return results

def fetch_data_from_db(table_name, start_date, end_date):
    """
    Fetch data from the SQL Server database.
    Replace this with your actual function to fetch data.
    """
    query = f"SELECT * FROM {table_name} WHERE date BETWEEN '{start_date}' AND '{end_date}' ORDER BY DATE ASC"
    data = rd.get_table_data(query=query)  # Replace with your actual function
    return data

# Streamlit UI
def main():
    st.set_page_config(page_title="Advanced Trading Backtester", layout="wide")
    st.title("Advanced Trading Strategy Backtester")

    with st.sidebar:
        st.header("Configuration")
        strategy_choice = st.selectbox("Select Strategy", ["SMA Crossover", "RSI"])
        tickers = st.text_input("Tickers (comma-separated)", "AAPL,MSFT,GOOGL").split(',')
        tickers = [t.strip() for t in tickers]

        start_date = st.date_input("Start Date", pd.to_datetime("2020-01-01"))
        end_date = st.date_input("End Date", pd.to_datetime("2023-12-31"))

        cash = st.number_input("Initial Cash", 1000, 1000000, 10000)
        commission = st.number_input("Commission %", 0.0, 1.0, 0.2) / 100

        if strategy_choice == "SMA Crossover":
            fast_sma = st.slider("Fast SMA", 5, 50, 10)
            slow_sma = st.slider("Slow SMA", 10, 100, 30)
            params = {'n1': fast_sma, 'n2': slow_sma}
            strategy_class = SmaCross
        else:
            rsi_period = st.slider("RSI Period", 5, 30, 14)
            rsi_upper = st.slider("RSI Upper", 50, 90, 70)
            rsi_lower = st.slider("RSI Lower", 10, 50, 30)
            params = {'rsi_period': rsi_period, 'rsi_upper': rsi_upper, 'rsi_lower': rsi_lower}
            strategy_class = RsiStrategy

        stop_loss = st.slider("Stop Loss %", 1, 20, 5) / 100
        take_profit = st.slider("Take Profit %", 5, 30, 10) / 100
        optimize = st.checkbox("Run Optimization")

        params.update({
            'start_date': start_date,
            'end_date': end_date,
            'cash': cash,
            'commission': commission,
            'stop_loss': stop_loss,
            'take_profit': take_profit
        })

    if st.sidebar.button("Run Backtest"):
        with st.spinner("Running backtest..."):
            try:
                results = run_portfolio_backtest(tickers, strategy_class, params)

                if not results:
                    st.error("No valid data retrieved for any ticker. Please check your inputs.")
                    return

                portfolio_equity = pd.DataFrame()
                portfolio_stats = {}

                for ticker, stats in results.items():
                    col1, col2 = st.columns(2)

                    with col1:
                        st.subheader(f"{ticker} Performance")
                        st.write(f"Return: {stats['Return [%]']:.2f}%")
                        st.write(f"Sharpe: {stats['Sharpe Ratio']:.2f}")
                        st.write(f"Max DD: {stats['Max. Drawdown [%]']:.2f}%")
                        st.write(f"Win Rate: {stats['Win Rate [%]']:.2f}%")
                        st.write(f"Trades: {stats['# Trades']}")
                        st.write(f"Sortino: {stats['Sortino Ratio']:.2f}")
                        st.write(f"Calmar: {stats['Calmar Ratio']:.2f}")

                    with col2:
                        st.subheader(f"{ticker} Equity Curve")
                        equity = stats['_equity_curve']['Equity']
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=equity.index, y=equity, mode='lines'))
                        fig.update_layout(height=400, xaxis_title="Date", yaxis_title="Equity ($)")
                        st.plotly_chart(fig, use_container_width=True)

                    portfolio_equity[ticker] = stats['_equity_curve']['Equity']
                    portfolio_stats[ticker] = stats

                st.subheader("Portfolio Performance")
                portfolio_total = portfolio_equity.sum(axis=1)
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=portfolio_total.index, y=portfolio_total, mode='lines'))
                fig.update_layout(height=400, title="Portfolio Equity Curve")
                st.plotly_chart(fig, use_container_width=True)

                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer) as writer:
                    for ticker, stats in portfolio_stats.items():
                        stats['_trades'].to_excel(writer, sheet_name=f"{ticker}_Trades")
                        stats['_equity_curve'].to_excel(writer, sheet_name=f"{ticker}_Equity")
                st.download_button(
                    label="Download Results",
                    data=buffer.getvalue(),
                    file_name=f"backtest_results_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.ms-excel"
                )

                if optimize:
                    st.subheader("Optimization Results")
                    if strategy_choice == "SMA Crossover":
                        opt_params = {'n1': range(5, 50, 5), 'n2': range(10, 100, 10)}
                    else:
                        opt_params = {
                            'rsi_period': range(5, 30, 5),
                            'rsi_upper': range(50, 90, 10),
                            'rsi_lower': range(10, 50, 10)
                        }

                    for ticker in tickers:
                        try:
                            data = fetch_data_from_db(ticker, start_date, end_date)
                            bt = Backtest(data, strategy_class, cash=cash / len(tickers), commission=commission)
                            opt_stats = bt.optimize(**opt_params, maximize='Sharpe Ratio')
                            st.write(f"Optimal parameters for {ticker}: {opt_stats._strategy}")
                            st.write(f"Optimal Sharpe Ratio: {opt_stats['Sharpe Ratio']:.2f}")
                        except Exception as e:
                            st.error(f"Optimization failed for {ticker}: {str(e)}")

            except Exception as e:
                st.error(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()