import streamlit as st
from dataload import dataload
from trading import trading
# from screener import screener
from market_snapshot import market_snapshot
from strategy_tester import strategy_tester


# Title of the application
st.set_page_config(page_title="Analytics dashboard",
                    page_icon=":money_bag:",
                    layout="wide",
                    initial_sidebar_state="expanded")

with open('styles.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

st.markdown('<style>div.block-container{padding-top:2rem;padding-right:0rem;}</style>', unsafe_allow_html=True)

portfolio = st.Page('portfolio.py', title='Portfolio Summary', icon=':material/dashboard:')
analysis = st.Page("stock_analysis.py", title='Stock Analysis', icon=':material/dashboard:')
dataload = st.Page(dataload, title='Data Load/View', icon=':material/dashboard:')
snapshot = st.Page(market_snapshot, title="Market Snapshot", icon=':material/dashboard:')
# screener = st.Page('screener.py', title="Screener", icon=':material/dashboard:')
trading = st.Page(trading, title="Stock Screener", icon=':material/monitoring:')
strategy = st.Page(strategy_tester, title="Strategy Tester", icon=':material/monitoring:')

# pg = st.navigation([portfolio, analysis, dataload, snapshot, screener, trading])
pg = st.navigation([portfolio, analysis, dataload, snapshot, trading, strategy])

pg.run()
