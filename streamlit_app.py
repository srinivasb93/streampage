import streamlit as st
from dataload import dataload
from trading import trading
from market_snapshot import market_snapshot


# Title of the application
st.set_page_config(page_title="Analytics dashboard",
                    page_icon=":money_bag:",
                    layout="wide",
                    initial_sidebar_state="expanded")

with open('styles.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

st.markdown('<style>div.block-container{padding-top:2rem;}</style>', unsafe_allow_html=True)

portfolio = st.Page('portfolio.py', title='Portfolio Summary', icon=':material/dashboard:')
analysis = st.Page("stock_analysis.py", title='Stock Analysis', icon=':material/dashboard:')
dataload = st.Page(dataload, title='Data Load', icon=':material/dashboard:')
snapshot = st.Page(market_snapshot, title="Market Snapshot", icon=':material/dashboard:')
trading = st.Page(trading, title="Trading", icon=':material/monitoring:')

pg = st.navigation([portfolio, analysis, dataload, snapshot, trading])

pg.run()
