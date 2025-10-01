import logging
import streamlit as st

from common_utils.logging_utils import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

# Set the page configuration. This should be the first Streamlit command.
st.set_page_config(page_title="Analytics Dashboard",
                   page_icon=":money_bag:",
                   layout="wide",
                   initial_sidebar_state="expanded")


@st.cache_data
def load_css(file_name: str) -> str:
    """A simple function to load and cache CSS content."""
    with open(file_name) as f:
        return f.read()


# Apply CSS styles
css = load_css('styles.css')
st.markdown(f'<style>{css}</style>', unsafe_allow_html=True)
st.markdown('<style>div.block-container{padding-top:2rem;padding-right:0rem;}</style>', unsafe_allow_html=True)

# --- Main Home Page Content ---

logger.info("Rendering Financial Analytics Dashboard home page")

st.title("Financial Analytics Dashboard")

st.markdown("""
This application is a comprehensive suite of tools for financial market analysis, portfolio tracking, and strategy backtesting.

**Navigate through the different modules using the sidebar on the left.**
""")

st.info("Select a page from the navigation bar to get started.", icon="\U0001F448")

st.subheader("Key Features")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    - **Portfolio Summary**: Get an overall view of your Equity and MF holdings.
    - **Stock Analysis**: Perform in-depth technical analysis on individual stocks.
    """)

with col2:
    st.markdown("""
    - **Stock Screener**: Filter stocks based on technical criteria to find opportunities.
    - **Market Snapshot**: Get a pulse of the market with summaries of indices and sectors.
    """)

with col3:
    st.markdown("""
    - **Strategy Tester**: Backtest systematic investment strategies on historical data.
    - **Data Load/View**: Manage and view the underlying financial data.
    """)

st.sidebar.success("Select a page above to begin.")

