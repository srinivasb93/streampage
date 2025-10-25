import datetime
import streamlit as st
from common_utils import read_write_sql_data as rd
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from common_utils.auth import require_authentication

st.set_page_config(layout="wide")

# Require authentication for this page
require_authentication()

@st.cache_data
def fetch_portfolio_data(req_portfolio='Equity'):
    """
    Fetches portfolio data from the database.
    This function is cached to avoid re-running on every interaction.
    """
    ref_tables = {
        'Equity': 'EQUITY_HOLDINGS_OVERALL',
        'MF': 'MF_HOLDINGS_OVERALL',
        'Total': 'OVERALL_SUMMARY_ACCOUNT_WISE',
        'Srini': 'OVERALL_SUMMARY_ACCOUNT_WISE',
        'Amma': 'OVERALL_SUMMARY_ACCOUNT_WISE'
    }
    # Assumes 'analytics' database exists in your PostgreSQL setup
    pf_data = rd.get_table_data(selected_database='analytics', selected_table=ref_tables.get(req_portfolio))
    if req_portfolio in ['Srini', 'Amma']:
        # Filter data based on the account name if a specific person is selected
        pf_data = pf_data[pf_data["Account_Name"].str.contains(str(req_portfolio), case=False)]
    return pf_data


def portfolio():
    """
    Main function to render the entire portfolio summary page.
    """
    st.subheader('**Portfolio Summary**')
    selected_pf = st.radio(
        "Select the Portfolio",
        options=['Equity', 'MF', 'Total', 'Amma', 'Srini'],
        horizontal=True,
        label_visibility="collapsed"
    )
    df = fetch_portfolio_data(selected_pf)

    if df.empty:
        st.warning("No portfolio data found for the selected option.")
        return

    # --- Calculations ---
    total_cost = int(df['Buy_Value'].sum())
    total_present_value = int(df['Current_Value'].sum())
    total_prev_value = df['Prev_Value'].sum()
    # ENHANCEMENT: Added check to prevent division by zero
    total_return = round(((total_present_value - total_cost) / total_cost) * 100, 1) if total_cost else 0
    day_change = int(df['Daily_Change'].sum())
    day_change_pct = round(((total_present_value - total_prev_value) / total_prev_value) * 100,
                           1) if total_prev_value else 0
    pnl = int(total_present_value - total_cost)

    # --- Index Data Sidebar ---
    index_data = rd.get_table_data(selected_table='BHAVCOPY_INDICES', selected_database='nsedata')
    if not index_data.empty:
        active_date = pd.to_datetime(index_data['Index Date'], format="%d-%m-%Y").max()
        req_indices = [
            "Nifty 50", "Nifty Next 50", "Nifty Midcap 50", "Nifty Auto", "Nifty Bank", "NIFTY Smallcap 100",
            "Nifty Energy", "Nifty Financial Services", "Nifty FMCG", "Nifty IT", "Nifty Media", "Nifty Metal",
            "Nifty MNC", "Nifty PSU Bank", "Nifty Pharma", "Nifty Realty", "Nifty 500"
        ]
        req_cols = ["Index Name", "Change(%)", "P/E"]
        index_data_df = index_data[index_data['Index Name'].isin(req_indices)][req_cols].copy()

        st.sidebar.subheader(f"Index Summary : {datetime.date.strftime(active_date, format='%d-%m-%Y')}")
        st.sidebar.dataframe(index_data_df.style.background_gradient(cmap="RdYlGn", subset=["Change(%)"]),
                             hide_index=True)

    # --- Main Metrics ---
    col1, col2, col3, col4, col5, col6 = st.columns([1, 1, 1, 1, .75, .75])

    # ENHANCEMENT: Replaced utils.insert_commas with standard f-string formatting
    with col1:
        st.metric("Buy Value", value=f"₹{total_cost:,}")
    with col2:
        st.metric("Current Value", value=f"₹{total_present_value:,}")
    with col4:
        st.metric("Day Change", value=f"₹{day_change:,}", delta=f'{day_change_pct:.2f} %')
    with col3:
        st.metric("Profit/Loss", value=f"₹{pnl:,}", delta=f'{total_return:.2f} %')

    if not index_data.empty:
        with col5:
            nifty50_data = index_data[index_data['Index Name'] == 'Nifty 50']
            if not nifty50_data.empty:
                nifty50_close_val = int(float(nifty50_data['Closing Index Value'].iloc[0]))
                st.metric("Nifty 50", value=nifty50_close_val,
                          delta=f"{nifty50_data['Change(%)'].iloc[0]} %")
        with col6:
            nifty500_data = index_data[index_data['Index Name'] == 'Nifty 500']
            if not nifty500_data.empty:
                nifty500_close_val = int(float(nifty500_data['Closing Index Value'].iloc[0]))
                st.metric("Nifty 500", value=nifty500_close_val,
                          delta=f"{nifty500_data['Change(%)'].iloc[0]} %")

    # --- Charts ---
    df.sort_values(by="Current_Value", ascending=False, inplace=True)
    df_copy = df.head(8).copy()

    buy_curr_col, pnl_col = st.columns([1, .75])
    names_dict = {
        'Equity': 'Stock_Symbol', 'MF': 'Scheme_Name', 'Total': 'Account_Name',
        'Srini': 'Account_Name', 'Amma': 'Account_Name'
    }

    with buy_curr_col:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_copy[names_dict[selected_pf]], y=df_copy['Buy_Value'],
            name='Buy Value', marker_color='rgb(55, 83, 109)'
        ))
        fig.add_trace(go.Bar(
            x=df_copy[names_dict[selected_pf]], y=df_copy['Current_Value'],
            name='Current Value', marker_color='rgb(26, 118, 255)'
        ))
        fig.update_layout(title='Buy Value vs Current Value', xaxis_tickfont_size=14,
                          yaxis=dict(title='Value (in Rupees)'),
                          legend=dict(x=.8, y=.95, bgcolor='rgba(255, 255, 255, 0)',
                                      bordercolor='rgba(255, 255, 255, 0)'),
                          barmode='group', bargap=0.15, bargroupgap=0.1)
        st.plotly_chart(fig, width='stretch')

    with pnl_col:
        chart_title = "Profit/Loss Summary" if selected_pf != "Total" else "Equity vs MF Weightage Summary"
        chart_values = "PnL" if selected_pf != "Total" else "Current_Value"
        chart_names = names_dict[selected_pf] if selected_pf != "Total" else "Account_Type"

        st.plotly_chart(px.pie(
            df_copy, names=chart_names, values=chart_values, title=chart_title,
            color_discrete_sequence=px.colors.sequential.Bluered
        ), width='stretch')

    # --- Data Table ---
    st.dataframe(df.style
                 .format(precision=2, thousands=",")
                 .background_gradient(cmap='RdYlGn', subset=["PnL", "PnL_%", "Daily_Pct_Change"])
                 .highlight_max(subset=["Current_Value", "Daily_Change"], color='#3ee27a')
                 .highlight_min(subset=["PnL", "Daily_Change"], color="#ea3c34"),
                 hide_index=True, width='stretch')


# ADDED: This makes the script executable as a Streamlit page.
# When you run streamlit, it will treat this file as a navigable page.
if __name__ == "__main__":
    portfolio()
