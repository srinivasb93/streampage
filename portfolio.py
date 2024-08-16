import datetime
import streamlit as st
from common_utils import read_write_sql_data as rd, utils as utils
import pandas as pd
import plotly.figure_factory as ff
import plotly.express as px
import plotly.graph_objs as go


@st.cache_data
def fetch_portfolio_data(req_portfolio='Equity'):
    """
    :param req_portfolio:
    :return:
    """
    ref_tables = {'Equity': 'EQUITY_HOLDINGS_OVERALL',
                  'MF': 'MF_HOLDINGS_OVERALL',
                  'Total': 'OVERALL_SUMMARY_ACCOUNT_WISE',
                  'Srini': 'OVERALL_SUMMARY_ACCOUNT_WISE',
                  'Amma': 'OVERALL_SUMMARY_ACCOUNT_WISE'}
    pf_data = rd.get_table_data(selected_database='ANALYTICS', selected_table=ref_tables.get(req_portfolio))
    if req_portfolio in ['Srini', 'Amma']:
        pf_data = pf_data[pf_data["Account_Name"].str.contains(str(req_portfolio), case=False)]
    return pf_data


st.subheader('**Portfolio Summary**')
selected_pf = st.radio("Select the Portfolio", options=['Equity', 'MF', 'Total', 'Amma', 'Srini'],
                       horizontal=True,
                       label_visibility="collapsed")
df = fetch_portfolio_data(selected_pf)

total_cost = int(df['Buy_Value'].sum())
total_present_value = int(df['Current_Value'].sum())
total_prev_value = df['Prev_Value'].sum()
total_return = round(((total_present_value - total_cost)/total_cost)*100, 1)
day_change = int(df['Daily_Change'].sum())
day_change_pct = round(((total_present_value - total_prev_value)/total_prev_value)*100, 1)
pnl = int(total_present_value - total_cost)


index_data = rd.get_table_data(selected_table='BHAVCOPY_INDICES')
active_date = pd.to_datetime(index_data['Index Date'], format="%d-%m-%Y").max()
req_indices = ["Nifty 50", "Nifty Next 50", "Nifty Midcap 50", "Nifty Auto", "Nifty Bank", "NIFTY Smallcap 100",
               "Nifty Energy", "Nifty Financial Services", "Nifty FMCG", "Nifty IT", "Nifty Media", "Nifty Metal",
               "Nifty MNC", "Nifty PSU Bank", "Nifty Pharma", "Nifty Realty", "Nifty 500"]
req_cols = ["Index Name", "Change(%)", "P/E"]
index_data_df = index_data[index_data['Index Name'].isin(req_indices)][req_cols].copy()
# index_data.rename(columns={"Index Name": "Index", "Change(%)": "Change_Pct", "P/E": "PE"}, inplace=True)


st.sidebar.subheader(f"Index Summary : {datetime.date.strftime(active_date, format='%d-%m-%Y')}")
st.sidebar.dataframe(index_data_df.style.background_gradient(cmap="RdYlGn", subset=["Change(%)"]), hide_index=True)

col1, col2, col3, col4, col5, col6 = st.columns([1, 1, 1, 1, .75, .75])

with col1:
    # st.metric("Buy Value", value=f'{int(total_cost):,}')
    st.metric("Buy Value", value=utils.insert_commas(total_cost))

with col2:
    # st.metric("Current Value", value=f'{int(total_present_value):,}')
    st.metric("Current Value", value=utils.insert_commas(total_present_value))

with col4:
    st.metric("Day Change", value=f'{int(day_change):,}', delta=f'{day_change_pct} %')

with col3:
    # st.metric("Profit/Loss", value=f'{int(pnl):,}', delta=f'{total_return} %')
    st.metric("Profit/Loss", value=utils.insert_commas(pnl), delta=f'{total_return} %')

with col5:
    nifty50_close_val = int(float(index_data[index_data['Index Name'] == 'Nifty 50']['Closing Index Value'].values[0]))
    st.metric("Nifty 50", value=nifty50_close_val,
              delta=f"{index_data[index_data['Index Name'] == 'Nifty 50']['Change(%)'].values[0]} %")

with col6:
    nifty500_close_val = int(float(index_data[index_data['Index Name'] == 'Nifty 500']['Closing Index Value'].values[0]))
    st.metric("Nifty 500", value=nifty500_close_val,
              delta=f"{index_data[index_data['Index Name'] == 'Nifty 500']['Change(%)'].values[0]} %")

df.sort_values(by="Current_Value", ascending=False, inplace=True)
df_copy = df.head(8).copy()

buy_curr_col, pnl_col = st.columns([1, .75])
names_dict = {'Equity': 'Stock_Symbol',
              'MF': 'Scheme_Name',
              'Total': 'Account_Name',
              'Srini': 'Account_Name',
              'Amma': 'Account_Name'}

with buy_curr_col:
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df_copy[names_dict[selected_pf]].values,
                         y=df_copy['Buy_Value'].values,
                         name='Buy Value',
                         marker_color='rgb(55, 83, 109)'
                         ))
    fig.add_trace(go.Bar(x=df_copy[names_dict[selected_pf]].values,
                         y=df_copy['Current_Value'].values,
                         name='Current Value',
                         marker_color='rgb(26, 118, 255)'
                         ))

    fig.update_layout(
        title='Buy Value vs Current Value',
        xaxis_tickfont_size=14,
        yaxis=dict(
            title='Value (in Rupees)',
            titlefont_size=16,
            tickfont_size=14,
        ),
        legend=dict(
            x=.8,
            y=.95,
            bgcolor='rgba(255, 255, 255, 0)',
            bordercolor='rgba(255, 255, 255, 0)'
        ),
        barmode='group',
        bargap=0.15,  # gap between bars of adjacent location coordinates.
        bargroupgap=0.1  # gap between bars of the same location coordinate.
    )
    st.plotly_chart(fig)

with pnl_col:
    if selected_pf != "Total":
        st.plotly_chart(px.pie(df_copy,
                               names=names_dict[selected_pf],
                               values="PnL",
                               title="Profit/Loss Summary",
                               color_discrete_sequence=px.colors.sequential.Bluered))
    else:
        st.plotly_chart(px.pie(df_copy,
                               names='Account_Type',
                               values="Current_Value",
                               title="Equity vs MF Weightage Summary",
                               color_discrete_sequence=px.colors.sequential.Bluered))

# with day_chg_col:
#     st.plotly_chart(px.bar(df_copy,
#                            x=names_dict[selected_pf],
#                            y="Daily_Change",
#                            title="Daily Change in Price",
#                            color='Daily_Pct_Change',
#                            color_discrete_sequence=px.colors.sequential.Bluered))

st.dataframe(df.style
             .highlight_max(subset=["Buy_Value", "Daily_Change", "Current_Value",
                                    "PnL", "PnL_%", "Daily_Pct_Change"],
                            color='#3ee27a')
             .highlight_min(subset=["Buy_Value", "Daily_Change", "Current_Value",
                                    "PnL", "PnL_%", "Daily_Pct_Change"],
                            color="#ea3c34")
             .format(precision=2, thousands=",")
             .background_gradient(cmap='RdYlGn'),
             hide_index=True,
             use_container_width=False,
             )

# styled_df = ff.create_table(df, colorscale='delta')
# st.plotly_chart(styled_df)

