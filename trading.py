import pandas as pd
import streamlit as st
from common_utils import read_write_sql_data as rd


def fetch_agg_data(based_on='No Condition'):
    agg_raw_data = rd.get_table_data(selected_table='AGG_DATA')
    agg_data = agg_raw_data.copy()

    if based_on == 'All Time High':
        agg_data = agg_data[agg_data['Date'] == agg_data['ATH_Date']]
    elif based_on == 'All Time Low':
        agg_data = agg_data[agg_data['Date'] == agg_data['ATL_Date']]
    else:
        agg_data = pd.DataFrame()

    return agg_data if based_on != 'No Condition' else agg_raw_data


def trading():
    st.subheader("Trading")

    trade_options = ['All Time High', 'All Time Low', '52 Week High', '52 Week Low',
                     'Trending Up', 'Trending Down', 'Swing Trade']

    trade_option = st.sidebar.selectbox("Choose one option", options=trade_options)

    filtered_data = fetch_agg_data(based_on=trade_option)
    if len(filtered_data) > 1:
        st.markdown(f"#### Stocks that are @ :rainbow[{trade_option}]")
        st.dataframe(filtered_data)
    else:
        st.markdown(f"#### There are no stocks that are @ :rainbow[{trade_option}]")


if __name__ == '__main__':
    trading()
