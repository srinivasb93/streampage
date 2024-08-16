import openpyxl
import streamlit as st
import datetime as dt
import pandas as pd
from common_utils import read_write_sql_data as rd, run_python_script as rps
from python_scripts.stocks_data_load import daily_data_load as eq_daily_load, load_agg_data as ag
from python_scripts.mf_data_load import mf_hist_data_load as mf_hist_load
from python_scripts.stocks_data_load.utilities import update_portfolio
from python_scripts.get_market_data.market_data import load_index_and_stocks_data


def display_toaster(status, msg, custom_icon=':material/info_i:', use_default_icon=True):
    colour = 'green' if status == 'Success' else 'red'
    if use_default_icon:
        icon_to_use = ":material/check:" if status == 'Success' else ":material/error:"
    else:
        icon_to_use = custom_icon
    st.toast(f':{colour if status != "Skipped" else "blue"}' + f'[{msg}]', icon=icon_to_use)


# Function to simulate data loading or updating portfolio
def perform_data_load(data_type='Equity', load_freq='Daily'):
    st.spinner(f"Performing {data_type} {load_freq} data load/update...")
    if data_type == 'Equity' and load_freq == 'Daily':
        load_status = eq_daily_load.equity_daily_data_load()
    if data_type == 'Equity' and load_freq == 'Agg_Data':
        load_status = ag.stocks_agg_data_load()
    elif data_type == 'MF' and load_freq == 'Historical':
        load_status = mf_hist_load.extract_and_load_latest_mf_hist_data()
    elif data_type in ['Index_data_load', "Index_Stocks_data_load", "Stocks_Ref_data_load"]:
        load_status = load_index_and_stocks_data(data_type)
    else:
        load_status = 'Skipped'

    load_msg = f'{data_type} {load_freq} data load is {load_status}'
    display_toaster(status=load_status, msg=load_msg)


# Main Streamlit app
def dataload():
    st.subheader('Equity and Mutual Fund Data Loader')

    col1, upload_col = st.columns([2, 1], vertical_alignment="top", gap="medium")

    with col1:
        eq_col, mf_col, index_col = st.columns([1, 1, 1], vertical_alignment='bottom', gap="medium")

        # Select box for Equity data load
        equity_load_type = eq_col.selectbox('Select Equity Data Load Type',
                                                ['Daily', 'Agg_Data', 'Historical'])
        if eq_col.button('Load Equity Data'):
            with st.spinner(f"Equity {equity_load_type} data load in progress.."):
                perform_data_load(data_type='Equity', load_freq=equity_load_type)

        # Select box for Mutual Fund data load
        mf_load_type = mf_col.selectbox('Select Mutual Fund Data Load Type',
                                           ['Daily', 'Agg_Data', 'Historical'])
        if mf_col.button('MF Data Load'):
            with st.spinner(f"MF {mf_load_type} data load in progress.."):
                perform_data_load(data_type='MF', load_freq=mf_load_type)

        with index_col:
            index_load_type = st.selectbox("Choose Index data load type",
                                           options=['Index_data_load', "Index_Stocks_data_load", "Stocks_Ref_data_load"])
            if st.button('Load Index Data'):
                with st.spinner(f"{index_load_type} in progress.."):
                    perform_data_load(data_type=index_load_type)

        st.markdown('---')
        st.markdown('### Update Portfolio in the SQL database..')
        control_data = rd.get_table_data(selected_database="ANALYTICS", selected_table="ANALYTICS_LOAD_CONTROL")
        st.write(f":rainbow[Bhav last updated on ***{control_data['BHAV_UPDATED_ON'].iloc[0]}*** "
                 f"for the date *{control_data['BHAV_DATE'].iloc[0]}*. Status - *{control_data['BHAV_LOAD'].iloc[0]}*]")

        st.write(f":rainbow[MF Snapshot last updated on ***{control_data['MFSNAP_UPDATED_ON'].iloc[0]}*** "
                 f"for the date *{control_data['MF_SNAP_DATE'].iloc[0]}*. Status - *{control_data['MF_SNAP_LOAD'].iloc[0]}*]")

        st.write(f":rainbow[Portfolio last updated on ***{control_data['PF_UPDATED_ON'].iloc[0]}***."
                 f" Status - *{control_data['BHAV_LOAD'].iloc[0]}*]")

        # Button to update portfolio in SQL Server
        if st.button('Update Portfolio'):
            # Replace with actual SQL update logic
            current_date = dt.date.today()
            with st.spinner("Updating portfolio in SQL Server..."):
                date_day = current_date.strftime("%A")
                if date_day == 'Sunday':
                    for_date = current_date - dt.timedelta(days=2)
                elif date_day == 'Saturday':
                    for_date = current_date - dt.timedelta(days=1)
                elif date_day == 'Monday':
                    if dt.datetime.now().hour < 19:
                        for_date = current_date - dt.timedelta(days=3)
                    else:
                        for_date = current_date
                else:
                    if dt.datetime.now().hour < 19:
                        for_date = current_date - dt.timedelta(days=1)
                    else:
                        for_date = current_date
                pf_load_msg = update_portfolio.update_overall_portfolio_summary(fetch_type='load_and_fetch',
                                                                                for_date=for_date,
                                                                                mf_snap_reload=False,
                                                                                bhavcopy_reload=False)
                load_status = 'Success' if 'success' in pf_load_msg else "Failure"
                display_toaster(status=load_status, msg=pf_load_msg)

    with upload_col:
        uploaded_file = st.file_uploader("Upload the file", type=['xlsx'])
        if uploaded_file is not None:
            wb = openpyxl.load_workbook(uploaded_file)
            sheet_selector = st.selectbox("Select sheet", options=wb.sheetnames)
            database_list = ["ANALYTICS", "NSEDATA", "MFDATA", "STRATEGY"]
            database = st.selectbox("Select database", options=database_list)
            table_list = ["EQUITY_HOLDINGS", "MF_HOLDINGS", "RETURNS_RECEIVED", "OTHERS"]
            table_selected = st.selectbox("Select table", options=table_list)
            if table_selected == 'OTHERS':
                table_name = st.text_input("Enter table name")
            else:
                table_name = table_selected

            data = pd.read_excel(uploaded_file, sheet_selector)
            view_col, load_col = st.columns(2, vertical_alignment="center")

            with view_col:
                view_btn = st.button("View Data")
            with load_col:
                sub_btn = st.button("Load Data")

            if sub_btn:
                load_msg = rd.load_sql_data(data_to_load=data, table_name=table_name, database=database)
                if "success" in load_msg:
                    st.success(load_msg)
                else:
                    st.error(load_msg)
            if view_btn:
                st.write(data)
        else:
            st.write('No file chosen yet for upload')


if __name__ == '__main__':
    dataload()
