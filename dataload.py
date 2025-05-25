import openpyxl
import streamlit as st
import datetime as dt
import pandas as pd
import threading
import schedule
import time
from common_utils import read_write_sql_data as rd, run_python_script as rps
from python_scripts.stocks_data_load import daily_data_load as eq_daily_load, load_agg_data as ag
from python_scripts.mf_data_load import mf_hist_data_load as mf_hist_load
from python_scripts.stocks_data_load.utilities import update_portfolio
from python_scripts.get_market_data.market_data import load_index_and_stocks_data
from python_scripts.analysis.EOD_analysis import EODAnalysis
# from python_scripts.analysis.EOD_analysis_grok import StockAnalyzer, ReportGenerator


def display_toaster(status, msg, custom_icon=':material/info_i:', use_default_icon=True):
    colour = 'green' if status == 'Success' else 'red'
    if use_default_icon:
        icon_to_use = ":material/check:" if status == 'Success' else ":material/error:"
    else:
        icon_to_use = custom_icon
    st.toast(f':{colour if status != "Skipped" else "blue"}' + f'[{msg}]', icon=icon_to_use)
    if status == 'Success':
        st.success(msg, icon=icon_to_use)
    else:
        st.error(msg, icon=icon_to_use)


# Function to simulate data loading or updating portfolio
def perform_data_load(data_type='Equity', load_freq='Daily', **kwargs):
    st.spinner(f"Performing {data_type} {load_freq} data load/update...")
    if data_type == 'Equity' and load_freq == 'Daily':
        load_status = eq_daily_load.equity_daily_data_load()
    elif data_type == 'Equity' and load_freq == 'Agg_Data':
        load_status = ag.stocks_agg_data_load()
    elif data_type == 'Equity' and load_freq == 'EOD_Analysis':
        stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
        stock_list = stock_list_df['SYMBOL'].values.tolist()
        indices_df = rd.get_table_data(selected_table="STOCK_INDICES")
        indices_list = indices_df['name'].values.tolist()
        sectors_df = rd.get_table_data(selected_table="STOCK_SECTORS")
        sectors_list = sectors_df['name'].values.tolist()

        stocks_indices_sectors = stock_list + indices_list + sectors_list
        eod_analysis = EODAnalysis(stocks_list=stocks_indices_sectors,
                                   adhoc_date=kwargs.get('date', dt.date.today()),
                                   analysis_days=kwargs.get('analysis_days', 365))

        load_status = eod_analysis.run_analysis()
        # eod_analysis = StockAnalyzer(stocks_list=stocks_indices_sectors,
        #                              adhoc_date=kwargs.get('date', dt.date.today()),
        #                              analysis_days=kwargs.get('analysis_days', 365))
        #
        # load_status = eod_analysis.analyze()
        print(load_status)
        # analysis_report = ReportGenerator()
        eod_analysis.print_summary_data_analysis()
    elif data_type == 'MF' and load_freq == 'Historical':
        load_status = mf_hist_load.extract_and_load_latest_mf_hist_data()
    elif data_type in ['Index_data_load', "Index_Stocks_data_load", "Stocks_Ref_data_load"]:
        load_status = load_index_and_stocks_data(data_type)
    else:
        load_status = 'Skipped'

    load_msg = f'{data_type} {load_freq} data load is {load_status}'
    display_toaster(status=load_status, msg=load_msg)


@st.cache_data
def fetch_stocks_data(data_type='Daily', equity_type='Stocks', bhav_copy=False,
                      fetch_count=False, fetch_date=dt.date.today()):
    stocks_data = pd.DataFrame()

    if equity_type == 'Events':
        stocks_data = rd.get_table_data(selected_table='SPLIT_BONUS_DATA')
    else:
        if bhav_copy:
            if equity_type == 'Stocks':
                stocks_data = rd.get_table_data(selected_table='BHAVCOPY')
            elif equity_type == 'Index':
                stocks_data = rd.get_table_data(selected_table='BHAVCOPY_INDICES')
        else:
            stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
            stocks_list = stock_list_df['SYMBOL'].values.tolist()
            if data_type != 'Daily':
                stock_suffix = {'Weekly': '_W', 'Monthly': '_M', 'Yearly': '_Y'}
                stocks_list = [stk_name + stock_suffix.get(data_type, '') for stk_name in stocks_list]

            stocks_data = pd.DataFrame()
            for stock_name in stocks_list:
                stock_data = rd.get_table_data(selected_table=stock_name, sort_order='DESC', sort=True,
                                               sample=True, sample_count=1)
                stock_data.insert(0, 'Symbol', stock_name)
                if fetch_count:
                    query = f"select count(*) from NSEDATA.dbo.{stock_name} where Date >= '{fetch_date}'"
                    query_data = rd.get_table_data(query=query)
                    row_count = query_data.values.tolist()[0][0]
                    stock_data.insert(1, 'Row_Count', row_count)
                stocks_data = pd.concat([stocks_data, stock_data], axis=0, ignore_index=True)

    return stocks_data


# Scheduler function
def run_scheduled_tasks():
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


def schedule_data_loads():
    # Schedule daily equity data load at 19:00
    schedule.every().day.at("19:00").do(lambda: perform_data_load(
        data_type='Equity',
        load_freq='Daily'
    ))

    # Schedule EOD Analysis at 19:15
    schedule.every().day.at("19:00").do(lambda: perform_data_load(
        data_type='Equity',
        load_freq='Daily'
    ))

    # Schedule portfolio update at 1:30 AM
    schedule.every().day.at("01:30").do(lambda: update_portfolio.update_overall_portfolio_summary(
        fetch_type='load_and_fetch',
        for_date=dt.date.today(),
        mf_snap_reload=False,
        bhavcopy_reload=False
    ))

    # Add more schedules as needed
    # schedule.every().day.at("02:00").do(...)

# Main Streamlit app
def dataload():
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=run_scheduled_tasks, daemon=True)
    scheduler_thread.start()

    load_or_view = st.sidebar.radio("Choose option", options=['Load', 'View', 'Schedule'], horizontal=True)

    if load_or_view == 'Schedule':
        st.subheader('Schedule Data Loads')

        st.write("Current Scheduled Tasks:")
        st.write("- Equity Daily Data Load: 1:00 AM")
        st.write("- Portfolio Update: 1:30 AM")

        if st.sidebar.button("Schedule Data Loads"):
            schedule_data_loads()

        if st.button("Run Scheduled Tasks Now"):
            with st.spinner("Running scheduled tasks..."):
                perform_data_load(data_type='Equity', load_freq='Daily')
                update_portfolio.update_overall_portfolio_summary(
                    fetch_type='load_and_fetch',
                    for_date=dt.date.today(),
                    mf_snap_reload=False,
                    bhavcopy_reload=False
                )
                st.success("Scheduled tasks completed!")

    elif load_or_view == 'View':
        stock_or_events = st.sidebar.selectbox('Choose data to view',
                                                    ['Stocks', 'Indices', 'Events'])

        if stock_or_events == 'Stocks':
            data_type = st.sidebar.radio("Choose data type",
                                         options=['Daily', 'Weekly', 'Monthly', 'Yearly'],
                                         horizontal=True)
            stocks_data_radio = st.sidebar.radio("Stocks/Bhavcopy Data", options=["Stocks", 'Bhavcopy'], horizontal=True)
            if stocks_data_radio:
                st.subheader("View data in SQL database")
                st.write(f'Latest :rainbow[{data_type} snapshot] of {stocks_data_radio} data stored in SQL database')

                if stocks_data_radio == 'Stocks':
                    fetch_date = st.sidebar.date_input("Choose date", value="today", max_value=dt.date.today())
                    st.dataframe(fetch_stocks_data(data_type, fetch_count=True, fetch_date=fetch_date), hide_index=True)
                elif stocks_data_radio == 'Bhavcopy':
                    stocks_index_radio = st.sidebar.radio("Stocks/Index Bhav", options=['Stocks', "Index"],
                                                          horizontal=True)
                    st.dataframe(fetch_stocks_data(equity_type=stocks_index_radio, bhav_copy=True).dropna(),
                                 hide_index=True)
        elif stock_or_events == 'Events':
            st.dataframe(fetch_stocks_data(equity_type=stock_or_events, fetch_count=True), hide_index=True)

    elif load_or_view == 'Load':
        st.subheader('Equity and Mutual Fund Data Loader')

        col1, upload_col = st.columns([2, 1], vertical_alignment="top", gap="medium")

        with col1:
            eq_col, mf_col, index_col = st.columns([1, 1, 1], vertical_alignment='top', gap="medium")

            # Select box for Equity data load
            equity_load_type = eq_col.selectbox('Select Equity Data Load Type',
                                                    ['Daily', 'Agg_Data', 'EOD_Analysis', 'Historical'])

            if equity_load_type == 'EOD_Analysis':
                date = eq_col.date_input("Select date for analysis", max_value=dt.date.today(), format='YYYY-MM-DD')
                analysis_days = eq_col.number_input("Lookback period for Analysis", min_value=5, max_value=5000, value=365)

            if eq_col.button('Load Equity Data'):
                with st.spinner(f"Equity {equity_load_type} data load in progress.."):
                    perform_data_load(data_type='Equity', load_freq=equity_load_type,
                                      analysis_date=date if equity_load_type == 'EOD_Analysis' else None,
                                      analysis_period=analysis_days if equity_load_type == 'EOD_Analysis' else None)

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
                # if view_btn:
                #     st.write(data)
            else:
                st.write('No file chosen yet for upload')
        try:
            if view_btn:
                st.write(data)
        except Exception:
            pass


if __name__ == '__main__':
    dataload()
