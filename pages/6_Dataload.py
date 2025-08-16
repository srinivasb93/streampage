import openpyxl
import streamlit as st
import datetime as dt
import pandas as pd
import threading
import schedule
import time
from sqlalchemy import text
from common_utils import upstox_utils
from common_utils.utils import fetch_indicies_sectors_list
from common_utils import read_write_sql_data as rd, run_python_script as rps
from python_scripts.stocks_data_load import daily_data_load as eq_daily_load, load_agg_data as ag
from python_scripts.mf_data_load import mf_hist_data_load as mf_hist_load
from python_scripts.stocks_data_load.utilities import update_portfolio
from python_scripts.get_market_data.market_data import load_index_and_stocks_data
from python_scripts.analysis.EOD_analysis import EODAnalysis
# from python_scripts.analysis.EOD_analysis_grok import StockAnalyzer, ReportGenerator

st.set_page_config(layout="wide")

def display_toaster(status, msg, custom_icon=':material/info_i:', use_default_icon=True):
    colour = 'green' if status == 'Success' else 'red'
    if use_default_icon:
        icon_to_use = ":material/check:" if status == 'Success' else ":material/error:"
    else:
        icon_to_use = custom_icon
    st.toast(f':{colour if status != "Skipped" else "blue"}' + f'[{msg}]', icon=icon_to_use)
    if status == 'Success':
        st.success(msg, icon=icon_to_use)
    elif 'fail' in msg.lower():
        st.error(msg, icon=icon_to_use)
    else:
        st.info(msg)


# Function to simulate data loading or updating portfolio
def perform_data_load(data_type='Equity', load_freq='Daily', **kwargs):
    st.spinner(f"Performing {data_type} {load_freq} data load/update...")
    if data_type == 'Equity' and load_freq == 'Agg_Data':
        load_status = ag.stocks_agg_data_load()
    elif data_type == 'Equity' and load_freq == 'EOD_Analysis':
        stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
        stock_list = stock_list_df['SYMBOL'].values.tolist()
        # indices_df = rd.get_table_data(selected_table="STOCK_INDICES")
        # indices_list = indices_df['name'].values.tolist()
        # sectors_df = rd.get_table_data(selected_table="STOCK_SECTORS")
        # sectors_list = sectors_df['name'].values.tolist()

        # stocks_indices_sectors = stock_list + indices_list + sectors_list
        stocks_indices_sectors = stock_list
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
    elif data_type in ['Index_data_load', "Index_Stocks_data_load", "Stocks_Ref_data_load", "FnO_snapshot_load",
                       "NSE_Events_load", "ETF_data_load"]:
        load_status = load_index_and_stocks_data(data_type)
    else:
        load_status = 'Skipped'

    load_msg = f'{data_type} {load_freq} data load is {load_status}'
    display_toaster(status=load_status, msg=load_msg)


@st.cache_data
def fetch_stocks_data(data_type='Daily', equity_type='Stocks', bhav_copy=False,
                      fetch_count=False, fetch_date=dt.date.today()):
    if equity_type == 'Events':
        return rd.get_table_data(selected_table='SPLIT_BONUS_DATA')

    if bhav_copy:
        table = 'BHAVCOPY' if equity_type == 'Stocks' else 'BHAVCOPY_INDICES'
        return rd.get_table_data(selected_table=table, selected_database='nsedata')

    stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB', selected_database='nsedata')
    stocks_list = stock_list_df['SYMBOL'].values.tolist()
    if not stocks_list:
        return pd.DataFrame()

    stock_suffix = {'Weekly': '_W', 'Monthly': '_M', 'Yearly': '_Y'}.get(data_type, '')

    # This query uses a window function to get the latest row for each stock table efficiently.
    union_queries = []
    for stock_name in stocks_list:

        table_name_with_suffix = f'"{stock_name}{stock_suffix}"'
        count_clause = f"""(SELECT count(*) FROM public.{table_name_with_suffix} WHERE timestamp >= '{fetch_date}') as "Row_Count",""" if fetch_count else ""

        union_queries.append(
            f"""
                (SELECT 
                    '{stock_name}' as "Symbol",
                    {count_clause}
                    * FROM public.{table_name_with_suffix} 
                ORDER BY timestamp DESC 
                LIMIT 1)
                """
        )

    full_query = " UNION ALL ".join(union_queries)
    return rd.get_table_data(query=full_query, selected_database='nsedata')


def load_historical_stock_data_in_chunks(stock_symbol, instrument_key):
    """
    NEW: Fetches historical data for a stock in two 9-year chunks and loads it to the DB.
    """
    try:
        with st.spinner(f"Initiating historical data load for {stock_symbol}..."):
            start_date_chunk1 = dt.date(2007, 1, 1)
            end_date_chunk1 = start_date_chunk1 + dt.timedelta(days=9 * 365)  # Approx. 9 years

            start_date_chunk2 = end_date_chunk1 + dt.timedelta(days=1)
            end_date_chunk2 = dt.date.today()

            st.write(f"Fetching Chunk 1: {start_date_chunk1} to {end_date_chunk1}")

            # Fetch and load the first chunk
            data_chunk1 = upstox_utils.get_historical_data(
                instrument_token=instrument_key,
                interval="days",
                unit="1",
                start_date=start_date_chunk1,
                end_date=end_date_chunk1
            )

            if not data_chunk1.empty:
                data_chunk1['timestamp'] = data_chunk1['timestamp'].dt.tz_localize(None)
                load_msg1 = rd.load_sql_data(
                    data_to_load=data_chunk1,
                    table_name=stock_symbol,
                    load_type='replace',  # Replace table with the first chunk
                    database='nsedata',
                    schema='public'
                )
                display_toaster('Success' if 'success' in load_msg1 else 'Failure', f"Chunk 1: {load_msg1}")

                # After successfully loading the first chunk, register the stock.
                rd.add_stock_to_registry(stock_symbol, instrument_key, database='nsedata')
            else:
                display_toaster('Failure', f"Chunk 1: No data received for {stock_symbol}.")
                return  # Stop if the first chunk fails

            st.write(f"Fetching Chunk 2: {start_date_chunk2} to {end_date_chunk2}")

            # Fetch and load the second chunk
            data_chunk2 = upstox_utils.get_historical_data(
                instrument_token=instrument_key,
                interval='days',
                unit="1",
                start_date=start_date_chunk2,
                end_date=end_date_chunk2
            )

            if not data_chunk2.empty:
                data_chunk2['timestamp'] = data_chunk2['timestamp'].dt.tz_localize(None)
                load_msg2 = rd.load_sql_data(
                    data_to_load=data_chunk2,
                    table_name=stock_symbol,
                    load_type='append',  # Append the second chunk
                    database='nsedata',
                    schema='public'
                )
                display_toaster('Success' if 'success' in load_msg2 else 'Failure', f"Chunk 2: {load_msg2}")

            else:
                display_toaster('Skipped', f"Chunk 2: No data received for {stock_symbol}.")

        return 'Success'
    except Exception as e:
        st.error(f"An error occurred during historical data load for {stock_symbol}: {e}")
        return 'Failure'


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

        load_tabs = st.tabs(["Ad-hoc & Portfolio Loaders", "Stock History", "Index/Sector History", "File Upload"])

        with load_tabs[0]:
            st.markdown("##### Ad-hoc Data Loaders")

            eq_col, mf_col, index_col = st.columns([1, 1, 1], vertical_alignment='top', gap="medium")

            # Select box for Equity data load
            equity_load_type = eq_col.selectbox('Select Equity Data Load Type',
                                                    ['Agg_Data', 'EOD_Analysis'])

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
                                               options=['Index_data_load',
                                                        "Index_Stocks_data_load",
                                                        "Stocks_Ref_data_load",
                                                        "FnO_snapshot_load",
                                                        "NSE_Events_load",
                                                        "ETF_data_load"],)
                if st.button('Load Index Data'):
                    with st.spinner(f"{index_load_type} in progress.."):
                        perform_data_load(data_type=index_load_type)

            st.markdown('---')
            st.markdown('### Update Portfolio in the SQL database..')
            # control_data = rd.get_table_data(selected_database="analytics", selected_table="analytics_LOAD_CONTROL")
            # st.write(f":rainbow[Bhav last updated on ***{control_data['BHAV_UPDATED_ON'].iloc[0]}*** "
            #          f"for the date *{control_data['BHAV_DATE'].iloc[0]}*. Status - *{control_data['BHAV_LOAD'].iloc[0]}*]")
            #
            # st.write(f":rainbow[MF Snapshot last updated on ***{control_data['MFSNAP_UPDATED_ON'].iloc[0]}*** "
            #          f"for the date *{control_data['MF_SNAP_DATE'].iloc[0]}*. Status - *{control_data['MF_SNAP_LOAD'].iloc[0]}*]")
            #
            # st.write(f":rainbow[Portfolio last updated on ***{control_data['PF_UPDATED_ON'].iloc[0]}***."
            #          f" Status - *{control_data['BHAV_LOAD'].iloc[0]}*]")

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

        with load_tabs[1]:
            st.markdown("##### Historical Stock Data Loader")
            data_src_cols = st.columns(5)
            data_source = data_src_cols[0].selectbox("Select Data Source",
                                                       options=['NSE', 'Upstox'],
                                                       index=0,
                                                       help="Choose the source for historical data.")
            # --- NEW: Batch daily update for stocks ---
            st.markdown("---")

            load_cols = st.columns([.3, .4, .4], gap="medium")

            with load_cols[0]:
                st.subheader("Batch Daily Update")
                if st.button("Update All Stocks Daily", use_container_width=True):
                    stocks_to_update = rd.get_table_data("nsedata", "STOCKS_IN_DB")
                    if not stocks_to_update.empty:
                        success_count = 0
                        fail_count = 0
                        with st.status("Performing batch daily update for stocks...", expanded=True) as status:
                            for index, row in stocks_to_update.iterrows():
                                symbol = row['SYMBOL']
                                st.write(f"Updating {symbol}...")
                                result = rd.update_stock_daily(symbol)
                                if "success" in result or "up to date" in result:
                                    success_count += 1
                                else:
                                    fail_count += 1
                                display_toaster('Success' if 'success' in result or 'up to date' in result else 'Failure',
                                                result)

                            status.update(label=f"Stock update complete! Success: {success_count}, Failed: {fail_count}",
                                          state="complete")
                    else:
                        st.warning(
                            "No stocks found in the 'STOCKS_IN_DB' registry. Please load historical data for a stock first.")
            with load_cols[1]:
                st.subheader("Batch Historical Load by Index/Sector")

                all_stocks_df = rd.get_table_data(selected_database='nsedata', selected_table='ALL_STOCKS')

                if "instruments" not in st.session_state:
                    instruments_df = rd.get_table_data(selected_table='instruments', selected_database='trading_db')
                    st.session_state.instruments = instruments_df

                if not all_stocks_df.empty:
                    # Get unique indices and sectors for the dropdown
                    index_sector_list = sorted(all_stocks_df['STK_INDEX_SYMBOL'].unique().tolist())

                    selected_group = st.selectbox(
                        "Select an Index or Sector to load all its constituent stocks",
                        options=index_sector_list,
                        index=None,
                        placeholder="Choose a group..."
                    )

                    start_date = st.date_input("Select Start Date for Historical Load",
                                               value=dt.date(2007, 1, 1),
                                               min_value=dt.date(2000, 1, 1),
                                               max_value=dt.date.today()
                                               )

                    if selected_group and st.button(f"Load History for All Stocks in {selected_group}",
                                                    use_container_width=True):
                        # Filter stocks for the selected group
                        stocks_to_load = all_stocks_df[all_stocks_df['STK_INDEX_SYMBOL'] == selected_group]

                        with st.status(f"Loading history for {len(stocks_to_load)} stocks in {selected_group}...",
                                       expanded=True) as status:
                            for index, row in stocks_to_load.iterrows():
                                symbol = row['SYMBOL']

                                instrument_key = st.session_state.instruments[
                                    st.session_state.instruments['trading_symbol'] == symbol]['instrument_token'].iloc[0]

                                st.write(f"Loading {symbol}...")

                                if data_source == 'NSE':
                                    end_date = dt.date.today()
                                    load_msg = rd.load_stock_history(symbol, start_date, end_date)
                                    if "success" in load_msg:
                                        rd.add_stock_to_registry(symbol, instrument_key, database='nsedata')

                                elif data_source == 'Upstox':
                                    load_msg = load_historical_stock_data_in_chunks(symbol, instrument_key)

                                display_toaster('Success' if 'success' in load_msg else 'Failure', load_msg)
                            status.update(label="Batch historical load complete!", state="complete")
                else:
                    st.warning("Could not retrieve stock list from 'ALL_STOCKS' for batch loading.")

            with load_cols[2]:
                st.subheader("Manual Historical Load")
                # Fetch all stocks to populate the selector
                all_stocks_df = rd.get_table_data(selected_table='ALL_STOCKS', selected_database='nsedata')
                etfs_df = rd.get_table_data(selected_table='ETF_DATA', selected_database='nsedata')
                instruments_df = rd.get_table_data(selected_table='instruments', selected_database='trading_db')
                stocks_in_db = rd.get_table_data(
                    selected_table='STOCKS_IN_DB', selected_database='nsedata')["SYMBOL"].values.tolist()

                if not all_stocks_df.empty:

                    selected_stock = st.selectbox(
                        "Select Stock to Load Historical Data",
                        options=sorted(set(all_stocks_df['SYMBOL'].values.tolist() + etfs_df["Symbol"].values.tolist())),
                        index=None,
                        placeholder="Choose a stock..."
                    )

                    if selected_stock:
                        instrument_key = instruments_df[
                            instruments_df['trading_symbol'] == selected_stock]['instrument_token'].iloc[0]

                        st.markdown("<br>", unsafe_allow_html=True)
                        if st.button(f"Load History for {selected_stock}", use_container_width=True):
                            if data_source == 'NSE':
                                start_date = dt.date(2007, 1, 1)
                                end_date = dt.date.today() - dt.timedelta(days=1)
                                load_msg = rd.load_stock_history(selected_stock, start_date, end_date)
                                if "success" in load_msg:
                                    rd.add_stock_to_registry(selected_stock, instrument_key, database='nsedata')
                                display_toaster('Success' if 'success' in load_msg else 'Failure', load_msg)
                            elif data_source == 'Upstox':
                                load_historical_stock_data_in_chunks(selected_stock, instrument_key)

                        if selected_stock in stocks_in_db:
                            st.markdown("<br>", unsafe_allow_html=True)
                            st.markdown("##### :rainbow[Stock already exists in the database.]")
                        else:
                            st.markdown("<br>", unsafe_allow_html=True)
                            st.markdown("##### :red[Stock does not exist in the database.]")
                else:
                    st.warning("Could not retrieve the list of stocks. Please ensure 'ALL_STOCKS' table is populated.")

        with load_tabs[2]:
            st.markdown("##### NSE Index and Sector Data Loader")
            st.info("Use this tool to load or update historical data for major NSE indices and sectors.")

            indices = fetch_indicies_sectors_list(required='indices')
            sectors = fetch_indicies_sectors_list(required='sectors')
            all_symbols = indices + sectors

            # --- NEW: Button for batch daily update ---
            st.markdown("---")
            st.subheader("Batch Daily Update")
            if st.button("Update All Indices & Sectors Daily", use_container_width=True):
                success_count = 0
                fail_count = 0
                with st.status("Performing batch daily update...", expanded=True) as status:
                    for symbol in all_symbols:
                        st.write(f"Updating {symbol}...")
                        result = rd.update_index_sector_daily(symbol)
                        if "success" in result or "up to date" in result:
                            success_count += 1
                        else:
                            fail_count += 1
                        display_toaster('Success' if 'success' in result or 'up to date' in result else 'Failure',
                                        result)

                    status.update(label=f"Batch update complete! Success: {success_count}, Failed: {fail_count}",
                                  state="complete")

            st.markdown("---")
            st.subheader("Manual Load/Update")

            # UI for selection
            load_type = st.radio("Select Load Type", ["Full History", "Daily Update"], horizontal=True)

            col1, col2 = st.columns(2)
            with col1:
                selected_index = st.selectbox("Select Index", indices, index=None, placeholder="Choose an index...")
            with col2:
                selected_sector = st.selectbox("Select Sector", sectors, index=None, placeholder="Choose a sector...")

            # Determine the selected symbol
            symbol_to_load = selected_index if selected_index else selected_sector

            if symbol_to_load:
                if load_type == "Full History":
                    st.markdown(f"**Mode:** Load complete history for `{symbol_to_load}`.")
                    col_start, col_end, col_btn = st.columns([1, 1, 1])
                    with col_start:
                        start_date = st.date_input("Start Date", dt.date(2007, 1, 1))
                    with col_end:
                        end_date = st.date_input("End Date", dt.date.today())
                    with col_btn:
                        st.markdown("<br>", unsafe_allow_html=True)
                        if st.button(f"Load Full History for {symbol_to_load}", use_container_width=True):
                            with st.spinner(f"Loading full history for {symbol_to_load}..."):
                                result = rd.load_index_sector_history(symbol_to_load, start_date, end_date)
                                display_toaster('Success' if 'success' in result else 'Failure', result)

                else:  # Daily Update
                    st.markdown(f"**Mode:** Incrementally update daily data for `{symbol_to_load}`.")
                    if st.button(f"Run Daily Update for {symbol_to_load}", use_container_width=True):
                        with st.spinner(f"Updating {symbol_to_load}..."):
                            result = rd.update_index_sector_daily(symbol_to_load)
                            display_toaster('Success' if 'success' in result else 'Info', result)
            else:
                st.warning("Please select an index or a sector to proceed.")
        with load_tabs[3]:
            uploaded_file = st.file_uploader("Upload the file", type=['xlsx'])
            if uploaded_file is not None:
                wb = openpyxl.load_workbook(uploaded_file)
                sheet_selector = st.selectbox("Select sheet", options=wb.sheetnames)
                database_list = ["analytics", "nsedata", "mfdata", "STRATEGY"]
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
            else:
                st.write('No file chosen yet for upload')
        try:
            if view_btn:
                st.write(data)
        except Exception:
            pass


if __name__ == '__main__':
    dataload()
