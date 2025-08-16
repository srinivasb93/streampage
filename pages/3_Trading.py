import pandas as pd
import streamlit as st
from common_utils import read_write_sql_data as rd
import datetime as dt
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from common_utils.utils import fetch_indicies_sectors_list
from lightweight_charts.widgets import StreamlitChart

st.set_page_config(layout="wide")

@st.cache_data
def load_and_prepare_data():
    agg_df = rd.get_table_data(selected_table='AGG_DATA')
    eod_df = rd.get_table_data(selected_table='EOD_Summary')
    eod_data_cols = ['timestamp', 'Symbol', 'Pct_Chg_5D', 'Pct_Chg_20D', 'Pct_Chg_365D', 'HH', 'LL', 'High_20', 'Low_20',
                     'ATR', 'Range_ATR', 'Vol_Avg20', 'EMA_20', 'EMA_60', 'EMA_200', 'Reg_6', 'Reg_18', 'Reg_6_Chg',
                     'Reg_Cross', 'Vol_Abv_Avg20', 'Cls_Abv_EMA20', 'Cls_Abv_EMA60', 'Cls_Abv_EMA200',
                     'Cls_Abv_Reg6', 'Curr_Supp', 'Prev_Supp', 'Curr_Res', 'Prev_Res', 'Resistance', 'Support',
                     'EMA20_Sig', 'EMA20_Cnt', 'Reg6_Sig', 'Vol20_Sig', 'Break_Sup_Res', 'Reg_Cross_Sig',
                     'Breakout_20', 'ATR_20', 'Range_Pct', 'Avg_Range_Pct_20', 'Narrow_Range', 'Narrow_Range_Count',
                     'Narrow_Range_Breakout']
    eod_df = eod_df[eod_data_cols]

    merged_df = agg_df.join(eod_df, rsuffix='_right', how='outer')
    merged_df['ATH'] = merged_df['high'][merged_df['timestamp'] == merged_df['ATH_Date']]
    merged_df['ATL'] = merged_df['low'][merged_df['timestamp'] == merged_df['ATL_Date']]
    return merged_df


@st.cache_data
def fetch_stock_or_index_data(symbol, start_date=None, end_date=None, regular_data=False):
    stock_name = symbol.replace("-", "_")
    if start_date and end_date:
        query = f"SELECT timestamp, close from public.\"{stock_name}\" WHERE timestamp between '{start_date}' AND '{end_date}' ORDER BY timestamp"
    else:
        query = f"SELECT * from public.\"{stock_name}\" ORDER BY timestamp"

    # Fetch data from SQL Server
    stock_data = rd.get_table_data(query=query)
    stock_data.set_index(keys='timestamp', inplace=True)
    if regular_data:
        return stock_data

    stock_data['Cum_Return'] = round(((1 + stock_data['close'].pct_change()).cumprod() - 1)*100, 1)
    stock_data['Cum_Return'] = stock_data['Cum_Return'].fillna(value=0)
    stock_data['Symbol'] = symbol
    return stock_data


@st.cache_data
def plot_line_chart(data):
    fig = px.line(data, x=data.index, y='Cum_Return', color='Symbol', markers=False, hover_data='close',
                  height=580, width=1300)
    fig.update_layout({
        'title': 'Returns Comparison Chart',
        'hovermode': 'closest',
        'dragmode': 'pan'
    })
    return fig


def display_interactive_dataframe(df):
    # df.Stock_Name = df.Symbol
    df['TradingView_Link'] = df.Symbol.map(lambda x: "https://in.tradingview.com/chart?symbol=" + x)
    # df.Symbol = df.Symbol.map(lambda x: f'<a href="{x}" target="_blank">{x.split("=")[-1]}</a>')
    # st.write(df.to_html(escape=False, index=False, index_names=False), unsafe_allow_html=True)

    st.dataframe(df.style
                 .format({
                        'close': '{:.2f}',
                        'Day_Chg_%': '{:.2f}%',
                        '5D_Chg_%': '{:.2f}%',
                        '20D_Chg_%': '{:.2f}%',
                        '365D_Chg_%': '{:.2f}%',
                        'Volume/Avg': '{:.2f}x'
                        },
                        precision=2)
                 .background_gradient(cmap='RdYlGn',
                                      subset=['Day_Chg_%', '5D_Chg_%', '20D_Chg_%', '365D_Chg_%'],
                                      vmin=-5, vmax=5),
                 hide_index=True,
                 column_config={'TradingView_Link': st.column_config.LinkColumn(display_text='Tradingview Chart')})

    if len(df) > 0:
        # Create a form for adding to watchlist
        add_wl, view_wl = st.columns(2, vertical_alignment='center')

        with add_wl:
            with st.expander("Add to Watchlist"):
                with st.form("watchlist_form"):
                    # Create a dropdown to select the symbol
                    symbols = df['Symbol'].tolist()
                    selected_symbol = st.selectbox("Select Symbol", symbols)

                    # Get the selected row
                    selected_row = df[df['Symbol'] == selected_symbol].iloc[0]
                    selected_row['close'] = float(selected_row['close'])

                    # Input field for reason
                    reason = st.text_area("Enter reason:")

                    selected_row['Reason'] = reason
                    selected_row['Date_Added'] = dt.datetime.now()

                    # Submit button
                    if st.form_submit_button("Add to Watchlist"):
                        try:
                            # Call your existing database save function here
                            req_cols = ['Symbol', 'close', 'Reason', 'Date_Added']
                            load_msg = rd.load_sql_data(data_to_load=selected_row[req_cols].to_frame().T,
                                                        table_name='WATCHLIST',
                                                        load_type='append')
                            if 'success' in load_msg:
                                st.success(f"Added {selected_row['Symbol']} to watchlist!")
                            else:
                                st.error(f"Failed to add {selected_row['Symbol']} to watchlist!")
                        except Exception as e:
                            st.error(f"Error: {str(e)}")


def create_stock_screener():
    # Load data
    try:
        df = load_and_prepare_data()
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return

    # Generic Filters
    st.sidebar.subheader("Generic Filters")
    filter_options = ['Breakout_20', 'Near All Time High', 'Near 52 Week High', 'At All Time High', 'At 52 Week High',
                      'Near All Time Low', 'At 52 Week Low', 'At All Time Low', 'At 52 Week Low', 'My Stocks']
    generic_filters = st.sidebar.multiselect("Choose Filter", options=filter_options)

    # Support/Resistance Filters
    st.sidebar.subheader('Support/Resistance Check')
    sup_res_filter = st.sidebar.selectbox(
        'At Support Resistance',
        ['All', 'At Support', 'At Resistance', 'Break_Sup_Res']
    )

    # Volume Filters
    st.sidebar.subheader('Volume Filters')
    volume_filter = st.sidebar.selectbox(
        'Volume Criteria',
        ['All', 'Above Average Volume', 'Super High Volume (2x Avg)',
         'Daily Volume GT Weekly', 'Daily Volume GT Monthly']
    )

    # Technical Indicators
    st.sidebar.subheader('Technical Indicators')
    ema_filter = st.sidebar.multiselect(
        'EMA Criteria',
        ['Above 20 EMA', 'Above 60 EMA', 'Above 200 EMA']
    )

    # Trend Filters
    st.sidebar.subheader('Trend Filters')
    trend_filter = st.sidebar.selectbox(
        'Trend Criteria',
        ['All', 'Uptrend', 'Downtrend']
    )

    # Performance Filters
    # st.sidebar.subheader('Performance Filters')
    # performance_period = st.sidebar.selectbox(
    #     'Performance Period',
    #     ['5 Day', '20 Day', '1 Year']
    # )

    # Price Filters
    st.sidebar.subheader('Price Filters')
    price_range = st.sidebar.slider(
        'Price Range (₹)',
        df['close'].values.min(),
        df['close'].values.max(),
        (df['close'].values.min(), df['close'].values.max())
    )

    performance_threshold = st.sidebar.slider(
        'Minimum Performance (%)',
        -50.0,
        50.0,
        0.0
    )

    # Apply filters
    filtered_df = df.copy()

    if 'Breakout_20' in generic_filters:
        filtered_df = filtered_df[~filtered_df['Breakout_20'].isna()]
    if 'Near All Time High' in generic_filters:
        filtered_df = filtered_df[
            abs(((filtered_df['ATH'] - filtered_df['close']) / filtered_df['close'])*100) <= 5][
            filtered_df['high'] < filtered_df['ATH']][(filtered_df['ATH_Date'] - filtered_df['timestamp']).dt.days > 20]
    if 'Near 52 Week High' in generic_filters:
        # filtered_df = filtered_df[
        #     abs(((filtered_df['High_52W'] - filtered_df['close']) / filtered_df['close'])*100) <= 5][
        #     filtered_df['high'] < filtered_df['High_52W']][
        #     (filtered_df['High_52W_Date'] - filtered_df['timestamp']).dt.days > 20]
        filtered_df = filtered_df[
            abs(((filtered_df['High_52W'] - filtered_df['close']) / filtered_df['close']) * 100) <= 5][
            filtered_df['high'] < filtered_df['High_52W']]
    if 'At All Time High' in generic_filters:
        filtered_df = filtered_df[filtered_df['ATH_Date'] == filtered_df['timestamp'].max()]
    if 'At All Time Low' in generic_filters:
        filtered_df = filtered_df[filtered_df['ATL_Date'] == filtered_df['timestamp'].max()]
    if 'At 52 Week High' in generic_filters:
        filtered_df = filtered_df[filtered_df['high'] == filtered_df['High_52W']]
    if 'At 52 Week Low' in generic_filters:
        filtered_df = filtered_df[filtered_df['low'] == filtered_df['Low_52W']]

    # Support/ Resistance Filters
    if sup_res_filter == 'At Support':
        filtered_df = filtered_df[filtered_df['Support'] != '']
    elif sup_res_filter == 'At Resistance':
        filtered_df = filtered_df[filtered_df['Resistance'] != '']
    elif sup_res_filter == 'Break_Sup_Res':
        filtered_df = filtered_df[~filtered_df['Break_Sup_Res'].isna()]

    # Price filter
    filtered_df = filtered_df[
        (filtered_df['close'] >= price_range[0]) &
        (filtered_df['close'] <= price_range[1])
        ]

    # Volume filter
    if volume_filter == 'Above Average Volume':
        filtered_df = filtered_df[filtered_df['Vol_Abv_Avg20'] > 1]
    elif volume_filter == 'Super High Volume (2x Avg)':
        filtered_df = filtered_df[filtered_df['Vol_Abv_Avg20'] > 2]
    elif volume_filter == 'Daily Volume GT Weekly':
        filtered_df = filtered_df[filtered_df['volume'] > filtered_df['Volume_W']]
    elif volume_filter == 'Daily Volume GT Monthly':
        filtered_df = filtered_df[filtered_df['volume'] > filtered_df['Volume_M']]

    # EMA filters
    if 'Above 20 EMA' in ema_filter:
        filtered_df = filtered_df[filtered_df['Cls_Abv_EMA20'] > 0]
    if 'Above 60 EMA' in ema_filter:
        filtered_df = filtered_df[filtered_df['Cls_Abv_EMA60'] > 0]
    if 'Above 200 EMA' in ema_filter:
        filtered_df = filtered_df[filtered_df['Cls_Abv_EMA200'] > 0]

    # Trend filter
    # if trend_filter == 'Uptrend':
    #     filtered_df = filtered_df[filtered_df['High_Low'] == 'HH_HL']
    # elif trend_filter == 'Downtrend':
    #     filtered_df = filtered_df[filtered_df['High_Low'] == 'LH_LL']

    # # Performance filter
    # if performance_period == '5 Day':
    #     filtered_df = filtered_df[filtered_df['Pct_Chg_5D'] >= performance_threshold]
    # elif performance_period == '20 Day':
    #     filtered_df = filtered_df[filtered_df['Pct_Chg_20D'] >= performance_threshold]
    # elif performance_period == '1 Year':
    #     filtered_df = filtered_df[filtered_df['Pct_Chg_365D'] >= performance_threshold]

    # Display results
    st.write(f':rainbow[Found {len(filtered_df)} stocks matching your criteria]')

    # Select columns to display
    display_columns = [
        'Symbol', 'close', 'Pct_Chg_D', 'Pct_Chg_5D', 'Pct_Chg_20D', 'Pct_Chg_365D', 'Reg_Cross_Sig',
        'Vol_Abv_Avg20', 'Resistance', 'Support', 'Break_Sup_Res', 'Breakout_20'
    ]

    # Format the display dataframe
    display_df = filtered_df[display_columns].copy()
    display_df.columns = [
        'Symbol', 'close', 'Day_Chg_%', '5D_Chg_%', '20D_Chg_%', '365D_Chg_%',  'Reg_Cross_Sig',
        'Volume/Avg', 'Resistance', 'Support', 'Break_Sup_Res', 'Breakout_20'
    ]

    # Display the interactive dataframe
    display_interactive_dataframe(display_df)

    # Add a select box to choose a stock
    selected_stock = st.selectbox("Select a stock to display its chart:", display_df['Symbol'])

    # Fetch stock data
    if selected_stock:
        data = fetch_stock_or_index_data(selected_stock, regular_data=True)

        # Create a lightweight chart
        chart = StreamlitChart(height=650, toolbox=True, scale_candles_only=True)
        chart.legend(True, font_size=20, color_based_on_candle=True, color="#1e81b0")
        chart.watermark(selected_stock)
        chart.set(data, keep_drawings=True)

        # Display the chart in Streamlit
        st.write("### Interactive Stock Chart")
        chart.load()
    else:
        st.write("Please enter a valid stock symbol.")


def trading():
    st.subheader("Technical Stock Screener")

    analysis = st.sidebar.radio("Select the Option", options=['Screener', 'Comparison'], horizontal=True)

    if analysis == 'Screener':
        create_stock_screener()

    elif analysis == 'Comparison':

        stock_index = st.sidebar.radio("Select the option", options=['Stock', 'Indices', 'Sectors'], horizontal=True)
        duration = st.sidebar.radio("Select the duration type", options=['Date Range', 'Duration'], horizontal=True)

        if stock_index == 'Sectors':
            top_row = st.columns([.15, .85], vertical_alignment='center', gap='small')
        else:
            top_row = st.columns([.4, .6], vertical_alignment='center', gap='medium')
        stocks_list_df = rd.get_table_data(selected_table='ALL_STOCKS')

        stock_options = stocks_list_df['SYMBOL'].unique().tolist()
        stock_indices = fetch_indicies_sectors_list(required="indices")
        stock_sectors = fetch_indicies_sectors_list(required="sectors")

        with top_row[0]:
            if duration == 'Date Range':
                if stock_index == 'Sectors':
                    start_date = st.date_input("Start Date",
                                               value=dt.date.today() - dt.timedelta(365),
                                               max_value=dt.date.today() - dt.timedelta(5))
                    end_date = st.date_input("End Date", value=dt.date.today(), max_value=dt.date.today())
                else:
                    date_columns = st.columns(2, vertical_alignment='top')
                    with date_columns[0]:
                        start_date = st.date_input("Start Date",
                                                   value=dt.date.today() - dt.timedelta(365),
                                                   max_value=dt.date.today() - dt.timedelta(5))
                    with date_columns[1]:
                        end_date = st.date_input("End Date", value=dt.date.today(), max_value=dt.date.today())
            else:
                period = st.selectbox("Duration", options=['YTD', '1 Month', '1 Week', '1 Quarter', '6 Months',
                                                           '1 Year', '3 Years', '5 Years', 'Max'])
                period_days_mapping = {'1 Month': 30, '1 Week': 7, '1 Quarter': 121, '6 Months': 182,
                                       '1 Year': 365, '3 Years': 1095, '5 Years': 1825}
                end_date = dt.date.today()
                if period == 'YTD':
                    start_date = dt.date(dt.date.today().year, 1, 1)
                elif period == 'Max':
                    start_date = dt.date(2007, 1, 1)
                else:
                    start_date = end_date - dt.timedelta(period_days_mapping.get(period, 180))

        final_data = pd.DataFrame()
        returns_data = pd.DataFrame()

        if stock_index == 'Stock':
            stock = st.sidebar.selectbox("Choose the stock", options=stock_options)
            index_options = stocks_list_df['STK_INDEX_SYMBOL'][stocks_list_df['SYMBOL'] == stock].unique()
            index_options = [index.replace(" ", "_").replace("&", "AND") for index in index_options]
            index_list = [index for index in index_options if index in stock_indices + stock_sectors]
            selective_comp = st.sidebar.checkbox("Selective Comparison")
            nse_index = st.sidebar.multiselect("Choose the benchmark index", options=index_list)

            if not selective_comp:
                stock_data = fetch_stock_or_index_data(symbol=stock, start_date=start_date, end_date=end_date)
                final_data = pd.concat([final_data, stock_data], axis=0)
                returns_data = pd.concat([returns_data, stock_data[-1:]], axis=0)
                for stk_index in index_list:
                    index_data = fetch_stock_or_index_data(symbol=stk_index, start_date=start_date, end_date=end_date)
                    final_data = pd.concat([final_data, index_data], axis=0)
                    returns_data = pd.concat([returns_data, index_data[-1:]], axis=0)

                st.plotly_chart(plot_line_chart(final_data))  # Plot the comparison chart

                returns_data = returns_data[['Symbol', 'Cum_Return']]
                pivot_df = returns_data.set_index('Symbol').T
                pivot_df.reset_index(drop=True, inplace=True)

                with top_row[1]:
                    st.dataframe(pivot_df.style.background_gradient(cmap='RdYlGn', axis=1)
                                 .format(precision=2),
                                 hide_index=True)

        elif stock_index == 'Indices':
            selective_comp = st.sidebar.checkbox("Selective Comparison")
            if not selective_comp:
                for stk_index in stock_indices:
                    index_data = fetch_stock_or_index_data(symbol=stk_index, start_date=start_date, end_date=end_date)
                    final_data = pd.concat([final_data, index_data], axis=0)
                    returns_data = pd.concat([returns_data, index_data[-1:]], axis=0)

                st.plotly_chart(plot_line_chart(final_data))  # Plot the comparison chart

                returns_data = returns_data[['Symbol', 'Cum_Return']]
                pivot_df = returns_data.set_index('Symbol').T
                pivot_df.reset_index(drop=True, inplace=True)

                with top_row[1]:
                    st.dataframe(pivot_df.style.background_gradient(cmap='RdYlGn', axis=1)
                                 .format(precision=2),
                                 hide_index=True)

            stock = st.sidebar.multiselect("Choose one or more Indices", options=stock_indices)

        elif stock_index == 'Sectors':
            selective_comp = st.sidebar.checkbox("Selective Comparison")
            if not selective_comp:
                for stk_index in stock_sectors+['NIFTY_500']:
                    index_data = fetch_stock_or_index_data(symbol=stk_index, start_date=start_date, end_date=end_date)
                    final_data = pd.concat([final_data, index_data], axis=0)
                    returns_data = pd.concat([returns_data, index_data[-1:]], axis=0)

                returns_data_copy = returns_data[['Symbol', 'Cum_Return']].copy()
                returns_data_copy.reset_index(drop=True, inplace=True)
                returns_data_copy.rename({'Cum_Return': 'Return'}, inplace=True, axis=1)
                # pivot_df = returns_data.set_index('Symbol').T
                # pivot_df.reset_index(drop=True, inplace=True)

                with top_row[1]:
                    top_low_columns = st.columns(4, vertical_alignment='center')
                    with top_low_columns[0]:
                        st.write("Top 5 Sectors Trending UP")
                        st.dataframe(returns_data_copy.sort_values(by='Return', ascending=False).head(5)
                                     .style
                                     .background_gradient(cmap='Greens')
                                     .format(precision=2),
                                     hide_index=True)

                    with top_low_columns[1]:
                        st.write("Top 5 Sectors Trending DOWN")
                        # base_return = returns_data_copy[]
                        st.dataframe(returns_data_copy.sort_values(by='Return').head(5)
                                     .style
                                     .background_gradient(cmap='autumn')
                                     .format(precision=2),
                                     hide_index=True)

                    with top_low_columns[2]:
                        st.write("Sectors above Nifty 500")
                        st.dataframe(returns_data_copy.reset_index(drop=True).sort_values(
                            by='Return').head(5).style
                                     .background_gradient(cmap='RdYlGn')
                                     .format(precision=2),
                                     hide_index=True)

                    with top_low_columns[3]:
                        st.write("Sectors below Nifty 500")
                        st.dataframe(returns_data_copy.reset_index(drop=True).sort_values(
                            by='Return').head(5).style
                                     .background_gradient(cmap='RdYlGn')
                                     .format(precision=2),
                                     hide_index=True)
                st.markdown('---')
                st.plotly_chart(plot_line_chart(final_data))  # Plot the comparison chart

            else:
                sector = st.sidebar.selectbox("Choose one Sector", options=stock_sectors)

                ref_data = rd.get_table_data(selected_table=sector.replace(" ", "_")+'_REF')
                stocks_with_index = ref_data['Symbol'].unique().tolist()
                sector_symbol = stocks_with_index[0]
                stocks_list = stocks_with_index[1:]
                missing_stocks = []
                for stk_index in stocks_list:
                    try:
                        stock_data = fetch_stock_or_index_data(symbol=stk_index,
                                                               start_date=start_date,
                                                               end_date=end_date)
                    except:
                        missing_stocks.append(stk_index)
                        continue
                    final_data = pd.concat([final_data, stock_data], axis=0)
                    returns_data = pd.concat([returns_data, stock_data[-1:]], axis=0)
                st.plotly_chart(plot_line_chart(final_data))  # Plot the comparison chart
                st.write(final_data)
                st.write(returns_data)
                if missing_stocks:
                    st.write(f"Stocks with no data - {missing_stocks}")
                else:
                    st.write("Data present for all stocks")


if __name__ == '__main__':
    trading()
