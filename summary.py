import streamlit as st
import pandas as pd
import numpy as np
from common_utils import read_write_sql_data as rd


def load_and_prepare_data():
    # Read the CSV files
    agg_df = rd.get_table_data(selected_table='AGG_DATA')
    eod_df = rd.get_table_data(selected_table='EOD_Summary')
    eod_data_cols = ['Date', 'Symbol', 'Pct_Chg_5D', 'Pct_Chg_20D', 'Pct_Chg_365D', 'HH', 'LL',
                     'High_20', 'Low_20', 'ATR', 'Range_ATR', 'Vol_Avg20', 'EMA_20',
                     'EMA_60', 'EMA_200', 'Reg_6', 'Reg_18', 'Reg_6_Chg', 'Reg_Cross',
                     'Vol_Abv_Avg20', 'Cls_Abv_EMA20', 'Cls_Abv_EMA60', 'Cls_Abv_EMA200',
                     'Cls_Abv_Reg6', 'Curr_Supp', 'Prev_Supp', 'Curr_Res', 'Prev_Res',
                     'Resistance', 'Support', 'EMA20_Sig', 'EMA20_Cnt', 'Reg6_Sig',
                     'Vol20_Sig', 'High_Low', 'Bull_Signal', 'Bear_Signal', 'Break_Sup_Res',
                     'Reg_Cross_Sig', 'Breakout_20', 'ATR_20', 'Range_Pct',
                     'Avg_Range_Pct_20', 'Narrow_Range', 'Narrow_Range_Count',
                     'Narrow_Range_Breakout']
    eod_df = eod_df[eod_data_cols]

    # Merge the dataframes on Symbol and Date
    # merged_df = pd.concat([agg_df, eod_df], axis=1, verify_integrity=True)
    merged_df = agg_df.join(eod_df, rsuffix='_right', how='outer').reset_index()
    return merged_df


def create_stock_screener():
    st.title('Technical Stock Screener')

    # Load data
    try:
        df = load_and_prepare_data()
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return

    st.sidebar.header('Screening Criteria')

    # Support/Resistance Filters
    st.sidebar.subheader('Support/Resistance')
    sup_res_filter = st.sidebar.selectbox(
        'At Support Resistance',
        ['All', 'At Support', 'At Resistance', 'Break_Sup_Res']
    )

    # Volume Filters
    st.sidebar.subheader('Volume Filters')
    volume_filter = st.sidebar.selectbox(
        'Volume Criteria',
        ['All', 'Above Average Volume', 'Super High Volume (2x Avg)']
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
        ['All', 'Uptrend (Higher Highs)', 'Downtrend (Lower Lows)']
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
        df['Close'].values.min(),
        df['Close'].values.max(),
        (df['Close'].values.min(), df['Close'].values.max())
    )

    performance_threshold = st.sidebar.slider(
        'Minimum Performance (%)',
        -50.0,
        50.0,
        0.0
    )

    # Apply filters
    filtered_df = df.copy()

    # Support/ Resistance Filters
    if sup_res_filter == 'At Support':
        filtered_df = filtered_df[filtered_df['Support'] != '']
    elif sup_res_filter == 'At Resistance':
        filtered_df = filtered_df[filtered_df['Resistance'] != '']
    elif sup_res_filter == 'Break_Sup_Res':
        filtered_df = filtered_df[~filtered_df['Break_Sup_Res'].isna()]

    # Price filter
    filtered_df = filtered_df[
        (filtered_df['Close'] >= price_range[0]) &
        (filtered_df['Close'] <= price_range[1])
        ]

    # Volume filter
    if volume_filter == 'Above Average Volume':
        filtered_df = filtered_df[filtered_df['Vol_Abv_Avg20'] > 1]
    elif volume_filter == 'Super High Volume (2x Avg)':
        filtered_df = filtered_df[filtered_df['Vol_Abv_Avg20'] > 2]

    # EMA filters
    if 'Above 20 EMA' in ema_filter:
        filtered_df = filtered_df[filtered_df['Cls_Abv_EMA20'] > 0]
    if 'Above 60 EMA' in ema_filter:
        filtered_df = filtered_df[filtered_df['Cls_Abv_EMA60'] > 0]
    if 'Above 200 EMA' in ema_filter:
        filtered_df = filtered_df[filtered_df['Cls_Abv_EMA200'] > 0]

    # Trend filter
    if trend_filter == 'Uptrend (Higher Highs)':
        filtered_df = filtered_df[filtered_df['High_Low'] == 'HH_HL']
    elif trend_filter == 'Downtrend (Lower Lows)':
        filtered_df = filtered_df[filtered_df['High_Low'] == 'LH_LL']

    # # Performance filter
    # if performance_period == '5 Day':
    #     filtered_df = filtered_df[filtered_df['Pct_Chg_5D'] >= performance_threshold]
    # elif performance_period == '20 Day':
    #     filtered_df = filtered_df[filtered_df['Pct_Chg_20D'] >= performance_threshold]
    # elif performance_period == '1 Year':
    #     filtered_df = filtered_df[filtered_df['Pct_Chg_365D'] >= performance_threshold]

    # Display results
    st.subheader('Screened Stocks')
    st.write(f'Found {len(filtered_df)} stocks matching your criteria')

    # Select columns to display
    display_columns = [
        'Symbol', 'Close', 'Volume', 'Pct_Chg_D',
        'EMA_20', 'EMA_60', 'EMA_200', 'Vol_Abv_Avg20',
        'High_Low', 'Resistance', 'Support', 'Break_Sup_Res'
    ]

    # Format the display dataframe
    display_df = filtered_df[display_columns].copy()
    display_df.columns = [
        'Symbol', 'Close', 'Volume', 'Daily Change %',
        '20 EMA', '60 EMA', '200 EMA', 'Volume/Avg',
        'Trend', 'Resistance', 'Support', 'Break_Sup_Res'
    ]

    st.dataframe(display_df.style.format({
        'Close': '{:.2f}',
        'Volume': '{:,.0f}',
        'Daily Change %': '{:.2f}%',
        '20 EMA': '{:.2f}',
        '60 EMA': '{:.2f}',
        '200 EMA': '{:.2f}',
        'Volume/Avg': '{:.2f}x'
    }), hide_index=True)

    # Add export functionality
    if not display_df.empty:
        csv = display_df.to_csv(index=False)
        st.download_button(
            label="Download Screener Results",
            data=csv,
            file_name="stock_screener_results.csv",
            mime="text/csv"
        )


if __name__ == "__main__":
    st.set_page_config(
        page_title="Technical Stock Screener",
        page_icon="📈",
        layout="wide"
    )
    create_stock_screener()