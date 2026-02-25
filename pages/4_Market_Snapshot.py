import streamlit as st
from common_utils import read_write_sql_data as rd
import numpy as np
import pandas as pd
from python_scripts.get_market_data import market_data
from common_utils.utils import fetch_indicies_sectors_list
from common_utils.auth import require_authentication
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

st.set_page_config(layout="wide")

# Require authentication for this page
require_authentication()
md = market_data.MarketData()


@st.cache_data
def fetch_table_data(asset_type='Stocks', table_name=''):
    asset_map = {'Stocks': 'BHAVCOPY', 'Indices': 'NSE_INDICES_DATA', 'Mutual_Fund': 'LATEST_PREV_NAV_SNAPSHOT'}
    database = 'analytics' if asset_type == 'Mutual_Fund' else 'nsedata'
    if not table_name:
        asset_snapshot = rd.get_table_data(selected_database=database,
                                           selected_table=asset_map.get(asset_type, 'Stocks'))
    else:
        asset_snapshot = rd.get_table_data(selected_database=database,
                                           selected_table=table_name)
    return asset_snapshot


@st.cache_data
def get_stocks_ref_data():
    all_stocks = rd.get_table_data(selected_table='ALL_STOCKS')
    return all_stocks


def display_data_as_per_asset_type(asset_snapshot, subset_cols=[]):
    # Display fetched data
    if asset_snapshot is not None:
        st.dataframe(asset_snapshot.style
                     .background_gradient(cmap='RdYlGn',
                                          subset=subset_cols)
                     .format(precision=2, na_rep=0),
                     hide_index=True
                     )
    else:
        st.write("No data available for display")


def market_snapshot():

    summary = st.sidebar.radio("Choose option", options=['Summary', 'Index Snapshot'], horizontal=True)

    if summary == 'Index Snapshot':
        asset_type = st.sidebar.selectbox("Choose Asset Type", options=["Stocks", "Indices", "Mutual_Fund"])
        radio_btn = st.sidebar.radio(label="Required Data", options=["Prev_Day", "Live"], horizontal=True)

        indices_list = fetch_indicies_sectors_list(required="indices")
        sectors_list = fetch_indicies_sectors_list(required="sectors")

        # Fetch data based on asset type selection
        if asset_type == 'Stocks':

            index_sector = st.sidebar.radio("View Index/Sector/Thematic data",
                                            options=['Index', 'Sector', 'Thematic'],
                                            horizontal=True)

            if index_sector == 'Index':
                selected_type = st.sidebar.selectbox("Choose Index", options=md.broad_indices_list,
                                                     index=md.broad_indices_list.index("NIFTY 50"))
            elif index_sector == 'Sector':
                selected_type = st.sidebar.selectbox("Choose Sector", options=md.sector_indices_list,
                                                     index=md.sector_indices_list.index("NIFTY BANK"))
            else:
                selected_type = st.sidebar.selectbox("Choose Sector", options=md.thematic_indices_list,
                                                     index=md.thematic_indices_list.index("NIFTY ENERGY"))

            table_name = selected_type.replace(" ", "_").replace('&', 'AND') + "_REF"
            stock_snapshot = fetch_table_data("Stocks", table_name=table_name)
            stock_snapshot.rename({"Pct_Change": "Pct_Chg",
                                   "Pct_Change_365d": "Pct_Chg_365d",
                                   "Pct_Change_30d": "Pct_Chg_30d",
                                   "Traded_Volume": "Volume"
                                   }, inplace=True, axis=1)

            index_val = stock_snapshot.iloc[0]
            index_date = index_val['Last_Updated']
            index_data = stock_snapshot.iloc[1:, :].copy()
            index_data = index_data.replace('-', np.nan).ffill()
            display_cols = ['Symbol', 'Close', 'Pct_Chg']
            display_cols_365d = ['Symbol', 'Close', 'Pct_Chg_365d']
            display_cols_30d = ['Symbol', 'Close', 'Pct_Chg_30d']
            display_cols_with_vol = ['Symbol', 'Close', 'Pct_Chg', 'Volume']
            display_row_count = 6

            st.markdown(f"##### :blue-background[:rainbow[{selected_type}] *Summary as on* : :rainbow[{index_date}]]")

            row1 = st.columns([1.3, 1.1, 1.1, 1.2], vertical_alignment="top", gap="medium")

            with row1[0]:
                st.markdown("##### :rainbow[***High Volume Stocks***]")
                display_data_as_per_asset_type(
                    index_data.sort_values(
                        by='Volume',
                        ascending=False)[display_cols_with_vol].head(display_row_count),
                    subset_cols=["Pct_Chg"])

            with row1[1]:
                st.markdown("##### :rainbow[***Top Gainers - Latest***]")
                display_data_as_per_asset_type(
                    index_data.sort_values(
                        by='Pct_Chg', ascending=False)[display_cols].head(display_row_count),
                    subset_cols=['Pct_Chg'])

            with row1[2]:
                st.markdown("##### :rainbow[***Top Losers - Latest***]")
                display_data_as_per_asset_type(
                    index_data.sort_values(
                        by='Pct_Chg', ascending=True)[display_cols].head(display_row_count),
                    subset_cols=['Pct_Chg'])

            with row1[3]:
                st.markdown("##### :rainbow[***High Traded Value Stocks***]")
                display_data_as_per_asset_type(
                    index_data.sort_values(
                        by='Traded_Value', ascending=False)[display_cols].head(display_row_count),
                    subset_cols=['Pct_Chg'])

            row2 = st.columns([1, 1, 1, 1], vertical_alignment="top", gap="small")

            with row2[0]:
                st.markdown("##### :rainbow[***Top Gainers - 30 Days***]")
                display_data_as_per_asset_type(
                    index_data.sort_values(
                        by='Pct_Chg_30d', ascending=False)[display_cols_30d].head(display_row_count),
                    subset_cols=['Pct_Chg_30d'])

            with row2[1]:
                st.markdown("##### :rainbow[***Top Losers - 30 Days***]")
                display_data_as_per_asset_type(
                    index_data.sort_values(
                        by='Pct_Chg_30d', ascending=True)[display_cols_30d].head(display_row_count),
                    subset_cols=['Pct_Chg_30d'])

            with row2[2]:
                st.markdown("##### :rainbow[***Top Gainers - 365 Days***]")
                display_data_as_per_asset_type(
                    index_data.sort_values(
                        by='Pct_Chg_365d', ascending=False)[display_cols_365d].head(display_row_count),
                    subset_cols=['Pct_Chg_365d'])

            with row2[3]:
                st.markdown("##### :rainbow[***Top Losers - 365 Days***]")
                display_data_as_per_asset_type(
                    index_data.sort_values(
                        by='Pct_Chg_365d', ascending=True)[display_cols_365d].head(display_row_count),
                    subset_cols=['Pct_Chg_365d'])

            st.markdown("")
            display_data_as_per_asset_type(index_data, subset_cols=['Pct_Chg'])

        if asset_type == 'Mutual_Fund':
            asset_snapshot = fetch_table_data("Mutual_Fund")
            display_data_as_per_asset_type(asset_snapshot,
                                           subset_cols=['Daily_Pct_Change', 'nav_chg_wkly', 'nav_chg_mth'])

        if asset_type == 'Indices':
            asset_snapshot = md.get_nse_indices_data() if radio_btn == "Live" else rd.get_table_data(
                selected_table='NSE_INDICES_DATA')
            asset_snapshot.fillna(0, inplace=True)
            
            # Convert all object type columns to fix Arrow serialization issues
            # NSE API returns many columns as object type which causes PyArrow errors
            for col in asset_snapshot.columns:
                if asset_snapshot[col].dtype == 'object':
                    # Try to convert to numeric first
                    converted = pd.to_numeric(asset_snapshot[col], errors='coerce')
                    if not converted.isna().all():  # If conversion was successful for at least some values
                        asset_snapshot[col] = converted.fillna(0)
                        # Convert integer-like columns to int64
                        if col in ['advances', 'declines', 'unchanged', 'totalTradedVolume']:
                            asset_snapshot[col] = asset_snapshot[col].astype('int64')
                    else:
                        # If numeric conversion failed, keep as string
                        asset_snapshot[col] = asset_snapshot[col].astype('str')

            metrics_cols = st.columns([.75, .75, .75, 1, 1, 1], vertical_alignment="top")

            with metrics_cols[0]:
                st.metric("NIFTY 50",
                          value=int(asset_snapshot[asset_snapshot['indexSymbol'] == 'NIFTY 50']['last'].values[0]),
                          delta=asset_snapshot[asset_snapshot['indexSymbol'] == 'NIFTY 50']['variation'].values[0])

            with metrics_cols[1]:
                st.metric("NIFTY MIDCAP 100",
                          value=int(
                              asset_snapshot[asset_snapshot['indexSymbol'] == 'NIFTY MIDCAP 100']['last'].values[0]),
                          delta=asset_snapshot[
                              asset_snapshot['indexSymbol'] == 'NIFTY MIDCAP 100']['variation'].values[0])

            with metrics_cols[2]:
                st.metric("NIFTY SMLCAP 100",
                          value=int(asset_snapshot[
                                        asset_snapshot['indexSymbol'] == 'NIFTY SMLCAP 100']['last'].values[0]),
                          delta=asset_snapshot[
                              asset_snapshot['indexSymbol'] == 'NIFTY SMLCAP 100']['variation'].values[0])

            asset_snapshot = asset_snapshot[["indexSymbol", "percentChange", 'advances', 'declines',
                                             "perChange365d", "perChange30d"]].copy()

            sectors_data = asset_snapshot[asset_snapshot['indexSymbol'].isin(sectors_list)].copy()
            indices_data = asset_snapshot[asset_snapshot['indexSymbol'].isin(indices_list)].copy()

            sectors_data['ADR'] = sectors_data['advances'] / (
                        sectors_data['advances'] + sectors_data['declines']) * 100
            sectors_data['ADR'] = sectors_data['ADR'].astype(dtype='int64')
            sectors_data["perChange365d"] = round(sectors_data["perChange365d"].astype("float"), 2)

            indices_data['ADR'] = indices_data['advances'] / (
                    indices_data['advances'] + indices_data['declines']) * 100
            indices_data['ADR'] = indices_data['ADR'].astype(dtype='int64')
            indices_data["perChange365d"] = round(indices_data["perChange365d"].astype("float"), 2)

            indices_data.drop(labels=['advances', 'declines'], axis=1, inplace=True)
            sectors_data.drop(labels=['advances', 'declines'], axis=1, inplace=True)

            top_sectors_30d = sectors_data.sort_values(
                by="perChange30d", ascending=False)[["indexSymbol", "perChange30d"]]
            top_sectors_1d = sectors_data.sort_values(
                by="percentChange", ascending=False)[["indexSymbol", "percentChange"]]

            with metrics_cols[3]:
                st.markdown(f"Top Sectors(30D)\n- "
                            f"{top_sectors_30d.iloc[0, 0]} :  {top_sectors_30d.iloc[0, 1]}% \n- "
                            f"{top_sectors_30d.iloc[1, 0]} :  {top_sectors_30d.iloc[1, 1]}%")

            with metrics_cols[4]:
                st.markdown(f"Top Sectors(1D)\n- "
                            f"{top_sectors_1d.iloc[0, 0]} :  {top_sectors_1d.iloc[0, 1]}% \n- "
                            f"{top_sectors_1d.iloc[1, 0]} :  {top_sectors_1d.iloc[1, 1]}%")

            with metrics_cols[5]:
                st.markdown(f"Bottom Sectors(30D)\n- "
                            f"{top_sectors_30d.iloc[-1, 0]} :  {top_sectors_30d.iloc[-1, 1]}% \n- "
                            f"{top_sectors_30d.iloc[-2, 0]} :  {top_sectors_30d.iloc[-2, 1]}%")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Indices Summary")
                display_data_as_per_asset_type(indices_data,
                                               subset_cols=['percentChange', 'perChange365d', 'perChange30d', 'ADR'])

            with col2:
                st.markdown("#### Sectors Summary")
                display_data_as_per_asset_type(sectors_data,
                                               subset_cols=['percentChange', 'perChange365d', 'perChange30d', 'ADR'])
    else:
        # Comprehensive Trading Analytics Dashboard
        st.markdown("### 📊 Trading Analytics Dashboard")

        
        # Market Overview from NSE_INDICES_DATA
        st.markdown("### 🌐 Market Overview")
        
        try:
            # Get market indices data
            indices_data = md.get_nse_indices_data()
            indices_data.fillna(0, inplace=True)
            
            # Convert all object type columns to fix Arrow serialization issues
            # NSE API returns many columns as object type which causes PyArrow errors
            for col in indices_data.columns:
                if indices_data[col].dtype == 'object':
                    # Try to convert to numeric first
                    converted = pd.to_numeric(indices_data[col], errors='coerce')
                    if not converted.isna().all():  # If conversion was successful for at least some values
                        indices_data[col] = converted.fillna(0)
                        # Convert integer-like columns to int64
                        if col in ['advances', 'declines', 'unchanged', 'totalTradedVolume']:
                            indices_data[col] = indices_data[col].astype('int64')
                    else:
                        # If numeric conversion failed, keep as string
                        indices_data[col] = indices_data[col].astype('str')
            
            if indices_data is not None and not indices_data.empty:
                # Key Market Metrics
                col1, col2, col3, col4 = st.columns(4)
                
                # NIFTY 50
                nifty_50 = indices_data[indices_data['indexSymbol'] == 'NIFTY 50']
                if not nifty_50.empty:
                    with col1:
                        st.metric(
                            "NIFTY 50",
                            value=f"{nifty_50['last'].iloc[0]:,.0f}",
                            delta=f"{nifty_50['variation'].iloc[0]:.2f}%"
                        )
                
                # NIFTY BANK
                nifty_bank = indices_data[indices_data['indexSymbol'] == 'NIFTY BANK']
                if not nifty_bank.empty:
                    with col2:
                        st.metric(
                            "NIFTY BANK",
                            value=f"{nifty_bank['last'].iloc[0]:,.0f}",
                            delta=f"{nifty_bank['variation'].iloc[0]:.2f}%"
                        )
                
                # NIFTY MIDCAP 100
                nifty_midcap = indices_data[indices_data['indexSymbol'] == 'NIFTY MIDCAP 100']
                if not nifty_midcap.empty:
                    with col3:
                        st.metric(
                            "NIFTY MIDCAP 100",
                            value=f"{nifty_midcap['last'].iloc[0]:,.0f}",
                            delta=f"{nifty_midcap['variation'].iloc[0]:.2f}%"
                        )
                
                # NIFTY SMLCAP 100
                nifty_smallcap = indices_data[indices_data['indexSymbol'] == 'NIFTY SMLCAP 100']
                if not nifty_smallcap.empty:
                    with col4:
                        st.metric(
                            "NIFTY SMLCAP 100",
                            value=f"{nifty_smallcap['last'].iloc[0]:,.0f}",
                            delta=f"{nifty_smallcap['variation'].iloc[0]:.2f}%"
                        )
                
                # Market Breadth Analysis
                st.markdown("#### 📈 Market Breadth Analysis")
                with st.expander("View Market Breadth Analysis"):
                    st.dataframe(indices_data, hide_index=True)
                # Calculate market breadth

                broad_market_data = indices_data[indices_data['key'] == 'BROAD MARKET INDICES']
                total_advances = broad_market_data['advances'][broad_market_data['indexSymbol'] == 'NIFTY 500'].astype(int).sum()
                total_declines = broad_market_data['declines'][broad_market_data['indexSymbol'] == 'NIFTY 500'].astype(int).sum()
                advance_decline_ratio = (total_advances / total_declines) if total_declines > 0 else 0
                
                breadth_col1, breadth_col2, breadth_col3, breadth_col4 = st.columns(4)
                
                with breadth_col1:
                    st.metric("Total Advances", f"{total_advances:,}")
                with breadth_col2:
                    st.metric("Total Declines", f"{total_declines:,}")
                with breadth_col3:
                    st.metric("Advance/Decline Ratio", f"{advance_decline_ratio:.2f}")
                with breadth_col4:
                    market_sentiment = "🟢 Bullish" if advance_decline_ratio > 1.2 else "🔴 Bearish" if advance_decline_ratio < 0.8 else "🟡 Neutral"
                    st.metric("Market Sentiment", market_sentiment)
                
                # Top Performing Sectors
                st.markdown("#### 🏆 Top Performing Sectors")
                
                # Filter sector indices
                sector_indices = indices_data[indices_data['key'].isin([
                    'SECTORAL INDICES'
                ])]
                
                if not sector_indices.empty:
                    top_gainers = sector_indices.nlargest(5, 'percentChange')[['indexSymbol', 'percentChange']]
                    top_losers = sector_indices.nsmallest(5, 'percentChange')[['indexSymbol', 'percentChange']]
                    
                    sector_col1, sector_col2 = st.columns(2)
                    
                    with sector_col1:
                        st.markdown("**Top Gainers**")
                        for idx, row in top_gainers.iterrows():

                            st.write(f"• {row['indexSymbol']}: {row['percentChange']:.2f}%")
                    
                    with sector_col2:
                        st.markdown("**Top Losers**")
                        for idx, row in top_losers.iterrows():
                            st.write(f"• {row['indexSymbol']}: {row['percentChange']:.2f}%")
            
        except Exception as e:
            st.warning(f"Could not fetch live market data: {str(e)}")
        
        # Trading Opportunities Analysis
        st.markdown("### 💼 Trading Opportunities Analysis")
        
        # Create tabs for different analysis
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Top Performers", "📊 Technical Analysis", "🎯 Trading Signals", "📋 Market Summary", "📊 PE/PB/Dividend Analysis"])
        
        with tab1:
            st.markdown("#### 🚀 Top Gainers Analysis")

            df = rd.get_table_data(selected_database='nsedata', selected_table='EOD_Summary')
            df = df[['timestamp', 'Symbol', 'close', 'Pct_Chg', 'Pct_Chg_5D', 'Pct_Chg_20D',
                     'volume', 'Vol_Avg20', 'RSI_14', 'EMA_20', 'EMA_60', 'EMA_200']]
            df.rename(
                columns={
                    'close': 'Close',
                    'Pct_Chg': 'Pct_Chg_D',
                    'volume': 'Volume',
                    'Vol_Avg20': 'Avg_Vol_20D'
                },
                inplace=True
            )
            df.fillna(0, inplace=True)
            
            # Top performers by percentage change
            st.markdown("**Top 10 Gainers Today**")
            top_gainers = df.nlargest(10, 'Pct_Chg_D')[['timestamp', 'Symbol', 'Close', 'Pct_Chg_D', 'Volume', 'RSI_14']]
            st.dataframe(
                top_gainers.style
                .background_gradient(cmap='RdYlGn', subset=['Pct_Chg_D'])
                .format({'Close': '{:.2f}', 'Pct_Chg_D': '{:.2f}%', 'RSI_14': '{:.1f}'}),
                hide_index=True
            )
            
            # Volume analysis
            st.markdown("**High Volume Stocks**")
            high_volume = df.nlargest(5, 'Volume')[['Symbol', 'Close', 'Pct_Chg_D', 'Volume']]
            st.dataframe(high_volume, hide_index=True)
        
        with tab2:
            st.markdown("#### 📊 Technical Analysis Dashboard")
            
            # RSI Analysis
            st.markdown("**RSI Analysis**")
            rsi_col1, rsi_col2, rsi_col3 = st.columns(3)
            
            with rsi_col1:
                st.markdown("**Oversold (RSI < 30)**")
                oversold = df[df['RSI_14'] < 30]
                if not oversold.empty:
                    st.dataframe(oversold[['Symbol', 'Close', 'RSI_14', 'Pct_Chg_D']].style.background_gradient(
                        cmap='RdYlGn', subset=['Pct_Chg_D', 'RSI_14']).format({'RSI_14': '{:.1f}', 'Pct_Chg_D': '{:.2f}%'}), hide_index=True)
                else:
                    st.write("No oversold stocks")
            
            with rsi_col2:
                st.markdown("**Neutral (30 ≤ RSI ≤ 70)**")
                neutral = df[(df['RSI_14'] >= 30) & (df['RSI_14'] <= 70)]
                if not neutral.empty:
                    st.dataframe(neutral[['Symbol', 'Close', 'RSI_14', 'Pct_Chg_D']].head(10).style.background_gradient(
                        cmap='RdYlGn', subset=['Pct_Chg_D', 'RSI_14']).format({'RSI_14': '{:.1f}', 'Pct_Chg_D': '{:.2f}%'}), hide_index=True)
                else:
                    st.write("No neutral RSI stocks")
            
            with rsi_col3:
                st.markdown("**Overbought (RSI > 70)**")
                overbought = df[df['RSI_14'] > 70]
                if not overbought.empty:
                    st.dataframe(overbought[['Symbol', 'Close', 'RSI_14', 'Pct_Chg_D']].style.background_gradient(
                        cmap='RdYlGn', subset=['Pct_Chg_D', 'RSI_14']).format({'RSI_14': '{:.1f}', 'Pct_Chg_D': '{:.2f}%'}), hide_index=True)
                else:
                    st.write("No overbought stocks")
            
            # EMA Crossover Detection
            st.markdown("**EMA Crossover Opportunities (Within 2%)**")
            
            # Calculate proximity to EMAs
            df['EMA20_proximity'] = abs(df['Close'] - df['EMA_20']) / df['Close'] * 100
            df['EMA60_proximity'] = abs(df['Close'] - df['EMA_60']) / df['Close'] * 100
            df['EMA200_proximity'] = abs(df['Close'] - df['EMA_200']) / df['Close'] * 100
            
            crossover_cols = st.columns(3)
            
            with crossover_cols[0]:
                st.markdown("**Near EMA 20 (±2%)**")
                near_ema20 = df[df['EMA20_proximity'] <= 2.0]
                if not near_ema20.empty:
                    st.dataframe(near_ema20[['Symbol', 'Close', 'EMA_20', 'Pct_Chg_D']].style.background_gradient(
                        cmap='RdYlGn', subset=['Pct_Chg_D']), hide_index=True)
                else:
                    st.write("No stocks near EMA 20")
            
            with crossover_cols[1]:
                st.markdown("**Near EMA 60 (±2%)**")
                near_ema60 = df[df['EMA60_proximity'] <= 2.0]
                if not near_ema60.empty:
                    st.dataframe(near_ema60[['Symbol', 'Close', 'EMA_60', 'Pct_Chg_D']].style.background_gradient(
                        cmap='RdYlGn', subset=['Pct_Chg_D']), hide_index=True)
                else:
                    st.write("No stocks near EMA 60")
            
            with crossover_cols[2]:
                st.markdown("**Near EMA 200 (±2%)**")
                near_ema200 = df[df['EMA200_proximity'] <= 2.0]
                if not near_ema200.empty:
                    st.dataframe(near_ema200[['Symbol', 'Close', 'EMA_200', 'Pct_Chg_D']].style.background_gradient(
                        cmap='RdYlGn', subset=['Pct_Chg_D']), hide_index=True)
                else:
                    st.write("No stocks near EMA 200")
            
            # EMA Analysis
            st.markdown("**EMA Trend Analysis**")
            
            # Calculate EMA signals
            df['EMA20_Signal'] = np.where(df['Close'] > df['EMA_20'], 'Above', 'Below')
            df['EMA60_Signal'] = np.where(df['Close'] > df['EMA_60'], 'Above', 'Below')
            df['EMA200_Signal'] = np.where(df['Close'] > df['EMA_200'], 'Above', 'Below')
            
            # Strong uptrend (above all EMAs)
            strong_uptrend = df[(df['EMA20_Signal'] == 'Above') & 
                              (df['EMA60_Signal'] == 'Above') & 
                              (df['EMA200_Signal'] == 'Above')]
            
            st.markdown("**Strong Uptrend (Above All EMAs)**")
            if not strong_uptrend.empty:
                st.dataframe(strong_uptrend[['Symbol', 'Close', 'Pct_Chg_D', 'EMA_20', 'EMA_60', 'EMA_200', 'RSI_14']].style.background_gradient(
                    cmap='RdYlGn', subset=['Pct_Chg_D', 'RSI_14']).format({'RSI_14': '{:.1f}', 'Pct_Chg_D': '{:.2f}%'}), hide_index=True)
            else:
                st.write("No stocks in strong uptrend")
            
            # Support/Resistance Detection
            st.markdown("**Stocks at Support/Resistance Levels (±2%)**")
            
            # Get EOD_Summary data for support/resistance levels
            try:
                eod_summary = rd.get_table_data(selected_table='EOD_Summary', selected_database='nsedata')
                
                if not eod_summary.empty:
                    # Check proximity to support/resistance
                    df_with_sr = df.merge(
                        eod_summary[['Symbol', 'Curr_Supp', 'Prev_Supp', 'Curr_Res', 'Prev_Res']],
                        on='Symbol', how='left'
                    )
                    
                    df_with_sr['at_curr_supp'] = abs(df_with_sr['Close'] - df_with_sr['Curr_Supp']) / df_with_sr['Close'] * 100 <= 2
                    df_with_sr['at_prev_supp'] = abs(df_with_sr['Close'] - df_with_sr['Prev_Supp']) / df_with_sr['Close'] * 100 <= 2
                    df_with_sr['at_curr_res'] = abs(df_with_sr['Close'] - df_with_sr['Curr_Res']) / df_with_sr['Close'] * 100 <= 2
                    df_with_sr['at_prev_res'] = abs(df_with_sr['Close'] - df_with_sr['Prev_Res']) / df_with_sr['Close'] * 100 <= 2
                    
                    sr_cols = st.columns(2)
                    
                    with sr_cols[0]:
                        st.markdown("**At Support Levels**")
                        at_support = df_with_sr[df_with_sr['at_curr_supp'] | df_with_sr['at_prev_supp']]
                        if not at_support.empty:
                            display_df = at_support[['Symbol', 'Close', 'Curr_Supp', 'Prev_Supp', 'Pct_Chg_D']]
                            st.dataframe(display_df.style.background_gradient(
                                cmap='RdYlGn', subset=['Pct_Chg_D']), hide_index=True)
                        else:
                            st.write("No stocks at support levels")
                    
                    with sr_cols[1]:
                        st.markdown("**At Resistance Levels**")
                        at_resistance = df_with_sr[df_with_sr['at_curr_res'] | df_with_sr['at_prev_res']]
                        if not at_resistance.empty:
                            display_df = at_resistance[['Symbol', 'Close', 'Curr_Res', 'Prev_Res', 'Pct_Chg_D']]
                            st.dataframe(display_df.style.background_gradient(
                                cmap='RdYlGn', subset=['Pct_Chg_D']), hide_index=True)
                        else:
                            st.write("No stocks at resistance levels")
                else:
                    st.warning("EOD_Summary data not available for support/resistance analysis")
            except Exception as e:
                st.warning(f"Could not load support/resistance data: {str(e)}")
        
        with tab3:
            st.markdown("#### 🎯 Trading Signals")
            
            # Generate trading signals based on technical analysis
            signals = []
            
            for idx, row in df.iterrows():
                signal = {
                    'Symbol': row['Symbol'],
                    'Close': row['Close'],
                    'Pct_Chg_D': row['Pct_Chg_D'],
                    'RSI_14': row['RSI_14'],
                    'Signal': '',
                    'Strength': '',
                    'Reason': ''
                }
                
                # RSI signals
                if row['RSI_14'] < 30:
                    signal['Signal'] = 'BUY'
                    signal['Strength'] = 'Strong'
                    signal['Reason'] = 'Oversold - RSI below 30'
                elif row['RSI_14'] > 70:
                    signal['Signal'] = 'SELL'
                    signal['Strength'] = 'Strong'
                    signal['Reason'] = 'Overbought - RSI above 70'
                elif 30 <= row['RSI_14'] <= 70:
                    signal['Signal'] = 'HOLD'
                    signal['Strength'] = 'Neutral'
                    signal['Reason'] = 'RSI in neutral zone'
                
                # EMA trend confirmation
                if row['Close'] > row['EMA_20'] and row['Close'] > row['EMA_60']:
                    if signal['Signal'] == 'BUY':
                        signal['Strength'] = 'Very Strong'
                        signal['Reason'] += ' + Above EMAs'
                    elif signal['Signal'] == 'HOLD':
                        signal['Signal'] = 'BUY'
                        signal['Strength'] = 'Moderate'
                        signal['Reason'] = 'Above EMAs + RSI neutral'
                
                signals.append(signal)
            
            signals_df = pd.DataFrame(signals)
            
            # Display buy signals
            buy_signals = signals_df[signals_df['Signal'] == 'BUY']
            if not buy_signals.empty:
                st.markdown("**🟢 BUY Signals**")
                st.dataframe(
                    buy_signals[['Symbol', 'Close', 'Pct_Chg_D', 'RSI_14', 'Strength', 'Reason']].style
                    .background_gradient(cmap='RdYlGn', subset=['Pct_Chg_D']),
                    hide_index=True
                )
            
            # Display sell signals
            sell_signals = signals_df[signals_df['Signal'] == 'SELL']
            if not sell_signals.empty:
                st.markdown("**🔴 SELL Signals**")
                st.dataframe(
                    sell_signals[['Symbol', 'Close', 'Pct_Chg_D', 'RSI_14', 'Strength', 'Reason']].style
                    .background_gradient(cmap='RdYlGn', subset=['Pct_Chg_D']),
                    hide_index=True
                )
        
        with tab4:
            st.markdown("#### 📋 Market Summary")
            
            # Market statistics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Stocks Analyzed", len(df))
            
            with col2:
                gainers = len(df[df['Pct_Chg_D'] > 0])
                st.metric("Gainers", gainers)
            
            with col3:
                losers = len(df[df['Pct_Chg_D'] < 0])
                st.metric("Losers", losers)
            
            with col4:
                avg_change = df['Pct_Chg_D'].mean()
                st.metric("Avg Change", f"{avg_change:.2f}%")
            
            # Sector performance (simulated)
            st.markdown("**Sector Performance**")
            sector_data = {
                'Sector': ['NIFTY METAL', 'NIFTY PSU BANK', 'NIFTY IND DEFENCE', 'NIFTY CONSR DURBL', 'NIFTY CPSE'],
                'Change': [1.82, 1.12, 1.96, 1.09, 1.03],
                '30D_Change': [9.51, 10.7, 6.31, -4.32, 3.78],
                '365D_Change': [0.53, 12.14, 0.0, -13.83, -9.06]
            }
            
            sector_df = pd.DataFrame(sector_data)
            st.dataframe(
                sector_df.style
                .background_gradient(cmap='RdYlGn', subset=['Change', '30D_Change', '365D_Change']),
                hide_index=True
            )
            
            # Risk metrics
            st.markdown("**Risk Metrics**")
            risk_col1, risk_col2, risk_col3 = st.columns(3)
            
            with risk_col1:
                st.metric("High RSI Stocks", len(df[df['RSI_14'] > 70]))
            
            with risk_col2:
                st.metric("Low RSI Stocks", len(df[df['RSI_14'] < 30]))
            
            with risk_col3:
                st.metric("Neutral RSI Stocks", len(df[(df['RSI_14'] >= 30) & (df['RSI_14'] <= 70)]))
            
            # Returns Summary from AGG_DATA
            st.markdown("**Returns Summary (from AGG_DATA)**")
            
            try:
                # Fetch AGG_DATA table
                agg_data = rd.get_table_data(selected_table='AGG_DATA', selected_database='nsedata')
                
                if not agg_data.empty:
                    # Select returns columns
                    returns_columns = ['Symbol', 'Pct_Chg_D', 'Percent_Chg_W', 'Percent_Chg_M', 
                                      'Percent_Chg_Y', '3_Year_Returns', '5_Year_Returns', 'Max_Returns']
                    
                    # Filter to available columns
                    available_cols = [col for col in returns_columns if col in agg_data.columns]
                    
                    if available_cols:
                        returns_df = agg_data[available_cols].copy()
                        returns_df.rename(columns={
                            'Pct_Chg_D': 'Day%',
                            'Percent_Chg_W': 'Week%',
                            'Percent_Chg_M': 'Month%',
                            'Percent_Chg_Y': 'Year%',
                            '3_Year_Returns': '3Yr%',
                            '5_Year_Returns': '5Yr%',
                            'Max_Returns': 'Max%'
                        }, inplace=True)
                        
                        st.dataframe(
                            returns_df.style.background_gradient(cmap='RdYlGn', 
                                subset=[col for col in returns_df.columns if col != 'Symbol']),
                            hide_index=True
                        )
                    else:
                        st.warning("No returns data available in AGG_DATA")
                else:
                    st.warning("AGG_DATA table is empty")
            except Exception as e:
                st.warning(f"Could not load returns data: {str(e)}")
        
        with tab5:
            st.markdown("#### 📊 Historical PE/PB/Dividend Analysis")
            st.markdown("*Investment opportunities based on historical patterns and percentiles*")
            
            # Analysis period selection
            analysis_period = st.selectbox(
                "Select Analysis Period",
                options=[30, 90, 180, 365, 730, 1825, 3650, "max"],  # Added 5 years, 10 years, max
                index=3,  # Default to 365 days
                format_func=lambda x: "Maximum Available" if x == "max" else f"{x} days" if x < 365 else f"{x//365} year{'s' if x > 365 else ''}"
            )
            
            # Chart visualization options
            chart_type = st.radio(
                "Select Visualization Type",
                options=["📊 Tables Only", "📈 Interactive Charts", "📊 Both Tables & Charts"],
                horizontal=True,
                index=2  # Default to both
            )
            
            try:
                # Get historical analysis for all indices
                if analysis_period == "max":
                    st.info("Analyzing historical data for maximum available period...")
                    historical_analysis = md.get_all_indices_historical_analysis(days_back=9999)  # Large number for max
                else:
                    st.info(f"Analyzing historical data for the last {analysis_period} days...")
                    historical_analysis = md.get_all_indices_historical_analysis(days_back=analysis_period)
                
                if historical_analysis:
                    # Convert to DataFrame for easier analysis
                    analysis_df = pd.DataFrame([
                        {
                            'index_name': index,
                            'current_pe': data['current_pe'],
                            'current_pb': data['current_pb'],
                            'current_div': data['current_div'],
                            'pe_percentile': data['pe_percentile'],
                            'pb_percentile': data['pb_percentile'],
                            'div_percentile': data['div_percentile'],
                            'pe_trend': data['pe_trend'],
                            'pb_trend': data['pb_trend'],
                            'div_trend': data['div_trend'],
                            'data_points': data['data_points']
                        }
                        for index, data in historical_analysis.items()
                    ])
                    
                    # Chart visualization functions
                    def create_opportunity_score_chart(df):
                        """Create opportunity score bar chart"""
                        fig = px.bar(
                            df.head(15), 
                            x='opportunity_score', 
                            y='index_name',
                            orientation='h',
                            title="Top Investment Opportunities (by Score)",
                            color='opportunity_score',
                            color_continuous_scale='RdYlGn'
                        )
                        fig.update_layout(
                            height=600,
                            xaxis_title="Opportunity Score",
                            yaxis_title="Index Name",
                            showlegend=False
                        )
                        return fig
                    
                    def create_percentile_scatter_chart(df):
                        """Create percentile scatter chart"""
                        fig = go.Figure()
                        
                        # Add scatter points for each metric
                        fig.add_trace(go.Scatter(
                            x=df['pe_percentile'],
                            y=df['pb_percentile'],
                            mode='markers+text',
                            text=df['index_name'],
                            textposition="top center",
                            marker=dict(
                                size=df['div_percentile']/5,  # Size based on dividend percentile
                                color=df['opportunity_score'],
                                colorscale='RdYlGn',
                                showscale=True,
                                colorbar=dict(title="Opportunity Score")
                            ),
                            name='Indices',
                            hovertemplate='<b>%{text}</b><br>' +
                                        'PE Percentile: %{x:.1f}%<br>' +
                                        'PB Percentile: %{y:.1f}%<br>' +
                                        'Dividend Percentile: %{marker.size:.1f}%<br>' +
                                        'Opportunity Score: %{marker.color}<extra></extra>'
                        ))
                        
                        # Add quadrant lines
                        fig.add_hline(y=50, line_dash="dash", line_color="gray", opacity=0.5)
                        fig.add_vline(x=50, line_dash="dash", line_color="gray", opacity=0.5)
                        
                        fig.update_layout(
                            title="PE vs PB Percentiles (Bubble size = Dividend Percentile)",
                            xaxis_title="PE Percentile",
                            yaxis_title="PB Percentile",
                            height=600
                        )
                        
                        return fig
                    
                    def create_trend_analysis_chart(df):
                        """Create trend analysis chart"""
                        fig = make_subplots(
                            rows=1, cols=3,
                            subplot_titles=('PE Trends', 'PB Trends', 'Dividend Trends'),
                            specs=[[{"secondary_y": False}, {"secondary_y": False}, {"secondary_y": False}]]
                        )
                        
                        # PE Trends
                        fig.add_trace(
                            go.Bar(
                                x=df['index_name'],
                                y=df['pe_trend'],
                                name='PE Trend',
                                marker_color=['green' if x < 0 else 'red' for x in df['pe_trend']]
                            ),
                            row=1, col=1
                        )
                        
                        # PB Trends
                        fig.add_trace(
                            go.Bar(
                                x=df['index_name'],
                                y=df['pb_trend'],
                                name='PB Trend',
                                marker_color=['green' if x < 0 else 'red' for x in df['pb_trend']]
                            ),
                            row=1, col=2
                        )
                        
                        # Dividend Trends
                        fig.add_trace(
                            go.Bar(
                                x=df['index_name'],
                                y=df['div_trend'],
                                name='Dividend Trend',
                                marker_color=['green' if x > 0 else 'red' for x in df['div_trend']]
                            ),
                            row=1, col=3
                        )
                        
                        fig.update_layout(
                            title="30-Day Trend Analysis",
                            height=400,
                            showlegend=False
                        )
                        
                        # Rotate x-axis labels
                        fig.update_xaxes(tickangle=45)
                        
                        return fig
                    
                    def create_historical_time_series(index_name, historical_data):
                        """Create historical time series chart for a specific index"""
                        if historical_data.empty:
                            return None
                        
                        fig = make_subplots(
                            rows=3, cols=1,
                            subplot_titles=(f'{index_name} - PE Ratio', f'{index_name} - PB Ratio', f'{index_name} - Dividend Yield'),
                            vertical_spacing=0.1
                        )
                        
                        # PE Ratio
                        fig.add_trace(
                            go.Scatter(
                                x=historical_data['timestamp'],
                                y=historical_data['pe'],
                                mode='lines+markers',
                                name='PE Ratio',
                                line=dict(color='blue')
                            ),
                            row=1, col=1
                        )
                        
                        # PB Ratio
                        fig.add_trace(
                            go.Scatter(
                                x=historical_data['timestamp'],
                                y=historical_data['pb'],
                                mode='lines+markers',
                                name='PB Ratio',
                                line=dict(color='red')
                            ),
                            row=2, col=1
                        )
                        
                        # Dividend Yield
                        fig.add_trace(
                            go.Scatter(
                                x=historical_data['timestamp'],
                                y=historical_data['div_yield'],
                                mode='lines+markers',
                                name='Dividend Yield',
                                line=dict(color='green')
                            ),
                            row=3, col=1
                        )
                        
                        fig.update_layout(
                            title=f"{index_name} - Historical Analysis",
                            height=800,
                            showlegend=False
                        )
                        
                        return fig
                    
                    # Investment Opportunities Analysis
                    st.markdown("### 🎯 Investment Opportunities")
                    
                    # Define investment criteria based on historical percentiles
                    opportunities = []
                    
                    for index, data in historical_analysis.items():
                        opportunity_score = 0
                        reasons = []
                        
                        # PE Analysis (lower percentile = better opportunity)
                        if data['pe_percentile'] <= 25:
                            opportunity_score += 3
                            reasons.append(f"PE at {data['pe_percentile']:.1f}th percentile (historically low)")
                        elif data['pe_percentile'] <= 40:
                            opportunity_score += 1
                            reasons.append(f"PE at {data['pe_percentile']:.1f}th percentile (below median)")
                        
                        # PB Analysis
                        if data['pb_percentile'] <= 25:
                            opportunity_score += 2
                            reasons.append(f"PB at {data['pb_percentile']:.1f}th percentile (historically low)")
                        elif data['pb_percentile'] <= 40:
                            opportunity_score += 1
                            reasons.append(f"PB at {data['pb_percentile']:.1f}th percentile (below median)")
                        
                        # Dividend Analysis (higher percentile = better)
                        if data['div_percentile'] >= 75:
                            opportunity_score += 2
                            reasons.append(f"Dividend yield at {data['div_percentile']:.1f}th percentile (historically high)")
                        elif data['div_percentile'] >= 60:
                            opportunity_score += 1
                            reasons.append(f"Dividend yield at {data['div_percentile']:.1f}th percentile (above median)")
                        
                        # Trend Analysis
                        if data['pe_trend'] < 0 and data['pb_trend'] < 0:
                            opportunity_score += 1
                            reasons.append("Improving valuations (PE & PB trending down)")
                        
                        if data['div_trend'] > 0:
                            opportunity_score += 1
                            reasons.append("Dividend yield trending up")
                        
                        if opportunity_score > 0:
                            opportunities.append({
                                'index_name': index,
                                'opportunity_score': opportunity_score,
                                'current_pe': data['current_pe'],
                                'current_pb': data['current_pb'],
                                'current_div': data['current_div'],
                                'pe_percentile': data['pe_percentile'],
                                'pb_percentile': data['pb_percentile'],
                                'div_percentile': data['div_percentile'],
                                'reasons': '; '.join(reasons)
                            })
                    
                    # Sort by opportunity score
                    opportunities_df = pd.DataFrame(opportunities)
                    if not opportunities_df.empty:
                        opportunities_df = opportunities_df.sort_values('opportunity_score', ascending=False)
                        
                        # Add opportunity score to analysis_df for charting
                        analysis_df['opportunity_score'] = analysis_df['index_name'].map(
                            {row['index_name']: row['opportunity_score'] for row in opportunities}
                        ).fillna(0)
                        
                        # Display top opportunities
                        st.markdown("#### 🏆 Top Investment Opportunities")
                        
                        # Show charts if selected
                        if chart_type in ["📈 Interactive Charts", "📊 Both Tables & Charts"]:
                            chart_col1, chart_col2 = st.columns(2)
                            
                            with chart_col1:
                                st.plotly_chart(
                                    create_opportunity_score_chart(opportunities_df), 
                                    use_container_width=True,
                                    config={'displayModeBar': False}
                                )
                            
                            with chart_col2:
                                st.plotly_chart(
                                    create_percentile_scatter_chart(analysis_df), 
                                    use_container_width=True,
                                    config={'displayModeBar': False}
                                )
                        
                        # Show tables if selected
                        if chart_type in ["📊 Tables Only", "📊 Both Tables & Charts"]:
                            top_opportunities = opportunities_df.head(10)
                            
                            for idx, row in top_opportunities.iterrows():
                                with st.expander(f"**{row['index_name']}** (Score: {row['opportunity_score']})"):
                                    col1, col2, col3 = st.columns(3)
                                    
                                    with col1:
                                        st.metric("PE Ratio", f"{row['current_pe']:.2f}", 
                                                 f"{row['pe_percentile']:.1f}th percentile")
                                        st.metric("PB Ratio", f"{row['current_pb']:.2f}", 
                                                 f"{row['pb_percentile']:.1f}th percentile")
                                    
                                    with col2:
                                        st.metric("Dividend Yield", f"{row['current_div']:.2f}%", 
                                                 f"{row['div_percentile']:.1f}th percentile")
                                    
                                    with col3:
                                        st.write("**Investment Reasons:**")
                                        st.write(row['reasons'])
                    
                    # Historical Percentile Analysis
                    st.markdown("### 📊 Historical Percentile Analysis")
                    
                    percentile_col1, percentile_col2, percentile_col3 = st.columns(3)
                    
                    with percentile_col1:
                        st.markdown("**🔻 PE Ratios - Historical Lows**")
                        low_pe = analysis_df[analysis_df['pe_percentile'] <= 30].sort_values('pe_percentile')
                        if not low_pe.empty:
                            st.dataframe(
                                low_pe[['index_name', 'current_pe', 'pe_percentile']].style.background_gradient(
                                    cmap='Greens', subset=['pe_percentile']),
                                hide_index=True
                            )
                        else:
                            st.write("No indices at historical PE lows")
                    
                    with percentile_col2:
                        st.markdown("**🔻 PB Ratios - Historical Lows**")
                        low_pb = analysis_df[analysis_df['pb_percentile'] <= 30].sort_values('pb_percentile')
                        if not low_pb.empty:
                            st.dataframe(
                                low_pb[['index_name', 'current_pb', 'pb_percentile']].style.background_gradient(
                                    cmap='Greens', subset=['pb_percentile']),
                                hide_index=True
                            )
                        else:
                            st.write("No indices at historical PB lows")
                    
                    with percentile_col3:
                        st.markdown("**🔺 Dividend Yields - Historical Highs**")
                        high_div = analysis_df[analysis_df['div_percentile'] >= 70].sort_values('div_percentile', ascending=False)
                        if not high_div.empty:
                            st.dataframe(
                                high_div[['index_name', 'current_div', 'div_percentile']].style.background_gradient(
                                    cmap='Greens', subset=['div_percentile']),
                                hide_index=True
                            )
                        else:
                            st.write("No indices at historical dividend highs")
                    
                    # Trend Analysis
                    st.markdown("### 📈 Trend Analysis (Last 30 Days)")
                    
                    # Show trend chart if selected
                    if chart_type in ["📈 Interactive Charts", "📊 Both Tables & Charts"]:
                        st.plotly_chart(
                            create_trend_analysis_chart(analysis_df), 
                            use_container_width=True,
                            config={'displayModeBar': False}
                        )
                    
                    # Show trend tables if selected
                    if chart_type in ["📊 Tables Only", "📊 Both Tables & Charts"]:
                        trend_col1, trend_col2, trend_col3 = st.columns(3)
                        
                        with trend_col1:
                            st.markdown("**📉 Improving PE Trends**")
                            improving_pe = analysis_df[analysis_df['pe_trend'] < 0].sort_values('pe_trend')
                            if not improving_pe.empty:
                                st.dataframe(
                                    improving_pe[['index_name', 'current_pe', 'pe_trend']].style.background_gradient(
                                        cmap='Greens', subset=['pe_trend']),
                                    hide_index=True
                                )
                            else:
                                st.write("No indices with improving PE trends")
                        
                        with trend_col2:
                            st.markdown("**📉 Improving PB Trends**")
                            improving_pb = analysis_df[analysis_df['pb_trend'] < 0].sort_values('pb_trend')
                            if not improving_pb.empty:
                                st.dataframe(
                                    improving_pb[['index_name', 'current_pb', 'pb_trend']].style.background_gradient(
                                        cmap='Greens', subset=['pb_trend']),
                                    hide_index=True
                                )
                            else:
                                st.write("No indices with improving PB trends")
                        
                        with trend_col3:
                            st.markdown("**📈 Improving Dividend Trends**")
                            improving_div = analysis_df[analysis_df['div_trend'] > 0].sort_values('div_trend', ascending=False)
                            if not improving_div.empty:
                                st.dataframe(
                                    improving_div[['index_name', 'current_div', 'div_trend']].style.background_gradient(
                                        cmap='Greens', subset=['div_trend']),
                                    hide_index=True
                                )
                            else:
                                st.write("No indices with improving dividend trends")
                    
                    # Risk Assessment
                    st.markdown("### ⚠️ Risk Assessment")
                    
                    risk_col1, risk_col2 = st.columns(2)
                    
                    with risk_col1:
                        st.markdown("**🔴 High PE Risk (Above 80th Percentile)**")
                        high_pe_risk = analysis_df[analysis_df['pe_percentile'] >= 80].sort_values('pe_percentile', ascending=False)
                        if not high_pe_risk.empty:
                            st.dataframe(
                                high_pe_risk[['index_name', 'current_pe', 'pe_percentile']].style.background_gradient(
                                    cmap='Reds', subset=['pe_percentile']),
                                hide_index=True
                            )
                        else:
                            st.write("No indices at high PE risk levels")
                    
                    with risk_col2:
                        st.markdown("**🔴 High PB Risk (Above 80th Percentile)**")
                        high_pb_risk = analysis_df[analysis_df['pb_percentile'] >= 80].sort_values('pb_percentile', ascending=False)
                        if not high_pb_risk.empty:
                            st.dataframe(
                                high_pb_risk[['index_name', 'current_pb', 'pb_percentile']].style.background_gradient(
                                    cmap='Reds', subset=['pb_percentile']),
                                hide_index=True
                            )
                        else:
                            st.write("No indices at high PB risk levels")
                    
                    # Individual Index Historical Analysis
                    st.markdown("### 📈 Individual Index Historical Analysis")
                    
                    # Index selector for detailed historical analysis
                    selected_index = st.selectbox(
                        "Select Index for Detailed Historical Analysis",
                        options=sorted(analysis_df['index_name'].tolist()),
                        index=0
                    )
                    
                    if selected_index and selected_index in historical_analysis:
                        index_data = historical_analysis[selected_index]
                        historical_data = index_data['historical_data']
                        
                        if not historical_data.empty:
                            # Show historical time series chart
                            if chart_type in ["📈 Interactive Charts", "📊 Both Tables & Charts"]:
                                st.plotly_chart(
                                    create_historical_time_series(selected_index, historical_data),
                                    use_container_width=True,
                                    config={'displayModeBar': False}
                                )
                            
                            # Show historical statistics
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                st.metric("Data Points", index_data['data_points'])
                                st.metric("PE Range", f"{index_data['pe_min']:.2f} - {index_data['pe_max']:.2f}")
                            
                            with col2:
                                st.metric("Current PE", f"{index_data['current_pe']:.2f}")
                                st.metric("PE Percentile", f"{index_data['pe_percentile']:.1f}%")
                            
                            with col3:
                                st.metric("Current PB", f"{index_data['current_pb']:.2f}")
                                st.metric("PB Percentile", f"{index_data['pb_percentile']:.1f}%")
                            
                            with col4:
                                st.metric("Current Dividend", f"{index_data['current_div']:.2f}%")
                                st.metric("Dividend Percentile", f"{index_data['div_percentile']:.1f}%")
                            
                            # Show historical data table
                            if chart_type in ["📊 Tables Only", "📊 Both Tables & Charts"]:
                                st.markdown("**Historical Data Table**")
                                st.dataframe(
                                    historical_data[['timestamp', 'pe', 'pb', 'div_yield']].tail(20).style.background_gradient(
                                        cmap='RdYlGn', subset=['pe', 'pb', 'div_yield']),
                                    hide_index=True
                                )
                        else:
                            st.warning(f"No historical data available for {selected_index}")
                    
                    # Backtesting Section
                    st.markdown("### 🔍 Investment Signal Backtesting")
                    
                    with st.expander("📊 Historical Percentile Analysis & Signal Validation", expanded=False):
                        st.markdown(f"**Backtesting Investment Signals for {selected_index}**")
                        
                        if selected_index and selected_index in historical_analysis:
                            index_data = historical_analysis[selected_index]
                            historical_data = index_data['historical_data']
                            
                            if not historical_data.empty and len(historical_data) > 30:
                                # Calculate historical percentiles for each data point
                                backtest_data = historical_data.copy()
                                
                                # Calculate rolling percentiles (using 30-day window for stability)
                                window_size = min(30, len(historical_data) // 3)
                                
                                backtest_data['pe_percentile'] = backtest_data['pe'].rolling(window=window_size, min_periods=10).rank(pct=True) * 100
                                backtest_data['pb_percentile'] = backtest_data['pb'].rolling(window=window_size, min_periods=10).rank(pct=True) * 100
                                backtest_data['div_percentile'] = backtest_data['div_yield'].rolling(window=window_size, min_periods=10).rank(pct=True) * 100
                                
                                # Generate investment signals based on percentiles
                                def generate_signal(row):
                                    if pd.isna(row['pe_percentile']) or pd.isna(row['pb_percentile']) or pd.isna(row['div_percentile']):
                                        return "No Data"
                                    
                                    score = 0
                                    reasons = []
                                    
                                    # PE Analysis
                                    if row['pe_percentile'] <= 25:
                                        score += 3
                                        reasons.append("PE≤25%")
                                    elif row['pe_percentile'] <= 40:
                                        score += 1
                                        reasons.append("PE≤40%")
                                    
                                    # PB Analysis
                                    if row['pb_percentile'] <= 25:
                                        score += 2
                                        reasons.append("PB≤25%")
                                    elif row['pb_percentile'] <= 40:
                                        score += 1
                                        reasons.append("PB≤40%")
                                    
                                    # Dividend Analysis
                                    if row['div_percentile'] >= 75:
                                        score += 2
                                        reasons.append("Div≥75%")
                                    elif row['div_percentile'] >= 60:
                                        score += 1
                                        reasons.append("Div≥60%")
                                    
                                    # Signal classification
                                    if score >= 5:
                                        return f"🟢 Strong Buy ({score}) - {', '.join(reasons)}"
                                    elif score >= 3:
                                        return f"🟡 Buy ({score}) - {', '.join(reasons)}"
                                    elif score >= 1:
                                        return f"🟠 Weak Buy ({score}) - {', '.join(reasons)}"
                                    else:
                                        return f"🔴 Hold ({score})"
                                
                                backtest_data['signal'] = backtest_data.apply(generate_signal, axis=1)
                                
                                # Display backtesting results
                                st.markdown("**Historical Signal Analysis (Last 50 Data Points)**")
                                
                                # Show recent signals
                                recent_signals = backtest_data[['timestamp', 'pe', 'pb', 'div_yield', 
                                                              'pe_percentile', 'pb_percentile', 'div_percentile', 'signal']].tail(50)
                                
                                # Format the display
                                recent_signals_display = recent_signals.copy()
                                recent_signals_display['pe_percentile'] = recent_signals_display['pe_percentile'].round(1)
                                recent_signals_display['pb_percentile'] = recent_signals_display['pb_percentile'].round(1)
                                recent_signals_display['div_percentile'] = recent_signals_display['div_percentile'].round(1)
                                
                                st.dataframe(
                                    recent_signals_display.style.background_gradient(
                                        cmap='RdYlGn', 
                                        subset=['pe_percentile', 'pb_percentile', 'div_percentile']
                                    ),
                                    hide_index=True,
                                    width='stretch'
                                )
                                
                                # Signal statistics
                                st.markdown("**Signal Statistics**")
                                
                                col1, col2, col3, col4 = st.columns(4)
                                
                                with col1:
                                    strong_buy = len(backtest_data[backtest_data['signal'].str.contains('Strong Buy', na=False)])
                                    st.metric("Strong Buy Signals", strong_buy)
                                
                                with col2:
                                    buy_signals = len(backtest_data[backtest_data['signal'].str.contains('Buy', na=False)])
                                    st.metric("Buy Signals", buy_signals)
                                
                                with col3:
                                    hold_signals = len(backtest_data[backtest_data['signal'].str.contains('Hold', na=False)])
                                    st.metric("Hold Signals", hold_signals)
                                
                                with col4:
                                    total_signals = len(backtest_data[backtest_data['signal'] != "No Data"])
                                    st.metric("Total Signals", total_signals)
                                
                                # Performance analysis
                                st.markdown("**Signal Performance Analysis**")
                                
                                # Calculate forward returns for signal validation
                                if len(backtest_data) > 10:
                                    backtest_data['pe_future_1m'] = backtest_data['pe'].shift(-5)  # 1 month forward
                                    backtest_data['pe_future_3m'] = backtest_data['pe'].shift(-15)  # 3 months forward
                                    
                                    # Calculate performance by signal type
                                    performance_data = []
                                    
                                    for signal_type in ['Strong Buy', 'Buy', 'Weak Buy', 'Hold']:
                                        signal_data = backtest_data[backtest_data['signal'].str.contains(signal_type, na=False)]
                                        
                                        if len(signal_data) > 0:
                                            # Calculate average forward PE change
                                            avg_pe_change_1m = signal_data['pe_future_1m'].sub(signal_data['pe']).mean()
                                            avg_pe_change_3m = signal_data['pe_future_3m'].sub(signal_data['pe']).mean()
                                            
                                            performance_data.append({
                                                'Signal Type': signal_type,
                                                'Count': len(signal_data),
                                                'Avg PE Change (1M)': f"{avg_pe_change_1m:.2f}" if not pd.isna(avg_pe_change_1m) else "N/A",
                                                'Avg PE Change (3M)': f"{avg_pe_change_3m:.2f}" if not pd.isna(avg_pe_change_3m) else "N/A"
                                            })
                                    
                                    if performance_data:
                                        performance_df = pd.DataFrame(performance_data)
                                        st.dataframe(performance_df, hide_index=True)
                                
                                # Download option
                                csv_data = recent_signals.to_csv(index=False)
                                st.download_button(
                                    label="📥 Download Historical Signals Data",
                                    data=csv_data,
                                    file_name=f"{selected_index}_historical_signals.csv",
                                    mime="text/csv"
                                )
                                
                            else:
                                st.warning(f"Insufficient historical data for backtesting {selected_index}. Need at least 30 data points.")
                        else:
                            st.warning("Please select an index to view backtesting analysis.")
                    
                    # Complete Analysis Table
                    st.markdown("### 📋 Complete Historical Analysis")
                    
                    # Add opportunity score to the main dataframe
                    analysis_df['opportunity_score'] = analysis_df['index_name'].map(
                        {row['index_name']: row['opportunity_score'] for row in opportunities}
                    ).fillna(0)
                    
                    # Show complete analysis table
                    if chart_type in ["📊 Tables Only", "📊 Both Tables & Charts"]:
                        st.dataframe(
                            analysis_df[['index_name', 'current_pe', 'current_pb', 'current_div', 
                                       'pe_percentile', 'pb_percentile', 'div_percentile', 
                                       'pe_trend', 'pb_trend', 'div_trend', 'opportunity_score']].style.background_gradient(
                                cmap='RdYlGn', subset=['pe_percentile', 'pb_percentile', 'div_percentile', 'opportunity_score']),
                            hide_index=True
                        )
                    
                    # Summary Statistics
                    st.markdown("### 📊 Summary Statistics")
                    
                    summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
                    
                    with summary_col1:
                        st.metric("Total Indices Analyzed", len(analysis_df))
                    
                    with summary_col2:
                        high_opportunity = len(analysis_df[analysis_df['opportunity_score'] >= 5])
                        st.metric("High Opportunity Indices", high_opportunity)
                    
                    with summary_col3:
                        low_pe_count = len(analysis_df[analysis_df['pe_percentile'] <= 25])
                        st.metric("Low PE Indices (≤25th %ile)", low_pe_count)
                    
                    with summary_col4:
                        high_div_count = len(analysis_df[analysis_df['div_percentile'] >= 75])
                        st.metric("High Dividend Indices (≥75th %ile)", high_div_count)
                
                else:
                    st.warning("No historical analysis data available. Please ensure PE/PB/Dividend data has been loaded for sufficient time period.")
                    
            except Exception as e:
                st.error(f"Error in historical analysis: {str(e)}")
                st.info("This feature requires historical PE/PB/Dividend data to be loaded. Please check if the data loading process has completed successfully.")
                
                # Show debugging information
                with st.expander("Debug Information"):
                    st.write("**Error Details:**")
                    st.code(str(e))
                    st.write("**Troubleshooting Steps:**")
                    st.markdown("""
                    1. Ensure PE/PB/Dividend data has been loaded for indices
                    2. Check if the data tables exist in the database
                    3. Verify that sufficient historical data is available
                    4. Try running the data loading process again
                    """)
        
        # Trading Recommendations
        st.markdown("### 💡 Trading Recommendations")

        try:
            latest_date = df['timestamp'].max()
            latest_df = df[df['timestamp'] == latest_date].copy()

            if latest_df.empty:
                st.info("No EOD summary available for trading recommendations.")
            else:
                latest_df['Volume_vs_Avg20'] = np.where(
                    latest_df['Avg_Vol_20D'] > 0,
                    latest_df['Volume'] / latest_df['Avg_Vol_20D'],
                    np.nan
                )
                latest_df['Volume_vs_Avg20'] = latest_df['Volume_vs_Avg20'].replace([np.inf, -np.inf], np.nan).fillna(0)

                latest_df['ema_stack'] = (
                    (latest_df['Close'] > latest_df['EMA_20']) &
                    (latest_df['EMA_20'] > latest_df['EMA_60']) &
                    (latest_df['EMA_60'] > latest_df['EMA_200'])
                )

                short_term = latest_df[
                    (latest_df['Pct_Chg_D'] >= 2)
                    & (latest_df['RSI_14'].between(55, 70))
                    & (latest_df['Volume_vs_Avg20'] >= 1.2)
                    & (latest_df['Close'] > latest_df['EMA_20'])
                ].copy()
                short_term['Insight'] = short_term.apply(
                    lambda row: f"{row['Pct_Chg_D']:.1f}% today | RSI {row['RSI_14']:.1f} | Vol x{row['Volume_vs_Avg20']:.1f}",
                    axis=1
                )
                short_term = short_term.sort_values(['Pct_Chg_D', 'Volume_vs_Avg20'], ascending=False).head(5)

                medium_term = latest_df[
                    latest_df['ema_stack']
                    & (latest_df['Pct_Chg_20D'] >= 5)
                    & (latest_df['RSI_14'].between(50, 70))
                ].copy()
                medium_term['Insight'] = medium_term.apply(
                    lambda row: f"{row['Pct_Chg_20D']:.1f}% in 20D | RSI {row['RSI_14']:.1f}",
                    axis=1
                )
                medium_term = medium_term.sort_values(['Pct_Chg_20D', 'RSI_14'], ascending=False).head(5)

                overbought = latest_df[
                    (latest_df['RSI_14'] >= 75)
                    | ((latest_df['Pct_Chg_D'] >= 5) & (latest_df['RSI_14'] >= 70))
                ].copy()
                overbought['Alert'] = overbought.apply(
                    lambda row: f"Overbought: RSI {row['RSI_14']:.1f}, {row['Pct_Chg_D']:.1f}% today",
                    axis=1
                )

                breakdown = latest_df[
                    (latest_df['Close'] < latest_df['EMA_20'])
                    & (latest_df['Pct_Chg_D'] <= -2)
                ].copy()
                breakdown['Alert'] = breakdown.apply(
                    lambda row: f"Breakdown: {row['Pct_Chg_D']:.1f}% drop, below EMA20",
                    axis=1
                )

                risk_alerts = pd.concat(
                    [
                        overbought[['Symbol', 'Close', 'Pct_Chg_D', 'RSI_14', 'Alert']],
                        breakdown[['Symbol', 'Close', 'Pct_Chg_D', 'RSI_14', 'Alert']]
                    ],
                    ignore_index=True
                )
                if not risk_alerts.empty:
                    risk_alerts = risk_alerts.drop_duplicates(subset='Symbol').sort_values(
                        ['RSI_14', 'Pct_Chg_D'],
                        ascending=[False, True]
                    ).head(5)

                st.caption(f"Based on EOD data as of {latest_date:%d %b %Y}")

                rec_col1, rec_col2, rec_col3 = st.columns(3)

                with rec_col1:
                    st.markdown("**⚡ Short-term Momentum**")
                    if short_term.empty:
                        st.write("No qualifying setups today.")
                    else:
                        short_term_display = short_term[
                            ['Symbol', 'Close', 'Pct_Chg_D', 'RSI_14', 'Volume_vs_Avg20', 'Insight']
                        ].copy()
                        short_term_display.rename(
                            columns={
                                'Pct_Chg_D': 'Pct Chg %',
                                'RSI_14': 'RSI',
                                'Volume_vs_Avg20': 'Vol x20D'
                            },
                            inplace=True
                        )
                        st.dataframe(
                            short_term_display.style.format(
                                {
                                    'Close': '{:.2f}',
                                    'Pct Chg %': '{:.2f}',
                                    'RSI': '{:.1f}',
                                    'Vol x20D': '{:.1f}'
                                }
                            ),
                            hide_index=True
                        )

                with rec_col2:
                    st.markdown("**📈 Medium-term Trend Watch**")
                    if medium_term.empty:
                        st.write("No medium-term setups detected.")
                    else:
                        medium_term_display = medium_term[
                            ['Symbol', 'Close', 'Pct_Chg_20D', 'RSI_14', 'Insight']
                        ].copy()
                        medium_term_display.rename(
                            columns={
                                'Pct_Chg_20D': '20D %',
                                'RSI_14': 'RSI'
                            },
                            inplace=True
                        )
                        st.dataframe(
                            medium_term_display.style.format(
                                {
                                    'Close': '{:.2f}',
                                    '20D %': '{:.2f}',
                                    'RSI': '{:.1f}'
                                }
                            ),
                            hide_index=True
                        )

                with rec_col3:
                    st.markdown("**⚠️ Risk Management Alerts**")
                    if risk_alerts.empty:
                        st.write("No immediate risk alerts.")
                    else:
                        risk_display = risk_alerts[['Symbol', 'Close', 'Pct_Chg_D', 'RSI_14', 'Alert']].copy()
                        risk_display.rename(
                            columns={
                                'Pct_Chg_D': 'Pct Chg %',
                                'RSI_14': 'RSI'
                            },
                            inplace=True
                        )
                        st.dataframe(
                            risk_display.style.format(
                                {
                                    'Close': '{:.2f}',
                                    'Pct Chg %': '{:.2f}',
                                    'RSI': '{:.1f}'
                                }
                            ),
                            hide_index=True
                        )

        except Exception as e:
            st.error(f"Unable to build trading recommendations: {str(e)}")
            st.info("Please confirm EOD_Summary data is available and contains the required columns.")
        
        # Footer
        st.markdown("---")
        st.markdown("*Dashboard updated in real-time. Always verify data before making trading decisions.*")


if __name__ == "__main__":
    market_snapshot()
