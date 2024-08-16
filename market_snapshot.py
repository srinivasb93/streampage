import streamlit as st
from common_utils import read_write_sql_data as rd
import pandas as pd
from python_scripts.get_market_data import market_data
from st_aggrid import AgGrid, GridOptionsBuilder
from streamlit_extras.dataframe_explorer import dataframe_explorer


md = market_data.MarketData()


@st.cache_data
def fetch_table_data(asset_type='Stocks', table_name=''):
    asset_map = {'Stocks': 'BHAVCOPY', 'Indices': 'BHAVCOPY_INDICES', 'Mutual_Fund': 'LATEST_PREV_NAV_SNAPSHOT'}
    database = 'ANALYTICS' if asset_type == 'Mutual_Fund' else 'NSEDATA'
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
        st.write(f"No data available for display")


def market_snapshot():
    st.subheader(f"Market Summary")
    asset_type = st.sidebar.selectbox("Choose Asset Type", options=["Stocks", "Indices", "Mutual_Fund"])
    radio_btn = st.sidebar.radio(label="Required Data", options=["Prev_Day", "Live"], horizontal=True)

    sectors_df = rd.get_table_data(selected_table="STOCK_SECTORS")
    indices_df = rd.get_table_data(selected_table="STOCK_INDICES")
    indices = indices_df['name'].values.tolist()
    sectors = sectors_df['name'].values.tolist()
    sectors_list = [sector.replace("_", " ") for sector in sectors]
    indices_list = [indice.replace("_", " ") for indice in indices]

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

        cols = st.columns(6, vertical_alignment="center")
        # high_volume = cols[0].checkbox("High Volume", value=True)
        # top_gainers = cols[1].checkbox("Top Gainers", value=True)
        # top_losers = cols[2].checkbox("Top Losers", value=True)
        # high_delivery = cols[3].checkbox("High Delivery", value=True)
        # my_stocks = cols[4].checkbox("My Stocks", value=True)
        # high_turnover = cols[5].checkbox("High Turnover", value=True)

        table_name = selected_type.replace(" ", "_").replace('&', 'AND') + "_REF"
        stock_snapshot = fetch_table_data("Stocks", table_name=table_name)
        stock_snapshot.rename({"Pct_Change": "Pct_Chg",
                               "Pct_Change_365d": "Pct_Chg_365d",
                               "Pct_Change_30d": "Pct_Chg_30d",
                               "Traded_Volume": "Volume"
                               }, inplace=True, axis=1)

        index_val = stock_snapshot.iloc[0]
        index_date = index_val['Last_Updated']
        index_data = stock_snapshot.iloc[1:, :]
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
            st.markdown(f"##### :rainbow[***Top Gainers - Latest***]")
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
            st.markdown(f"##### :rainbow[***High Traded Value Stocks***]")
            display_data_as_per_asset_type(
                index_data.sort_values(
                    by='Traded_Value', ascending=False)[display_cols].head(display_row_count),
                subset_cols=['Pct_Chg'])

        row2 = st.columns([1, 1, 1, 1], vertical_alignment="top", gap="small")

        with row2[0]:
            st.markdown(f"##### :rainbow[***Top Gainers - 30 Days***]")
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
            st.markdown(f"##### :rainbow[***Top Gainers - 365 Days***]")
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


        # convert_dict = {"PREV_CLOSE": float, "OPEN_PRICE": 'float32', "HIGH_PRICE": float, "LOW_PRICE": float,
        #                 "LAST_PRICE": float, "CLOSE_PRICE": float, "AVG_PRICE": float, "TURNOVER_LACS": float,
        #                 "DELIV_PER": float, "TTL_TRD_QNTY": int, "NO_OF_TRADES": int}
        #
        st.markdown("")
        display_data_as_per_asset_type(index_data, subset_cols=['Pct_Chg'])
        # new_df = dataframe_explorer(asset_snapshot, case=False)
        # st.dataframe(new_df)
        # gb = GridOptionsBuilder.from_dataframe(asset_snapshot)
        # gb.configure_side_bar()
        # AgGrid(asset_snapshot.astype('int64', errors='ignore'), gridOptions=gb.build(), theme="balham",
        #        data_return_mode="filtered_and_sorted",
        #        fit_columns_on_grid_load=True)

    if asset_type == 'Mutual_Fund':
        asset_snapshot = fetch_table_data("Mutual_Fund")
        display_data_as_per_asset_type(asset_snapshot,  subset_cols=['Daily_Pct_Change', 'nav_chg_wkly', 'nav_chg_mth'])

    if asset_type == 'Indices':
        asset_snapshot = md.get_nse_indices_data() if radio_btn == "Live" else rd.get_table_data(
            selected_table='NSE_INDICES_DATA')

        metrics_cols = st.columns([.75, .75, .75, 1, 1, 1], vertical_alignment="top")

        with metrics_cols[0]:
            st.metric("NIFTY 50",
                      value=int(asset_snapshot[asset_snapshot['indexSymbol'] == 'NIFTY 50']['last'].values[0]),
                      delta=asset_snapshot[asset_snapshot['indexSymbol'] == 'NIFTY 50']['variation'].values[0])

        with metrics_cols[1]:
            st.metric("NIFTY MIDCAP 100",
                      value=int(asset_snapshot[asset_snapshot['indexSymbol'] == 'NIFTY MIDCAP 100']['last'].values[0]),
                      delta=asset_snapshot[asset_snapshot['indexSymbol'] == 'NIFTY MIDCAP 100']['variation'].values[0])

        with metrics_cols[2]:
            st.metric("NIFTY SMLCAP 100",
                      value=int(asset_snapshot[asset_snapshot['indexSymbol'] == 'NIFTY SMLCAP 100']['last'].values[0]),
                      delta=asset_snapshot[asset_snapshot['indexSymbol'] == 'NIFTY SMLCAP 100']['variation'].values[0])

        asset_snapshot = asset_snapshot[["indexSymbol", "percentChange", 'advances', 'declines',
                                         "perChange365d", "perChange30d"]]

        sectors_data = asset_snapshot[asset_snapshot['indexSymbol'].isin(sectors_list)]
        indices_data = asset_snapshot[asset_snapshot['indexSymbol'].isin(indices_list)]

        sectors_data[['advances', 'declines']] = sectors_data[['advances', 'declines']].astype('int64')
        indices_data[['advances', 'declines']] = indices_data[['advances', 'declines']].astype('int64')

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

        top_sectors_30d = sectors_data.sort_values(by="perChange30d", ascending=False)[["indexSymbol", "perChange30d"]]
        top_sectors_1d = sectors_data.sort_values(by="percentChange", ascending=False)[["indexSymbol", "percentChange"]]

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


if __name__ == "__main__":
    market_snapshot()
