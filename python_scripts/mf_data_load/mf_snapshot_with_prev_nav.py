import datetime as dt
import pandas as pd
from common_utils import read_write_sql_data as rd
from python_scripts.stocks_data_load.utilities import fetch_returns_data as fd


my_funds_table_names = fd.fetch_fund_table_names(fd.fetch_my_funds_codes())


def load_or_get_latest_prev_nav_snapshot(fetch_type="load_and_fetch", reload=False, for_date=dt.date.today()):
    """
    Load or Get the MF NAV Snapshot for the latest date
    :return:
    """
    control_data = rd.get_table_data(selected_database="ANALYTICS", selected_table="ANALYTICS_LOAD_CONTROL")
    snap_last_updated = pd.to_datetime(control_data['MFSNAP_UPDATED_ON'].iloc[0]).date()
    if not reload:
        if snap_last_updated == dt.date.today():
            msg = "Latest NAV Snapshot is already loaded for today's date : {}".format(snap_last_updated)
            print(msg)
            snapshot_df = rd.get_table_data(selected_database="ANALYTICS", selected_table="LATEST_PREV_NAV_SNAPSHOT")
            return snapshot_df, msg
    snap_df = pd.DataFrame()
    for fund_code, table_name in my_funds_table_names.items():
        try:
            """
            if for_date != dt.date.today():
                prev_date = for_date - dt.timedelta(days=2)
                query = f"Select top 2* from MFDATA.dbo.{table_name} where date" \
                        f" between '{prev_date}' and '{for_date}' order by date desc"
            else:
                query = f"Select top 2* from MFDATA.dbo.{table_name} order by date desc"
            """
            query = f"Select top 2* from MFDATA.dbo.{table_name} order by date desc"
            fund_df = rd.get_table_data(selected_database="MFDATA",
                                        selected_table=table_name,
                                        query=query)
            prev_nav = fund_df['nav'].iloc[1]
            fund_df['prev_nav'] = prev_nav
        except Exception as e:
            print(e)
            print("Data not present for the Fund : {}".format(table_name))
            continue
        fund_df['scheme_code'] = fund_code
        fund_df['Scheme_Name'] = table_name
        snap_df = snap_df._append(fund_df.iloc[0], ignore_index=True)

    if "load" in fetch_type:
        try:
            snap_df['scheme_code'] = snap_df['scheme_code'].astype(int).astype(str)
            snap_df.rename(columns={"date": "Date",
                                    "nav": "Latest_NAV",
                                    "prev_nav": "Prev_NAV",
                                    "dayChange": "Daily_Change",
                                    "nav_chg_daily": "Daily_Pct_Change"},
                               inplace=True)
            snap_df["Latest_NAV"] = snap_df["Latest_NAV"].astype(float)
            snap_df["Prev_NAV"] = snap_df["Prev_NAV"].astype(float)
            snap_df["Date"] = pd.to_datetime(snap_df["Date"]).dt.date
            mf_table = "LATEST_PREV_NAV_SNAPSHOT" if for_date == dt.date.today() else "NAV_SNAPSHOT_ADHOC"
            msg = rd.load_sql_data(snap_df, mf_table, database="ANALYTICS")

            if for_date == dt.date.today():
                snapshot_load_state = 'SUCCESS' if "success" in msg else 'FAILED'
                control_data['MF_SNAP_LOAD'] = snapshot_load_state
                control_data['MF_SNAP_DATE'] = pd.to_datetime(snap_df['Date'].max()).date()
                control_data['MFSNAP_UPDATED_ON'] = dt.datetime.now()
                control_load_msg = rd.load_sql_data(control_data, "ANALYTICS_LOAD_CONTROL", database="ANALYTICS")
                print(control_load_msg)
        except Exception as e:
            msg = "Snapshot Data load failed due to {}".format(e)

    print(msg)

    return snap_df, msg


if __name__ == '__main__':
    print(load_or_get_latest_prev_nav_snapshot(fetch_type="load_and_fetch", reload=False, for_date=dt.date(2024, 7, 15)))

