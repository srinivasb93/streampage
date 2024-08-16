from mftool import Mftool
import pandas as pd
from common_utils import read_write_sql_data as rd

mf = Mftool()

mfunds_df = rd.get_table_data(selected_database="ANALYTICS",
                              selected_table="MF_HOLDINGS")
my_funds_codes = mfunds_df['Scheme_Code'].unique().tolist()

# Uncomment the below line to fetch NAV snapshot for adhoc codes
# my_funds_codes = (135781, 120603, 119755, 118825, 118834, 120505, 120503, 118269, 146130, 118778, 122639,
#                   125307, 118791, 109445, 113177, 112932, 112323, 135783)


def load_or_get_mf_nav_snapshot_latest(fetch_type="load_and_fetch"):
    """
    Load or Get the MF NAV Snapshot for the latest date
    :return:
    """
    df1 = pd.DataFrame()
    for code in my_funds_codes:
        try:
            mf_quote_df = pd.DataFrame([mf.get_scheme_quote(code=code)])
        except Exception as e:
            print(e)
            print("Data Load not done for the Fund : {}".format(code))
            continue
        df1 = df1.append(mf_quote_df, ignore_index=True)

    if "load" in fetch_type:
        try:
            msg = rd.load_sql_data(df1, "LATEST_NAV_SNAPSHOT", database="ANALYTICS")
        except Exception as e:
            msg = "Snapshot Data load failed due to {}".format(e)

    print(msg)
    return df1, msg


if __name__ == '__main__':
    print(load_or_get_mf_nav_snapshot_latest())

