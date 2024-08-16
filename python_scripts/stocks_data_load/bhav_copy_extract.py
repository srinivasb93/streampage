import datetime
from jugaad_data import nse
import pandas as pd
import sys
from common_utils import read_write_sql_data as rd
import datetime as dt


def extract_or_load_bhav_copy(for_date=dt.date.today(), reload=False):
    """function to load bhavcopy
    :param reload: boolean to reload bhavcopy. change reload to True to reload bhavcopy for adhoc date
    :param for_date: date for which bhavcopy is to be fetched. pass date for adhoc date's bhavcopy
    :return: bhavcopy data
    """
    control_data = rd.get_table_data(selected_database="ANALYTICS", selected_table="ANALYTICS_LOAD_CONTROL")
    bhav_last_updated = pd.to_datetime(control_data['BHAV_UPDATED_ON'].iloc[0]).date()
    if not reload:
        if bhav_last_updated == dt.date.today():
            msg = "Latest BHAV copy is already loaded for today's date : {}".format(bhav_last_updated)
            print(msg)
            bhav_df = rd.get_table_data(selected_database="ANALYTICS", selected_table="BHAVCOPY")
            return bhav_df

    if for_date != dt.date.today():
        if for_date.strftime("%A") == ['Sunday']:
            ext_date = for_date - dt.timedelta(days=2)
        elif for_date.strftime("%A") == ['Saturday']:
            ext_date = for_date - dt.timedelta(days=1)
        else:
            ext_date = for_date
    else:
        # Get day name for today's date
        date_day = for_date.strftime("%A")
        if date_day == 'Sunday':
            ext_date = for_date - dt.timedelta(days=2)
        elif date_day == 'Saturday':
            ext_date = for_date - dt.timedelta(days=2)
        else:
            if datetime.datetime.now().hour < 19:
                ext_date = for_date - dt.timedelta(days=1)
            else:
                ext_date = for_date

    bhav_data = nse.full_bhavcopy_raw(dt=ext_date).split("\n")
    row_columns = bhav_data[0].split(", ")
    data_rows = [row.split(", ") for row in bhav_data[1:]]

    bcopy = pd.DataFrame(data_rows, columns=row_columns)
    bcopy.dropna(inplace=True)
    bcopy = bcopy[bcopy['SERIES'].isin(['EQ', 'BE'])]
    bcopy["CLOSE_PRICE"] = bcopy["CLOSE_PRICE"].astype(float)
    bcopy["PREV_CLOSE"] = bcopy["PREV_CLOSE"].astype(float)
    bcopy['PCT_CHANGE'] = round(
        (bcopy['CLOSE_PRICE'] - bcopy['PREV_CLOSE']) / bcopy['PREV_CLOSE'] * 100, 1)

    bcopy = bcopy[["SYMBOL", "DATE1", "CLOSE_PRICE", "PREV_CLOSE", "PCT_CHANGE"]]
    bcopy.rename(columns={"DATE1": "Date",
                          "CLOSE_PRICE": "Close",
                          "PREV_CLOSE": "Prev_Close",
                          "PCT_CHANGE": "Daily_Pct_Change"},
                 inplace=True)
    # Covert DATE1 column to datetime format and convert CLOSE_PRICE column to float
    bcopy["Date"] = pd.to_datetime(bcopy["Date"]).dt.date
    bcopy["Close"] = bcopy["Close"].astype(float)
    bcopy["Prev_Close"] = bcopy["Prev_Close"].astype(float)

    bhav_table = "BHAVCOPY_ADHOC" if for_date != dt.date.today() else "BHAVCOPY"

    try:
        msg = rd.load_sql_data(bcopy, bhav_table, database="ANALYTICS")
    except Exception as e:
        msg = "Bhavcopy data load failed due to {}".format(e)
    print(msg)

    # Extract Index specific bhav copy into a DataFrame
    try:
        bhav_index_data = nse.bhavcopy_index_raw(dt=ext_date).split("\n")
        row_columns = bhav_index_data[0].split(",")
        data_rows = [row.split(",") for row in bhav_index_data[1:]]
        bcopy_indices = pd.DataFrame(data_rows, columns=row_columns)
    except Exception as e:
        print("Bhav Index data extraction failed due to {}".format(e))

    try:
        index_load_msg = rd.load_sql_data(bcopy_indices, 'BHAVCOPY_INDICES', database="ANALYTICS")

        if for_date == dt.date.today():
            bhav_load_state = 'SUCCESS' if "success" in msg else 'FAILED'
            control_data['BHAV_LOAD'] = bhav_load_state
            control_data['BHAV_DATE'] = pd.to_datetime(bcopy['Date'].max()).date()
            control_data['BHAV_UPDATED_ON'] = dt.datetime.now()
            control_load_msg = rd.load_sql_data(control_data, "ANALYTICS_LOAD_CONTROL", database="ANALYTICS")
            print(control_load_msg)
    except Exception as e:
        index_load_msg = "Bhavcopy load of Index data failed due to {}".format(e)
    print(index_load_msg)

    return bcopy


def extract_or_load_index_bhav_copy(for_date=dt.date.today()):
    """function to load Index bhavcopy
    :param for_date: date for which bhavcopy is to be fetched. pass date for adhoc date's bhavcopy
    :return: bhavcopy data
    """
    if for_date != dt.date.today():
        if for_date.strftime("%A") == ['Sunday']:
            ext_date = for_date - dt.timedelta(days=2)
        elif for_date.strftime("%A") == ['Saturday']:
            ext_date = for_date - dt.timedelta(days=1)
        else:
            ext_date = for_date
    else:
        # Get day name for today's date
        date_day = for_date.strftime("%A")
        if date_day == 'Sunday':
            ext_date = for_date - dt.timedelta(days=2)
        elif date_day == 'Saturday':
            ext_date = for_date - dt.timedelta(days=2)
        else:
            if datetime.datetime.now().hour < 19:
                ext_date = for_date - dt.timedelta(days=1)
            else:
                ext_date = for_date

    # Extract Index specific bhav copy into a DataFrame
    try:
        bhav_index_data = nse.bhavcopy_index_raw(dt=ext_date).split("\n")
        row_columns = bhav_index_data[0].split(",")
        data_rows = [row.split(",") for row in bhav_index_data[1:]]
        bcopy_indices = pd.DataFrame(data_rows, columns=row_columns)
    except Exception as e:
        print("Bhav Index data extraction failed due to {}".format(e))

    try:
        index_load_msg = rd.load_sql_data(bcopy_indices, 'BHAVCOPY_INDICES', database="ANALYTICS")
    except Exception as e:
        index_load_msg = "Bhavcopy load of Index data failed due to {}".format(e)
    print(index_load_msg)
    return bcopy_indices


if __name__ == '__main__':
    # For extracting bhavcopy for adhoc date, pass required date + 1 as for_date argument and reload=True
    # print(extract_or_load_bhav_copy(for_date=dt.date(2023, 3, 11), reload=True))
    print(extract_or_load_index_bhav_copy(dt.date(2024, 6, 11)))
    # print(extract_or_load_bhav_copy(reload=True))





