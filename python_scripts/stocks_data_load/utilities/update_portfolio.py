from common_utils import read_write_sql_data as rd
import datetime as dt
import pandas as pd
from python_scripts.stocks_data_load import bhav_copy_extract as bhav
from python_scripts.mf_data_load import mf_snapshot_with_prev_nav as mf_snap


def fecth_or_load_equity_holdings(fetch_type='load', for_date=dt.date.today(), bhav_reload=False):
    """
    Fetch equity holdings and bhavcopy data from SQL database or load and fetch bhavcopy data for the given date
     if it is not present in bhavcopy.
    :param fetch_type: 'load_and_fetch' or 'fetch' or 'load'
    :param for_date: date for which bhavcopy data is to be fetched
    :param bhav_reload: boolean to reload bhavcopy. change reload to True to reload bhavcopy for adhoc date
    :return: equity holdings
    """
    # Fetch equity holdings and bhavcopy data
    equity_holdings = rd.get_table_data("ANALYTICS", "EQUITY_HOLDINGS")
    bhav_copy = bhav.extract_or_load_bhav_copy(for_date=for_date, reload=bhav_reload)

    # Fetch equity holdings account wise
    equity_holdings = equity_holdings.join(bhav_copy.set_index("SYMBOL"), on="Stock_Symbol")
    equity_holdings["Buy_Value"] = round(equity_holdings["Buy_Value"], 2)
    equity_holdings["Current_Value"] = round(equity_holdings["Quantity"] * equity_holdings["Close"], 2)
    equity_holdings["Prev_Value"] = round(equity_holdings["Quantity"] * equity_holdings["Prev_Close"], 2)
    equity_holdings["Daily_Change"] = round(equity_holdings["Current_Value"] - equity_holdings["Prev_Value"], 2)
    equity_holdings["PnL"] = round(equity_holdings["Current_Value"] - equity_holdings["Buy_Value"], 2)
    equity_holdings["PnL_%"] = round((equity_holdings["PnL"] / equity_holdings["Buy_Value"]) * 100, 2)

    # Fetch consolidated portfolio data account wise
    eq_holdings_account_wise = equity_holdings.groupby("Broker_Name")[
        ["Quantity", "Buy_Value", "Current_Value", "Prev_Value", "Daily_Change", "PnL"]].sum().reset_index()
    eq_holdings_account_wise = eq_holdings_account_wise[["Broker_Name", "Buy_Value", "Current_Value",
                                                         "Prev_Value", "Daily_Change", "PnL"]]

    eq_holdings_account_wise["Date"] = equity_holdings["Date"].max()
    eq_holdings_account_wise["Buy_Value"] = round(eq_holdings_account_wise["Buy_Value"], 2)
    eq_holdings_account_wise["Current_Value"] = round(eq_holdings_account_wise["Current_Value"], 2)
    eq_holdings_account_wise["Prev_Value"] = round(eq_holdings_account_wise["Prev_Value"], 2)
    eq_holdings_account_wise["PnL_%"] = round(
        (eq_holdings_account_wise["PnL"]/eq_holdings_account_wise["Buy_Value"])*100, 2)
    eq_holdings_account_wise["Daily_Change"] = round(eq_holdings_account_wise["Daily_Change"], 2)
    eq_holdings_account_wise["Daily_Pct_Change"] = round(
        (eq_holdings_account_wise["Daily_Change"]/eq_holdings_account_wise["Prev_Value"])*100, 2)

    # Fetch consolidated portfolio data
    consolidated_eq_holdings = equity_holdings.groupby("Stock_Symbol")[
        ["Quantity", "Buy_Value", "Current_Value", "Prev_Value", "Daily_Change", "PnL"]].sum().reset_index()
    consolidated_eq_holdings = consolidated_eq_holdings[["Stock_Symbol", "Quantity", "Buy_Value"]]
    consolidated_eq_holdings["Avg_Buy_Price"] = round(
        consolidated_eq_holdings["Buy_Value"] / consolidated_eq_holdings["Quantity"], 2)

    consolidated_eq_holdings = consolidated_eq_holdings.join(bhav_copy.set_index("SYMBOL"), on="Stock_Symbol")
    consolidated_eq_holdings["Current_Value"] = round(
        consolidated_eq_holdings["Quantity"] * consolidated_eq_holdings["Close"], 2)
    consolidated_eq_holdings["Prev_Value"] = round(
        consolidated_eq_holdings["Quantity"] * consolidated_eq_holdings["Prev_Close"], 2)
    consolidated_eq_holdings["Daily_Change"] = round(
        consolidated_eq_holdings["Current_Value"] - consolidated_eq_holdings["Prev_Value"], 2)
    consolidated_eq_holdings["Daily_Pct_Change"] = round(
        (consolidated_eq_holdings["Daily_Change"] / consolidated_eq_holdings["Prev_Value"])*100, 2)
    consolidated_eq_holdings["PnL"] = round(
        consolidated_eq_holdings["Current_Value"] - consolidated_eq_holdings["Buy_Value"], 2)
    consolidated_eq_holdings["PnL_%"] = round(
        (consolidated_eq_holdings["PnL"] / consolidated_eq_holdings["Buy_Value"]) * 100, 2)

    equity_holdings = equity_holdings[["Broker_Name", "Stock_Symbol", "Quantity", "Buy_Price", "Buy_Value", "Date",
                                       "Close", "Current_Value", "Prev_Value", "PnL", "PnL_%", "Daily_Change",
                                       "Daily_Pct_Change"]]

    consolidated_eq_holdings = consolidated_eq_holdings[["Stock_Symbol", "Quantity", "Buy_Value", "Avg_Buy_Price",
                                                         "Date", "Close", "Current_Value", "Prev_Value", "PnL", "PnL_%",
                                                         "Daily_Change", "Daily_Pct_Change"]]

    eq_holdings_account_wise = eq_holdings_account_wise[["Broker_Name", "Buy_Value", "Date", "Current_Value",
                                                         "Prev_Value", "PnL", "PnL_%", "Daily_Change",
                                                         "Daily_Pct_Change"]]
    
    if fetch_type == 'fetch':
        return equity_holdings, consolidated_eq_holdings, eq_holdings_account_wise
    elif 'load' in fetch_type:
        equity_load_msg = rd.load_sql_data(data_to_load=equity_holdings,
                                            table_name="EQUITY_HOLDINGS_ACCOUNT_WISE",
                                            database='ANALYTICS')
        print(equity_load_msg)

        con_equity_load_msg = rd.load_sql_data(data_to_load=consolidated_eq_holdings,
                                                table_name="EQUITY_HOLDINGS_OVERALL",
                                                database='ANALYTICS')
        print(con_equity_load_msg)

        equity_summary_load_msg = rd.load_sql_data(data_to_load=eq_holdings_account_wise,
                                                   table_name="EQUITY_SUMMARY_ACCOUNT_WISE",
                                                   database='ANALYTICS')
        print(equity_summary_load_msg)

        if fetch_type == 'load_and_fetch':
            return equity_holdings, consolidated_eq_holdings, eq_holdings_account_wise
        return ("Equity Holdings loaded successfully",
                "Consolidated Equity Holdings loaded successfully",
                "Equity holdings account wise loaded successfully")


def fecth_or_load_mf_holdings(fetch_type='load', for_date=dt.date.today(), mf_snap_reload=False):
    """
    Fetch mutual fund holdings and bhavcopy data from SQL database or load and fetch bhavcopy data for the given date
     if it is not present in bhavcopy.
    :param fetch_type: 'load_and_fetch' or 'fetch' or 'load'
    :param for_date: date for which bhavcopy data is to be fetched
    :param mf_snap_reload: boolean to reload bhavcopy. change reload to True to reload bhavcopy for adhoc date
    :return: mutual fund holdings
    """
    # Fetch mutual fund holdings and bhavcopy data
    mf_holdings = rd.get_table_data("ANALYTICS", "MF_HOLDINGS")
    mf_holdings["Scheme_Code"] = mf_holdings["Scheme_Code"].astype(str)

    nav_snaphot, _ = mf_snap.load_or_get_latest_prev_nav_snapshot(for_date=for_date, reload=mf_snap_reload)

    mf_holdings = mf_holdings.join(nav_snaphot.set_index("scheme_code"), on="Scheme_Code")
    mf_holdings["Buy_Value"] = round(mf_holdings["Buy_Value"], 2)
    mf_holdings["Current_Value"] = round(mf_holdings["Quantity"] * mf_holdings["Latest_NAV"], 2)
    mf_holdings["Prev_Value"] = round(mf_holdings["Quantity"] * mf_holdings["Prev_NAV"], 2)
    mf_holdings["Daily_Change"] = round(mf_holdings["Current_Value"] - mf_holdings["Prev_Value"], 2)
    mf_holdings["PnL"] = round(mf_holdings["Current_Value"] - mf_holdings["Buy_Value"], 2)
    mf_holdings["PnL_%"] = round((mf_holdings["PnL"] / mf_holdings["Buy_Value"]) * 100, 2)

    # Fetch consolidated portfolio data account wise
    mf_holdings_account_wise = mf_holdings.groupby("Broker_Name")[
        ["Quantity", "Buy_Value", "Current_Value", "Prev_Value", "Daily_Change", "PnL"]].sum().reset_index()
    mf_holdings_account_wise = mf_holdings_account_wise[["Broker_Name", "Buy_Value", "Current_Value",
                                                         "Prev_Value", "Daily_Change", "PnL"]]
    mf_holdings_account_wise["Date"] = mf_holdings["Date"].max()
    mf_holdings_account_wise["Buy_Value"] = round(mf_holdings_account_wise["Buy_Value"], 2)
    mf_holdings_account_wise["Current_Value"] = round(mf_holdings_account_wise["Current_Value"], 2)
    mf_holdings_account_wise["Prev_Value"] = round(mf_holdings_account_wise["Prev_Value"], 2)
    mf_holdings_account_wise["PnL_%"] = round(
        (mf_holdings_account_wise["PnL"] / mf_holdings_account_wise["Buy_Value"]) * 100, 2)
    mf_holdings_account_wise["Daily_Change"] = round(mf_holdings_account_wise["Daily_Change"], 2)
    mf_holdings_account_wise["Daily_Pct_Change"] = round(
        (mf_holdings_account_wise["Daily_Change"] / mf_holdings_account_wise["Prev_Value"]) * 100, 2)

    # Fetch consolidated portfolio data
    consolidated_mf_holdings = mf_holdings.groupby("Scheme_Code")[
        ["Quantity", "Buy_Value", "Current_Value", "Prev_Value", "Daily_Change", "PnL"]].sum().reset_index()
    consolidated_mf_holdings = consolidated_mf_holdings[["Scheme_Code", "Quantity", "Buy_Value"]]
    consolidated_mf_holdings["Avg_Buy_Price"] = round(
        consolidated_mf_holdings["Buy_Value"] / consolidated_mf_holdings["Quantity"], 2)

    consolidated_mf_holdings = consolidated_mf_holdings.join(nav_snaphot.set_index("scheme_code"),
                                                             on="Scheme_Code")
    consolidated_mf_holdings["Current_Value"] = round(
        consolidated_mf_holdings["Quantity"] * consolidated_mf_holdings["Latest_NAV"], 2)
    consolidated_mf_holdings["Prev_Value"] = round(
        consolidated_mf_holdings["Quantity"] * consolidated_mf_holdings["Prev_NAV"], 2)
    consolidated_mf_holdings["Daily_Change"] = round(
        consolidated_mf_holdings["Current_Value"] - consolidated_mf_holdings["Prev_Value"], 2)
    consolidated_mf_holdings["Daily_Pct_Change"] = round(
        (consolidated_mf_holdings["Daily_Change"] / consolidated_mf_holdings["Prev_Value"]) * 100, 2)
    consolidated_mf_holdings["PnL"] = round(
        consolidated_mf_holdings["Current_Value"] - consolidated_mf_holdings["Buy_Value"], 2)
    consolidated_mf_holdings["PnL_%"] = round(
        (consolidated_mf_holdings["PnL"] / consolidated_mf_holdings["Buy_Value"]) * 100, 2)

    mf_holdings = mf_holdings[["Broker_Name", "Scheme_Name", "Quantity", "Buy_Price", "Buy_Value", "Date",
                               "Latest_NAV", "Current_Value", "Prev_Value", "PnL", "PnL_%", "Daily_Change",
                               "Daily_Pct_Change"]]

    consolidated_mf_holdings = consolidated_mf_holdings[["Scheme_Code", "Scheme_Name", "Quantity", "Buy_Value",
                                                         "Avg_Buy_Price", "Date", "Latest_NAV", "Current_Value",
                                                         "Prev_Value", "PnL", "PnL_%", "Daily_Change",
                                                         "Daily_Pct_Change"]]

    mf_holdings_account_wise = mf_holdings_account_wise[["Broker_Name", "Buy_Value", "Date", "Current_Value",
                                                         "Prev_Value", "PnL", "PnL_%", "Daily_Change",
                                                         "Daily_Pct_Change"]]

    if fetch_type == 'fetch':
        return mf_holdings, consolidated_mf_holdings, mf_holdings_account_wise
    elif 'load' in fetch_type:
        mf_load_msg = rd.load_sql_data(data_to_load=mf_holdings,
                                       table_name="MF_HOLDINGS_ACCOUNT_WISE",
                                       database='ANALYTICS')
        print(mf_load_msg)

        con_mf_load_msg = rd.load_sql_data(data_to_load=consolidated_mf_holdings,
                                           table_name="MF_HOLDINGS_OVERALL",
                                           database='ANALYTICS')
        print(con_mf_load_msg)

        mf_summary_load_msg = rd.load_sql_data(data_to_load=mf_holdings_account_wise,
                                               table_name="MF_SUMMARY_ACCOUNT_WISE",
                                               database='ANALYTICS')
        print(mf_summary_load_msg)

        if fetch_type == 'load_and_fetch':
            return mf_holdings, consolidated_mf_holdings, mf_holdings_account_wise
        return ("MF Holdings loaded successfully",
                "Consolidated MF Holdings loaded successfully",
                "MF Holdings account wise loaded successfully")


def update_overall_portfolio_summary(fetch_type='load_and_fetch',
                                     fetch_data=False,
                                     for_date=dt.date.today(),
                                     mf_snap_reload=False,
                                     bhavcopy_reload=False):
    """
    Update overall portfolio summary.
    :return:
    """
    try:
        eq_holdings, con_eq_holdings, eq_holdings_account_wise = fecth_or_load_equity_holdings(
            fetch_type, for_date, bhavcopy_reload)
        print("Successfully Extracted Equity Summary Data")
        mf_holdings, con_mf_holdings, mf_holdings_account_wise = fecth_or_load_mf_holdings(
            fetch_type, for_date, mf_snap_reload)
        print("Successfully Extracted MF Summary Data")
    except Exception as e:
        print(e)
        return pd.DataFrame() if fetch_data else "Overall Portfolio Summary update failed"

    # Combine equity and mf summary data into one dataframe
    eq_holdings_account_wise['Account_Type'] = 'Equity'
    mf_holdings_account_wise['Account_Type'] = 'Mutual Fund'
    overall_portfolio_summary = pd.concat([eq_holdings_account_wise, mf_holdings_account_wise], ignore_index=True)
    overall_portfolio_summary["PnL"] = round(overall_portfolio_summary["PnL"], 2)
    overall_portfolio_summary["PnL_%"] = round(
        (overall_portfolio_summary["PnL"] / overall_portfolio_summary["Buy_Value"]) * 100, 2)
    overall_portfolio_summary.rename(columns={"Broker_Name": "Account_Name"}, inplace=True)
    overall_portfolio_summary = overall_portfolio_summary[["Account_Name", "Account_Type", "Buy_Value", "Date",
                                                           "Current_Value", "Prev_Value",  "PnL", "PnL_%",
                                                           "Daily_Change", "Daily_Pct_Change"]]

    overall_summary_load_msg = rd.load_sql_data(data_to_load=overall_portfolio_summary,
                                                table_name="OVERALL_SUMMARY_ACCOUNT_WISE",
                                                database='ANALYTICS')
    print(overall_summary_load_msg)
    return overall_portfolio_summary if fetch_data else "Overall Portfolio Summary updated successfully"


if __name__ == '__main__':
    # print(fecth_or_load_equity_holdings('load_and_fetch'))
    print(fecth_or_load_mf_holdings('load_and_fetch'), for_date=dt.date(2024, 7, 15))
    # print(update_overall_portfolio_summary('load',
    #                                        fetch_data=False,
    #                                        for_date=dt.date(2024, 7, 12),
    #                                        mf_snap_reload=True,
    #                                        bhavcopy_reload=False))
