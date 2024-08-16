import time
import mftool
from mftool import Mftool
import pandas as pd
from common_utils import read_write_sql_data as rd
import logging

log = logging.getLogger()
logging.basicConfig(filename=r"C:\Users\sba400\MyProject\streampage\python_scripts\logfiles\MF_HISTDATA_LOAD.log",
                    format=f"%(asctime)s : %(name)s : %(message)s",
                    level='DEBUG')


def extract_and_load_latest_mf_hist_data():
    """
    Function to extract and load latest historical data for the data in MF_HOLDINGS table
    :return:
    """
    mf = Mftool()

    mfunds_df = rd.get_table_data(selected_database="ANALYTICS",
                                  selected_table="MF_HOLDINGS")
    my_funds_codes = mfunds_df['Scheme_Code'].unique().tolist()
    query = f"SELECT * FROM MFDATA.dbo.MF_SCHEME_DETAILS WHERE SCHEME_CODE IN {tuple(my_funds_codes)}"

    df = rd.get_table_data(query=query)
    status_list = []

    for data in df.itertuples():
        code = data[4]
        log.info("Extract data for the Fund : {}".format(data[5]))
        # if str(code) != '120841':
        #     continue
        fund_house = data[1].split(' ')[0]
        fund_type = "DIRECT" if "DIRECT" in data[5].upper() else "REGULAR"
        fund_name = data[5].split('-')[0].split(' ')

        fund_name = fund_name[0] + '_' + '_'.join(fund_name[-4:-1])
        fund_name = fund_name.replace('_&_', '_') if '&' in fund_name else fund_name

        tbl_name = fund_name.upper() + f'_{str(code)}_' + fund_type
        df1 = pd.DataFrame()

        try:
            mf_hist_data = mf.get_scheme_historical_nav(code=code, as_Dataframe=True)
        except Exception as e:
            print(e)
            print("Unable to extract historical data for the Fund : {}".format(data[5]))
            continue
        mf_hist_data.reset_index(inplace=True)
        mf_hist_data['date'] = pd.to_datetime(mf_hist_data['date'], format='%d-%m-%Y')
        mf_hist_data['nav'] = mf_hist_data['nav'].astype(dtype=float)
        mf_hist_data['nav'].replace(0, method='pad', inplace=True)
        mf_hist_data.sort_values(by='date', ascending=True, inplace=True)
        mf_hist_data['nav_chg_daily'] = round(mf_hist_data['nav'].pct_change() * 100, 2)
        mf_hist_data['nav_chg_wkly'] = round(mf_hist_data['nav'].pct_change(periods=5) * 100, 2)
        mf_hist_data['nav_chg_mth'] = round(mf_hist_data['nav'].pct_change(periods=20) * 100, 2)

        try:
            # mf_hist_data.to_sql(name=tbl_name, con=conn, if_exists='replace', index=False)
            rd.load_sql_data(data_to_load=mf_hist_data, table_name=tbl_name, database='MFDATA')
            status_list.append(True)
        except Exception as e:
            status_list.append(False)
            log.exception('error is : {}'.format(e))
            log.error("Data Load not done for the Fund : {}".format(data[5]))
            continue
        log.info("Data Load done for the Fund : {}".format(data[5]))
        time.sleep(2)

    return 'Success' if all(status_list) else 'Failure'


if __name__ == '__main__':
    extract_and_load_latest_mf_hist_data()