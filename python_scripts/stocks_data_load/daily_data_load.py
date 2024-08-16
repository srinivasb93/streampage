import datetime
import pandas as pd
from jugaad_data import nse
from datetime import date
import re
from common_utils import read_write_sql_data as rd
import logging

log = logging.getLogger()
logging.basicConfig(filename=r"C:\Users\sba400\MyProject\streampage\python_scripts\logfiles\DATALOAD.log",
                    format=f"%(asctime)s : %(name)s : %(message)s",
                    level='DEBUG')


class Dataload:
    def extract_bhav_copy(self, extract_date):
        # Method to extract bhav copy for the passed date
        try:
            # Extract stock specific bhav copy into a DataFrame
            bhav_data = nse.full_bhavcopy_raw(dt=extract_date).split("\n")
            row_columns = bhav_data[0].split(", ")
            data_rows = [row.split(", ") for row in bhav_data[1:]]
            bcopy = pd.DataFrame(data_rows, columns=row_columns)
            bcopy = bcopy[bcopy['SERIES'].isin(['EQ', 'BE'])]
            rd.load_sql_data(data_to_load=bcopy, table_name='BHAVCOPY')
            # bcopy.to_sql(name='BHAVCOPY', con=self.conn, if_exists='replace', index=False)

            # Extract Index specific bhav copy into a DataFrame
            bhav_index_data = nse.bhavcopy_index_raw(dt=extract_date).split("\n")
            row_columns = bhav_index_data[0].split(",")
            data_rows = [row.split(",") for row in bhav_index_data[1:]]
            bcopy_indices = pd.DataFrame(data_rows, columns=row_columns)

            # Load entire bhav copy contents on stock specific data into SQL table
            rd.load_sql_data(data_to_load=bcopy_indices, table_name='BHAVCOPY_INDICES')
            # bcopy_indices.to_sql(name='BHAVCOPY_INDICES', con=self.conn, if_exists='replace', index=False)
            return 'Bhav Copy extraction and data load is complete'
        except Exception as e:
            log.exception('Bhav copy is not available for Date {} : Error Msg :  {}'.format(
                extract_date, e.__str__()))
            return 'File not available'
    ############################################################################################################

    def get_stocks_index_data(self, data_type='Stock'):
        """ Method to get stocks and indices list from SQL database """
        if data_type == 'Stock':
            log.info("Fetching Stock names from SQL")
            all_stocks = rd.get_table_data(selected_table='ALL_STOCKS')
            my_holdings = rd.get_table_data(selected_database='ANALYTICS', selected_table='EQUITY_HOLDINGS')
            stocks = list(set(all_stocks[all_stocks['STK_INDEX'] == "NIFTY 200"]['SYMBOL'].values.tolist() +
                                my_holdings['Stock_Symbol'].values.tolist()))
        else:
            log.info("Fetching Index/Sector names from SQL")
            indices = rd.get_table_data(selected_table='STOCK_INDICES')
            sectors = rd.get_table_data(selected_table='STOCK_SECTORS')
            all_indices = indices['name'].values.tolist() + sectors['name'].values.tolist()
        return stocks if data_type == 'Stock' else all_indices
    ############################################################################################################

    def read_bhav_data(self, data_type='Stock'):
        """ Method to read bhav data for both stocks and indices """
        log.info(f"Read bhavdata for {data_type}")
        if data_type == 'Stock':
            bhav_table = 'BHAVCOPY'
        else:
            bhav_table = 'BHAVCOPY_INDICES'

        # Extract stock/index bhavcopy data into a dataframe
        bhav_query = "SELECT * FROM NSEDATA.dbo." + bhav_table
        # data = pd.read_sql_query(bhav_query, con=self.conn, parse_dates=True)
        data = rd.get_table_data(query=bhav_query)
        # Extract data of required columns from BHAVCOPY data as in the SQL tables format
        if data_type == 'Stock':
            self.df_today = data.loc[:, ['SYMBOL', 'DATE1', 'OPEN_PRICE', 'HIGH_PRICE',
                                         'LOW_PRICE', 'CLOSE_PRICE', 'TTL_TRD_QNTY']]
            self.df_today.rename(columns={'DATE1': 'Date', 'OPEN_PRICE': 'Open', 'HIGH_PRICE': 'High',
                                          'LOW_PRICE': 'Low', 'CLOSE_PRICE': 'Close', 'TTL_TRD_QNTY': 'Volume'},
                                 inplace=True)
            self.df_today.set_index('SYMBOL', inplace=True)
        else:
            self.df_today = data.loc[:, ['Index Name', 'Index Date', 'Open Index Value', 'High Index Value',
                                         'Low Index Value', 'Closing Index Value', 'Volume']]
            self.df_today.rename(columns={'Index Date': 'Date', 'Open Index Value': 'Open', 'High Index Value': 'High',
                                          'Low Index Value': 'Low', 'Closing Index Value': 'Close', 'Volume': 'Volume'},
                                 inplace=True)
            self.df_today.set_index('Index Name', inplace=True)
            self.df_today['Date'] = pd.to_datetime(self.df_today['Date'], format='%d-%m-%Y')
    ##############################################################################################################

    def load_stock_data(self, stock):
        """ Method to load data for each stock """
        if stock == 'BAJAJAUTO':
            stock = 'BAJAJ-AUTO'
        if stock == 'MM':
            stock = 'M&M'
        if stock == 'MCDOWELL':
            stock = 'MCDOWELL-N'
        if stock == 'LTFH':
            stock = 'L&TFH'
        if stock == 'MMFIN':
            stock = 'M&MFIN'

        data_to_add = self.df_today[self.df_today.index == stock]

        log.info("Start Data Load for the stock : {}".format(stock))
        if '&' in stock or '-' in stock:
            stock = stock.replace('&', '').replace('-', '')

        # Write data read from bhav table to SQL Server table
        # data_to_add.to_sql(name=stock, con=self.conn, if_exists='append', index=False)
        rd.load_sql_data(data_to_load=data_to_add, table_name=stock, load_type='append')
        log.info("Data Load done for the stock : {}".format(stock))
    ################################################################################################################

    def load_index_data(self, stock):
        """ Method to append index data from bhav table to Index table for the required date """
        self.df_today.index = self.df_today.index.str.upper()
        # Converting index table names to be in sync with the index names as in the BHAVCOPY INDICES table
        if stock == 'NIFTY_SMLCAP_100':
            stock = 'NIFTY Smallcap 100'
        if stock == 'NIFTY_SMLCAP_250':
            stock = 'Nifty Smallcap 250'
        if stock == 'NIFTY_SMLCAP_50':
            stock = 'Nifty Smallcap 50'
        if stock == 'NIFTY_INFRA':
            stock = 'Nifty Infrastructure'
        if stock == 'NIFTY_SERV_SECTOR':
            stock = 'Nifty Services Sector'
        if stock == 'NIFTY_CONSUMPTION':
            stock = 'Nifty India Consumption'
        if stock == 'NIFTY_FIN_SERVICE':
            stock = 'Nifty Financial Services'
        # Converting index table names to be in sync with the index names as in the BHAVCOPY INDICES table
        data_to_add = self.df_today[self.df_today.index == ' '.join(stock.upper().split('_'))]
        # Converting index names in the BHAVCOPY INDICES table to be in sync with the index table names in the database
        if stock == 'NIFTY Smallcap 100':
            stock = 'NIFTY_SMLCAP_100'
        if stock == 'Nifty Smallcap 250':
            stock = 'NIFTY_SMLCAP_250'
        if stock == 'Nifty Smallcap 50':
            stock = 'NIFTY_SMLCAP_50'
        if stock == 'Nifty Infrastructure':
            stock = 'NIFTY_INFRA'
        if stock == 'Nifty Services Sector':
            stock = 'NIFTY_SERV_SECTOR'
        if stock == 'Nifty India Consumption':
            stock = 'NIFTY_CONSUMPTION'
        if stock == 'Nifty Financial Services':
            stock = 'NIFTY_FIN_SERVICE'
        log.info("Start Data Load for the Index : {}".format(stock))
        # Write data read from bhav table to SQL Server table
        # data_to_add.to_sql(name=stock, con=self.conn, if_exists='append', index=False)
        rd.load_sql_data(data_to_load=data_to_add, table_name=stock, load_type='append')
        log.info("Data Load done for the Index : {}".format(stock))
    #################################################################################################################

    @staticmethod
    def get_max_date(stock='SBIN'):
        """ Method to extract max date to identify the data to be loaded for days until current date """
        if '&' in stock or '-' in stock:
            stock = stock.replace('&', '').replace('-', '')
        if stock == 'NIFTY Smallcap 100':
            stock = 'NIFTY_SMLCAP_100'
        if stock == 'Nifty Smallcap 250':
            stock = 'NIFTY_SMLCAP_250'
        if stock == 'Nifty Smallcap 50':
            stock = 'NIFTY_SMLCAP_50'
        if stock == 'Nifty Infrastructure':
            stock = 'NIFTY_INFRA'
        if stock == 'Nifty Services Sector':
            stock = 'NIFTY_SERV_SECTOR'
        if stock == 'Nifty India Consumption':
            stock = 'NIFTY_CONSUMPTION'
        if stock == 'Nifty Financial Services':
            stock = 'NIFTY_FIN_SERVICE'
        # Extract maximum date for until which data is present in SQL for a stock
        query_max_date = "SELECT max(DATE) FROM dbo." + stock
        start_date = rd.get_table_data(query=query_max_date)
        start_dt = start_date.loc[0][0]
        start_dt = pd.to_datetime(start_dt).date()
        return start_dt


def equity_daily_data_load(for_date=datetime.date.today(), adhoc_date=False):
    log.info("Initiate Daily Data load")
    # Specify stock or index type
    stock_index = ['Stock', 'Index']
    # stock_index = ['Stock']

    # Create dataload object to start with the data load
    dataload = Dataload()
    # cm_holidays = nse.NSELive().holiday_list()['CM']
    # nse_holidays = [pd.to_datetime(rec['tradingDate']).date() for rec in cm_holidays]
    nse_holidays = []
    # Capture stocks/indices for which data load is not complete
    data_load_failed_stocks = []
    data_load_failed_indices = []

    if not adhoc_date:
        # Get max date of a base stock like 'SBIN'
        # max_date = dataload.get_max_date('DHANBANK')
        max_date = dataload.get_max_date()
        # Increment max date to identify the start date for the data load
        extract_start_date = max_date + pd.to_timedelta('1 day')
        # Create a date range series for all the dates pending data load
        extract_date_range = pd.Series(pd.date_range(start=extract_start_date, end=date.today(), freq='D'))
    else:
        extract_date_range = [for_date]

    log.info(f"Data to be extracted for the range {extract_date_range}")
    # Loop through the date range to call methods for the data load
    for extract_date in extract_date_range:
        log.info(f"Extracting data for the date:  {extract_date}")
        extract_date = pd.to_datetime(extract_date).date()
        # Skip load if the date is falling on a weekend
        day_is = extract_date.strftime('%A')
        if day_is in ['Saturday', 'Sunday']:
            log.info('{} - {} is a weekend Holiday'.format(extract_date, day_is))
            continue
        elif extract_date in nse_holidays:
            log.info(f"It's a holiday on {extract_date}. No Data found")
            continue
        elif datetime.datetime.now().hour < 19 and extract_date == datetime.date.today():
            msg = f"Latest data is already loaded. Try data load after 19:00 for {extract_date} data"
            log.info(msg)
            return msg
        # Call method to extract bhav copy for stocks and indices
        log.info('Extracting bhav data for the Date : {}'.format(extract_date))
        bhav_copy_load = dataload.extract_bhav_copy(extract_date)
        # If bhav copy is available, proceed with the data load for each stock and index
        if re.search(r"complete", bhav_copy_load):
            log.info(f'Bhav Data extraction is complete for the Date : {extract_date}')
            for stock_type in stock_index:
                # Get list of stocks/indices
                stocks_list = dataload.get_stocks_index_data(stock_type)
                # stocks_list = ['UNIONBANK']
                # Read Bhav Data for Stocks or Indices
                dataload.read_bhav_data(stock_type)
                if stock_type == 'Stock':
                    # Call data load method for each stock
                    for stock in stocks_list:
                        try:
                            date_diff = extract_date - dataload.get_max_date(stock)
                            # Exception handling for no data in SQL for the stock
                        except:
                            log.error('No data for the stock : {}'.format(stock))
                            continue
                        if date_diff.days <= 0 and not adhoc_date:
                            log.info('skipped load for Stock : {}'.format(stock))
                            continue
                        try:
                            dataload.load_stock_data(stock)
                        except:
                            log.error('Data Load is not complete for {}'.format(stock))
                            data_load_failed_stocks.append(stock)
                else:
                    # Call data load method for each index
                    for nse_index in stocks_list:
                        date_diff = extract_date - dataload.get_max_date(nse_index)

                        if date_diff.days <= 0 and not adhoc_date:
                            log.info('skipped load for Index : {}'.format(nse_index))
                            continue
                        try:
                            dataload.load_index_data(nse_index)
                        except:
                            log.error('Data Load is not complete for {}'.format(nse_index))
                            data_load_failed_indices.append(nse_index)
            log.debug(f"Data load failed for stocks on date {extract_date} - {data_load_failed_stocks}")
            log.debug(f"Data load failed for indices on date {extract_date} - {data_load_failed_indices}")
        else:
            continue

    return 'Success' if not data_load_failed_stocks and not data_load_failed_indices else 'Failed'


if __name__ == '__main__':
    """ Call required methods in this module for data load """
    equity_daily_data_load(for_date=datetime.date(2024,8,6), adhoc_date=True)
