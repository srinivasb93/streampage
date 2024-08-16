"""
Author: Yash Roongta
On running this code, you can download full_bhavcopy from new NSE website into the path you define.
The user is also expected to give the correct dates and holidays, read comments in code to know more.
"""


#Making all necessary imports for the code
from datetime import date
from jugaad_data import nse, holidays
from jugaad_data.nse import bhavcopy_save
import pandas as pd
from jugaad_data.holidays import holidays
from random import randint
import time, os
print(holidays)
exit()

# date_range = pd.bdate_range(start='12/01/2020', end = '12/31/2020',
#                          freq='C', holidays = holidays(2020,12))

ext_date = pd.to_datetime('12/01/2020').date()

                         
savepath = os.path.join('C:', os.sep, 'Users\sba400\PycharmProjects\data_analysis\stocks_data_load\input')
                                                  
# start and end dates in "MM-DD-YYYY" format
# holidays() function in (year,month) format
#freq = 'C' is for custom

# dates_list = [x.date() for x in date_range]
# x = nse.bhavcopy_raw(ext_date)
# df = pd.DataFrame(data=x)
bhav_data = nse.full_bhavcopy_raw(dt=ext_date).split("\n")
row_columns = bhav_data[0].split(", ")
data_rows = [row.split(", ") for row in bhav_data[1:]]
bcopy = pd.DataFrame(data_rows, columns=row_columns)
print(bcopy)
print(bcopy.shape)
# print(nse.bhavcopy_raw(ext_date))
# print(nse.full_bhavcopy_raw(ext_date))
# print(df)
exit()

try:
     bhavcopy_save(ext_date, savepath)
except (ConnectionError, TimeoutError) as e:
     time.sleep(10)  # stop program for 10 seconds and try again.
     try:
          bhavcopy_save(ext_date, savepath)
     except (ConnectionError, TimeoutError) as e:
          print(f'{ext_date}: File not Found')
               
