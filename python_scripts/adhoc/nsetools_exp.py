import nsepy
import nsetools
from nsetools import Nse
nse = Nse()

stock = nse.get_active_monthly()
# stock = nse.get_advances_declines()
#stock = nse.get_index_list()
# stock = nse.get_active_monthly()
# stock = nse.get_bhavcopy_filename(d='2021-01-08')
# stock = nse.get_bhavcopy_url(d='2021-01-08')
# stock = nse.get_top_gainers()
# stock = nse.get_top_losers()
# stock = nse.get_top_fno_gainers()
# stock = nse.get_top_fno_losers()
# stock = nse.get_year_high()
# stock = nse.get_year_low()
# stock = nse.nse_opener()
print(stock)
import nsepy

stock_quote = nsepy.history.equity_symbol_list_url.url

print(stock_quote)

javascript:;javascript:;
