import time
import nsetools

nse = nsetools.Nse()
print(nse._get_json_response_from_url(url="https://www.nseindia.com/api/index-names", as_json=True))
exit()
from selenium import webdriver
import pandas as pd
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select

chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--headless')
nse_link = 'https://www.nseindia.com/market-data/live-equity-market'

# browser = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
browser = webdriver.Chrome(ChromeDriverManager().install())
browser.maximize_window()
browser.get(nse_link)

time.sleep(5)
dwnload = browser.find_element_by_xpath("//a[@onclick='dnldEquityStock()']").click()

browser.quit()