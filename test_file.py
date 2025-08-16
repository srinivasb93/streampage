# import datetime
# from openchart import NSEData
# nse = NSEData()
#
# end_date = datetime.datetime.now()
# start_date = end_date - datetime.timedelta(days=3650)
#
# nse.download()
# ref_data = nse.nse_data
# data = nse.historical(
#     symbol='Nifty 50',
#     exchange='NSE',
#     start=start_date,
#     end=end_date,
#     interval='1d'
# )
#
# print(data.head())

import requests

url = 'https://api.upstox.com/v3/login/auth/token/request/4501ed1e-be0e-43b3-abf0-25c7c910f28d'
headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}

data = {
    'client_secret': 'qocrrnpqjs'
}

response = requests.post(url, headers=headers, json=data)

print(response.status_code)
print(response.json())