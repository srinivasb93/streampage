import os
import pyotp
from kiteconnect import KiteConnect
from dotenv import load_dotenv
import requests
import json
load_dotenv()

# Replace with your actual API key and access token
api_key = os.getenv('ZERODHA_API_KEY')
access_token = os.getenv("ZERODHA_ACCESS_TOKEN")
ZERODHA_API_KEY = os.getenv("ZERODHA_API_KEY")
ZERODHA_API_SECRET = os.getenv("ZERODHA_API_SECRET")
ZERODHA_USERNAME = os.getenv("ZERODHA_USERNAME")
ZERODHA_PASSWORD = os.getenv("ZERODHA_PASSWORD")
ZERODHA_TOTP_TOKEN = os.getenv("ZERODHA_TOTP_TOKEN")

# Zerodha API URLs
ZERODHA_LOGIN_URL = "https://kite.zerodha.com/api/login"
ZERODHA_TWOFA_URL = "https://kite.zerodha.com/api/twofa"

# # Initialize KiteConnect
# kite = KiteConnect(api_key=api_key)
# kite.set_access_token(access_token)



# NEW: Zerodha authentication
# def fetch_zerodha_access_token():
#     try:
#         session = requests.Session()
#         response = session.post(ZERODHA_LOGIN_URL, data={'user_id': ZERODHA_USERNAME, 'password': ZERODHA_PASSWORD})
#         response.raise_for_status()
#         response_data = json.loads(response.text)
#         if response_data.get('status') != 'success':
#             raise ValueError(f"Login failed: {response_data.get('message', 'Unknown error')}")
#         request_id = response_data['data']['request_id']
#
#         twofa_pin = pyotp.TOTP(ZERODHA_TOTP_TOKEN).now()
#         response = session.post(ZERODHA_TWOFA_URL, data={
#             'user_id': ZERODHA_USERNAME,
#             'request_id': request_id,
#             'twofa_value': twofa_pin,
#             'twofa_type': 'totp'
#         })
#         response.raise_for_status()
#         response_data = json.loads(response.text)
#         if response_data.get('status') != 'success':
#             raise ValueError(f"TOTP authentication failed: {response_data.get('message', 'Unknown error')}")
#
#         kite = KiteConnect(api_key=ZERODHA_API_KEY)
#         kite_url = kite.login_url()
#
#         res_data = requests.get(kite_url)
#         request_token = res_data.url.split("&sess_id=")[1]
#         data = kite.generate_session(request_token, ZERODHA_API_SECRET)
#         access_token = data['access_token']
#         kite.set_access_token(access_token)
#         with open(".env", "r+") as f:
#             lines = f.readlines()
#             f.seek(0)
#             for line in lines:
#                 if not line.startswith("ZERODHA_ACCESS_TOKEN"):
#                     f.write(line)
#             f.write(f"ZERODHA_ACCESS_TOKEN={access_token}\n")
#         return access_token
#     except Exception as e:
#         print(f"Zerodha authentication failed: {str(e)}")
#         return None


import hashlib
import requests

def generate_checksum(api_key, request_token, api_secret):
    checksum_data = f"{api_key}{request_token}{api_secret}"
    checksum_hash = hashlib.sha256(checksum_data.encode()).hexdigest()
    return checksum_hash

def fetch_zerodha_access_token(api_key, api_secret, request_token):
    url = "https://api.kite.trade/session/token "

    checksum = generate_checksum(api_key, request_token, api_secret)

    payload = {
        "api_key": api_key,
        "request_token": request_token,
        "checksum": checksum
    }

    headers = {
        "X-Kite-Version": "3",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.post(url, data=payload, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch access token: {response.text}")

# Example usage:
if __name__ == "__main__":
    API_KEY = "your_api_key_here"
    API_SECRET = "your_api_secret_here"
    REQUEST_TOKEN = "received_request_token_from_redirect_url"

    try:
        token_data = fetch_zerodha_access_token(API_KEY, API_SECRET, REQUEST_TOKEN)
        print("Access Token Response:")
        print(token_data)
        print("\nUse this access_token in subsequent requests.")
    except Exception as e:
        print(e)