import os

import streamlit as st
import time
import json
from websocket import create_connection
from dotenv import load_dotenv

load_dotenv()
access_token = os.getenv('UPSTOX_ACCESS_TOKEN')

# WebSocket URL for Upstox market data feed
ws_url = "wss://api.upstox.com/v2/feed/market-data-feed"


# Function to fetch live market data
def fetch_live_market_data(access_token, instrument_token):
    # Create WebSocket connection
    ws = create_connection(ws_url)

    # Authenticate the WebSocket connection using the access token
    auth_message = {
        "action": "auth",
        "params": {
            "apiKey": "your_api_key",  # Replace with your API key
            "accessToken": access_token
        }
    }
    ws.send(json.dumps(auth_message))

    # Wait for authentication response
    auth_response = ws.recv()
    st.write(f"Authentication Response: {auth_response}")

    # Subscribe to the instrument_token
    subscribe_message = {
        "action": "subscribe",
        "params": {
            "mode": "full",  # Use "ltp" for last traded price only
            "instrumentKeys": [instrument_token]
        }
    }
    ws.send(json.dumps(subscribe_message))

    # Continuously receive and yield market data
    while True:
        data = ws.recv()
        yield data


# Streamlit App
st.title("Live Market Data Viewer")

# Input for access token and instrument_token
instrument_token = st.text_input("Enter the instrument token (e.g., NSE_EQ|INE002A01018):")

if access_token and instrument_token:
    st.write(f"Fetching live market data for instrument token {instrument_token}...")

    # Display live data
    live_data_placeholder = st.empty()

    for data in fetch_live_market_data(access_token, instrument_token):
        live_data_placeholder.write(data)
        time.sleep(1)  # Adjust the delay as needed