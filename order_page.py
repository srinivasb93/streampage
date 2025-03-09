import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import upstox_client
import time
import smtplib
from email.mime.text import MIMEText


# Initialize Upstox API
def initialize_upstox(api_key, api_secret, redirect_uri, access_token=None):
    upstox = upstox_client(api_key, access_token)
    if not access_token:
        auth_url = upstox.get_login_url()
        st.write(f"Please login using this URL: [Login]({auth_url})")
        auth_code = st.text_input("Enter the authorization code:")
        if auth_code:
            upstox.set_code(auth_code)
            access_token = upstox.get_access_token()
            st.success("Successfully logged in!")
            st.session_state['access_token'] = access_token
    return upstox


# Place order function
def place_order(upstox, transaction_type, symbol, quantity, order_type, price=None, stop_loss=None, target=None):
    try:
        order = upstox.place_order(
            transaction_type=transaction_type,  # 'BUY' or 'SELL'
            symbol=symbol,
            quantity=quantity,
            order_type=order_type,  # 'MARKET', 'LIMIT', 'SL', 'SL-M'
            price=price,
            trigger_price=stop_loss,
            disclosed_quantity=0,
            squareoff=target,
            stoploss=stop_loss
        )
        st.success(f"Order placed successfully! Order ID: {order['order_id']}")
        send_notification(
            f"Order placed: {transaction_type} {quantity} {symbol} at {price if price else 'Market Price'}")
    except Exception as e:
        st.error(f"Error placing order: {e}")


# Modify or delete order function
def modify_or_delete_order(upstox, order_id, action, new_quantity=None, new_price=None):
    try:
        if action == 'MODIFY':
            upstox.modify_order(order_id, new_quantity, new_price)
            st.success(f"Order {order_id} modified successfully!")
        elif action == 'DELETE':
            upstox.cancel_order(order_id)
            st.success(f"Order {order_id} deleted successfully!")
        send_notification(f"Order {action.lower()}ed: {order_id}")
    except Exception as e:
        st.error(f"Error: {e}")


# View order book
def view_order_book(upstox):
    try:
        orders = upstox.get_order_book()
        if orders:
            st.write("### Order Book")
            order_df = pd.DataFrame(orders)
            st.dataframe(order_df)
        else:
            st.info("No orders found.")
    except Exception as e:
        st.error(f"Error fetching order book: {e}")


# View portfolio
def view_portfolio(upstox):
    try:
        portfolio = upstox.get_portfolio()
        if portfolio:
            st.write("### Portfolio")
            portfolio_df = pd.DataFrame(portfolio)
            st.dataframe(portfolio_df)
        else:
            st.info("No holdings found.")
    except Exception as e:
        st.error(f"Error fetching portfolio: {e}")


# Fetch historical data
def fetch_historical_data(upstox, symbol, interval, start_date, end_date):
    try:
        historical_data = upstox.get_historical_data(symbol, interval, start_date, end_date)
        if historical_data:
            st.write(f"### Historical Data for {symbol}")
            historical_df = pd.DataFrame(historical_data)
            st.dataframe(historical_df)
            st.line_chart(historical_df['close'])
        else:
            st.info("No historical data found.")
    except Exception as e:
        st.error(f"Error fetching historical data: {e}")


# Send email notification
def send_notification(message):
    if st.session_state.get('email_notifications', False):
        sender_email = st.session_state['sender_email']
        receiver_email = st.session_state['receiver_email']
        smtp_server = st.session_state['smtp_server']
        smtp_port = st.session_state['smtp_port']
        smtp_username = st.session_state['smtp_username']
        smtp_password = st.session_state['smtp_password']

        msg = MIMEText(message)
        msg['Subject'] = 'Trade Notification'
        msg['From'] = sender_email
        msg['To'] = receiver_email

        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.sendmail(sender_email, receiver_email, msg.as_string())
            st.success("Notification sent successfully!")
        except Exception as e:
            st.error(f"Error sending notification: {e}")


# Main Streamlit app
def main():
    st.title("Automated Trading with Upstox")

    # Sidebar for API credentials
    st.sidebar.header("Upstox API Credentials")
    api_key = st.sidebar.text_input("API Key")
    api_secret = st.sidebar.text_input("API Secret", type="password")
    redirect_uri = st.sidebar.text_input("Redirect URI")
    access_token = st.sidebar.text_input("Access Token (if available)", type="password")

    # Sidebar for email notifications
    st.sidebar.header("Email Notifications")
    email_notifications = st.sidebar.checkbox("Enable Email Notifications")
    if email_notifications:
        st.session_state['email_notifications'] = True
        st.session_state['sender_email'] = st.sidebar.text_input("Sender Email")
        st.session_state['receiver_email'] = st.sidebar.text_input("Receiver Email")
        st.session_state['smtp_server'] = st.sidebar.text_input("SMTP Server")
        st.session_state['smtp_port'] = st.sidebar.number_input("SMTP Port", value=587)
        st.session_state['smtp_username'] = st.sidebar.text_input("SMTP Username")
        st.session_state['smtp_password'] = st.sidebar.text_input("SMTP Password", type="password")

    if api_key and api_secret and redirect_uri:
        upstox = initialize_upstox(api_key, api_secret, redirect_uri, access_token)

        if 'access_token' in st.session_state:
            # Main page options
            st.header("Trade Management")
            action = st.selectbox("Choose Action",
                                  ["Place Order", "Modify/Delete Order", "View Order Book", "Schedule Order",
                                   "View Portfolio", "Historical Data"])

            if action == "Place Order":
                st.subheader("Place Order")
                with st.form("order_form"):
                    symbol = st.text_input("Symbol (e.g., SBIN-EQ)")
                    quantity = st.number_input("Quantity", min_value=1, value=1)
                    order_type = st.selectbox("Order Type", ["MARKET", "LIMIT", "SL", "SL-M"])
                    price = st.number_input("Price (for LIMIT/SL orders)", min_value=0.0, value=0.0) if order_type in [
                        "LIMIT", "SL", "SL-M"] else None
                    stop_loss = st.number_input("Stop Loss Price", min_value=0.0, value=0.0) if order_type in ["SL",
                                                                                                               "SL-M"] else None
                    target = st.number_input("Target Price", min_value=0.0, value=0.0) if order_type in ["SL",
                                                                                                         "SL-M"] else None
                    transaction_type = st.radio("Transaction Type", ["BUY", "SELL"])
                    if st.form_submit_button("Place Order"):
                        place_order(upstox, transaction_type, symbol, quantity, order_type, price, stop_loss, target)

            elif action == "Modify/Delete Order":
                st.subheader("Modify or Delete Order")
                order_id = st.text_input("Order ID")
                action_type = st.radio("Action", ["MODIFY", "DELETE"])
                new_quantity = st.number_input("New Quantity", min_value=1,
                                               value=1) if action_type == "MODIFY" else None
                new_price = st.number_input("New Price", min_value=0.0, value=0.0) if action_type == "MODIFY" else None
                if st.button("Submit"):
                    modify_or_delete_order(upstox, order_id, action_type, new_quantity, new_price)

            elif action == "View Order Book":
                st.subheader("Order Book")
                view_order_book(upstox)

            elif action == "Schedule Order":
                st.subheader("Schedule Order")
                schedule_time = st.time_input("Schedule Time")
                if st.button("Schedule"):
                    current_time = datetime.now().time()
                    delay = (datetime.combine(datetime.today(), schedule_time) - datetime.combine(datetime.today(),
                                                                                                  current_time)).total_seconds()
                    if delay > 0:
                        st.info(f"Order will be placed at {schedule_time}")
                        time.sleep(delay)
                        st.experimental_rerun()
                    else:
                        st.error("Selected time must be in the future.")

            elif action == "View Portfolio":
                st.subheader("Portfolio")
                view_portfolio(upstox)

            elif action == "Historical Data":
                st.subheader("Historical Data")
                symbol = st.text_input("Symbol (e.g., SBIN-EQ)")
                interval = st.selectbox("Interval", ["1MIN", "5MIN", "15MIN", "30MIN", "1HOUR", "1DAY"])
                start_date = st.date_input("Start Date")
                end_date = st.date_input("End Date")
                if st.button("Fetch Historical Data"):
                    fetch_historical_data(upstox, symbol, interval, start_date, end_date)

    else:
        st.warning("Please enter your Upstox API credentials in the sidebar.")


if __name__ == "__main__":
    main()