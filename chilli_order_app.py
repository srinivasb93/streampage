import streamlit as st
import sqlite3
from datetime import datetime
import os
import pandas as pd
import io

st.set_page_config(layout='wide')

# Initialize SQLite database
def init_db():
    with sqlite3.connect('orders.db') as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS orders
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      product_name TEXT,
                      quantity REAL,
                      society_name TEXT,
                      flat_no TEXT,
                      mobile_no TEXT,
                      total_price REAL,
                      order_date TIMESTAMP)''')
        conn.commit()

# Product details
PRODUCT = {
    'name': 'Dry Chili',
    'price_per_kg': 200,  # Price in INR per kg
    'price_5kg': 950,     # Discounted price for 5kg
    'price_10kg': 1800    # Discounted price for 10kg
}

# List of society names
SOCIETIES = ['Sunrise Apartments', 'Green Valley Society', 'River View Residency', 'Palm Grove']

# Calculate total price
def calculate_total(quantity):
    if quantity >= 10:
        return (quantity / 10) * PRODUCT['price_10kg']
    elif quantity >= 5:
        return (quantity / 5) * PRODUCT['price_5kg']
    else:
        return quantity * PRODUCT['price_per_kg']

# Custom CSS for styling
st.markdown("""
    <style>
    .main { background-color: #f9fafb; padding: 20px; }
    .stButton>button {
        background-color: #16a34a;
        color: white;
        border-radius: 8px;
        padding: 10px 20px;
        font-weight: bold;
    }
    .stButton>button:hover {
        background-color: #15803d;
    }
    .stTextInput>div>input, .stNumberInput>div>input, .stSelectbox>div>select {
        border: 1px solid #d1d5db;
        border-radius: 8px;
        padding: 8px;
    }
    .card {
        background-color: white;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        padding: 20px;
        margin-bottom: 20px;
    }
    .header {
        color: #1f2937;
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 20px;
    }
    .subheader {
        color: #374151;
        font-size: 1.5rem;
        font-weight: 600;
        margin-bottom: 15px;
    }
    .price-text {
        color: #dc2626;
        font-weight: bold;
        font-size: 1.2rem;
    }
    .success-message {
        background-color: #dcfce7;
        color: #166534;
        padding: 10px;
        border-radius: 8px;
        margin-bottom: 15px;
    }
    .error-message {
        background-color: #fee2e2;
        color: #991b1b;
        padding: 10px;
        border-radius: 8px;
        margin-bottom: 15px;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize database
init_db()

# Initialize session state for quantity
if 'quantity' not in st.session_state:
    st.session_state.quantity = 1

# Sidebar for navigation
page = st.sidebar.selectbox("Select Page", ["Place Order", "Admin View"])

if page == "Place Order":
    # Product details display
    st.markdown('<div class="subheader">Our Premium Dry Chili</div>', unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        col1, col2 = st.columns([1, 2])
        with col1:
            if os.path.exists('chili.jpg'):
                st.image('chili.jpg', caption='Dry Chili', width=200)
            else:
                st.markdown('<div class="error-message">Product image not found. Please add "chili.jpg" to the project folder.</div>', unsafe_allow_html=True)
        with col2:
            st.markdown(f"**{PRODUCT['name']}**")
            st.markdown(f'<div class="price-text">Price per kg: ₹{PRODUCT["price_per_kg"]}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="price-text">Price for 5kg: ₹{PRODUCT["price_5kg"]}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="price-text">Price for 10kg: ₹{PRODUCT["price_10kg"]}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # Quantity input (outside form for real-time updates)
    st.markdown('<div class="subheader">Place Your Order</div>', unsafe_allow_html=True)
    st.session_state.quantity = st.number_input(
        "Quantity (kg)",
        min_value=1,
        step=1,
        value=1
    )
    total_price = calculate_total(st.session_state.quantity)
    st.markdown(f'<div class="price-text">Total Price: ₹{total_price:.2f}</div>', unsafe_allow_html=True)

    # Order form
    with st.form(key='order_form'):
        society_name = st.selectbox("Society Name", SOCIETIES)
        flat_no = st.text_input("Flat No")
        mobile_no = st.text_input("Mobile Number (10 digits)", max_chars=10)
        submit_button = st.form_submit_button("Place Order")

        if submit_button:
            if not flat_no:
                st.markdown('<div class="error-message">Please enter Flat No.</div>', unsafe_allow_html=True)
            elif not mobile_no.isdigit() or len(mobile_no) != 10:
                st.markdown('<div class="error-message">Please enter a valid 10-digit mobile number.</div>', unsafe_allow_html=True)
            else:
                # Save order to database
                with sqlite3.connect('orders.db') as conn:
                    c = conn.cursor()
                    c.execute('''INSERT INTO orders (product_name, quantity, society_name, flat_no, mobile_no, total_price, order_date)
                                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
                              (PRODUCT['name'], st.session_state.quantity, society_name, flat_no, mobile_no, total_price, datetime.now()))
                    conn.commit()
                st.markdown('<div class="success-message">Order placed successfully!</div>', unsafe_allow_html=True)

elif page == "Admin View":
    # Password protection
    st.markdown('<div class="subheader">Admin Login</div>', unsafe_allow_html=True)
    password = st.text_input("Enter Password", type="password")
    if password != "admin123":  # Change this password for production
        st.markdown('<div class="error-message">Incorrect password. Access denied.</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="subheader">All Orders</div>', unsafe_allow_html=True)
        with sqlite3.connect('orders.db') as conn:
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute('SELECT * FROM orders ORDER BY order_date DESC')
            orders = c.fetchall()

        if orders:
            # Prepare data for display and download
            order_data = [
                {
                    "Order ID": order['id'],
                    "Product": order['product_name'],
                    "Quantity (kg)": order['quantity'],
                    "Society": order['society_name'],
                    "Flat No": order['flat_no'],
                    "Mobile": order['mobile_no'],
                    "Total Price (₹)": order['total_price'],
                    "Order Date": order['order_date']
                }
                for order in orders
            ]
            df = pd.DataFrame(order_data)
            st.dataframe(df, use_container_width=True)

            # CSV download button
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            st.download_button(
                label="Download Orders as CSV",
                data=csv_buffer.getvalue(),
                file_name="orders.csv",
                mime="text/csv"
            )
        else:
            st.markdown('<div class="error-message">No orders found.</div>', unsafe_allow_html=True)