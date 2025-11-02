"""
Centralized authentication module for the Streamlit application.
This module provides authentication functions that can be imported by all pages.
Uses Streamlit's built-in session state for reliable authentication.
"""
import streamlit as st
import logging
import bcrypt

logger = logging.getLogger(__name__)


def check_credentials(email, password):
    """Check user credentials against the database."""
    try:
        from common_utils import read_write_sql_data as rd
        
        # Query users table from trading_db database
        query = f"""
            SELECT user_id, email, hashed_password 
            FROM users 
            WHERE email = '{email}'
        """
        
        result_df = rd.get_table_data(
            query=query,
            selected_database='trading_db'
        )
        
        # Check if user exists
        if result_df is None or result_df.empty:
            logger.warning(f"Authentication failed: User not found - {email}")
            return None
        
        # Get user data
        user_data = result_df.iloc[0]
        stored_hash = user_data['hashed_password']
        
        # Verify password using bcrypt
        if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
            logger.info(f"Authentication successful for user: {email}")
            return {
                'user_id': user_data['user_id'],
                'email': user_data['email'],
                'hashed_password': stored_hash
            }
        else:
            logger.warning(f"Authentication failed: Invalid password for user - {email}")
            return None
            
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        return None


def login_form():
    """Display login form."""
    # Hide sidebar navigation when not authenticated
    st.markdown(
        """
        <style>
            [data-testid="stSidebar"] {
                display: none;
            }
        </style>
        """,
        unsafe_allow_html=True
    )
    
    st.title("🔐 Login to Analytics Dashboard")
    st.markdown("Please enter your credentials to access the dashboard.")
    st.info("ℹ️ Your session will remain active for 4 hours of use.")
    
    with st.form("login_form"):
        email = st.text_input("Email", placeholder="Enter your email")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        submit_button = st.form_submit_button("Login")
        
        if submit_button:
            if email and password:
                user = check_credentials(email, password)
                if user:
                    # Set session state
                    st.session_state.authenticated = True
                    st.session_state.user_email = email
                    st.session_state.user_id = user['user_id']
                    
                    st.success("Login successful!")
                    logger.info(f"User logged in: {email}")
                    st.rerun()
                else:
                    st.error("Invalid email or password. Please try again.")
            else:
                st.error("Please enter both email and password.")


def logout():
    """Logout user and clear session state."""
    user_email = st.session_state.get('user_email', 'Unknown')
    
    # Clear session state
    st.session_state.authenticated = False
    st.session_state.user_email = None
    st.session_state.user_id = None
    
    logger.info(f"User logged out: {user_email}")
    st.rerun()


def require_authentication():
    """
    Check if user is authenticated. If not, show login form and stop execution.
    This function should be called at the beginning of each page.
    """
    # Initialize session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'user_email' not in st.session_state:
        st.session_state.user_email = None
    if 'user_id' not in st.session_state:
        st.session_state.user_id = None
    
    # Check if authenticated in session state
    if not st.session_state.authenticated:
        # Show login form
        login_form()
        st.stop()


def get_current_user():
    """Get the currently logged in user's information."""
    if st.session_state.get('authenticated', False):
        return {
            'user_id': st.session_state.get('user_id'),
            'email': st.session_state.get('user_email')
        }
    return None
