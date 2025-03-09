import streamlit as st
from upstox_client import Configuration, ApiClient, LoginApi
from upstox_client.rest import ApiException

def auth_module():
    if 'access_token' not in st.session_state:
        with st.expander("Upstox API Credentials"):
            client_id = st.text_input("Client ID")
            client_secret = st.text_input("Client Secret", type='password')
            redirect_uri = st.text_input("Redirect URI")

            if st.button("Authenticate"):
                config = Configuration()
                login_api = LoginApi(ApiClient(config))
                auth_url = login_api.authorize(
                    client_id=client_id,
                    redirect_uri=redirect_uri,
                    api_version='v2'
                )
                st.session_state.auth_url = auth_url
                st.markdown(f"[Authorize Application]({auth_url})")


def refresh_token():
    try:
        login_api = LoginApi(ApiClient(Configuration()))
        response = login_api.token(
            grant_type='refresh_token',
            refresh_token=st.session_state.refresh_token,
            client_id=st.secrets['CLIENT_ID'],
            client_secret=st.secrets['CLIENT_SECRET']
        )
        st.session_state.access_token = response.access_token
        st.session_state.refresh_token = response.refresh_token
    except ApiException as e:
        st.error(f"Token refresh failed: {e.reason}")


if __name__ == '__main__':
    auth_module()