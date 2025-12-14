import streamlit as st

st.set_page_config(page_title="Login / Register", page_icon="🔑", layout="centered")

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "username" not in st.session_state:
    st.session_state.username = ""

if "role" not in st.session_state:
    st.session_state.role = "user"

from app.data.db import connect_database
from app.data.schema import create_users_table
conn = connect_database()
create_users_table(conn)
conn.close()

st.title("🔐 Welcome")

if st.session_state.logged_in:
    st.success(f"Already logged in as **{st.session_state.username}**.")
    if st.button("Go to dashboard"):
        # Use the official navigation API to switch pages
            st.switch_page("pages/1_dashboard.py")
tab_login, tab_register = st.tabs(["Login", "Register"])

with tab_login:
    st.subheader("Login")

    login_username = st.text_input("Username", key="login_username")
    login_password = st.text_input("Password", type="password", key="login_password")

    if st.button("Log in", type="primary"):
        from app.services.user_service import login_user
        from app.data.users import get_user_by_username

        success, msg = login_user(login_username, login_password)
        if success:
            st.session_state.logged_in = True
            st.session_state.username = login_username
            user = get_user_by_username(login_username)
            st.session_state.role = user[3] if user and len(user) > 3 else "user"
            st.success(f"Welcome back, {login_username}! ")
            st.switch_page("pages/1_dashboard.py")
        else:
            st.error(msg)


with tab_register:
    st.subheader("Register")

    new_username = st.text_input("Choose a username", key="register_username")
    new_password = st.text_input("Choose a password", type="password", key="register_password")
    confirm_password = st.text_input("Confirm password", type="password", key="register_confirm")

    if st.button("Create account"):
        from app.services.user_service import register_user

        if not new_username or not new_password:
            st.warning("Please fill in all fields.")
        elif new_password != confirm_password:
            st.error("Passwords do not match.")
        else:
            success, msg = register_user(new_username, new_password)
            if success:
                st.success(msg)
                st.info("Tip: go to the Login tab and sign in with your new account.")
            else:
                st.error(msg)
