import streamlit as st


def safe_rerun():
    
    try:
        st.experimental_rerun()
    except Exception:
        st.session_state["_refresh"] = st.session_state.get("_refresh", 0) + 1
        st.stop()
