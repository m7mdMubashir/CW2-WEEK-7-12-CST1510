import streamlit as st
import pandas as pd
import numpy as np
import sys
import os

sys.path.append(os.getcwd())

from app.data.db import connect_database
from app.data.incidents import insert_incident, get_all_incidents, update_incident, delete_incident
from app.utils.stream_helpers import safe_rerun

st.set_page_config(page_title="Dashboard", page_icon="📊", layout="wide")

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "username" not in st.session_state:
    st.session_state.username = ""

if not st.session_state.logged_in:
    st.error("You must be logged in to view the dashboard.")
    if st.button("Go to login page"):
        st.switch_page("pages/1_🔒_Login.py") 
    st.stop()

st.markdown(
    f"""
    <div class="banner">
        <div style="display:flex;align-items:center;justify-content:space-between;">
            <div>
                <h1 style="margin:0;padding:0;">📊 Dashboard</h1>
                <div style="opacity:0.9;margin-top:6px;">Welcome back, <strong>{st.session_state.username}</strong></div>
            </div>
            <div style="text-align:right;opacity:0.9;">Compact analytics & quick insights</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <style>
    .banner { background: linear-gradient(90deg,#4e54c8,#8f94fb); color: white; padding:18px; border-radius:10px; margin-bottom:14px; }
    .kpi { background: white; padding:12px 18px; border-radius:8px; box-shadow:0 6px 20px rgba(16,24,40,0.06); }
    .small { color:#6b7280; font-size:13px; }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Filters & Actions")
    n_points = st.slider("Number of data points", 10, 400, 80)
    st.caption("Adjust the number of random points used for demo charts.")
    if st.button("Refresh data"):
        safe_rerun()
    st.divider()
    if st.button("Log out"):
        st.session_state.logged_in = False
        st.session_state.username = ""
        st.info("You have been logged out.")
        st.switch_page("pages/1_🔒_Login.py")

conn = connect_database()
try:
    inc_df = pd.read_sql_query("SELECT * FROM cyber_incidents", conn)
except Exception:
    inc_df = pd.DataFrame()
try:
    tickets_df = pd.read_sql_query("SELECT * FROM it_tickets", conn)
except Exception:
    tickets_df = pd.DataFrame()
try:
    datasets_df = pd.read_sql_query("SELECT * FROM datasets_metadata", conn)
except Exception:
    datasets_df = pd.DataFrame()
conn.close()

now = pd.Timestamp.now()
inc_total = len(inc_df)
inc_open = inc_df[inc_df.get("status", "").str.lower() == "open"].shape[0] if not inc_df.empty else 0

phishing_recent = 0
if not inc_df.empty:
    if "category" in inc_df.columns:
        phishing_recent = inc_df[inc_df["category"].str.contains("phish", case=False, na=False)].shape[0]
    elif "title" in inc_df.columns:
        phishing_recent = inc_df[inc_df["title"].str.contains("phish", case=False, na=False)].shape[0]

tickets_total = len(tickets_df)
tickets_open = tickets_df[tickets_df.get("status", "").str.lower() == "open"].shape[0] if not tickets_df.empty else 0

datasets_size_mb = datasets_df.get("file_size_mb", pd.Series([0.0])).sum() if not datasets_df.empty else 0.0

cols = st.columns(4)
for i, (title, value, delta) in enumerate([
    ("Incidents", f"{inc_total}", f"Open: {inc_open}"),
    ("Phishing Incidents", f"{phishing_recent}", ""),
    ("Tickets", f"{tickets_total}", f"Open: {tickets_open}"),
    ("Datasets (MB)", f"{datasets_size_mb:,.1f}", ""),
]):
    with cols[i]:
        st.markdown("<div class='kpi'>", unsafe_allow_html=True)
        if delta:
            st.metric(title, value, delta=delta)
        else:
            st.metric(title, value)
        st.markdown("</div>", unsafe_allow_html=True)

left, right = st.columns([2, 1])

with left:
    st.subheader("Trends (Demo)")
    chart_data = pd.DataFrame(
        np.random.randn(20, 3),
        columns=['Incidents', 'Tickets', 'Datasets']
    )
    st.line_chart(chart_data)

    st.markdown("---")
    st.subheader("Incidents Management")

    with st.form("add_incident_form"):
        st.write("Add new incident")
        i_title = st.text_input("Title", key="new_inc_title")
        i_severity = st.selectbox("Severity", ["Low","Medium","High","Critical"], key="new_inc_sev")
        i_status = st.selectbox("Status", ["open","Closed","Investigating"], index=0, key="new_inc_status")
        i_date = st.date_input("Date", key="new_inc_date")
        if st.form_submit_button("Create Incident"):
            conn = connect_database()
            insert_incident(conn, i_title, i_severity, i_status, i_date.isoformat())
            conn.close()
            st.success("Incident created")
            safe_rerun()

    incidents = []
    try:
        conn2 = connect_database()
        incidents = get_all_incidents(conn2)
        conn2.close()
    except Exception:
        incidents = []

    if incidents:
        first_row_len = len(incidents[0])
        if first_row_len == 7:
             cols = ["id","title","severity","status","date","resolved_date","created_at"]
        elif first_row_len == 6:
             cols = ["id","title","severity","status","date","resolved_date"]
        else:
             cols = None # Let pandas auto-number columns
        
        inc_df_local = pd.DataFrame(incidents, columns=cols)
        
        st.dataframe(inc_df_local, width='stretch')

        if "id" in inc_df_local.columns:
            sel = st.selectbox("Select incident to manage", options=inc_df_local["id"].tolist(), key="sel_inc")
            if not inc_df_local[inc_df_local["id"]==sel].empty:
                row = inc_df_local[inc_df_local["id"]==sel].iloc[0]
                with st.form("edit_inc_form"):
                    st.write(f"Edit Incident #{sel}")
                    e_title = st.text_input("Title", value=row["title"], key="edit_inc_title")
                    
                    # Safe Index Finding
                    sev_opts = ["Low","Medium","High","Critical"]
                    curr_sev = row["severity"] if row["severity"] in sev_opts else "Low"
                    e_severity = st.selectbox("Severity", sev_opts, index=sev_opts.index(curr_sev), key="edit_inc_sev")
                    
                    stat_opts = ["open","Closed","Investigating"]
                    curr_stat = row["status"] if row["status"] in stat_opts else "open"
                    e_status = st.selectbox("Status", stat_opts, index=stat_opts.index(curr_stat), key="edit_inc_status")
                    
                    if st.form_submit_button("Update Incident"):
                        conn = connect_database()
                        update_incident(conn, int(sel), title=e_title, severity=e_severity, status=e_status)
                        conn.close()
                        st.success("Incident updated")
                        safe_rerun()
                    if st.form_submit_button("Delete Incident"):
                        conn = connect_database()
                        delete_incident(conn, int(sel))
                        conn.close()
                        st.warning("Incident deleted")
                        safe_rerun()

with right:
    st.subheader("Distribution")
    if not inc_df.empty and "severity" in inc_df.columns:
        st.write("Incidents by Severity")
        st.bar_chart(inc_df["severity"].value_counts())
    
    st.divider()
    st.subheader("Quick Actions")
    csv = inc_df.to_csv(index=False)
    st.download_button("Download Incident Data", csv, file_name="dashboard_data.csv", mime="text/csv")