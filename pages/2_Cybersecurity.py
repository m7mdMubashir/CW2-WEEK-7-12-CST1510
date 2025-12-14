import streamlit as st
import pandas as pd
import sys
import os

sys.path.append(os.getcwd())

from app.data.db import connect_database
from app.data.incidents import insert_incident, get_all_incidents, update_incident, delete_incident
from app.utils.stream_helpers import safe_rerun
from models.security_incident import SecurityIncident 

def cyber_hub_ui():
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#7f1d1d,#ef4444);padding:12px;border-radius:8px;color:white;">
            <h2 style="margin:0">🔐 Cyber Security</h2>
            <div style="opacity:0.9">Incident overview and management</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    conn = connect_database()
    try:
        df = pd.read_sql_query("SELECT * FROM cyber_incidents", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()

    total = len(df)
    high = df[df.get("severity", "").str.lower() == "high"].shape[0] if not df.empty else 0
    open_cnt = df[df.get("status", "").str.lower() == "open"].shape[0] if not df.empty else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Incidents", total)
    c2.metric("High Severity", high)
    c3.metric("Open Incidents", open_cnt)

    st.markdown("---")
    st.subheader("Incidents — Analytics & Management")

    conn = connect_database()
    try:
        full = pd.read_sql_query("SELECT * FROM cyber_incidents ORDER BY date DESC", conn)
    except Exception:
        full = pd.DataFrame()
    conn.close()

    st.markdown("---")
    st.subheader("Incidents by Severity & Trends")
    left, right = st.columns([2, 1])
    with left:
        if full.empty:
            st.info("No incidents available.")
        else:
            if "severity" in full.columns:
                st.bar_chart(full["severity"].value_counts())
            if "date" in full.columns:
                full["date"] = pd.to_datetime(full["date"], errors="coerce")
                ts = full.groupby(full["date"].dt.to_period("D")).size().sort_index()
                st.line_chart(ts)
    with right:
        st.subheader("Sample Incidents")
        st.dataframe(full.head(20), width='stretch')

    st.markdown("---")
    st.subheader("Phishing Spike & Response Bottleneck")

    if full.empty:
        st.info("No incidents to analyze.")
    else:
        for col in ["date", "created_at", "resolved_date"]:
            if col in full.columns:
                full[col] = pd.to_datetime(full[col], errors="coerce")

        if "category" not in full.columns:
            phishing = full[full["title"].str.contains("phish", case=False, na=False)] if "title" in full.columns else pd.DataFrame()
        else:
            phishing = full[full["category"].str.contains("phish", case=False, na=False) | (full["category"].str.lower() == "phishing")]

        if phishing.empty:
            st.write("No phishing incidents found.")
        else:
            phishing = phishing.copy()
            phishing["day"] = phishing["date"].dt.floor("D")
            daily = phishing.groupby("day").size().rename("count").reset_index().set_index("day").asfreq("D", fill_value=0)
            daily["rolling_mean"] = daily["count"].rolling(window=7, min_periods=1).mean()
            daily["is_spike"] = daily["count"] > (daily["rolling_mean"] * 2.0)
            st.line_chart(daily["count"])

    st.markdown("---")
    st.subheader("Manage Incidents")
    
    with st.expander("➕ Add New Incident"):
        title = st.text_input("Incident Title", key="add_title")
        severity = st.selectbox("Severity", ["High", "Medium", "Low"], key="add_sev")
        status = st.selectbox("Status", ["open", "Closed", "Investigating"], key="add_status")
        date = st.date_input("Date", key="add_date")
        
        if st.button("Add Incident"):
            new_incident = SecurityIncident(
                id=None,
                title=title,
                severity=severity,
                status=status,
                date=date.isoformat()
            )
            conn = connect_database()
            insert_incident(conn, new_incident.title, new_incident.severity, new_incident.status, new_incident.date)
            conn.close()
            st.success(f"Incident '{new_incident.title}' added successfully!")
            safe_rerun()

    with st.expander("✏️ Update Incident"):
        incs = get_all_incidents(connect_database())
        if len(incs) > 0:
            if len(incs[0]) == 7: cols = ["id","title","severity","status","date","resolved_date","created_at"]
            elif len(incs[0]) == 6: cols = ["id","title","severity","status","date","resolved_date"]
            else: cols = None 
            inc_df_local = pd.DataFrame(incs, columns=cols)

            if "id" in inc_df_local.columns:
                incident_id = st.selectbox("Select ID to update", inc_df_local["id"].tolist(), key="upd_inc")
                new_title = st.text_input("New Title", key="new_title")
                new_sev = st.selectbox("New Severity", ["High", "Medium", "Low"], key="new_sev")
                new_status = st.selectbox("New Status", ["open", "Closed", "Investigating"], key="new_status")
                
                if st.button("Update Incident"):
                    conn = connect_database()
                    update_incident(conn, int(incident_id), title=new_title, severity=new_sev, status=new_status)
                    conn.close()
                    st.success("Incident updated!")
                    safe_rerun()
        else:
            st.info("No incidents to update.")

    with st.expander("🗑️ Delete Incident"):
        incs = get_all_incidents(connect_database())
        if len(incs) > 0:
            if len(incs[0]) == 7: cols = ["id","title","severity","status","date","resolved_date","created_at"]
            elif len(incs[0]) == 6: cols = ["id","title","severity","status","date","resolved_date"]
            else: cols = None 
            inc_df_local = pd.DataFrame(incs, columns=cols)

            if "id" in inc_df_local.columns:
                del_id = st.selectbox("Select ID to delete", inc_df_local["id"].tolist(), key="del_inc")
                if st.button("Delete Incident"):
                    conn = connect_database()
                    delete_incident(conn, int(del_id))
                    conn.close()
                    st.success("Incident deleted!")
                    safe_rerun()

cyber_hub_ui()