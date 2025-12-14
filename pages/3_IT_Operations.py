import streamlit as st
import pandas as pd
import sys
import os
import plotly.express as px 

sys.path.append(os.getcwd())

try:
    from app.data.db import connect_database
    from app.data.tickets import load_it_tickets_csv, update_ticket
    from app.utils.stream_helpers import safe_rerun
except ImportError:
    st.error("⚠️ Critical modules not found. Please ensure app/data and app/utils exist.")
    st.stop()

st.set_page_config(page_title="ITOps Command Center", page_icon="🛠️", layout="wide")

def get_data():
    """Fetches and pre-processes data to keep UI code clean."""
    conn = connect_database()
    try:
        df = pd.read_sql_query("SELECT * FROM it_tickets", conn)
    except Exception:
        df = pd.DataFrame()
    finally:
        conn.close()

    if not df.empty:
        if "created_date" in df.columns:
            df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
            df["age_days"] = (pd.Timestamp.now() - df["created_date"]).dt.total_seconds() / 86400.0
            df["age_days"] = df["age_days"].round(1)
        
        if "resolved_date" in df.columns:
            df["resolved_date"] = pd.to_datetime(df["resolved_date"], errors="coerce")
            df["resolution_days"] = (df["resolved_date"] - df["created_date"]).dt.total_seconds() / 86400.0
    
    return df

def handle_data_seeding(df):
    """Handles the logic for loading initial CSV data if DB is empty."""
    if len(df) == 0 and not st.session_state.get("_itops_auto_load_done", False):
        conn2 = connect_database()
        loaded = load_it_tickets_csv(conn2, force=False)
        conn2.close()
        st.session_state["_itops_auto_load_done"] = True
        if loaded > 0:
            st.toast(f"System initialized: {loaded} tickets loaded.", icon="🚀")
            safe_rerun()

def itops_hub_ui():
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#064e3b,#059669);padding:16px;border-radius:10px;color:white;margin-bottom:20px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
            <h1 style="margin:0; font-size: 1.8rem;">🛠️ IT Operations Hub</h1>
            <div style="opacity:0.8; font-size: 1rem;">Service Desk & Incident Resolution Console</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    df = get_data()
    handle_data_seeding(df)

    if not df.empty:
        total = len(df)
        open_cnt = df[df.get("status", "").str.lower() == "open"].shape[0]
        waiting = df[df.get("status", "").str.lower() == "waiting_user"].shape[0]
        
        avg_res_time = 0
        if "resolution_days" in df.columns:
            closed_tickets = df[df["resolution_days"].notna()]
            if not closed_tickets.empty:
                avg_res_time = closed_tickets["resolution_days"].mean()

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Tickets", total, border=True)
        k2.metric("Active (Open)", open_cnt, delta="Queue Load", delta_color="inverse", border=True)
        k3.metric("Waiting on User", waiting, border=True)
        k4.metric("Avg Resolution Time", f"{avg_res_time:.1f} days", border=True)

    tab_analytics, tab_ops, tab_admin = st.tabs(["📊 Analytics & Performance", "🎫 Ticket Operations", "⚙️ Admin & Settings"])

    with tab_analytics:
        if df.empty:
            st.info("No data available for analytics.")
        else:
            col_left, col_right = st.columns(2)

            with col_left:
                st.subheader("Ticket Aging")
                st.caption("Average age of open tickets by status")
                if "status" in df.columns and "age_days" in df.columns:
                    open_df = df[df["status"].str.lower() != "closed"]
                    if not open_df.empty:
                        avg_age = open_df.groupby("status")["age_days"].mean()
                        st.bar_chart(avg_age, color="#059669", use_container_width=True)
                    else:
                        st.success("No open tickets!")

            with col_right:
                st.subheader("Staff Performance")
                st.caption("Average resolution time (days) by assignee")
                if "assigned_to" in df.columns and "resolution_days" in df.columns:
                    resolved = df[df["resolution_days"].notna()]
                    if not resolved.empty:
                        staff_perf = resolved.groupby("assigned_to")["resolution_days"].mean().sort_values()
                        st.bar_chart(staff_perf, horizontal=True, use_container_width=True)
                    else:
                        st.info("No resolved tickets yet.")
                else:
                    st.warning("Missing assignment data.")

            st.markdown("---")
            st.subheader("🐢 Slowest Resolving Tickets")
            if "resolution_days" in df.columns:
                slowest = df.sort_values("resolution_days", ascending=False).head(10)
                st.dataframe(
                    slowest[["id", "title", "assigned_to", "resolution_days", "status"]], 
                    use_container_width=True,
                    hide_index=True
                )

    with tab_ops:
        c_list, c_edit = st.columns([2, 1], gap="large")

        with c_list:
            st.subheader("Ticket Queue")
            
            search_term = st.text_input("🔍 Search tickets...", placeholder="Type title, ID, or assignee")
            
            display_df = df.copy()
            if search_term:
                display_df = display_df[
                    display_df.astype(str).apply(lambda x: x.str.contains(search_term, case=False)).any(axis=1)
                ]

            st.dataframe(
                display_df, 
                use_container_width=True, 
                height=500,
                column_config={
                    "created_date": st.column_config.DateColumn("Created", format="YYYY-MM-DD"),
                    "status": st.column_config.SelectboxColumn("Status", width="small", options=["open", "closed", "waiting_user"]),
                    "age_days": st.column_config.NumberColumn("Age (Days)", format="%.1f")
                }
            )

        with c_edit:
            st.markdown("### ✏️ Quick Action")
            st.info("Select a ticket ID to update status or reassignment.")
            
            if not df.empty:
                with st.form("update_ticket_form"):
                    ticket_ids = df["id"].tolist()
                    ticket_ids.sort()
                    
                    selected_id = st.selectbox("Select Ticket ID", ticket_ids)
                    
                    current_ticket = df[df["id"] == selected_id].iloc[0]
                    curr_assign = current_ticket.get("assigned_to", "")
                    curr_status = current_ticket.get("status", "open")
                    
                    st.divider()
                    st.write(f"**Title:** {current_ticket.get('title', 'N/A')}")
                    
                    new_assignee = st.text_input("Assigned To", value=curr_assign)
                    new_status = st.selectbox("New Status", ["open", "closed", "waiting_user"], index=0 if curr_status not in ["open", "closed", "waiting_user"] else ["open", "closed", "waiting_user"].index(curr_status))
                    
                    submit_btn = st.form_submit_button("Update Ticket", type="primary", use_container_width=True)
                    
                    if submit_btn:
                        connu = connect_database()
                        update_ticket(connu, int(selected_id), status=new_status, assigned_to=new_assignee)
                        connu.close()
                        st.success(f"Ticket #{selected_id} updated!")
                        safe_rerun()
            else:
                st.warning("No tickets to edit.")

    with tab_admin:
        st.subheader("Data Management")
        st.write("Use this section to reset or force-load data.")
        
        col_act1, col_act2 = st.columns(2)
        with col_act1:
            if st.button("Force Reload from CSV", use_container_width=True):
                conn2 = connect_database()
                loaded = load_it_tickets_csv(conn2, force=True)
                conn2.close()
                st.success(f"Database reset. Loaded {loaded} rows.")
                safe_rerun()
        
        with col_act2:
            st.caption("Additional admin tools can be added here (e.g., Export to Excel, Delete All).")

if __name__ == "__main__":
    itops_hub_ui()