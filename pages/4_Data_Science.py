import streamlit as st
import pandas as pd
import sys
import os
import time

sys.path.append(os.getcwd())

try:
    from app.data.db import connect_database
    from app.data.datasets import load_datasets_metadata_csv
    from app.utils.stream_helpers import safe_rerun
except ImportError:
    st.error("⚠️ Critical modules not found. Ensure app/data and app/utils exist.")
    st.stop()

st.set_page_config(
    page_title="Data Governance Portal",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

def get_data():
    conn = connect_database()
    try:
        df = pd.read_sql_query("SELECT * FROM datasets_metadata ORDER BY id DESC", conn)
    except Exception:
        df = pd.DataFrame()
    finally:
        conn.close()
    
    if not df.empty:
        if "file_size_mb" in df.columns:
            df["file_size_mb"] = pd.to_numeric(df["file_size_mb"], errors="coerce").fillna(0.0)
        if "record_count" in df.columns:
            df["record_count"] = pd.to_numeric(df["record_count"], errors="coerce").fillna(0)
        if "last_updated" in df.columns:
            df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
            df["age_days"] = (pd.Timestamp.now() - df["last_updated"]).dt.days
    return df

def governance_dashboard_ui():
    st.markdown(
        """
        <div style="background-color:#1e293b; padding:20px; border-radius:10px; color:white; margin-bottom: 25px;">
            <h1 style="margin:0; font-size: 2rem;">📚 Enterprise Data Catalog</h1>
            <div style="color:#94a3b8; font-size: 1rem;">Governance, Discovery, and Lifecycle Management</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    df = get_data()

    with st.sidebar:
        st.header("🛡️ Governance Strategy")
        
        if not df.empty and "file_size_mb" in df.columns:
            total_size_mb = df["file_size_mb"].sum()
            if "source" in df.columns:
                top_src = df.groupby("source")["file_size_mb"].sum().sort_values(ascending=False)
                if not top_src.empty:
                    pct = (top_src.iloc[0] / total_size_mb * 100) if total_size_mb > 0 else 0
                    st.info(f"**Insight:** {pct:.1f}% of data volume comes from **{top_src.index[0]}**.")
        
        st.markdown("### 📌 Action Items")
        st.caption("1. Review 'Archiving Candidates' monthly.")
        st.caption("2. Enforce lifecycle policies on sources exceeding 1TB.")
        st.caption("3. Tag datasets with missing owners.")
        
        st.divider()
        st.markdown("### 📥 Export")
        if st.button("Download Full Catalog CSV"):
            st.download_button(
                "Click to Download",
                df.to_csv(index=False),
                file_name="full_catalog.csv",
                mime="text/csv"
            )

    if df.empty:
        st.warning("Catalog is empty.")
        if st.button("Initialize with CSV Data"):
            conn2 = connect_database()
            loaded = load_datasets_metadata_csv(conn2, force=True)
            conn2.close()
            st.success(f"Loaded {loaded} records.")
            time.sleep(1)
            safe_rerun()
        st.stop()

    tab_overview, tab_archive, tab_data = st.tabs(["📊 Catalog Overview", "🧹 Archiving Simulator", "💾 Raw Data Manager"])

    with tab_overview:
        total_ds = len(df)
        total_rows = df["record_count"].sum()
        total_size = df["file_size_mb"].sum()
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Datasets", total_ds, border=True)
        m2.metric("Total Records", f"{total_rows:,.0f}", border=True)
        m3.metric("Storage Used (MB)", f"{total_size:,.1f}", border=True)

        st.markdown("### 🌍 Landscape Analysis")
        
        col_charts_1, col_charts_2 = st.columns(2)
        
        with col_charts_1:
            st.subheader("Storage by Source")
            if "source" in df.columns:
                src_data = df.groupby("source")["file_size_mb"].sum().sort_values(ascending=True)
                st.bar_chart(src_data, horizontal=True, color="#6366f1")

        with col_charts_2:
            st.subheader("Category Distribution")
            if "category" in df.columns:
                cat_counts = df["category"].value_counts()
                st.bar_chart(cat_counts, color="#1e3a8a")

    with tab_archive:
        st.markdown(
            """
            <div style="background-color:#f1f5f9; padding:15px; border-radius:8px; border-left: 5px solid #ef4444; margin-bottom:20px; color: black">
                <strong>🧹 Archiving Simulator:</strong> Adjust the thresholds below to identify datasets that are old, large, or rarely used.
            </div>
            """, 
            unsafe_allow_html=True
        )

        c_filt1, c_filt2, c_filt3 = st.columns(3)
        with c_filt1:
            age_thresh = st.slider("📅 Days since last update", 30, 2000, 365)
        with c_filt2:
            size_thresh = st.slider("💾 Minimum Size (MB)", 1, 5000, 100)
        with c_filt3:
            row_thresh = st.slider("📉 Max Row Count (Sparse Data)", 0, 100000, 1000)

        candidates = df.copy()
        mask = (
            (candidates["age_days"] > age_thresh) | 
            ((candidates["file_size_mb"] > size_thresh) & (candidates["record_count"] < row_thresh))
        )
        results = candidates[mask].sort_values("file_size_mb", ascending=False)
        
        st.subheader(f"Results: {len(results)} Candidates Found")
        
        if not results.empty:
            potential_savings = results["file_size_mb"].sum()
            st.caption(f"Potential Storage Savings: **{potential_savings:,.1f} MB**")
            
            st.dataframe(
                results[["dataset_name", "source", "age_days", "file_size_mb", "record_count"]],
                use_container_width=True,
                column_config={
                    "dataset_name": "Dataset",
                    "file_size_mb": st.column_config.ProgressColumn(
                        "Size (MB)", 
                        format="%.1f MB", 
                        min_value=0, 
                        max_value=float(df["file_size_mb"].max())
                    ),
                    "age_days": st.column_config.NumberColumn("Days Inactive"),
                    "record_count": st.column_config.NumberColumn("Rows")
                }
            )
        else:
            st.success("✅ No datasets match these archiving criteria.")

    with tab_data:
        st.subheader("🎛️ Data Controls")
        
        col_ctrl, col_display = st.columns([1, 3])
        
        with col_ctrl:
            st.write("**Manage Source Data**")
            if st.button("Reload from CSV (Force)", type="primary"):
                conn2 = connect_database()
                load_datasets_metadata_csv(conn2, force=True)
                conn2.close()
                st.toast("Database reloaded successfully!", icon="🔄")
                time.sleep(1)
                safe_rerun()
            
            st.divider()
            st.write("**Quick Filters**")
            cats = df["category"].dropna().unique().tolist()
            sel_cat = st.multiselect("Filter Category", cats)
        
        with col_display:
            display_df = df if not sel_cat else df[df["category"].isin(sel_cat)]
            st.dataframe(
                display_df,
                use_container_width=True,
                height=500,
                column_config={
                    "last_updated": st.column_config.DateColumn("Last Updated"),
                    "file_size_mb": st.column_config.NumberColumn("Size (MB)", format="%.2f")
                }
            )

if __name__ == "__main__":
    governance_dashboard_ui()