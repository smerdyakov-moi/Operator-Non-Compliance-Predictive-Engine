import streamlit as st
import pandas as pd
import sqlite3
import os

st.set_page_config(page_title="BODS Compliance", layout="wide")
st.title("BODS Predictive Compliance Dashboard")

APP_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(APP_DIR) == 'scripts':
    BASE_DIR = os.path.dirname(APP_DIR) 
else:
    BASE_DIR = APP_DIR 

db_path = os.path.join(BASE_DIR, "data", "bods_analytics.db")

@st.cache_data
def load_data():
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM operator_compliance", conn)
    conn.close()
    return df

try:
    df = load_data()
    
    # Top KPI Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Logged Services", f"{len(df):,}")
    col2.metric("Unique Operators", f"{df['Operator'].nunique()}")
    col3.metric("System Status", "Live & Connected")
    
    st.divider()
    
    # Interactive Sidebar Filter
    st.sidebar.header("Dashboard Controls")
    operator_list = df['Operator'].dropna().unique().tolist()
    selected_ops = st.sidebar.multiselect("Filter by Operator:", operator_list, default=operator_list[:10])
    
    # Filtered Chart
    if selected_ops:
        filtered_df = df[df['Operator'].isin(selected_ops)]
        st.subheader("Regional Performance Trends (Filtered)")
        chart_data = filtered_df.groupby("Operator")["Status"].count()
        st.bar_chart(chart_data)
        
    st.caption("This dashboard visualizes the operational throughput identified by the distributed PySpark analytical engine.")
    
except Exception as e:
    st.error(f"Failed to load database. Looked for it here: {db_path}\nError: {e}")