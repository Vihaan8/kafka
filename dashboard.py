import time
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="Patient Vital Signs Monitor",
    page_icon="🏥",
    layout="wide"
)

st.title("Real-Time Patient Vital Signs Monitoring System")
st.markdown("---")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_vitals_data(patient_filter: str = "All", limit: int = 500) -> pd.DataFrame:
    base_query = "SELECT * FROM patient_vitals"
    params = {}
    
    if patient_filter and patient_filter != "All":
        base_query += " WHERE patient_id = :patient_id"
        params["patient_id"] = patient_filter
    
    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit
    
    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        if not df.empty and "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

def get_patient_list() -> list:
    try:
        query = "SELECT DISTINCT patient_id FROM patient_vitals ORDER BY patient_id"
        df = pd.read_sql_query(text(query), con=engine.connect())
        return ["All"] + df["patient_id"].tolist()
    except:
        return ["All"]

def get_latest_vitals_per_patient() -> pd.DataFrame:
    try:
        query = """
            WITH RankedVitals AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY timestamp DESC) as rn
                FROM patient_vitals
            )
            SELECT patient_id, timestamp, heart_rate, systolic_bp, diastolic_bp, 
                   spo2, temperature, respiratory_rate, condition, is_anomaly
            FROM RankedVitals
            WHERE rn = 1
            ORDER BY patient_id;
        """
        df = pd.read_sql_query(text(query), con=engine.connect())
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        st.error(f"Error fetching latest vitals: {e}")
        return pd.DataFrame()

st.sidebar.header("Dashboard Controls")

patient_options = get_patient_list()
selected_patient = st.sidebar.selectbox("Filter by Patient", patient_options)

update_interval = st.sidebar.slider(
    "Auto-refresh interval (seconds)", 
    min_value=2, 
    max_value=20, 
    value=5
)

limit_records = st.sidebar.number_input(
    "Records to load", 
    min_value=100, 
    max_value=2000, 
    value=500, 
    step=100
)

time_window = st.sidebar.selectbox(
    "Time window for charts",
    ["Last 5 minutes", "Last 15 minutes", "Last 30 minutes", "Last 1 hour", "All"]
)

if st.sidebar.button("Refresh Now"):
    st.rerun()

placeholder = st.empty()

while True:
    with placeholder.container():
        df_vitals = load_vitals_data(selected_patient, limit=int(limit_records))
        df_latest = get_latest_vitals_per_patient()
        
        if df_vitals.empty:
            st.warning("No data available yet. Waiting for vitals stream...")
            time.sleep(update_interval)
            continue
        
        if time_window != "All" and "timestamp" in df_vitals.columns:
            time_deltas = {
                "Last 5 minutes": timedelta(minutes=5),
                "Last 15 minutes": timedelta(minutes=15),
                "Last 30 minutes": timedelta(minutes=30),
                "Last 1 hour": timedelta(hours=1)
            }
            cutoff_time = datetime.now() - time_deltas[time_window]
            df_vitals = df_vitals[df_vitals["timestamp"] >= cutoff_time]
        
        st.subheader("Key Performance Indicators")
        
        total_readings = len(df_vitals)
        total_anomalies = df_vitals["is_anomaly"].sum() if "is_anomaly" in df_vitals.columns else 0
        alert_rate = (total_anomalies / total_readings * 100) if total_readings > 0 else 0
        
        critical_patients = 0
        if not df_latest.empty and "is_anomaly" in df_latest.columns:
            critical_patients = df_latest["is_anomaly"].sum()
        
        normal_readings = total_readings - total_anomalies
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        col1.metric("Total Readings", f"{total_readings:,}")
        col2.metric("Alert Rate", f"{alert_rate:.1f}%", f"{total_anomalies} alerts")
        col3.metric("Critical Patients", critical_patients)
        col4.metric("Normal Readings", f"{normal_readings:,}")
        col5.metric("Patients Monitored", df_vitals["patient_id"].nunique() if "patient_id" in df_vitals.columns else 0)
        
        st.markdown("---")
        
        st.subheader("Current Patient Status")
        
        if not df_latest.empty:
            display_df = df_latest[[
                "patient_id", "heart_rate", "systolic_bp", "diastolic_bp",
                "spo2", "temperature", "respiratory_rate", "condition", "is_anomaly"
            ]].copy()
            
            display_df.columns = [
                "Patient ID", "HR", "Systolic", "Diastolic",
                "SpO2 (%)", "Temp (C)", "RR", "Condition", "Alert"
            ]
            
            def highlight_alerts(row):
                if row["Alert"]:
                    return ['background-color: #ffcccc'] * len(row)
                return [''] * len(row)
            
            styled_df = display_df.style.apply(highlight_alerts, axis=1)
            st.dataframe(styled_df, use_container_width=True, height=400)
        else:
            st.info("Waiting for patient data...")
        
        st.markdown("---")
        
        st.subheader("Vital Signs Trends")
        
        if len(df_vitals) > 0 and "timestamp" in df_vitals.columns:
            df_plot = df_vitals.sort_values("timestamp")
            
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                fig_hr = px.line(
                    df_plot, 
                    x="timestamp", 
                    y="heart_rate",
                    color="patient_id" if selected_patient == "All" else None,
                    title="Heart Rate (bpm)",
                    markers=True
                )
                fig_hr.add_hline(y=60, line_dash="dash", line_color="green")
                fig_hr.add_hline(y=100, line_dash="dash", line_color="orange")
                st.plotly_chart(fig_hr, use_container_width=True)
                
                fig_bp = go.Figure()
                if selected_patient == "All":
                    for patient in df_plot["patient_id"].unique()[:5]:
                        patient_data = df_plot[df_plot["patient_id"] == patient]
                        fig_bp.add_trace(go.Scatter(
                            x=patient_data["timestamp"],
                            y=patient_data["systolic_bp"],
                            name=f"{patient} (Sys)",
                            mode='lines+markers'
                        ))
                else:
                    fig_bp.add_trace(go.Scatter(
                        x=df_plot["timestamp"],
                        y=df_plot["systolic_bp"],
                        name="Systolic",
                        mode='lines+markers'
                    ))
                    fig_bp.add_trace(go.Scatter(
                        x=df_plot["timestamp"],
                        y=df_plot["diastolic_bp"],
                        name="Diastolic",
                        mode='lines+markers'
                    ))
                
                fig_bp.update_layout(title="Blood Pressure (mmHg)")
                st.plotly_chart(fig_bp, use_container_width=True)
                
                fig_temp = px.line(
                    df_plot,
                    x="timestamp",
                    y="temperature",
                    color="patient_id" if selected_patient == "All" else None,
                    title="Temperature (C)",
                    markers=True
                )
                fig_temp.add_hline(y=36.5, line_dash="dash", line_color="green")
                fig_temp.add_hline(y=37.5, line_dash="dash", line_color="orange")
                st.plotly_chart(fig_temp, use_container_width=True)
            
            with chart_col2:
                fig_spo2 = px.line(
                    df_plot,
                    x="timestamp",
                    y="spo2",
                    color="patient_id" if selected_patient == "All" else None,
                    title="SpO2 (%)",
                    markers=True
                )
                fig_spo2.add_hline(y=95, line_dash="dash", line_color="green")
                st.plotly_chart(fig_spo2, use_container_width=True)
                
                fig_rr = px.line(
                    df_plot,
                    x="timestamp",
                    y="respiratory_rate",
                    color="patient_id" if selected_patient == "All" else None,
                    title="Respiratory Rate (breaths/min)",
                    markers=True
                )
                fig_rr.add_hline(y=12, line_dash="dash", line_color="green")
                fig_rr.add_hline(y=20, line_dash="dash", line_color="orange")
                st.plotly_chart(fig_rr, use_container_width=True)
                
                if "is_anomaly" in df_vitals.columns:
                    anomaly_counts = df_vitals["is_anomaly"].value_counts()
                    fig_anomaly = px.pie(
                        values=anomaly_counts.values,
                        names=["Normal", "Anomaly"],
                        title="Reading Distribution"
                    )
                    st.plotly_chart(fig_anomaly, use_container_width=True)
        
        st.markdown("---")
        
        if "is_anomaly" in df_vitals.columns:
            recent_anomalies = df_vitals[df_vitals["is_anomaly"] == True].head(10)
            
            if len(recent_anomalies) > 0:
                st.subheader("Recent Anomalies")
                display_anomalies = recent_anomalies[[
                    "patient_id", "timestamp", "heart_rate", "systolic_bp",
                    "spo2", "temperature", "condition"
                ]].copy()
                st.dataframe(display_anomalies, use_container_width=True)
        
        st.markdown("---")
        st.caption(
            f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
            f"Auto-refresh: {update_interval}s | "
            f"Displaying: {time_window}"
        )
    
    time.sleep(update_interval)
