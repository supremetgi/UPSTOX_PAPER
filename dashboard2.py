import streamlit as st
import pika
import json
import pandas as pd
import time

st.set_page_config(page_title="Insider Trade Detector", layout="wide")

# --- UI HEADER ---
st.title("🕵️ Live Equity Option Insider Detector")
status_placeholder = st.sidebar.empty()

# --- DATA STORAGE ---
if 'alerts' not in st.session_state:
    st.session_state.alerts = []

def consume_data():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', heartbeat=600))
        channel = connection.channel()
        channel.queue_declare(queue='insider_alerts')
        
        # Pull all available messages from the queue
        while True:
            method_frame, header_frame, body = channel.basic_get(queue='insider_alerts', auto_ack=True)
            if body:
                st.session_state.alerts.append(json.loads(body))
            else:
                break
        connection.close()
        status_placeholder.success("✅ Connected to RabbitMQ")
    except Exception as e:
        status_placeholder.error(f"❌ RabbitMQ Offline: {e}")

# Fetch new data
consume_data()

# --- DISPLAY LOGIC ---
if st.session_state.alerts:
    df = pd.DataFrame(st.session_state.alerts)
    
    # Ensure columns exist to prevent KeyErrors
    required_cols = ['ticker', 'category', 'value', 'option_type', 'strike', 'price_move', 'timestamp']
    for col in required_cols:
        if col not in df.columns:
            df[col] = "N/A"

    # Sidebar Filters
    st.sidebar.header("Filters")
    min_val = st.sidebar.number_input("Min Value (₹)", value=100000)
    selected_stocks = st.sidebar.multiselect("Filter Stocks", options=df['ticker'].unique())

    filtered_df = df[df['value'] >= min_val]
    if selected_stocks:
        filtered_df = filtered_df[filtered_df['ticker'].isin(selected_stocks)]

    # Dynamic Ticker Sections
    for ticker in filtered_df['ticker'].unique():
        with st.expander(f"📊 {ticker}", expanded=True):
            ticker_data = filtered_df[filtered_df['ticker'] == ticker]
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 🚀 Aggressive Buying")
                buying = ticker_data[ticker_data['category'] == "AGGRESSIVE_BUYING"]
                st.dataframe(buying, use_container_width=True)
                
            with col2:
                st.markdown("### 🧱 Stagnant / Bulk Selling")
                selling = ticker_data[ticker_data['category'] != "AGGRESSIVE_BUYING"]
                st.dataframe(selling, use_container_width=True)
else:
    st.info("📡 Waiting for market surges... (Check your Producer script is running)")

# Auto-refresh every 2 seconds
time.sleep(2)
st.rerun()