import streamlit as st
import pika
import json
import pandas as pd

st.set_page_config(page_title="Insider Trade Detector", layout="wide")
st.title("🕵️ Live Equity Option Insider Detector")

# Persistent storage for our web session
if 'alerts' not in st.session_state:
    st.session_state.alerts = []

def consume_data():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # Get one message at a time without blocking the UI
    method_frame, header_frame, body = channel.basic_get(queue='insider_alerts', auto_ack=True)
    if body:
        st.session_state.alerts.append(json.loads(body))

# Run the consumer
consume_data()

# Layout: Dynamic sections for each stock
df = pd.DataFrame(st.session_state.alerts)

if not df.empty:
    for ticker in df['ticker'].unique():
        st.header(f"Stock: {ticker}")
        col1, col2 = st.columns(2)
        
        ticker_data = df[df['ticker'] == ticker]
        
        with col1:
            st.subheader("🚀 Price Appreciation (Buying)")
            st.write(ticker_data[ticker_data['category'] == "AGGRESSIVE_BUYING"])
            
        with col2:
            st.subheader("🧱 Stagnant / Bulk Selling")
            st.write(ticker_data[ticker_data['category'] == "BULK_SELLING"])
else:
    st.info("Waiting for market data...")

# Auto-refresh the page every 2 seconds
st.rerun()