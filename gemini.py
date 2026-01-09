import asyncio
import json
import ssl
import websockets
import requests
import os
import pandas as pd
import time
from collections import defaultdict
from dotenv import load_dotenv
import MarketDataFeedV3_pb2 as pb

load_dotenv()
ACCESS_TOKEN = os.getenv("token")

# --- ENERGY SETTINGS ---
WINDOW_TIME = 3.0           # 3-second window to catch the 'sweep'
MIN_VAL_THRESHOLD = 200000 # ₹20 Lakhs in 3 seconds is high intensity
MIN_PRICE_MOVE = 0.05       # Price must move up at least 1 tick (adjust for your script)

# --- STATE ---
# We use these to sum up the 'Energy' in the 3-second window
energy_val = defaultdict(float)      # Sum of (LTP * LTQ)
window_start_price = {}              # Price at the beginning of the window
window_last_price = defaultdict(float)

def get_market_data_feed_authorize_v3():
    headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    r = requests.get(url, headers=headers)
    return r.json()

def process_energy_surge(decoded_pb):
    """Calculates the dollar-volume energy hitting the tape."""
    for key, feed in decoded_pb.feeds.items():
        if not feed.HasField('firstLevelWithGreeks'): continue
        
        ltpc = feed.firstLevelWithGreeks.ltpc
        price = float(ltpc.ltp)
        qty = float(ltpc.ltq)
        
        # 1. Store the starting price of the window for this stock
        if key not in window_start_price:
            window_start_price[key] = price
            
        # 2. Accumulate 'Energy' (Value)
        # This solves the fragmentation: 750 + 750 + 1500 + 750... 
        energy_val[key] += (price * qty)
        window_last_price[key] = price

async def energy_monitor():
    """Background task that checks for surges every WINDOW_TIME."""
    while True:
        await asyncio.sleep(WINDOW_TIME)
        
        for key in list(energy_val.keys()):
            total_window_value = energy_val[key]
            start_p = window_start_price.get(key, 0)
            end_p = window_last_price[key]
            price_change = end_p - start_p
            
            # TRIGGER: High Value AND Price Moving Up
            if total_window_value >= MIN_VAL_THRESHOLD and price_change >= MIN_PRICE_MOVE:
                print(f"\n[⚡ ENERGY SURGE] {time.strftime('%H:%M:%S')}", flush=True)
                print(f"Instrument : {key}", flush=True)
                print(f"Value Traded: ₹{total_window_value:,.2f} in {WINDOW_TIME}s", flush=True)
                print(f"Price Move  : {start_p} -> {end_p} ({price_change:+.2f})", flush=True)
                print("-" * 40, flush=True)
        
        # Reset the 'Carts' for the next 3-second window
        energy_val.clear()
        window_start_price.clear()
        window_last_price.clear()

async def fetch_market_data(instrument_list):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Start the Surge Monitor
    asyncio.create_task(energy_monitor())

    while True:
        try:
            auth_res = get_market_data_feed_authorize_v3()
            ws_url = auth_res["data"]["authorized_redirect_uri"]

            async with websockets.connect(ws_url, ssl=ssl_context, ping_interval=20, ping_timeout=10) as websocket:
                print(f"✅ Monitoring Energy Surge for {len(instrument_list)} stocks...", flush=True)
                
                sub_msg = {"guid": "surge_detect", "method": "sub", 
                           "data": {"mode": "option_greeks", "instrumentKeys": instrument_list}}
                await websocket.send(json.dumps(sub_msg).encode("utf-8"))

                feed_response = pb.FeedResponse()
                while True:
                    message = await websocket.recv()
                    feed_response.ParseFromString(message)
                    process_energy_surge(feed_response)

        except Exception as e:
            print(f"❌ Connection Interrupted: {e}. Reconnecting...", flush=True)
            await asyncio.sleep(5)

if __name__ == "__main__":
    # Load your keys from CSV...


    df = pd.read_csv("atm_option_table.csv")
    cols = ["atm_plus_2_ce_instrument","atm_minus_2_pe_instrument"]
    keys = list(set(df[cols].values.flatten().tolist()))
    keys = [str(x) for x in keys if str(x) != 'nan']

    if not keys:
        print("no instrumwent keys")
    else:
        asyncio.run(fetch_market_data(keys))


   