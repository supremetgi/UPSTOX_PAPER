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
WINDOW_TIME = 3.0             
MIN_VAL_THRESHOLD = 200000    # Set to ₹2 Lakhs (adjust as needed)
MIN_PRICE_MOVE = 0.05         

# --- GLOBAL STATE ---
energy_val = defaultdict(float)      
window_start_price = {}            
window_last_price = defaultdict(float)
INSTRUMENT_MAP = {} # This will hold our fast-lookup data

def get_market_data_feed_authorize_v3():
    headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    r = requests.get(url, headers=headers)
    return r.json()

def create_optimized_lookup(active_keys):
    """
    Creates a memory-efficient mapping for ONLY the stocks you are watching.
    Mapping: instrument_key -> {name, strike, type}
    """
    print("🔄 Building optimized instrument map...")
    master_df = pd.read_csv("companies_only.csv")
    lookup = {}
    active_set = set(active_keys)

    for _, row in master_df.iterrows():
        name = row['name']
        strike = row['strike_price']
        
        # Check CE key
        ce_key = str(row['ce_instrument_key'])
        if ce_key in active_set:
            lookup[ce_key] = {"name": name, "strike": strike, "type": "CE"}
            
        # Check PE key
        pe_key = str(row['pe_instrument_key'])
        if pe_key in active_set:
            lookup[pe_key] = {"name": name, "strike": strike, "type": "PE"}
            
    print(f"✅ Map Ready: {len(lookup)} active option contracts mapped.")
    print(len(lookup))
    print(type(lookup))
    return lookup

def process_energy_surge(decoded_pb):
    for key, feed in decoded_pb.feeds.items():
        if not feed.HasField('firstLevelWithGreeks'): continue
        
        ltpc = feed.firstLevelWithGreeks.ltpc
        price = float(ltpc.ltp)
        qty = float(ltpc.ltq)
        
        if key not in window_start_price:
            window_start_price[key] = price
            
        energy_val[key] += (price * qty)
        window_last_price[key] = price

async def energy_monitor():
    while True:
        await asyncio.sleep(WINDOW_TIME)
        
        for key in list(energy_val.keys()):
            total_window_value = energy_val[key]
            start_p = window_start_price.get(key, 0)
            end_p = window_last_price[key]
            price_change = end_p - start_p
            
            if total_window_value >= MIN_VAL_THRESHOLD and price_change >= MIN_PRICE_MOVE:
                # REVERSE MAPPING LOOKUP
                info = INSTRUMENT_MAP.get(key, {"name": key, "strike": "", "type": ""})
                
                alert = (
                    f"\n[⚡ SURGE] {time.strftime('%H:%M:%S')} | "
                    f"{info['name']} | {info['strike']} {info['type']}\n"
                    f"Value: ₹{total_window_value:,.2f} | Move: {start_p} -> {end_p} ({price_change:+.2f})\n"
                    f"{'-' * 50}"
                )
                print(alert, flush=True)

        energy_val.clear()
        window_start_price.clear()
        window_last_price.clear()

async def fetch_market_data(instrument_list):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    asyncio.create_task(energy_monitor())

    while True:
        try:
            auth_res = get_market_data_feed_authorize_v3()
            ws_url = auth_res["data"]["authorized_redirect_uri"]

            async with websockets.connect(ws_url, ssl=ssl_context, ping_interval=20, ping_timeout=10) as websocket:
                print(f"🚀 Monitoring {len(instrument_list)} options...")
                
                sub_msg = {"guid": "surge_detect", "method": "sub", 
                           "data": {"mode": "option_greeks", "instrumentKeys": instrument_list}}
                await websocket.send(json.dumps(sub_msg).encode("utf-8"))

                feed_response = pb.FeedResponse()
                while True:
                    message = await websocket.recv()
                    feed_response.ParseFromString(message)
                    process_energy_surge(feed_response)

        except Exception as e:
            print(f"❌ Error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    # 1. Get keys from the ATM table
    if not os.path.exists("atm_option_table.csv"):
        print("Error: atm_option_table.csv not found. Run your generator script first.")
    else:
        df = pd.read_csv("atm_option_table.csv")
        cols = ["atm_plus_2_ce_instrument","atm_minus_2_pe_instrument"]
        keys = list(set(df[cols].values.flatten().tolist()))
        keys = [str(x) for x in keys if str(x) != 'nan']

        if not keys:
            print("No instrument keys found.")
        else:
            # 2. Build the optimized reverse-lookup map
            INSTRUMENT_MAP = create_optimized_lookup(keys)
            
            # 3. Start the engine
            asyncio.run(fetch_market_data(keys))