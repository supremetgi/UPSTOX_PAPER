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
WINDOW_TIME = 5.0           # Look back period (seconds)
CHECK_INTERVAL = 0.5        # How often to check for surges (high frequency)
MIN_VAL_THRESHOLD = 100000  # ₹1 Lakh
MIN_PRICE_MOVE = 0      # Minimum price movement

# --- GLOBAL STATE ---
# Instead of a single sum, we store a list of (timestamp, value, price)
# trade_history[key] = [(171234567.1, 50000, 150.5), (171234568.2, 60000, 151.0)]
trade_history = defaultdict(list)
INSTRUMENT_MAP = {}

def get_market_data_feed_authorize_v3():
    headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    r = requests.get(url, headers=headers)
    return r.json()

def create_optimized_lookup(active_keys):
    print("🔄 Building optimized instrument map...")
    if not os.path.exists("companies_only.csv"):
        print("❌ Error: companies_only.csv missing.")
        return {}
        
    master_df = pd.read_csv("companies_only.csv")
    lookup = {}
    active_set = set(active_keys)

    for _, row in master_df.iterrows():
        name = row['name']
        strike = row['strike_price']
        ce_key = str(row['ce_instrument_key'])
        pe_key = str(row['pe_instrument_key'])

        if ce_key in active_set:
            lookup[ce_key] = {"name": name, "strike": strike, "type": "CE"}
        if pe_key in active_set:
            lookup[pe_key] = {"name": name, "strike": strike, "type": "PE"}
            
    print(f"✅ Map Ready: {len(lookup)} active option contracts mapped.")
    return lookup

def process_energy_surge(decoded_pb):
    """Stores every incoming tick with a timestamp for the sliding window."""
    now = time.time()
    for key, feed in decoded_pb.feeds.items():
        if not feed.HasField('firstLevelWithGreeks'): continue
        
        ltpc = feed.firstLevelWithGreeks.ltpc
        price = float(ltpc.ltp)
        qty = float(ltpc.ltq)
        value = price * qty
        
        # We store the timestamp, the trade value, and the current price
        trade_history[key].append((now, value, price))

async def energy_monitor():
    """Checks the 'Sliding Window' for every stock every 0.5 seconds."""
    print("⚡ Sliding Window Monitor Active...")
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()
        
        for key in list(trade_history.keys()):
            # 1. Clean up: Remove trades older than the WINDOW_TIME
            # This keeps only the 'Last 5 Seconds' in the list
            trade_history[key] = [t for t in trade_history[key] if now - t[0] <= WINDOW_TIME]
            
            if not trade_history[key]:
                continue
                
            # 2. Calculate Energy in this sliding window
            current_trades = trade_history[key]
            total_window_value = sum(t[1] for t in current_trades)
            
            # 3. Calculate Price Move in this sliding window
            start_p = current_trades[0][2] # Price 5 seconds ago (or at start of window)
            end_p = current_trades[-1][2]  # Most recent price
            price_change = end_p - start_p
            
            # TRIGGER
            if total_window_value >= MIN_VAL_THRESHOLD and price_change >= MIN_PRICE_MOVE:
                info = INSTRUMENT_MAP.get(key, {"name": key, "strike": "", "type": ""})
                
                # To prevent spamming the same surge every 0.5s, 
                # we clear the history for this key after an alert is fired
                trade_history[key].clear() 

                alert = (
                    f"\n[⚡ SLIDING SURGE] {time.strftime('%H:%M:%S')} | "
                    f"{info['name']} | {info['strike']} {info['type']}\n"
                    f"Value: ₹{total_window_value:,.2f} over last {WINDOW_TIME}s\n"
                    f"Move: {start_p} -> {end_p} ({price_change:+.2f})\n"
                    f"{'-' * 55}"
                )
                print(alert, flush=True)

async def fetch_market_data(instrument_list):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Start the background monitor
    asyncio.create_task(energy_monitor())

    while True:
        try:
            auth_res = get_market_data_feed_authorize_v3()
            ws_url = auth_res["data"]["authorized_redirect_uri"]

            async with websockets.connect(ws_url, ssl=ssl_context, ping_interval=20, ping_timeout=10) as websocket:
                print(f"🚀 Streaming {len(instrument_list)} options...", flush=True)
                
                sub_msg = {
                    "guid": "surge_detect", 
                    "method": "sub", 
                    "data": {"mode": "option_greeks", "instrumentKeys": instrument_list}
                }
                await websocket.send(json.dumps(sub_msg).encode("utf-8"))

                feed_response = pb.FeedResponse()
                while True:
                    message = await websocket.recv()
                    feed_response.ParseFromString(message)
                    process_energy_surge(feed_response)

        except Exception as e:
            print(f"❌ Connection Error: {e}. Reconnecting in 5s...", flush=True)
            await asyncio.sleep(5)

if __name__ == "__main__":
    if not os.path.exists("atm_option_table.csv"):
        print("Error: atm_option_table.csv not found.")
    else:
        df = pd.read_csv("atm_option_table.csv")
        cols = ["atm_plus_2_ce_instrument","atm_minus_2_pe_instrument"]
        keys = list(set(df[cols].values.flatten().tolist()))
        keys = [str(x) for x in keys if str(x) != 'nan']

        if not keys:
            print("No instrument keys found.")
        else:
            INSTRUMENT_MAP = create_optimized_lookup(keys)
            asyncio.run(fetch_market_data(keys))