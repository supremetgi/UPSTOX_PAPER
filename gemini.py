import asyncio
import json
import ssl
import websockets
import requests
import os
import time
from collections import defaultdict
from dotenv import load_dotenv

# Import the generated Protobuf class
import MarketDataFeedV3_pb2 as pb

# ===============================
# CONFIGURATION & THRESHOLDS
# ===============================
load_dotenv()
ACCESS_TOKEN = os.getenv("token")

# How long to wait to group fragmented orders (Seconds)
FLUSH_INTERVAL = 4.0  
# Minimum total value in the window to trigger an alert (INR)
MIN_SURGE_VALUE = 1000000  # ₹10 Lakhs (Adjust as needed)
# Minimum total OI increase in the window
MIN_OI_ACCUM = 50 

# ===============================
# FAST GLOBAL STATE
# ===============================
last_oi_seen = {}
# The "Shopping Carts" (Buffers)
cart_oi = defaultdict(float)
cart_val = defaultdict(float)
cart_price_start = {}
cart_price_last = defaultdict(float)

# ===============================
# AUTHENTICATION
# ===============================
def get_market_data_feed_authorize_v3():
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception(f"Auth failed: {r.text}")
    return r.json()

# ===============================
# CORE LOGIC: THE AGGREGATOR
# ===============================

def process_fragments_fast(decoded_pb):
    """Processes fragmented messages and adds them to the temporary cart."""
    for key, feed in decoded_pb.feeds.items():
        if not feed.HasField('firstLevelWithGreeks'):
            continue
            
        data = feed.firstLevelWithGreeks
        current_oi = data.oi
        
        # 1. NOISE FILTER: If OI hasn't moved, ignore the tick entirely
        prev_oi = last_oi_seen.get(key)
        if prev_oi is None:
            last_oi_seen[key] = current_oi
            continue
            
        if current_oi == prev_oi:
            continue # Skip - just a price/IV change, no new positions

        # 2. CALCULATE CHANGE
        oi_diff = current_oi - prev_oi
        last_oi_seen[key] = current_oi

        # 3. ACCUMULATE (The Shopping Cart)
        if oi_diff > 0:
            ltpc = data.ltpc
            price = ltpc.ltp
            qty = ltpc.ltq
            
            cart_oi[key] += oi_diff
            cart_val[key] += (float(qty) * float(price))
            cart_price_last[key] = price
            
            if key not in cart_price_start:
                cart_price_start[key] = price

# ===============================
# BACKGROUND TASK: THE ANALYZER (THE FLUSH)
# ===============================
async def checkout_and_analyze():
    """Runs every FLUSH_INTERVAL to check the totals in the cart."""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        
        # Check all active items in the cart
        for key in list(cart_val.keys()):
            total_val = cart_val[key]
            total_oi = cart_oi[key]
            
            # TRIGGER ALERT if total window activity is significant
            if total_val >= MIN_SURGE_VALUE and total_oi >= MIN_OI_ACCUM:
                move = cart_price_last[key] - cart_price_start.get(key, 0)
                sentiment = "BULLISH 🟢" if move >= 0 else "BEARISH 🔴"
                
                print(f"\n[🚨 WHALE DETECTED] {time.strftime('%H:%M:%S')}")
                print(f"Instrument : {key}")
                print(f"Total Value: ₹{total_val:,.2f}")
                print(f"Total OI   : +{int(total_oi)}")
                print(f"Sentiment  : {sentiment} (Price Move: {move:+.2f})")
                print("-" * 45)

        # CLEAR THE CART FOR THE NEXT WINDOW
        cart_oi.clear()
        cart_val.clear()
        cart_price_start.clear()
        cart_price_last.clear()

# ===============================
# MAIN WEBSOCKET WITH RECONNECT
# ===============================
async def fetch_market_data(instrument_list):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Start the analysis task once
    asyncio.create_task(checkout_and_analyze())

    while True: # The Reconnection Loop
        try:
            print(f"Attempting to connect to Upstox Feed...")
            auth_res = get_market_data_feed_authorize_v3()
            ws_url = auth_res["data"]["authorized_redirect_uri"]

            async with websockets.connect(
                ws_url, 
                ssl=ssl_context, 
                ping_interval=20, 
                ping_timeout=10
            ) as websocket:
                
                print(f"✅ Connection Established. Monitoring {len(instrument_list)} stocks.")

                sub_msg = {
                    "guid": "insider_detect",
                    "method": "sub",
                    "data": {
                        "mode": "option_greeks",
                        "instrumentKeys": instrument_list
                    }
                }
                await websocket.send(json.dumps(sub_msg).encode("utf-8"))

                # Reusable response object to save memory
                feed_response = pb.FeedResponse()

                while True:
                    message = await websocket.recv()
                    feed_response.ParseFromString(message)
                    process_fragments_fast(feed_response)

        except websockets.exceptions.ConnectionClosed as e:
            print(f"❌ Connection Closed ({e}). Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"⚠️ Unexpected Error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)

# ===============================
# RUN SCRIPT
# ===============================
if __name__ == "__main__":
    import pandas as pd
    
    # Load your instruments from the CSV
    try:
        df = pd.read_csv("atm_option_table.csv")
        cols = ["atm_plus_2_ce_instrument", "atm_minus_2_pe_instrument"]
        # Flatten and remove NaNs
        keys = list(set(df[cols].values.flatten().tolist()))
        keys = [str(x) for x in keys if str(x) != 'nan']
        
        if not keys:
            print("No instrument keys found in CSV!")
        else:
            asyncio.run(fetch_market_data(keys))
    except FileNotFoundError:
        print("Error: 'atm_option_table.csv' not found.")
    except Exception as e:
        print(f"Main Error: {e}")