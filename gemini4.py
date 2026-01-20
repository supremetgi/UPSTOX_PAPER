import asyncio
import json
import ssl
import websockets
import requests
import os
import pandas as pd
import time
import pika  # Added for RabbitMQ
from collections import defaultdict
from dotenv import load_dotenv
import MarketDataFeedV3_pb2 as pb

load_dotenv()
ACCESS_TOKEN = os.getenv("token")

# --- ENERGY SETTINGS ---
WINDOW_TIME = 5.0           
CHECK_INTERVAL = 0.5        
MIN_VAL_THRESHOLD = 100000  # ₹1 Lakh
MIN_PRICE_MOVE = 0.00001    

# --- GLOBAL STATE ---
trade_history = defaultdict(list)
INSTRUMENT_MAP = {}
last_trade_info = defaultdict(lambda: {"ltt": 0, "vtt": 0})
data_queue = asyncio.Queue()

# --- RABBITMQ PUBLISHER ---
class RabbitMQPublisher:
    def __init__(self, host='localhost', queue_name='insider_alerts'):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
        except Exception as e:
            print(f"❌ RabbitMQ Connection Error: {e}")

    def publish(self, data):
        if not self.channel or self.channel.is_closed:
            self.connect()
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=json.dumps(data)
            )
        except Exception as e:
            print(f"⚠️ Failed to publish to RabbitMQ: {e}")

# Initialize the publisher
publisher = RabbitMQPublisher()

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

async def queue_worker():
    print("👷 Queue Worker active...")
    while True:
        message = await data_queue.get()
        feed_response = pb.FeedResponse()
        try:
            feed_response.ParseFromString(message)
            now = time.time()

            for key, feed in feed_response.feeds.items():
                if not feed.HasField('firstLevelWithGreeks'): continue
                
                ltpc = feed.firstLevelWithGreeks.ltpc
                vtt = float(feed.firstLevelWithGreeks.vtt) 
                ltt = int(ltpc.ltt) 
                price = float(ltpc.ltp)

                if ltt > last_trade_info[key]["ltt"] or vtt > last_trade_info[key]["vtt"]:
                    if last_trade_info[key]["vtt"] > 0:
                        new_qty = vtt - last_trade_info[key]["vtt"]
                    else:
                        new_qty = float(ltpc.ltq)

                    if new_qty > 0:
                        new_value = price * new_qty
                        trade_history[key].append((now, new_value, price))

                    last_trade_info[key]["ltt"] = ltt
                    last_trade_info[key]["vtt"] = vtt

        except Exception as e:
            print(f"⚠️ Worker Error: {e}")
        finally:
            data_queue.task_done()

async def energy_monitor():
    print("⚡ Sliding Window Monitor + RabbitMQ Active...")
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()
        
        for key in list(trade_history.keys()):
            trade_history[key] = [t for t in trade_history[key] if now - t[0] <= WINDOW_TIME]
            if not trade_history[key]: continue
                
            current_trades = trade_history[key]
            total_window_value = sum(t[1] for t in current_trades)
            
            start_p = current_trades[0][2]
            end_p = current_trades[-1][2]
            price_change = end_p - start_p
            
            if total_window_value >= MIN_VAL_THRESHOLD and abs(price_change) >= MIN_PRICE_MOVE:
                info = INSTRUMENT_MAP.get(key, {"name": key, "strike": "", "type": ""})
                
                # --- CATEGORIZATION LOGIC ---
                if price_change > 0:
                    category = "AGGRESSIVE_BUYING"
                elif abs(price_change) < 0.05: # Stagnant threshold
                    category = "STAGNANT_ABSORPTION"
                else:
                    category = "BULK_SELLING"

                # Prepare Data for Dashboard
                alert_data = {
                    "timestamp": time.strftime('%H:%M:%S'),
                    "ticker": info['name'],
                    "strike": info['strike'],
                    "option_type": info['type'],
                    "value": round(total_window_value, 2),
                    "price_move": round(price_change, 2),
                    "category": category
                }

                # Send to RabbitMQ
                publisher.publish(alert_data)

                # Clear to prevent double-alerting same burst
                trade_history[key].clear() 

                # Keep a minimal console log for safety
                print(f" [📤 SENT] {info['name']} {info['type']} | {category} | ₹{total_window_value:,.0f}")

async def fetch_market_data(instrument_list):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Initialize RabbitMQ connection
    publisher.connect()

    asyncio.create_task(queue_worker())
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

                while True:
                    message = await websocket.recv()
                    data_queue.put_nowait(message)

        except Exception as e:
            print(f"❌ Connection Error: {e}. Reconnecting...", flush=True)
            await asyncio.sleep(5)

if __name__ == "__main__":
    if not os.path.exists("atm_option_table.csv"):
        print("Error: atm_option_table.csv not found.")
    else:
        df = pd.read_csv("atm_option_table.csv")
        cols = ["atm_plus_2_ce_instrument","atm_minus_2_pe_instrument"]
        keys = list(set(df[cols].values.flatten().tolist()))
        keys = [str(x) for x in keys if str(x) != 'nan']

        if keys:
            INSTRUMENT_MAP = create_optimized_lookup(keys)
            asyncio.run(fetch_market_data(keys))