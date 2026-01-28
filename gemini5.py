import asyncio
import json
import ssl
import websockets
import requests
import os
import pandas as pd
import time
import pika
from collections import defaultdict
from dotenv import load_dotenv
import MarketDataFeedV3_pb2 as pb

load_dotenv()
ACCESS_TOKEN = os.getenv("token")

# --- ENERGY SETTINGS ---
WINDOW_TIME = 3.0           
CHECK_INTERVAL = 0.5        
MIN_VAL_THRESHOLD = 100000  # ₹1 Lakh
MIN_PRICE_MOVE = 0.00001    

# --- GLOBAL STATE ---
trade_history = defaultdict(list)
INSTRUMENT_MAP = {}
# Track OI globally so we have a value even if a specific tick misses it
last_trade_info = defaultdict(lambda: {"ltt": 0, "vtt": 0, "oi": 0})
data_queue = asyncio.Queue()
alert_queue = asyncio.Queue()  

# --- ASYNC RABBITMQ WORKER ---
class RabbitMQWorker:
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
            return True
        except Exception as e:
            print(f"❌ RabbitMQ Connection Error: {e}", flush=True)
            return False

    async def run(self):
        print("📮 Alert Worker (RabbitMQ) started...", flush=True)
        if not self.connect():
            print("⚠️ RabbitMQ offline. Alerts will be logged but not sent.", flush=True)

        while True:
            alert_data = await alert_queue.get()
            try:
                if self.channel and not self.channel.is_closed:
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=self.queue_name,
                        body=json.dumps(alert_data)
                    )
            except Exception as e:
                print(f"⚠️ Failed to publish: {e}. Attempting reconnect...", flush=True)
                self.connect()
            finally:
                alert_queue.task_done()

mq_worker = RabbitMQWorker()

def get_market_data_feed_authorize_v3():
    headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    r = requests.get(url, headers=headers)
    return r.json()

def create_optimized_lookup(active_keys):
    print("🔄 Building optimized instrument map...", flush=True)
    if not os.path.exists("companies_only.csv"):
        return {}
    master_df = pd.read_csv("companies_only.csv")
    lookup = {}
    active_set = set(active_keys)
    for _, row in master_df.iterrows():
        name, strike = row['name'], row['strike_price']
        ce_key, pe_key = str(row['ce_instrument_key']), str(row['pe_instrument_key'])
        if ce_key in active_set: lookup[ce_key] = {"name": name, "strike": strike, "type": "CE"}
        if pe_key in active_set: lookup[pe_key] = {"name": name, "strike": strike, "type": "PE"}
    return lookup

async def queue_worker():
    while True:
        message = await data_queue.get()
        feed_response = pb.FeedResponse()
        try:
            feed_response.ParseFromString(message)
            now = time.time()
            for key, feed in feed_response.feeds.items():
                if not feed.HasField('firstLevelWithGreeks'): continue
                
                flwg = feed.firstLevelWithGreeks
                ltpc = flwg.ltpc
                vtt, ltt, price = float(flwg.vtt), int(ltpc.ltt), float(ltpc.ltp)
                
                # FIXED: OI is directly under flwg, not inside optionGreeks
                current_oi = float(flwg.oi) if hasattr(flwg, 'oi') else last_trade_info[key]["oi"]

                if ltt > last_trade_info[key]["ltt"] or vtt > last_trade_info[key]["vtt"]:
                    new_qty = vtt - last_trade_info[key]["vtt"] if last_trade_info[key]["vtt"] > 0 else float(ltpc.ltq)
                    if new_qty > 0:
                        # Append time, value, price, and OI to history
                        trade_history[key].append((now, price * new_qty, price, current_oi))
                    
                    last_trade_info[key].update({"ltt": ltt, "vtt": vtt, "oi": current_oi})
        except Exception:
            pass # Silent fail to prevent log spamming
        finally:
            data_queue.task_done()

async def energy_monitor():
    print("⚡ Real-time Monitor Active (Unbuffered Logs with OI)...", flush=True)
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()
        for key in list(trade_history.keys()):
            trade_history[key] = [t for t in trade_history[key] if now - t[0] <= WINDOW_TIME]
            if not trade_history[key]: continue
                
            current_trades = trade_history[key]
            val, change = sum(t[1] for t in current_trades), current_trades[-1][2] - current_trades[0][2]
            
            # Get latest OI snapshot from the trade history
            latest_oi = current_trades[-1][3]
            
            if val >= MIN_VAL_THRESHOLD:
                info = INSTRUMENT_MAP.get(key, {"name": key, "strike": "", "type": ""})
                
                # Original category logic preserved exactly
                category = "AGGRESSIVE_BUYING" if change > 0 else "BULK_SELLING"
                if abs(change) < 0.05: category = "STAGNANT_ABSORPTION"

                alert_data = {
                    "timestamp": time.strftime('%H:%M:%S'),
                    "ticker": info['name'], "strike": info['strike'], "option_type": info['type'],
                    "value": round(val, 2), "price_move": round(change, 2), 
                    "category": category, "oi": latest_oi 
                }

                alert_queue.put_nowait(alert_data)
                
                # Terminal print with OI
                print(f" [📤 SENT] {info['name']} {info['type']} | {category} | ₹{val:,.0f} | OI: {latest_oi:,.0f} | {time.strftime('%H:%M:%S')}", flush=True)
                trade_history[key].clear() 

async def fetch_market_data(instrument_list):
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = ssl_ctx.verify_mode = False
    
    asyncio.create_task(queue_worker())
    asyncio.create_task(energy_monitor())
    asyncio.create_task(mq_worker.run())

    while True:
        try:
            auth_res = get_market_data_feed_authorize_v3()
            async with websockets.connect(auth_res["data"]["authorized_redirect_uri"], ssl=ssl_ctx) as ws:
                print(f"🚀 Streaming {len(instrument_list)} options...", flush=True)
                # Ensure mode is set to 'option_greeks' to get OI data
                sub_msg = {"guid": "surge", "method": "sub", "data": {"mode": "option_greeks", "instrumentKeys": instrument_list}}
                await ws.send(json.dumps(sub_msg).encode("utf-8"))
                while True:
                    data_queue.put_nowait(await ws.recv())
        except Exception as e:
            print(f"❌ Connection Error: {e}. Reconnecting...", flush=True)
            await asyncio.sleep(5)

if __name__ == "__main__":
    df = pd.read_csv("atm_option_table.csv")
    cols = ["atm_plus_2_ce_instrument","atm_minus_2_pe_instrument"]
    keys = [str(x) for x in set(df[cols].values.flatten().tolist()) if str(x) != 'nan']
    if keys:
        INSTRUMENT_MAP = create_optimized_lookup(keys)
        asyncio.run(fetch_market_data(keys))