# ==========================================
# LIVE EQ PRICE + FO OI DASHBOARD (STABLE)
# ==========================================

import sys
import asyncio
import json
import ssl
import threading
import queue
import os
from datetime import datetime, timezone

import requests
import websockets
import numpy as np

from dotenv import load_dotenv
from google.protobuf.json_format import MessageToDict

from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout
from PyQt5.QtCore import QTimer

import pyqtgraph as pg
import MarketDataFeedV3_pb2 as pb


# ==========================================
# CONFIG
# ==========================================
EQ_KEY = "NSE_EQ|INE066F01020"
FO_1 = "NSE_FO|91614"
FO_2 = "NSE_FO|91611"

load_dotenv()
ACCESS_TOKEN = os.getenv("token")
if not ACCESS_TOKEN:
    raise RuntimeError("ACCESS TOKEN missing")


# ==========================================
# THREAD-SAFE QUEUE
# ==========================================
data_queue = queue.Queue()


# ==========================================
# UPSTOX HELPERS
# ==========================================
def get_market_data_feed_authorize_v3():
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    return requests.get(url, headers=headers, timeout=10).json()


def decode_protobuf(buffer):
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


# ==========================================
# ASYNC WEBSOCKET (WITH KEEPALIVE + RECONNECT)
# ==========================================
async def fetch_market_data():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    while True:  # 🔁 reconnect loop
        try:
            auth = get_market_data_feed_authorize_v3()
            ws_url = auth["data"]["authorized_redirect_uri"]

            async with websockets.connect(
                ws_url,
                ssl=ssl_context,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10
            ) as websocket:

                print("✅ WebSocket connected")

                sub_payload = {
                    "guid": "live-dashboard",
                    "method": "sub",
                    "data": {
                        "mode": "option_greeks",
                        "instrumentKeys": [EQ_KEY, FO_1, FO_2]
                    }
                }

                await websocket.send(json.dumps(sub_payload).encode())

                while True:
                    msg = await websocket.recv()
                    decoded = decode_protobuf(msg)
                    data = MessageToDict(decoded)

                    if "feeds" not in data:
                        continue

                    ts = float(datetime.now(timezone.utc).timestamp())

                    for ins, feed in data["feeds"].items():
                        flwg = feed.get("firstLevelWithGreeks", {})

                        if ins == EQ_KEY:
                            try:
                                price = float(flwg["ltpc"]["ltp"])
                                data_queue.put((ts, "EQ", price))
                            except Exception:
                                pass

                        elif ins == FO_1:
                            oi = flwg.get("oi")
                            if oi is not None:
                                data_queue.put((ts, "FO1", float(oi)))

                        elif ins == FO_2:
                            oi = flwg.get("oi")
                            if oi is not None:
                                data_queue.put((ts, "FO2", float(oi)))

        except Exception as e:
            print(f"⚠ WebSocket error: {e}")
            print("🔁 Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


def start_ws():
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_market_data())


# ==========================================
# PYQTGRAPH DASHBOARD
# ==========================================
class LiveDashboard(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("EQ Price + FO OI (Live)")
        self.resize(1200, 800)

        self.max_points = 2000

        self.time = []
        self.eq_price = []
        self.fo1_oi = []
        self.fo2_oi = []

        # IMPORTANT: numeric defaults
        self.last_eq = np.nan
        self.last_fo1 = np.nan
        self.last_fo2 = np.nan

        pg.setConfigOptions(antialias=True)

        central = QWidget()
        layout = QVBoxLayout(central)

        self.p_eq = pg.PlotWidget(title="EQ PRICE")
        self.p_fo1 = pg.PlotWidget(title="FO1 OI")
        self.p_fo2 = pg.PlotWidget(title="FO2 OI")

        self.p_fo1.setXLink(self.p_eq)
        self.p_fo2.setXLink(self.p_eq)

        self.c_eq = self.p_eq.plot(pen=pg.mkPen("y", width=2))
        self.c_fo1 = self.p_fo1.plot(pen=pg.mkPen("c", width=2))
        self.c_fo2 = self.p_fo2.plot(pen=pg.mkPen("m", width=2))

        for p in (self.p_eq, self.p_fo1, self.p_fo2):
            p.showGrid(x=True, y=True)

        layout.addWidget(self.p_eq)
        layout.addWidget(self.p_fo1)
        layout.addWidget(self.p_fo2)

        self.setCentralWidget(central)

        self.timer = QTimer()
        self.timer.timeout.connect(self.update_plot)
        self.timer.start(50)   # UI refresh ~20 FPS

    def update_plot(self):
        updated = False

        while not data_queue.empty():
            ts, typ, val = data_queue.get()

            self.time.append(float(ts))

            if typ == "EQ":
                self.last_eq = float(val)
            elif typ == "FO1":
                self.last_fo1 = float(val)
            elif typ == "FO2":
                self.last_fo2 = float(val)

            # forward-fill
            self.eq_price.append(self.last_eq)
            self.fo1_oi.append(self.last_fo1)
            self.fo2_oi.append(self.last_fo2)

            updated = True

        if not updated:
            return

        # trim buffers
        self.time = self.time[-self.max_points:]
        self.eq_price = self.eq_price[-self.max_points:]
        self.fo1_oi = self.fo1_oi[-self.max_points:]
        self.fo2_oi = self.fo2_oi[-self.max_points:]

        self.c_eq.setData(self.time, self.eq_price)
        self.c_fo1.setData(self.time, self.fo1_oi)
        self.c_fo2.setData(self.time, self.fo2_oi)


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    ws_thread = threading.Thread(target=start_ws, daemon=True)
    ws_thread.start()

    app = QApplication(sys.argv)
    win = LiveDashboard()
    win.show()
    sys.exit(app.exec_())
