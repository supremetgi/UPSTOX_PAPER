# ==============================
# FULL LIVE PYQTGRAPH + UPSTOX
# ==============================

import sys
import asyncio
import json
import ssl
import threading
import queue
from datetime import datetime, timezone
import os
import requests
import websockets

from dotenv import load_dotenv
from google.protobuf.json_format import MessageToDict

from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout
from PyQt5.QtCore import QTimer

import pyqtgraph as pg
import MarketDataFeedV3_pb2 as pb


# ==============================
# ENV SETUP
# ==============================
load_dotenv()
ACCESS_TOKEN = os.getenv("token")

if not ACCESS_TOKEN:
    raise RuntimeError("ACCESS TOKEN not found in .env file")


# ==============================
# THREAD-SAFE QUEUE
# ==============================
data_queue = queue.Queue()


# ==============================
# UPSTOX HELPERS
# ==============================
def get_market_data_feed_authorize_v3():
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    return requests.get(url, headers=headers).json()


def decode_protobuf(buffer):
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


# ==============================
# ASYNC WEBSOCKET
# ==============================
async def fetch_market_data():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    auth = get_market_data_feed_authorize_v3()
    ws_url = auth["data"]["authorized_redirect_uri"]

    async with websockets.connect(ws_url, ssl=ssl_context) as websocket:
        print("✅ WebSocket connected")

        sub_payload = {
            "guid": "live-test",
            "method": "sub",
            "data": {
                "mode": "option_greeks",
                "instrumentKeys": [
                    "NSE_EQ|INE584A01023"  # Ambuja Cement
                ]
            }
        }

        await websocket.send(json.dumps(sub_payload).encode())

        while True:
            msg = await websocket.recv()
            decoded = decode_protobuf(msg)
            data = MessageToDict(decoded)

            if "feeds" not in data:
                continue

            feed = list(data["feeds"].values())[0]
            flwg = feed.get("firstLevelWithGreeks", {})

            try:
                ltp = float(flwg["ltpc"]["ltp"])
                oi = float(flwg.get("oi", 0))
                iv = float(flwg.get("iv", 0))
            except KeyError:
                continue

            ts = datetime.now(timezone.utc).timestamp()

            # Push to queue
            data_queue.put((ts, ltp, oi, iv))


def start_ws():
    asyncio.run(fetch_market_data())


# ==============================
# PYQTGRAPH DASHBOARD
# ==============================
class LiveMarketDashboard(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Live Market Data – PyQtGraph")
        self.resize(1200, 800)

        self.max_points = 1500

        self.time = []
        self.ltp = []
        self.oi = []
        self.iv = []

        pg.setConfigOptions(antialias=True)

        central = QWidget()
        layout = QVBoxLayout(central)

        self.plot_ltp = pg.PlotWidget(title="LTP (Price)")
        self.plot_oi = pg.PlotWidget(title="Open Interest")
        self.plot_iv = pg.PlotWidget(title="Implied Volatility")

        # Share X-axis
        self.plot_oi.setXLink(self.plot_ltp)
        self.plot_iv.setXLink(self.plot_ltp)

        self.curve_ltp = self.plot_ltp.plot(pen=pg.mkPen("y", width=2))
        self.curve_oi = self.plot_oi.plot(pen=pg.mkPen("c", width=2))
        self.curve_iv = self.plot_iv.plot(pen=pg.mkPen("m", width=2))

        for p in (self.plot_ltp, self.plot_oi, self.plot_iv):
            p.showGrid(x=True, y=True)

        layout.addWidget(self.plot_ltp)
        layout.addWidget(self.plot_oi)
        layout.addWidget(self.plot_iv)

        self.setCentralWidget(central)

        # UI refresh timer (NOT data rate)
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_plot)
        self.timer.start(50)  # ~20 FPS

    def update_plot(self):
        updated = False

        while not data_queue.empty():
            ts, ltp, oi, iv = data_queue.get()

            self.time.append(ts)
            self.ltp.append(ltp)
            self.oi.append(oi)
            self.iv.append(iv)
            updated = True

        if not updated:
            return

        # Trim buffers
        self.time = self.time[-self.max_points:]
        self.ltp = self.ltp[-self.max_points:]
        self.oi = self.oi[-self.max_points:]
        self.iv = self.iv[-self.max_points:]

        self.curve_ltp.setData(self.time, self.ltp)
        self.curve_oi.setData(self.time, self.oi)
        self.curve_iv.setData(self.time, self.iv)


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    # Start WebSocket in background thread
    ws_thread = threading.Thread(target=start_ws, daemon=True)
    ws_thread.start()

    # Start Qt app
    app = QApplication(sys.argv)
    window = LiveMarketDashboard()
    window.show()
    sys.exit(app.exec_())
