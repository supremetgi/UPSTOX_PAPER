# -------------------------------
# Imports
# -------------------------------
import asyncio
import json
import ssl
import time
import os
import requests
import websockets
from dotenv import load_dotenv
from google.protobuf.json_format import MessageToDict

import MarketDataFeedV3_pb2 as pb


# -------------------------------
# ENV
# -------------------------------
load_dotenv()
ACCESS_TOKEN = os.getenv("token")


# -------------------------------
# AUTH
# -------------------------------
def get_market_data_feed_authorize_v3():
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    r = requests.get(url, headers=headers, timeout=5)
    r.raise_for_status()
    return r.json()


# -------------------------------
# PROTOBUF DECODE
# -------------------------------
def decode_protobuf(buffer):
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


# -------------------------------
# MAIN WS LOOP
# -------------------------------
async def fetch_market_data():

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    response = get_market_data_feed_authorize_v3()
    ws_url = response["data"]["authorized_redirect_uri"]

    async with websockets.connect(ws_url, ssl=ssl_context) as websocket:
        print("✅ WebSocket connected")

        # -------------------------------
        # SUBSCRIBE
        # -------------------------------
        sub_payload = {
            "guid": "fast-feed",
            "method": "sub",
            "data": {
                "mode": "option_greeks",
                "instrumentKeys": [
                    "NSE_FO|71471",
                    "NSE_FO|71413",
                    "NSE_EQ|INE079A01024"
                ]
            }
        }

        await websocket.send(json.dumps(sub_payload).encode("utf-8"))
        print("📡 Subscription sent")

        # -------------------------------
        # RECEIVE LOOP
        # -------------------------------
        while True:
            message = await websocket.recv()
            decoded = decode_protobuf(message)
            data_dict = MessageToDict(decoded)

            feeds = data_dict.get("feeds", {})

            for instrument_key, payload in feeds.items():
                try:
                    fl = payload["firstLevelWithGreeks"]

                    # --- extract ltt (epoch ms)
                    ltt_ms = int(fl["ltpc"]["ltt"])

                    # --- fast epoch → HH:MM:SS.mmm
                    sec, ms = divmod(ltt_ms, 1000)
                    t = time.localtime(sec)
                    readable_time = (
                        f"{t.tm_hour:02d}:"
                        f"{t.tm_min:02d}:"
                        f"{t.tm_sec:02d}."
                        f"{ms:03d}"
                    )

                    oi = fl.get("oi", 0)

                    print(f"{readable_time} : {instrument_key} : {oi}")

                except KeyError:
                    # NSE_EQ or partial update → skip silently
                    continue
                except Exception:
                    continue


# -------------------------------
# ENTRY
# -------------------------------
if __name__ == "__main__":
    asyncio.run(fetch_market_data())
