
import asyncio
import json
import ssl
import websockets
import requests
from google.protobuf.json_format import MessageToDict
import pandas as pd
import MarketDataFeedV3_pb2 as pb
from dotenv import load_dotenv
import os


# ===============================
# LOAD INSTRUMENTS
# ===============================
df_instruments = pd.read_csv("atm_option_table.csv")

# cols = [
#     "underlying_key",
#     "atm_plus_2_ce_instrument",
#     "atm_minus_2_pe_instrument"
# ]


cols = [
    "atm_plus_2_ce_instrument",
    "atm_minus_2_pe_instrument"
]

instrument_keys = set()
for col in cols:
    instrument_keys.update(df_instruments[col].dropna().astype(str))

instrument_keys = list(instrument_keys)


# ===============================
# AUTH
# ===============================
load_dotenv()
ACCESS_TOKEN = os.getenv("token")


def get_market_data_feed_authorize_v3():
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    r = requests.get(url, headers=headers)
    return r.json()


# ===============================
# PROTOBUF DECODER
# ===============================
def decode_protobuf(buffer):
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


# ===============================
# STATE (ONLY WHAT WE NEED)
# ===============================
prev_oi = {}


# ===============================
# CORE LOGIC
# ===============================
def detect_oi_increase(data_dict):
    feeds = data_dict.get("feeds", {})

    for instrument_key, payload in feeds.items():
        flwg = payload.get("firstLevelWithGreeks", {})

        oi = flwg.get("oi")
        ltpc = flwg.get("ltpc", {})

        ltp = ltpc.get("ltp")
        ltq = ltpc.get("ltq")

        # Skip non-option instruments or incomplete ticks
        if oi is None or ltp is None or ltq is None:
            continue

        # First observation
        if instrument_key not in prev_oi:
            prev_oi[instrument_key] = oi
            continue

        # OI increased → new positions opened
        if oi > prev_oi[instrument_key]:
            value = float(ltq) * float(ltp)
            print(f"{instrument_key} | {value:.2f}")

        prev_oi[instrument_key] = oi

    


# ===============================
# WEBSOCKET LOOP
# ===============================
async def fetch_market_data():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    response = get_market_data_feed_authorize_v3()

    async with websockets.connect(
        response["data"]["authorized_redirect_uri"],
        ssl=ssl_context
    ) as websocket:

        await asyncio.sleep(1)

        sub_msg = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "option_greeks",
                "instrumentKeys": instrument_keys
            }
        }

        await websocket.send(json.dumps(sub_msg).encode("utf-8"))

        while True:
            message = await websocket.recv()
            decoded = decode_protobuf(message)
            data_dict = MessageToDict(decoded)

            detect_oi_increase(data_dict)


# ===============================
# RUN
# ===============================
asyncio.run(fetch_market_data())
