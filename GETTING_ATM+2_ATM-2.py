import asyncio
import json
import ssl
import os
import bisect
import time
import requests
import websockets
import pandas as pd
from collections import defaultdict
from dotenv import load_dotenv
from google.protobuf.json_format import MessageToDict
import MarketDataFeedV3_pb2 as pb

# =========================================================
# 1️⃣ ENV SETUP
# =========================================================
load_dotenv()
ACCESS_TOKEN = os.getenv("token")

CSV_PATH = "companies_only.csv"

# =========================================================
# 2️⃣ LOAD CSV & BUILD FAST LOOKUPS (RUNS ONCE)
# =========================================================
def build_maps(csv_path):
    df = pd.read_csv(csv_path)

    df = df[[
        "underlying_key",
        "asset_symbol",
        "strike_price",
        "ce_instrument_key",
        "pe_instrument_key"
    ]].dropna(subset=["strike_price"])

    option_map = defaultdict(list)   # underlying_key → [(strike, ce, pe)]
    underlying_info = {}             # underlying_key → asset_symbol

    for row in df.itertuples(index=False):
        if row.underlying_key not in underlying_info:
            underlying_info[row.underlying_key] = row.asset_symbol

        option_map[row.underlying_key].append(
            (float(row.strike_price), row.ce_instrument_key, row.pe_instrument_key)
        )

    # sort strikes ONCE
    for k in option_map:
        option_map[k].sort(key=lambda x: x[0])

    return option_map, underlying_info


OPTION_MAP, UNDERLYING_INFO = build_maps(CSV_PATH)
UNDERLYINGS = list(OPTION_MAP.keys())

print(f"Loaded {len(UNDERLYINGS)} underlyings")

# =========================================================
# 3️⃣ ATM FINDER (BINARY SEARCH + INDEX)
# =========================================================
def find_atm_with_index(option_list, spot):
    strikes = [x[0] for x in option_list]
    idx = bisect.bisect_left(strikes, spot)

    if idx == 0:
        return option_list[0], 0
    if idx == len(strikes):
        return option_list[-1], len(strikes) - 1

    before = option_list[idx - 1]
    after = option_list[idx]

    if abs(before[0] - spot) <= abs(after[0] - spot):
        return before, idx - 1
    else:
        return after, idx

# =========================================================
# 4️⃣ RELATIVE STRIKE PICKER (±N ROWS)
# =========================================================
def get_relative_strike(option_list, atm_idx, offset):
    target_idx = atm_idx + offset
    if 0 <= target_idx < len(option_list):
        return option_list[target_idx]
    return None

# =========================================================
# 5️⃣ AUTHORIZE WEBSOCKET
# =========================================================
def get_market_data_feed_authorize_v3():
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

# =========================================================
# 6️⃣ PROTOBUF DECODER
# =========================================================
def decode_protobuf(buffer):
    msg = pb.FeedResponse()
    msg.ParseFromString(buffer)
    return msg

# =========================================================
# 7️⃣ MAIN LIVE LOOP
# =========================================================
async def fetch_market_data():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    auth = get_market_data_feed_authorize_v3()
    ws_url = auth["data"]["authorized_redirect_uri"]

    async with websockets.connect(ws_url, ssl=ssl_context) as ws:
        print("WebSocket connected")

        sub_msg = {
            "guid": "atm-relative-feed",
            "method": "sub",
            "data": {
                "mode": "ltpc",
                "instrumentKeys": UNDERLYINGS
            }
        }

        await ws.send(json.dumps(sub_msg).encode())
        print("Subscribed to spot LTPs")

        last_atm = {}

        while True:
            raw = await ws.recv()
            decoded = decode_protobuf(raw)
            data = MessageToDict(decoded)

            feeds = data.get("feeds", {})

            for underlying_key, feed in feeds.items():
                try:
                    spot = feed["ltpc"]["ltp"]
                except KeyError:
                    continue

                if underlying_key not in OPTION_MAP:
                    continue

                asset = UNDERLYING_INFO[underlying_key]

                (atm_strike, atm_ce, atm_pe), atm_idx = find_atm_with_index(
                    OPTION_MAP[underlying_key],
                    spot
                )

                plus_2 = get_relative_strike(OPTION_MAP[underlying_key], atm_idx, +2)
                minus_2 = get_relative_strike(OPTION_MAP[underlying_key], atm_idx, -2)

                # Print ONLY when ATM changes
                if last_atm.get(underlying_key) != atm_strike:
                    last_atm[underlying_key] = atm_strike

                    print(f"{time.strftime('%H:%M:%S')} | {asset}")
                    print(f"  Spot      : {spot:.2f}")
                    print(f"  ATM       : {atm_strike} | CE={atm_ce} | PE={atm_pe}")

                    if plus_2:
                        print(f"  ATM + 2   : {plus_2[0]} | CE={plus_2[1]} | PE={plus_2[2]}")
                    else:
                        print("  ATM + 2   : N/A")

                    if minus_2:
                        print(f"  ATM - 2   : {minus_2[0]} | CE={minus_2[1]} | PE={minus_2[2]}")
                    else:
                        print("  ATM - 2   : N/A")

                    print("-" * 80)

# =========================================================
# 8️⃣ RUN
# =========================================================
if __name__ == "__main__":
    asyncio.run(fetch_market_data())
