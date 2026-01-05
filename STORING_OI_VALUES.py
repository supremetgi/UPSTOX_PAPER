# Import necessary modules
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


df_instruments = pd.read_csv("atm_option_table.csv")
cols = [
    "underlying_key",
    "atm_plus_2_ce_instrument",
    "atm_minus_2_pe_instrument"
]


instrument_keys = set()

for col in cols:
    instrument_keys.update(df_instruments[col].dropna().astype(str))

instrument_keys = list(instrument_keys)

load_dotenv()
ACCESS_TOKEN = os.getenv("token")




def get_market_data_feed_authorize_v3():
    access_token = ACCESS_TOKEN
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
    api_response =   requests.get(url=url, headers=headers)
    print(api_response)
    return api_response.json()


def decode_protobuf(buffer):
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response



async def fetch_market_data():
  
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    response =   get_market_data_feed_authorize_v3()
    async with websockets.connect(response["data"]["authorized_redirect_uri"], ssl=ssl_context) as websocket:
        print('Connection established')

        await asyncio.sleep(1)  
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "option_greeks",
                "instrumentKeys": instrument_keys
            }
        }

       
        binary_data = json.dumps(data).encode('utf-8')
        await websocket.send(binary_data)

        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)

            data_dict = MessageToDict(decoded_data)
            print("\n\n\n\n\n**************************************************************\n\n\n")
            print(json.dumps(data_dict))





asyncio.run(fetch_market_data())
print('something')