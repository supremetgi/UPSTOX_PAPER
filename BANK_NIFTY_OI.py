import asyncio
import json
import ssl
import websockets
import requests
from google.protobuf.json_format import MessageToDict

import MarketDataFeedV3_pb2 as pb
from dotenv import load_dotenv
import os

load_dotenv()
ACCESS_TOKEN = os.getenv("token")




def get_market_data_feed_authorize_v3():
    """Get authorization for market data feed."""
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
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

#   PRICE:"CE_CODE,PE_CODE"
# price_dict = {
#     58700:"51395,51396",
#     58800:"51397,51398",
#     58900:"51412,51413",
#     59000:"51414,51415",
#     59100:"51416,51417",
#     59200:"51420,51421",
#     59300:"51439,51440",
# }




async def fetch_market_data():
    """Fetch market data using WebSocket and print it."""

    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Get market data feed authorization
    response =   get_market_data_feed_authorize_v3()
    # Connect to the WebSocket with SSL context
    async with websockets.connect(response["data"]["authorized_redirect_uri"], ssl=ssl_context) as websocket:
        print('Connection established')

        await asyncio.sleep(1)  # Wait for 1 second

        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "option_greeks",
                # "instrumentKeys": ['NSE_EQ|INE466L01038', 'NSE_EQ|INE117A01022', 'NSE_EQ|INE931S01010', 'NSE_EQ|INE423A01024', 'NSE_EQ|INE364U01010', 'NSE_EQ|INE742F01042', 'NSE_EQ|INE674K01013', 'NSE_EQ|INE540L01014', 'NSE_EQ|INE371P01015', 'NSE_EQ|INE079A01024', 'NSE_EQ|INE732I01013', 'NSE_EQ|INE702C01027', 'NSE_EQ|INE437A01024', 'NSE_EQ|INE208A01029', 'NSE_EQ|INE021A01026', 'NSE_EQ|INE006I01046', 'NSE_EQ|INE949L01017', 'NSE_EQ|INE406A01037', 'NSE_EQ|INE192R01011', 'NSE_EQ|INE238A01034', 'NSE_EQ|INE917I01010', 'NSE_EQ|INE296A01032', 'NSE_EQ|INE918I01026', 'NSE_EQ|INE545U01014']
                "instrumentKeys":["NSE_FO|134090"]
            }
        }




        binary_data = json.dumps(data).encode('utf-8')
        await websocket.send(binary_data)

        # Continuously receive and decode data from WebSocket
        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)
            
            # Convert the decoded data to a dictionary
            data_dict = MessageToDict(decoded_data)
            print("\n\n\n\n\n**************************************************************\n\n\n")
            # print(data_dict)
            try:
                # feeds = data_dict['feeds']
                # for k,v in feeds.items():
                #     print(k,":",v['ltpc']['ltp'])
        

                # oi = feed_data['firstLevelWithGreeks']['oi']

                # print("Instrument:", instrument_key)
                # print("OI:", oi)

                print(json.dumps(data_dict))
            except:
                continue





# Execute the function to fetch market data
asyncio.run(fetch_market_data())
print('something')