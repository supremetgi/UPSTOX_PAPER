import requests
import os
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("token")

INSTRUMENT_KEY = "NSE_EQ|INE079A01024"
INTERVAL = "minutes/1"

# Previous market date (Friday)
FROM_DATE = "2025-12-26"
TO_DATE   = "2025-12-26"

URL = (
    f"https://api.upstox.com/v3/historical-candle/"
    f"{INSTRUMENT_KEY}/{INTERVAL}/{FROM_DATE}/{TO_DATE}"
)

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

def fetch_historical_candles():
    r = requests.get(URL, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()

data = fetch_historical_candles()

candles = data["data"]["candles"]

print(f"Total candles fetched: {len(candles)}")

# Print first 5 candles
print(type(candles),"___",len(candles))
for c in candles:
    print(c)
