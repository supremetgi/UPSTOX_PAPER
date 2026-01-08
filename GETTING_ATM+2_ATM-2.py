import asyncio
import aiohttp
import async_timeout
import pandas as pd
import numpy as np
import time
from dotenv import load_dotenv
import os


load_dotenv()
TOKEN = os.getenv("token")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}





API_URL = "https://api.upstox.com/v3/market-quote/ltp?instrument_key="
CSV_PATH = "companies_only.csv"

# ===============================
# RATE LIMIT CONFIG
# ===============================
MAX_CONCURRENT_REQUESTS = 10
REQUESTS_PER_SECOND = 40
RETRY_LIMIT = 6

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
rate_lock = asyncio.Lock()
last_request_ts = 0.0

# ===============================
# HARD RATE LIMITER
# ===============================




async def rate_limit():
    global last_request_ts
    async with rate_lock:
        now = time.monotonic()
        wait = max(0, (1 / REQUESTS_PER_SECOND) - (now - last_request_ts))
        if wait > 0:
            await asyncio.sleep(wait)
        last_request_ts = time.monotonic()

# ===============================
# ASYNC LTP FETCH (DETERMINISTIC)
# ===============================
async def fetch_ltp(session, instrument_key):
    url = API_URL + instrument_key

    for attempt in range(RETRY_LIMIT):
        async with semaphore:
            await rate_limit()
            try:
                async with async_timeout.timeout(5):
                    async with session.get(url, headers=HEADERS) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            info = next(iter(data["data"].values()))
                            return instrument_key, info["last_price"]

                        if resp.status in (429, 500, 502, 503):
                            await asyncio.sleep(0.4 + attempt * 0.3)
                            continue

                        return instrument_key, None

            except asyncio.TimeoutError:
                await asyncio.sleep(0.3 + attempt * 0.2)

    return instrument_key, None

async def fetch_all_ltps(keys):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_ltp(session, k) for k in keys]
        results = await asyncio.gather(*tasks)

    return dict(results)

# ===============================
# ATM LOGIC (COLUMN STYLE)
# ===============================
def build_atm_table(df, ltp_map):
    rows = []

    for underlying, g in df.groupby("underlying_key"):
        spot = ltp_map.get(underlying)

        strikes = np.array(sorted(g["strike_price"].unique()))
        if spot is None or len(strikes) < 3:
            rows.append({
                "name": g.iloc[0]["name"],
                "underlying_key": underlying,
                "spot_price": spot,
                "atm_ce_instrument": None,
                "atm_plus_2_ce_instrument": None,
                "atm_minus_2_pe_instrument": None
            })
            continue

        atm_idx = np.abs(strikes - spot).argmin()

        def get_ce(idx):
            if 0 <= idx < len(strikes):
                r = g[g["strike_price"] == strikes[idx]].iloc[0]
                return r["ce_instrument_key"] or None
            return None

        def get_pe(idx):
            if 0 <= idx < len(strikes):
                r = g[g["strike_price"] == strikes[idx]].iloc[0]
                return r["pe_instrument_key"] or None
            return None

        rows.append({
            "name": g.iloc[0]["name"],
            "underlying_key": underlying,
            "spot_price": spot,
            "atm_ce_instrument": get_ce(atm_idx),
            "atm_plus_2_ce_instrument": get_ce(min(atm_idx + 2, len(strikes) - 1)),
            "atm_minus_2_pe_instrument": get_pe(max(atm_idx - 2, 0))
        })

    return pd.DataFrame(rows)

# ===============================
# MAIN
# ===============================
async def main():
    df = pd.read_csv(CSV_PATH)
    underlying_keys = df["underlying_key"].unique().tolist()

    print(f"Fetching LTPs for {len(underlying_keys)} underlyings (rate-limited)...")
    ltp_map = await fetch_all_ltps(underlying_keys)

    fetched = sum(v is not None for v in ltp_map.values())
    print(f"LTP fetched successfully for {fetched}/{len(ltp_map)}")

    final_df = build_atm_table(df, ltp_map)

    final_df.to_csv("atm_option_table.csv", index=False)
    print("Saved → atm_option_table.csv")

if __name__ == "__main__":
    asyncio.run(main())
