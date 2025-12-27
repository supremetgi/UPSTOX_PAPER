import asyncio
import aiohttp
import pandas as pd
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

semaphore = asyncio.Semaphore(5)



async def fetch_ltp(session, instrument_key, retries=3):
    async with semaphore:
        for attempt in range(retries):
            try:
                async with session.get(f"{API_URL}{instrument_key}") as resp:
                    js = await resp.json()

                    # ❌ rate limit or error
                    if js.get("status") != "success":
                        if attempt < retries - 1:
                            await asyncio.sleep(0.3 * (attempt + 1))  # backoff
                            continue
                        return instrument_key, None

                    _, data = next(iter(js["data"].items()))
                    return instrument_key, data["last_price"]

            except Exception:
                if attempt == retries - 1:
                    return instrument_key, None




async def fetch_all_ltps(underlying_keys):
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = [fetch_ltp(session, k) for k in underlying_keys]
        results = await asyncio.gather(*tasks)

        # remove failed ones
        return {k: v for k, v in results if v is not None}





def find_atm_rows(df, ltp_map):
    atm_rows = []

    for underlying, ltp in ltp_map.items():
        sub = df[df["underlying_key"] == underlying].copy()
        sub["atm_diff"] = (sub["strike_price"] - ltp).abs()
        atm_rows.append(sub.loc[sub["atm_diff"].idxmin()])

    return pd.DataFrame(atm_rows)







async def main(df):
    underlying_keys = df["underlying_key"].unique().tolist()[:50]

    ltp_map = await fetch_all_ltps(underlying_keys)

    atm_df = find_atm_rows(df, ltp_map)

    print("\n===== ATM STRIKES =====\n")
    print(atm_df)





if __name__ == "__main__":
    df = pd.read_csv("available_to_trade.csv")
    print(len(df))
    asyncio.run(main(df))








