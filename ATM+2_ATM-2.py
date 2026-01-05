import asyncio
import aiohttp
import async_timeout
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

# ===============================
# ENV
# ===============================
load_dotenv()
TOKEN = os.getenv("token")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

API_URL = "https://api.upstox.com/v3/market-quote/ltp?instrument_key="
MAX_CONCURRENT_REQUESTS = 40
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

CSV_PATH = "available_to_trade.csv"

# ===============================
# ASYNC LTP FETCH
# ===============================
async def fetch_ltp(session, instrument_key):
    async with semaphore:
        try:
            async with async_timeout.timeout(5):
                async with session.get(API_URL + instrument_key, headers=HEADERS) as r:
                    if r.status != 200:
                        return instrument_key, None
                    data = await r.json()
                    info = next(iter(data["data"].values()))
                    return instrument_key, info["last_price"]
        except:
            return instrument_key, None


async def fetch_all_ltps(keys):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_ltp(session, k) for k in keys]
        results = await asyncio.gather(*tasks)
    return dict(results)

# ===============================
# ATM LOGIC
# ===============================
def get_atm_bundle(df, ltp_map):
    output = []

    for underlying, g in df.groupby("underlying_key"):
        spot = ltp_map.get(underlying)
        if spot is None:
            continue

        strikes = np.array(sorted(g["strike_price"].unique()))
        atm_idx = np.abs(strikes - spot).argmin()
        atm_strike = strikes[atm_idx]

        ce_idx = atm_idx + 2
        pe_idx = atm_idx - 2

        atm_row = g[g["strike_price"] == atm_strike].iloc[0]

        # ATM CE
        if atm_row["ce_instrument_key"]:
            output.append({
                "type": "ATM_CE",
                "name": atm_row["name"],
                "strike": atm_strike,
                "instrument": atm_row["ce_instrument_key"],
                "spot": spot
            })

        # ATM + 2 CE
        if ce_idx < len(strikes):
            ce_row = g[g["strike_price"] == strikes[ce_idx]].iloc[0]
            if ce_row["ce_instrument_key"]:
                output.append({
                    "type": "ATM_PLUS_2_CE",
                    "name": ce_row["name"],
                    "strike": strikes[ce_idx],
                    "instrument": ce_row["ce_instrument_key"],
                    "spot": spot
                })

        # ATM - 2 PE
        if pe_idx >= 0:
            pe_row = g[g["strike_price"] == strikes[pe_idx]].iloc[0]
            if pe_row["pe_instrument_key"]:
                output.append({
                    "type": "ATM_MINUS_2_PE",
                    "name": pe_row["name"],
                    "strike": strikes[pe_idx],
                    "instrument": pe_row["pe_instrument_key"],
                    "spot": spot
                })

    return pd.DataFrame(output)

# ===============================
# MAIN
# ===============================
async def main():
    df = pd.read_csv(CSV_PATH)

    underlying_keys = df["underlying_key"].unique().tolist()

    print(f"Fetching LTPs for {len(underlying_keys)} underlyings...")
    ltp_map = await fetch_all_ltps(underlying_keys)

    result_df = get_atm_bundle(df, ltp_map)

    print("\n=== ATM OPTION SELECTION ===")
    print(result_df)

    result_df.to_csv("atm_selection.csv", index=False)
    print("\nSaved to atm_selection.csv")


if __name__ == "__main__":
    asyncio.run(main())
