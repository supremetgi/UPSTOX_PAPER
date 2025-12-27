import asyncio
import aiohttp
import async_timeout
from dotenv import load_dotenv
import os

load_dotenv()
TOKEN = os.getenv("token")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

UNDERLYING_KEYS_FILE = "underlying_keys.txt"
API_URL = "https://api.upstox.com/v3/market-quote/ltp?instrument_key="

MAX_CONCURRENT_REQUESTS = 40
RETRY_LIMIT = 5

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)


async def fetch_ltp(session, instrument_key):
    """Fetch LTP + previous close + stock name (Rate-limit safe)."""
    url = API_URL + instrument_key

    for attempt in range(RETRY_LIMIT):
        async with semaphore:
            try:
                async with async_timeout.timeout(5):
                    async with session.get(url, headers=HEADERS) as resp:

                        if resp.status == 429:
                            wait_time = 0.5 + attempt * 0.5
                            # print(f"Rate limit hit for {instrument_key}, retrying {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue

                        if resp.status >= 500:
                            await asyncio.sleep(0.3)
                            continue

                        if resp.status != 200:
                            return None

                        data = await resp.json()
                        data_block = data.get("data", {})
                        if not data_block:
                            return None

                        # Extract the dict key e.g. "NSE_EQ:KAYNES"
                        full_key = next(iter(data_block.keys()))
                        info = data_block[full_key]

                        # Extract name after the colon
                        name = full_key.split(":")[1] if ":" in full_key else full_key

                        return {
                            "instrument": instrument_key,
                            "name": name,
                            "last_price": info["last_price"],
                            "prev_close": info["cp"]
                        }

            except asyncio.TimeoutError:
                await asyncio.sleep(0.3)
            except Exception as e:
                print(f"Error for {instrument_key}: {e}")
                await asyncio.sleep(0.3)

    return None


async def main():
    with open(UNDERLYING_KEYS_FILE, "r") as f:
        instruments = [line.strip() for line in f if line.strip()]

    async with aiohttp.ClientSession() as session:
        print(f"Fetching {len(instruments)} stocks asynchronously…")
        tasks = [fetch_ltp(session, ins) for ins in instruments]
        results = await asyncio.gather(*tasks)

    results = [r for r in results if r]


    enriched = []
    for r in results:
        lp = r["last_price"]
        cp = r["prev_close"]
        change = lp - cp
        pct = (change / cp * 100) if cp else 0

        enriched.append({
            "name": r["name"],                # PRINT THE NAME
            "instrument": r["instrument"],
            "last_price": lp,
            "prev_close": cp,
            "change": change,
            "pct_change": pct
        })

    gainers = sorted([x for x in enriched if x["change"] > 0],
                     key=lambda x: x["pct_change"], reverse=True)

    losers = sorted([x for x in enriched if x["change"] < 0],
                    key=lambda x: x["pct_change"])

    print("\n=== TOP 20 GAINERS ===")
    for g in gainers[:20]:
        print(f"{g['name']}  |  {g['pct_change']:.2f}%  |  {g['change']:.2f}")

    print("\n=== TOP 20 LOSERS ===")
    for l in losers[:20]:
        print(f"{l['name']}  |  {l['pct_change']:.2f}%  |  {l['change']:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
