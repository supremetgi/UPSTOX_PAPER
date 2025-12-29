import pandas as pd

# Load the pickle file
df = pd.read_pickle("available_to_trade.pkl")

# Keep ONLY company instruments (exclude indices)
companies_df = df[~df["underlying_key"].str.contains("NSE_INDEX", na=False)]

# Save to CSV
companies_df.to_csv("companies_only.csv", index=False)

print("Saved companies_only.csv")
print("Rows before:", len(df))
print("Rows after :", len(companies_df))
