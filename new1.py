import pandas as pd

# df = pd.read_csv(...) or however you already have it
df = pd.read_csv("atm_option_table.csv")

cols = [
    "underlying_key",
    "atm_plus_2_ce_instrument",
    "atm_minus_2_pe_instrument"
]

instrument_keys = set()

for col in cols:
    instrument_keys.update(df[col].dropna().astype(str))

instrument_keys = list(instrument_keys)

print("Total instruments:", len(instrument_keys))

print(instrument_keys)

# eq = sum(k.contains("NSE_EQ") for k in instrument_keys)
# ce = sum(k.contains("ce") for k in instrument_keys)
# pe = sum(k.contains("pe") for k in instrument_keys)

# print("EQ:", eq, "CE:", ce, "PE:", pe)
