import yfinance as yf
import pandas as pd
import psycopg2
import os
import requests
from psycopg2.extras import execute_values

# =========================
# DB CONNECTION
# =========================
conn = psycopg2.connect(os.environ["DB_URL"])
cur = conn.cursor()

# =========================
# GET S&P 500 TICKERS (FIXED 403)
# =========================
url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(url, headers=headers)

sp = pd.read_html(response.text)[0]

sp = sp.rename(columns={
    "Symbol": "symbol",
    "Security": "security",
    "GICS Sector": "sector",
    "GICS Sub-Industry": "sub_industry",
    "Headquarters Location": "headquarters",
    "Date added": "date_added",
    "CIK": "cik"
})

sp = sp[[
    "symbol",
    "security",
    "sector",
    "sub_industry",
    "headquarters",
    "date_added",
    "cik"
]]

for _, row in sp.iterrows():
    cur.execute(
        """
        INSERT INTO dim_sp500 (
            symbol, security, sector, sub_industry,
            headquarters, date_added, cik
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol) DO NOTHING
        """,
        tuple(row)
    )

conn.commit()
print("DIM TABLE LOADED")

tickers = sp["symbol"].tolist()

# =========================
# DOWNLOAD DATA
# =========================
BATCH_SIZE = 25
start_date = "2022-01-01"

frames = []

for i in range(0, len(tickers), BATCH_SIZE):
    batch = tickers[i:i+BATCH_SIZE]
    print(f"Downloading {i} to {i+len(batch)}")

    df = yf.download(batch, start=start_date, group_by="ticker", progress=False)

    for t in batch:
        try:
            temp = df[t]["Close"].reset_index()
            temp["ticker"] = t
            temp.columns = ["date", "value", "ticker"]
            frames.append(temp)
        except:
            pass

# =========================
# COMBINE + CLEAN
# =========================
final_df = pd.concat(frames, ignore_index=True)

final_df = final_df.dropna()
final_df["date"] = pd.to_datetime(final_df["date"]).dt.date
final_df = final_df[["date", "value", "ticker"]]

print("Download complete. Starting insert...")

# =========================
# BULK INSERT (FAST)
# =========================
data_tuples = list(final_df.itertuples(index=False, name=None))

execute_values(
    cur,
    """
    INSERT INTO sp500_prices (date, value, ticker)
    VALUES %s
    ON CONFLICT DO NOTHING
    """,
    data_tuples
)

conn.commit()
cur.close()
conn.close()

print("SP500 DATA LOADED 🚀")
