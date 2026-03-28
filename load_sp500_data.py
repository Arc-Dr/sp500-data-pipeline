import yfinance as yf
import pandas as pd
import psycopg2
import os

# =========================
# DB CONNECTION
# =========================
conn = psycopg2.connect(os.environ["DB_URL"])
cur = conn.cursor()

# =========================
# GET S&P 500 TICKERS
# =========================
sp = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
tickers = sp['Symbol'].tolist()

# =========================
# DOWNLOAD DATA
# =========================
BATCH_SIZE = 25
start_date = "2022-01-01"

for i in range(0, len(tickers), BATCH_SIZE):
    batch = tickers[i:i+BATCH_SIZE]
    print(f"Downloading {i} to {i+len(batch)}")

    df = yf.download(batch, start=start_date, group_by="ticker", progress=False)

    for t in batch:
        try:
            temp = df[t]["Close"].reset_index()

            for _, row in temp.iterrows():
                cur.execute(
                    """
                    INSERT INTO sp500_prices (date, ticker, value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (date, ticker) DO NOTHING
                    """,
                    (row["Date"], t, row["Close"])
                )

        except:
            pass

    conn.commit()

cur.close()
conn.close()

print("DONE")