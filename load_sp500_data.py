import yfinance as yf
import pandas as pd
import psycopg2
import os
import requests
from psycopg2.extras import execute_values
from urllib.parse import urlparse

# =========================
# DB CONNECTION
# =========================
conn = psycopg2.connect(os.environ["DB_URL"])
cur = conn.cursor()

# =========================
# GET S&P 500 TICKERS
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

# =========================
# INSERT DIM TABLE
# =========================
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
# SHARES DATA
# =========================
print("Loading shares...")

shares_data = []

for t in tickers:
    try:
        info = yf.Ticker(t).fast_info
        shares = info.get("shares")

        if shares:
            shares_data.append((t, int(shares)))
    except:
        pass

for row in shares_data:
    cur.execute(
        """
        INSERT INTO sp500_shares (symbol, shares, date)
        VALUES (%s, %s, CURRENT_DATE)
        ON CONFLICT (symbol)
        DO UPDATE SET shares = EXCLUDED.shares
        """,
        row
    )

conn.commit()
print("SHARES LOADED")

# =========================
# PROFILE DATA + LOGO
# =========================
print("Loading company profiles...")

profile_data = []

API_KEY = "YOUR_API_KEY"

for t in tickers:
    try:
        info = yf.Ticker(t).info

        website = info.get("website")

        # -------- LOGO LOGIC --------
        logo = None

        if website:
            try:
                domain = urlparse(website).netloc

                if domain.startswith("www."):
                    domain = domain.replace("www.", "")

                logo = f"https://img.logo.dev/{domain}?token={API_KEY}"

            except:
                logo = None

        # fallback if website missing
        if not logo:
            try:
                domain = t.lower() + ".com"
                logo = f"https://img.logo.dev/{domain}?token={API_KEY}"
            except:
                logo = None

        # -------- CEO --------
        ceo_name = None
        officers = info.get("companyOfficers")
        if officers:
            ceo_name = officers[0].get("name")

        profile_data.append((
            t,
            info.get("longName"),
            info.get("sector"),
            info.get("industry"),
            info.get("country"),
            info.get("city"),
            info.get("state"),
            info.get("zip"),
            info.get("address1"),
            info.get("phone"),
            website,
            logo,
            ceo_name,
            info.get("fullTimeEmployees"),
            info.get("longBusinessSummary"),
            info.get("exchange"),
            info.get("currency"),
            info.get("quoteType")
        ))

    except:
        pass

# =========================
# INSERT PROFILE
# =========================
execute_values(
    cur,
    """
    INSERT INTO dim_sp500_profile (
        symbol, company_name, sector, industry,
        country, city, state, zip, address,
        phone, website, logo, ceo, employees,
        business_summary, exchange, currency, quote_type
    )
    VALUES %s
    ON CONFLICT (symbol)
    DO UPDATE SET
        company_name = EXCLUDED.company_name,
        sector = EXCLUDED.sector,
        industry = EXCLUDED.industry,
        website = EXCLUDED.website,
        logo = EXCLUDED.logo,
        ceo = EXCLUDED.ceo
    """,
    profile_data
)

conn.commit()
print("PROFILE TABLE LOADED")

# =========================
# PRICE DATA
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
# FINAL CLEAN
# =========================
final_df = pd.concat(frames, ignore_index=True)

final_df = final_df.dropna()
final_df["date"] = pd.to_datetime(final_df["date"]).dt.date
final_df = final_df[["date", "value", "ticker"]]

print("Download complete. Starting insert...")

# =========================
# INSERT PRICES
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
