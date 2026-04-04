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
# PROFILE DATA TABLE
# =========================
print("Loading company profiles...")

profile_data = []

for t in tickers:
    try:
        info = yf.Ticker(t).info

        website = info.get("website")

        # LOGO LOGIC
        logo = info.get("logo_url")
        if not logo and website:
            clean_site = website.replace("https://", "").replace("http://", "")
            logo = f"https://logo.clearbit.com/{clean_site}"

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

# BULK INSERT
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

print("SP500 DATA LOADED 🚀")

# =========================
# CUSTOM STOCK PRICE TABLE (IPO → TODAY)
# =========================
print("Loading custom stock prices...")

custom_tickers = ['UNP','X','PG','MMM','ITW']

frames = []

for t in custom_tickers:
    try:
        df = yf.download(t, start="1900-01-01", progress=False)

        temp = df['Close'].reset_index()
        temp['ticker'] = t
        temp.columns = ['date','value','ticker']

        frames.append(temp)

    except:
        pass

custom_df = pd.concat(frames, ignore_index=True)
custom_df = custom_df.dropna()

custom_df['date'] = pd.to_datetime(custom_df['date']).dt.date

# IPO + % CHANGE
custom_df = custom_df.sort_values(['ticker','date'])
custom_df['ipo_price'] = custom_df.groupby('ticker')['value'].transform('first')
custom_df['pct_change'] = (custom_df['value'] - custom_df['ipo_price']) / custom_df['ipo_price']

# INSERT
execute_values(
    cur,
    """
    INSERT INTO custom_stock_prices (date, value, ticker, ipo_price, pct_change)
    VALUES %s
    ON CONFLICT DO NOTHING
    """,
    list(custom_df[['date','value','ticker','ipo_price','pct_change']].itertuples(index=False, name=None))
)

conn.commit()
print("CUSTOM PRICE TABLE LOADED")

# =========================
# CUSTOM STOCK PROFILE TABLE (FULL LIKE MAIN)
# =========================
print("Loading custom stock profiles...")

profile_rows = []

for t in custom_tickers:
    try:
        info = yf.Ticker(t).info

        website = info.get("website")

        # LOGO LOGIC (same as your main script)
        logo = info.get("logo_url")
        if not logo and website:
            clean_site = website.replace("https://", "").replace("http://", "")
            logo = f"https://logo.clearbit.com/{clean_site}"

        ceo_name = None
        officers = info.get("companyOfficers")
        if officers:
            ceo_name = officers[0].get("name")

        profile_rows.append((
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

# BULK INSERT
execute_values(
    cur,
    """
    INSERT INTO custom_stock_profile (
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
    profile_rows
)

conn.commit()
print("CUSTOM PROFILE TABLE LOADED")

cur.close()
conn.close()

