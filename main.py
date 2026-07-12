import os
import logging
import sqlite3
import requests
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import threading
import feedparser
import re
import pandas as pd
from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# =============================================
# 1. YMPÄRISTÖMUUTTUJAT
# =============================================
TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", 10000))

# Trading 212 API -avaimet (pakolliset live-toiminnoille, ei käytetä enää
# tulevien osinkojen hakuun, koska T212 API:ssa ei ole ko. endpointtia)
T212_API_KEY = os.environ.get("T212_API_KEY")
T212_API_SECRET = os.environ.get("T212_API_SECRET")

# =============================================
# 2. LOGGING
# =============================================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# =============================================
# 3. TIETOKANTA
# =============================================
def init_db():
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

init_db()

USER_ID = None

# =============================================
# 4. PORTFOLIO HOLDINGS
# (Päivitetty Trading 212 "Confirmation of holdings" -asiakirjasta, 10.07.2026)
# =============================================

# ETFs (18 holdings) — hinnat EUR
ETF_HOLDINGS = [
    {"isin": "IE00B5BMR087", "symbol": "SPY5L",  "name": "iShares Core S&P 500 UCITS ETF",                      "quantity": 1.3195215,   "price": 711.48},
    {"isin": "IE00BFMXXD54", "symbol": "VUAA",   "name": "Vanguard S&P 500 UCITS ETF",                          "quantity": 6.78430694,  "price": 127.59},
    {"isin": "IE00B4L5Y983", "symbol": "IWDA",   "name": "iShares Core MSCI World UCITS ETF",                   "quantity": 7.86059909,  "price": 126.145},
    {"isin": "IE00BK5BQT80", "symbol": "VWRA",   "name": "Vanguard FTSE All-World UCITS ETF",                   "quantity": 3.95788178,  "price": 166.14},
    {"isin": "IE00B53SZB19", "symbol": "CNDX",   "name": "iShares NASDAQ 100 UCITS ETF",                        "quantity": 0.551902,    "price": 1493.8},
    {"isin": "IE000XZSV718", "symbol": "SPY5",   "name": "SPDR S&P 500 UCITS ETF",                              "quantity": 35.45098256, "price": 16.3422},
    {"isin": "IE00B3XXRP09", "symbol": "VUSA",   "name": "Vanguard S&P 500 UCITS ETF",                          "quantity": 5.07180738,  "price": 125.226},
    {"isin": "IE0031442068", "symbol": "IUSA",   "name": "iShares Core S&P 500 UCITS ETF USD Dist",             "quantity": 8.69214914,  "price": 65.83},
    {"isin": "IE00BYVQ9F29", "symbol": "EQQQ",   "name": "iShares NASDAQ 100 UCITS ETF",                        "quantity": 31.3511554,  "price": 17.26},
    {"isin": "IE00B4YBJ215", "symbol": "SPY4",   "name": "SPDR S&P 400 U.S. Mid Cap UCITS ETF",                 "quantity": 0.44622786,  "price": 102.76},
    {"isin": "IE00B1YZSC51", "symbol": "MEUD",   "name": "iShares Core MSCI Europe UCITS ETF",                  "quantity": 0.53227859,  "price": 40.205},
    {"isin": "IE000U9J8HX9", "symbol": "JEQP",   "name": "JPMorgan Nasdaq Equity Premium Income Active UCITS",  "quantity": 499.68183622,"price": 23.865},
    {"isin": "IE000U5MJOZ6", "symbol": "JEIP",   "name": "JPMorgan US Equity Premium Income Active UCITS",      "quantity": 9.86913538,  "price": 21.345},
    {"isin": "IE0003UVYC20", "symbol": "JGPI",   "name": "JPMorgan Global Equity Premium Income Active UCITS",  "quantity": 5.68901188,  "price": 22.42},
    {"isin": "IE00B8GKDB10", "symbol": "VHYL",   "name": "Vanguard FTSE All-World High Dividend Yield UCITS",   "quantity": 8.94573315,  "price": 79.882},
    {"isin": "IE00BM8R0J59", "symbol": "QYLD",   "name": "Global X Nasdaq 100 Covered Call UCITS ETF",          "quantity": 1.67276214,  "price": 14.91},
    {"isin": "IE00BMC38736", "symbol": "SMH",    "name": "VanEck Semiconductor UCITS ETF",                      "quantity": 0.24906248,  "price": 100.38},
    {"isin": "IE00B6YX5D40", "symbol": "UDVD",   "name": "SPDR S&P US Dividend Aristocrats UCITS ETF",          "quantity": 10.69542998, "price": 74.53},
]

# Stocks (27 holdings) — hinnat USD
STOCK_HOLDINGS = [
    {"isin": "US88160R1014", "symbol": "TSLA",  "name": "Tesla",                         "quantity": 1.77834002, "price": 407.59},
    {"isin": "US0231351067", "symbol": "AMZN",  "name": "Amazon",                        "quantity": 2.75130172, "price": 245.74},
    {"isin": "US5949181045", "symbol": "MSFT",  "name": "Microsoft",                     "quantity": 1.21883566, "price": 385.34},
    {"isin": "US67066G1040", "symbol": "NVDA",  "name": "NVIDIA",                        "quantity": 7.77317395, "price": 210.57},
    {"isin": "US1912161007", "symbol": "KO",    "name": "Coca-Cola",                     "quantity": 8.61417833, "price": 83.45},
    {"isin": "US1667641005", "symbol": "CVX",   "name": "Chevron",                       "quantity": 3.20399071, "price": 176.16},
    {"isin": "US46625H1005", "symbol": "JPM",   "name": "JPMorgan Chase",                "quantity": 3.85943752, "price": 336.88},
    {"isin": "US30303M1027", "symbol": "META",  "name": "Meta",                          "quantity": 0.75419097, "price": 668},
    {"isin": "US69608A1088", "symbol": "PLTR",  "name": "Palantir",                      "quantity": 5.97014166, "price": 126.59},
    {"isin": "US0378331005", "symbol": "AAPL",  "name": "Apple",                         "quantity": 4.40920169, "price": 314.97},
    {"isin": "US7170811035", "symbol": "PFE",   "name": "Pfizer",                        "quantity": 27.90076202,"price": 24.22},
    {"isin": "US7134481081", "symbol": "PEP",   "name": "PepsiCo",                       "quantity": 2.38419108, "price": 137.4},
    {"isin": "US5949724083", "symbol": "MSTR",  "name": "Strategy",                      "quantity": 0.01191,    "price": 94.89},
    {"isin": "US7427181091", "symbol": "PG",    "name": "Procter & Gamble",              "quantity": 1.59406827, "price": 147.05},
    {"isin": "US4781601046", "symbol": "JNJ",   "name": "Johnson & Johnson",             "quantity": 4.85085379, "price": 256.6},
    {"isin": "US11135F1012", "symbol": "AVGO",  "name": "Broadcom",                      "quantity": 0.13123632, "price": 400.39},
    {"isin": "US92343V1044", "symbol": "VZ",    "name": "Verizon",                       "quantity": 6.3336864,  "price": 42.15},
    {"isin": "US30233Q1085", "symbol": "XOM",   "name": "ExxonMobil",                    "quantity": 2.69717092, "price": 138.8},
    {"isin": "US0079031078", "symbol": "AMD",   "name": "AMD",                           "quantity": 1.70679677, "price": 559.77},
    {"isin": "US09290D1019", "symbol": "BLK",   "name": "BlackRock",                     "quantity": 0.09171826, "price": 1036},
    {"isin": "US92826C8394", "symbol": "V",     "name": "Visa",                          "quantity": 0.9188928,  "price": 349.13},
    {"isin": "US57636Q1040", "symbol": "MA",    "name": "Mastercard",                    "quantity": 0.53027754, "price": 526.12},
    {"isin": "US02079K3059", "symbol": "GOOGL", "name": "Alphabet",                      "quantity": 1.09276209, "price": 357.17},
    {"isin": "US9256521090", "symbol": "VICI",  "name": "VICI Properties",               "quantity": 4.45206682, "price": 26.01},
    {"isin": "US00287Y1091", "symbol": "ABBV",  "name": "AbbVie",                        "quantity": 0.80222733, "price": 249.9},
    {"isin": "US0605051046", "symbol": "BAC",   "name": "Bank of America",               "quantity": 2.2532337,  "price": 59.66},
    {"isin": "US7475251036", "symbol": "QCOM",  "name": "Qualcomm",                      "quantity": 0.82236603, "price": 188.9},
]

# Crypto holdings — nämä ovat ulkoisilla vaihdoilla / DCA-suunnitelmassa,
# EIVÄT Trading 212 -tilillä (T212 Crypto -tili on tällä hetkellä tyhjä,
# Holdings value: 0.00 EUR, vahvistettu 10.07.2026 confirmation-of-holdings-asiakirjassa)
CRYPTO_HOLDINGS = [
    {"symbol": "bitcoin", "name": "BTC", "quantity": 0.00674376, "value_eur": 379.43},
    {"symbol": "ethereum", "name": "ETH", "quantity": 0.36953452, "value_eur": 587.15},
    {"symbol": "solana", "name": "SOL", "quantity": 2.0039211, "value_eur": 136.76},
    {"symbol": "ripple", "name": "XRP", "quantity": 276.85936581, "value_eur": 269.02},
    {"symbol": "binancecoin", "name": "BNB", "quantity": 0.17903486, "value_eur": 91.00},
    {"symbol": "sui", "name": "SUI", "quantity": 51.96174103, "value_eur": 33.73},
    {"symbol": "stellar", "name": "XLM", "quantity": 197.60613615, "value_eur": 33.04},
    {"symbol": "cardano", "name": "ADA", "quantity": 206.73380095, "value_eur": 30.70},
    {"symbol": "chainlink", "name": "LINK", "quantity": 3.40208837, "value_eur": 23.86},
]

# DCA ja Trading 212
DCA_PLAN = {
    "name": "Aydaurus Dream",
    "amount_eur": 100,
    "day": 10,
    "allocation": {"BTC": 20, "ETH": 20, "BNB": 20, "SOL": 20, "XRP": 20},
    "next_trade": "2026-08-10"
}

TRADING212_PLAN = {
    "name": "Dream",
    "owner": "Aydaruus Ahmed Wehliye",
    "amount_eur": 450,
    "day": 10,
    "holdings": 39,
    "next_trade": "2026-08-10",
    "total_value": 27562.45,
    "profit": 2946.77,
    "profit_percent": 11.97,
}

# Perheen holdings – jokaisella oma pie Trading 212 -sovelluksessa
FAMILY_HOLDINGS = [
    {"name": "👨 Aydaruus Ahmed Wehliye (Dream)", "holdings": TRADING212_PLAN["holdings"], "value": TRADING212_PLAN["total_value"], "profit": TRADING212_PLAN["profit"], "profit_percent": TRADING212_PLAN["profit_percent"]},
    {"name": "Ismahaan Aydaurus", "holdings": 20, "value": 1184.98, "profit": 178.95, "profit_percent": 17.79},
    {"name": "Ilyaas Aydaurus", "holdings": 26, "value": 1181.39, "profit": 182.23, "profit_percent": 18.25},
    {"name": "Farhia Aydaurus", "holdings": 18, "value": 1180.87, "profit": 179.94, "profit_percent": 17.98},
    {"name": "Mahamed Aydaurus", "holdings": 19, "value": 1177.03, "profit": 156.81, "profit_percent": 15.38},
    {"name": "Yahye Aydaurus", "holdings": 25, "value": 966.85, "profit": 128.01, "profit_percent": 15.27},
]

# Koko tilin arvo (kaikki pie:t + käyttämätön käteinen) — Trading 212 "INVESTMENTS"-näkymä
TOTAL_INVESTMENTS = 33253.64
TOTAL_CRYPTO = sum(c["value_eur"] for c in CRYPTO_HOLDINGS)

# Nopea haku: ISIN -> nykyinen omistusmäärä (käytetään osinkolaskennassa)
CURRENT_QTY_BY_ISIN = {h["isin"]: h["quantity"] for h in ETF_HOLDINGS}
CURRENT_QTY_BY_ISIN.update({h["isin"]: h["quantity"] for h in STOCK_HOLDINGS})

# =============================================
# 5. HINTA-APIT (EUR)
# =============================================

def get_crypto_price(symbol):
    symbol_map = {
        "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
        "ripple": "XRP", "binancecoin": "BNB", "sui": "SUI",
        "stellar": "XLM", "cardano": "ADA", "chainlink": "LINK"
    }
    sym = symbol_map.get(symbol, symbol.upper())

    try:
        url = f"https://api.kraken.com/0/public/Ticker?pair={sym}EUR"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("result"):
                for pair, values in data["result"].items():
                    if "c" in values and len(values["c"]) > 0:
                        return float(values["c"][0])
    except Exception as e:
        logging.warning(f"Kraken error {symbol}: {e}")

    try:
        url = f"https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={sym}-EUR"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("data") and "price" in data["data"]:
                return float(data["data"]["price"])
    except Exception as e:
        logging.warning(f"KuCoin error {symbol}: {e}")

    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies=eur"
        r = requests.get(url, timeout=10, headers=headers)
        if r.status_code == 200:
            data = r.json()
            if symbol in data and "eur" in data[symbol]:
                return data[symbol]["eur"]
    except Exception as e:
        logging.warning(f"CoinGecko error {symbol}: {e}")

    return None

def get_btc_price(): return get_crypto_price("bitcoin")
def get_eth_price(): return get_crypto_price("ethereum")
def get_sol_price(): return get_crypto_price("solana")
def get_xrp_price(): return get_crypto_price("ripple")
def get_bnb_price(): return get_crypto_price("binancecoin")
def get_sui_price(): return get_crypto_price("sui")
def get_xlm_price(): return get_crypto_price("stellar")
def get_ada_price(): return get_crypto_price("cardano")
def get_link_price(): return get_crypto_price("chainlink")

def get_stock_price(symbol):
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d")
        if hist.empty:
            return None
        return round(hist["Close"].iloc[-1], 2)
    except Exception as e:
        logging.error(f"Error fetching stock {symbol}: {e}")
        return None

def get_etf_price(symbol):
    return get_stock_price(symbol)

# =============================================
# 6. HISTORIALLISET HINNAT (30 päivää) — /recommend -komentoa varten
# =============================================

def get_stock_historical(symbol, days=30):
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=f"{days}d")
        if not hist.empty:
            return round(hist["Close"].iloc[0], 2)
    except Exception as e:
        logging.error(f"Historical error {symbol}: {e}")
    return None

def get_crypto_historical(symbol, days=30):
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{symbol}/market_chart?vs_currency=eur&days={days}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        r = requests.get(url, timeout=10, headers=headers)
        if r.status_code == 200:
            data = r.json()
            if "prices" in data and len(data["prices"]) > 0:
                return data["prices"][0][1]
    except Exception as e:
        logging.error(f"Crypto historical error {symbol}: {e}")
    return None

def get_recommendation(current_price, old_price, name):
    if current_price is None or old_price is None or old_price == 0:
        return "❓", "Ei tarpeeksi dataa"
    change = ((current_price - old_price) / old_price) * 100
    if change >= 10:
        return "🔴 SELL", f"+{change:.1f}% (kallis)"
    elif change <= -10:
        return "🟢 BUY", f"{change:.1f}% (halpa)"
    else:
        return "🟡 HOLD", f"{change:+.1f}% (neutraali)"

# =============================================
# 7. OSINGOT — LUE CSV:STÄ (TODELLISET, TRADING 212 -TILIOTE)
# =============================================
# CSV-tiedoston sarakkeet (Trading 212 -vienti):
# Action, Time, ISIN, Ticker, Name, No. of shares, Price / share,
# Currency (Price / share), Exchange rate, Total, Currency (Total),
# Withholding tax, Currency (Withholding tax)

DIVIDENDS_CSV_PATH = "dividends.csv"

def _load_dividends_dataframe():
    df = pd.read_csv(DIVIDENDS_CSV_PATH)
    df = df[df['Action'] == 'Dividend (Dividend)'].copy()
    df['Total'] = df['Total'].astype(str).str.replace(',', '.').astype(float)
    df['Shares'] = df['No. of shares'].astype(str).str.replace(',', '.').astype(float)
    df['PerShare'] = df['Price / share'].astype(str).str.replace(',', '.').astype(float)
    df['Tax'] = df['Withholding tax'].astype(str).str.replace(',', '.').fillna('0').astype(float)
    df['Date'] = df['Time'].apply(lambda t: datetime.strptime(str(t).split(' ')[0], '%Y-%m-%d'))
    return df.sort_values('Date')

def get_dividend_details():
    """Palauttaa listan yksittäisistä osinkomaksuista (kuka maksoi, milloin, kuinka paljon)
    sekä koko ajanjakson yhteissumman, lukien dividends.csv:stä."""
    try:
        df = _load_dividends_dataframe()
    except Exception as e:
        logging.error(f"Virhe luettaessa CSV: {e}")
        return [], 0.0

    dividend_list = []
    for _, row in df.iterrows():
        dividend_list.append({
            "date": row['Date'].strftime('%d.%m.%Y'),
            "date_sort": row['Date'],
            "isin": row['ISIN'],
            "symbol": row['Ticker'],
            "name": row['Name'],
            "amount": round(row['Total'], 2),
            "quantity": row['Shares'],
            "per_share": round(row['PerShare'], 4),
            "currency": row.get('Currency (Price / share)', ''),
            "tax": round(row['Tax'], 2),
        })

    dividend_list.sort(key=lambda x: x['date_sort'], reverse=True)
    total = round(sum(d['amount'] for d in dividend_list), 2)
    return dividend_list, total

def get_dividends_by_month(dividend_list):
    """Ryhmittelee osingot kuukausittain (avain 'MM/YYYY') ja palauttaa
    (monthly_dict, yearly_total)."""
    monthly = {}
    for d in dividend_list:
        key = d['date_sort'].strftime('%m/%Y')
        monthly[key] = monthly.get(key, 0.0) + d['amount']
    monthly = {k: round(v, 2) for k, v in monthly.items()}
    yearly_total = round(sum(monthly.values()), 2)
    return monthly, yearly_total

# =============================================
# 8. TULEVAT OSINGOT — ARVIO CSV-HISTORIAN PERUSTEELLA
# =============================================
# Trading 212:n API:ssa ei ole "tulevat osingot" -endpointtia (vain
# toteutuneet maksut), joten tulevat osingot arvioidaan CSV-historian
# maksuvälin ja viimeisimmän €/osake-summan perusteella, kerrottuna
# NYKYISELLÄ omistusmäärällä (Confirmation of holdings -asiakirjasta).

def get_upcoming_dividends_estimated(until_date=None):
    """Arvioi tulevat osingot tähän päivään asti annettuun until_date-päivämäärään saakka.
    Oletus: kuluvan vuoden loppu (31.12.), jottei lista rönsyile seuraavaan vuoteen."""
    try:
        df = _load_dividends_dataframe()
    except Exception as e:
        logging.error(f"Virhe CSV:n luvussa (upcoming-arvio): {e}")
        return [], {}, 0.0

    projected = []
    today = datetime.now()
    horizon = until_date or datetime(today.year, 12, 31, 23, 59, 59)

    for isin, group in df.groupby('ISIN'):
        group = group.sort_values('Date')
        if len(group) < 2:
            continue  # ei tarpeeksi historiaa maksuvälin päättelyyn

        qty = CURRENT_QTY_BY_ISIN.get(isin)
        if not qty:
            continue  # ei enää (tai ei koskaan) salkussa

        name = group.iloc[-1]['Name']
        ticker = group.iloc[-1]['Ticker']
        dates = group['Date'].tolist()
        intervals = [(dates[i] - dates[i - 1]).days for i in range(1, len(dates))]
        avg_interval = sum(intervals) / len(intervals)

        last_row = group.iloc[-1]
        last_per_share = last_row['PerShare']
        next_date = dates[-1] + timedelta(days=avg_interval)

        while next_date <= horizon:
            if next_date >= today:
                projected.append({
                    "isin": isin,
                    "symbol": ticker,
                    "name": name,
                    "amount": round(last_per_share * qty, 2),
                    "per_share": round(last_per_share, 4),
                    "date": next_date.strftime('%d.%m.%Y'),
                    "date_sort": next_date,
                    "frequency_days": round(avg_interval),
                })
            next_date += timedelta(days=avg_interval)

    projected.sort(key=lambda x: x['date_sort'])

    monthly = {}
    yearly_total = 0.0
    for div in projected:
        key = div['date_sort'].strftime('%m/%Y')
        monthly[key] = monthly.get(key, 0.0) + div['amount']
        yearly_total += div['amount']
    monthly = {k: round(v, 2) for k, v in monthly.items()}

    return projected, monthly, round(yearly_total, 2)

# =============================================
# 9. TAVOITE (100k)
# =============================================

def calculate_goal(current_value, monthly_savings, target=100000, yearly_return_pct=0.07):
    remaining = target - current_value
    if remaining <= 0:
        return 0, datetime.now()
    monthly_return = (1 + yearly_return_pct) ** (1 / 12) - 1
    months = 0
    value = current_value
    while value < target and months < 600:
        value = value * (1 + monthly_return) + monthly_savings
        months += 1
    return months, datetime.now() + timedelta(days=months * 30)

def calculate_compounding_crossover(current_value, monthly_savings, yearly_return_pct=0.07):
    """Laskee kuukauden, jolloin sijoitusten kasvu (korkotuotto) ylittää
    kuukausittain lisättävän säästösumman — eli 'compounding' alkaa kantaa
    enemmän kuin oma kuukausisäästö."""
    monthly_return = (1 + yearly_return_pct) ** (1 / 12) - 1
    value = current_value
    months = 0
    while months < 600:
        interest_this_month = value * monthly_return
        if interest_this_month >= monthly_savings:
            return months, datetime.now() + timedelta(days=months * 30), interest_this_month
        value = value * (1 + monthly_return) + monthly_savings
        months += 1
    return None, None, None

# =============================================
# 10. UUTISET
# =============================================

def get_news(query, limit=3):
    try:
        url = f"https://news.google.com/rss/search?q={query}&hl=fi&gl=FI&ceid=FI:fi"
        feed = feedparser.parse(url)
        news_list = []
        for entry in feed.entries[:limit]:
            title = re.sub(r'<.*?>', '', entry.title)[:100]
            news_list.append({"title": title, "link": entry.link})
        return news_list
    except Exception as e:
        logging.error(f"News error: {e}")
        return None

async def news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📰 *Haetaan uutisia...*", parse_mode="Markdown")
    queries = [
        ("BTC", "bitcoin"), ("ETH", "ethereum"), ("SOL", "solana"),
        ("XRP", "ripple"), ("BNB", "binance"), ("SUI", "sui"),
        ("XLM", "stellar"), ("ADA", "cardano"), ("LINK", "chainlink"),
        ("TSLA", "tesla"), ("AAPL", "apple"), ("MSFT", "microsoft"),
        ("NVDA", "nvidia"), ("META", "meta"), ("GOOGL", "alphabet"),
        ("S&P 500", "sp500"), ("NASDAQ", "nasdaq")
    ]
    msg = "📰 *Uutiset omistamistasi kohteista*\n━━━━━━━━━━━━━━━━━\n\n"
    found = 0
    for name, q in queries[:5]:
        items = get_news(q, limit=2)
        if items:
            msg += f"🔹 *{name}*\n"
            for item in items:
                msg += f"• {item['title']}\n"
            msg += "\n"
            found += 1
    if found == 0:
        msg += "⚠️ Uutisia ei löytynyt tällä hetkellä."
    await update.message.reply_text(msg, parse_mode="Markdown")

# =============================================
# 11. WARBIXIN MAALINLE
# =============================================

async def send_daily_report():
    global USER_ID
    if USER_ID is None:
        return
    btc = get_btc_price()
    eth = get_eth_price()
    sol = get_sol_price()
    xrp = get_xrp_price()
    bnb = get_bnb_price()
    sui = get_sui_price()
    xlm = get_xlm_price()
    ada = get_ada_price()
    link = get_link_price()

    msg = "📊 *Subax wanaagsan, Aydaruus!*\n\n"
    msg += "💰 *Portfolio-gaaga*\n━━━━━━━━━━━━━━━━━\n"
    msg += f"💵 Wadarta guud: €{TOTAL_INVESTMENTS:,.2f}\n"
    msg += f"🪙 Crypto holdings: €{TOTAL_CRYPTO:,.2f}\n\n"

    msg += "🪙 *Crypto qiimaha hadda:*\n"
    for label, val in [("₿ BTC", btc), ("⟠ ETH", eth), ("◎ SOL", sol), ("✕ XRP", xrp),
                        ("⬡ BNB", bnb), ("🔷 SUI", sui), ("⭐ XLM", xlm),
                        ("🟣 ADA", ada), ("🔗 LINK", link)]:
        msg += f"{label}: €{val:,.2f}\n" if val else f"{label}: Laga ma helin\n"

    _, total_div = get_dividend_details()
    months, target_date = calculate_goal(TOTAL_INVESTMENTS, TRADING212_PLAN['amount_eur'] + DCA_PLAN['amount_eur'])
    msg += f"\n🎯 *100k € tavoite*\n"
    msg += f"📈 Puuttuu: €{100000 - TOTAL_INVESTMENTS:,.2f}\n"
    msg += f"📅 Arvio: {target_date.strftime('%d.%m.%Y')} ({months} kk)\n"
    msg += f"💵 Osingot (12 kk, todelliset): €{total_div:,.2f}\n"

    today = datetime.now()
    if today.day == 10:
        msg += f"\n🔔 *XASUUSIN! Maanta waa 10-da bil!*\n"
        msg += f"💵 Geli €{DCA_PLAN['amount_eur']} crypto + €{TRADING212_PLAN['amount_eur']} Trading 212!\n"
    else:
        next_month = today.month + 1 if today.month < 12 else 1
        next_year = today.year if today.month < 12 else today.year + 1
        msg += f"\n📌 Togga xiga: 10-{next_month:02d}-{next_year}"

    try:
        app = Application.builder().token(TOKEN).build()
        await app.bot.send_message(chat_id=USER_ID, text=msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Warbixin maalinle ah waa ay fashilantay: {e}")

# =============================================
# 12. QORSHEYNTA
# =============================================
scheduler = BackgroundScheduler()
scheduler.add_job(send_daily_report, 'cron', hour=9, minute=0, id="daily_report", replace_existing=True)
scheduler.start()

# =============================================
# 13. TELEGRAM KOMENNOT — PERUS
# =============================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global USER_ID
    user = update.effective_user
    USER_ID = user.id
    try:
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO users (id, username, first_name, last_seen) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                  (user.id, user.username, user.first_name))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"DB error: {e}")
    await update.message.reply_text(
        f"👋 *Hello, {user.first_name}!*\n\n"
        "📊 *Aydaruus Invest AI* waa diyaar!\n\n"
        "📌 *Komenno:*\n"
        "/help - Muuji dhammaan komenno\n"
        "/check - Soo dir warbixin degdeg ah\n"
        "/portfolio - Muuji portfolio-gaaga\n"
        "/etfs - Muuji ETF holdings\n"
        "/stocks - Muuji stock holdings\n"
        "/crypto - Muuji crypto holdings\n"
        "/testapi - Tijaabi API-yada\n"
        "/news - Uutiset omistuksista\n"
        "/goal - Tavoite 100k €\n"
        "/dividends - Tulevat osingot, kk-ryhmiteltynä\n"
        "/recommend - Sijoitusanalyysi & suositukset\n\n"
        "💰 Maalin kasta 9:00 subax waxaan kuu soo dirayaa warbixin!",
        parse_mode="Markdown"
    )

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏓 Pong!")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Käytettävissä olevat komennot:*\n\n"
        "/start - Tervehdys\n"
        "/ping - Ping Pong\n"
        "/help - Tämä ohje\n"
        "/stats - Näytä käyttäjämäärä\n"
        "/check - Warbixin degdeg ah (crypto)\n"
        "/portfolio - Muuji portfolio-gaaga\n"
        "/etfs - Muuji ETF holdings\n"
        "/stocks - Muuji stock holdings\n"
        "/crypto - Muuji crypto holdings\n"
        "/testapi - Tijaabi API-yada\n"
        "/news - Uutiset omistuksista\n"
        "/goal - Tavoite 100k €\n"
        "/dividends - Tulevat osingot, kk-ryhmiteltynä\n"
        "/recommend - Sijoitusanalyysi & suositukset\n\n"
        "💰 *DCA:* €100/bil (crypto) + €450/kk (Trading 212)\n"
        "📊 *Warbixin maalinle:* 9:00 subax",
        parse_mode="Markdown"
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        count = c.fetchone()[0]
        conn.close()
        await update.message.reply_text(f"👥 Botti waxaa isticmaalay {count} qof.")
    except Exception:
        await update.message.reply_text("⚠️ Kuma heli karo tirokoobka.")

async def check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    btc = get_btc_price(); eth = get_eth_price(); sol = get_sol_price()
    xrp = get_xrp_price(); bnb = get_bnb_price(); sui = get_sui_price()
    xlm = get_xlm_price(); ada = get_ada_price(); link = get_link_price()

    msg = "📊 *Warbixin degdeg ah*\n━━━━━━━━━━━━━━━━━\n\n🪙 *Crypto qiimaha hadda:*\n"
    for label, val in [("₿ BTC", btc), ("⟠ ETH", eth), ("◎ SOL", sol), ("✕ XRP", xrp),
                        ("⬡ BNB", bnb), ("🔷 SUI", sui), ("⭐ XLM", xlm),
                        ("🟣 ADA", ada), ("🔗 LINK", link)]:
        msg += f"{label}: €{val:,.2f}\n" if val else f"{label}: Laga ma helin\n"

    msg += f"\n💰 *Crypto holdings:* €{TOTAL_CRYPTO:,.2f}\n"
    msg += f"💵 *Wadarta guud:* €{TOTAL_INVESTMENTS:,.2f}"

    months, target_date = calculate_goal(TOTAL_INVESTMENTS, TRADING212_PLAN['amount_eur'] + DCA_PLAN['amount_eur'])
    msg += f"\n\n🎯 *100k €:* puuttuu €{100000 - TOTAL_INVESTMENTS:,.2f}, arvio {target_date.strftime('%d.%m.%Y')} ({months} kk)"

    await update.message.reply_text(msg, parse_mode="Markdown")

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📊 *Portfolio-gaaga*\n━━━━━━━━━━━━━━━━━\n\n"
    msg += f"💰 *Wadarta guud (T212 Invest):* €{TOTAL_INVESTMENTS:,.2f}\n"
    msg += f"🪙 *Crypto holdings (ulkoinen):* €{TOTAL_CRYPTO:,.2f}\n\n"
    msg += f"📈 *ETF holdings:* {len(ETF_HOLDINGS)} holdings\n"
    msg += f"📈 *Stock holdings:* {len(STOCK_HOLDINGS)} holdings\n"
    msg += f"🪙 *Crypto holdings:* {len(CRYPTO_HOLDINGS)} holdings\n\n"

    msg += f"📊 *Trading 212 -kuukausisijoitus*\n"
    msg += f"💰 €{TRADING212_PLAN['amount_eur']}/kk (10. päivä)\n\n"

    msg += "👨‍👩‍👧‍👦 *Perheen holdings*\n"
    for member in FAMILY_HOLDINGS:
        msg += f"• {member['name']}: {member['holdings']} hold. = €{member['value']:,.2f} (+€{member['profit']:,.2f} / +{member['profit_percent']:.2f}%)\n"

    msg += f"\n📌 *DCA qorshaha:* {DCA_PLAN['name']}\n"
    msg += f"💰 €{DCA_PLAN['amount_eur']}/bil (10-da bil)\n"
    msg += "📊 Qaybinta: BTC 20%, ETH 20%, BNB 20%, SOL 20%, XRP 20%"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def etfs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📈 *ETF Holdings*\n━━━━━━━━━━━━━━━━━\n\n"
    total = 0
    for etf in ETF_HOLDINGS:
        value = etf["quantity"] * etf["price"]
        total += value
        msg += f"{etf['name'][:30]}: {etf['quantity']:.4f} x €{etf['price']:,.2f} = €{value:,.2f}\n"
    msg += f"\n💰 *Wadarta ETF:* €{total:,.2f}"
    await update.message.reply_text(msg)

async def stocks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📈 *Stock Holdings*\n━━━━━━━━━━━━━━━━━\n\n"
    total = 0
    for stock in STOCK_HOLDINGS:
        value = stock["quantity"] * stock["price"]
        total += value
        msg += f"{stock['name'][:25]}: {stock['quantity']:.4f} x ${stock['price']:,.2f} = ${value:,.2f}\n"
    msg += f"\n💰 *Wadarta Stocks:* ${total:,.2f}"
    await update.message.reply_text(msg)

async def crypto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "🪙 *Crypto Holdings*\n━━━━━━━━━━━━━━━━━\n\n"
    total = 0
    for c in CRYPTO_HOLDINGS:
        total += c["value_eur"]
        msg += f"{c['name']}: {c['quantity']:.8f} = €{c['value_eur']:,.2f}\n"
    msg += f"\n💰 *Wadarta Crypto:* €{total:,.2f}"
    await update.message.reply_text(msg)

async def testapi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "🧪 *Tijaabo API (EUR)*\n\n"
    try:
        r = requests.get("https://api.kraken.com/0/public/Ticker?pair=BTCEUR", timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("result"):
                for pair, values in data["result"].items():
                    if "c" in values and len(values["c"]) > 0:
                        msg += f"✅ Kraken BTC/EUR: {float(values['c'][0]):,.0f} €\n"
                        break
        else:
            msg += f"❌ Kraken: {r.status_code}\n"
    except Exception as e:
        msg += f"❌ Kraken error: {e}\n"
    try:
        r = requests.get("https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=BTC-EUR", timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("data") and "price" in data["data"]:
                msg += f"✅ KuCoin BTC/EUR: {float(data['data']['price']):,.0f} €\n"
        else:
            msg += f"❌ KuCoin: {r.status_code}\n"
    except Exception as e:
        msg += f"❌ KuCoin error: {e}\n"
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        r = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=eur", timeout=10, headers=headers)
        if r.status_code == 200:
            data = r.json()
            if "bitcoin" in data and "eur" in data["bitcoin"]:
                msg += f"✅ CoinGecko BTC/EUR: {data['bitcoin']['eur']:,.0f} €\n"
        else:
            msg += f"❌ CoinGecko: {r.status_code}\n"
    except Exception as e:
        msg += f"❌ CoinGecko error: {e}\n"
    await update.message.reply_text(msg)

async def goal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    monthly_savings = TRADING212_PLAN['amount_eur'] + DCA_PLAN['amount_eur']

    msg = "🎯 *Sijoitustavoitteet*\n━━━━━━━━━━━━━━━━━\n\n"
    msg += f"💰 Nykyinen salkun arvo: €{TOTAL_INVESTMENTS:,.2f}\n"
    msg += f"📊 Kuukausisäästö: €{monthly_savings:,.0f} (Trading212 + crypto DCA)\n\n"

    if TOTAL_INVESTMENTS >= 50000:
        msg += "✅ *50 000 €* — saavutettu jo!\n\n"
    else:
        months_50k, date_50k = calculate_goal(TOTAL_INVESTMENTS, monthly_savings, target=50000)
        msg += f"🥉 *50 000 €*\n"
        msg += f"   📅 Arvioitu saavutus: {date_50k.strftime('%d.%m.%Y')} ({months_50k} kk)\n"
        msg += f"   📈 Puuttuu: €{50000 - TOTAL_INVESTMENTS:,.2f}\n\n"

    months_100k, date_100k = calculate_goal(TOTAL_INVESTMENTS, monthly_savings, target=100000)
    msg += f"🏆 *100 000 €*\n"
    msg += f"   📅 Arvioitu saavutus: {date_100k.strftime('%d.%m.%Y')} ({months_100k} kk)\n"
    msg += f"   📈 Puuttuu: €{100000 - TOTAL_INVESTMENTS:,.2f}\n\n"

    cross_months, cross_date, cross_interest = calculate_compounding_crossover(TOTAL_INVESTMENTS, monthly_savings)
    msg += "📊 *Compounding-piste*\n"
    if cross_months is not None:
        if cross_months == 0:
            msg += "   ✅ Korkotuotto ylittää jo kuukausisäästösi — kasvu kantaa itse itseään!\n"
        else:
            msg += f"   📅 {cross_date.strftime('%d.%m.%Y')} ({cross_months} kk)\n"
            msg += f"   ℹ️ Tästä eteenpäin salkun kuukausittainen korkotuotto (~€{cross_interest:,.0f}) ylittää €{monthly_savings:,.0f} kuukausisäästösi —\n"
            msg += "   sijoitusten oma kasvu alkaa tuottaa enemmän kuin itse laitat rahaa sisään."
    else:
        msg += "   ⚠️ Ei saavutettu laskenta-ajan (50 v) sisällä nykyisillä oletuksilla."

    await update.message.reply_text(msg, parse_mode="Markdown")

# =============================================
# 14. MENNEET OSINGOT — kuka maksoi, milloin, kuinka paljon
#     + kuukausittain + koko vuosi
# =============================================
FI_MONTHS = {
    "01": "Tammikuu", "02": "Helmikuu", "03": "Maaliskuu", "04": "Huhtikuu",
    "05": "Toukokuu", "06": "Kesäkuu", "07": "Heinäkuu", "08": "Elokuu",
    "09": "Syyskuu", "10": "Lokakuu", "11": "Marraskuu", "12": "Joulukuu",
}

async def dividends(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Tulevat osingot, siististi kuukausittain ryhmiteltynä:
    maksupäivä-järjestyksessä kuka maksaa ja paljonko, kuukausi kerralla + kk-summa,
    lopussa koko vuoden yhteissumma."""
    try:
        current_year = datetime.now().year
        projected, monthly, yearly_total = get_upcoming_dividends_estimated()

        if not projected:
            await update.message.reply_text(
                "⚠️ Osinkoja ei voitu arvioida.\n"
                "Varmista, että dividends.csv sisältää vähintään 2 maksua per osake/ETF."
            )
            return

        # Ryhmittele kuukausittain, säilytä maksupäiväjärjestys kunkin kuukauden sisällä
        by_month = {}
        for div in projected:
            key = div['date_sort'].strftime('%m/%Y')
            by_month.setdefault(key, []).append(div)

        months_sorted = sorted(by_month.keys(), key=lambda m: datetime.strptime(m, '%m/%Y'))

        header = "💰 *Dividends*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        blocks = []
        for key in months_sorted:
            month_num, year = key.split('/')
            month_name = FI_MONTHS.get(month_num, month_num)
            month_total = monthly[key]
            block = f"📅 *{month_name} {year}* — €{month_total:,.2f}\n"
            for div in by_month[key]:
                block += f"   {div['date']}  •  {div['name']} ({div['symbol']})  →  €{div['amount']:,.2f}\n"
            blocks.append(block)

        footer = f"\n💰 *Yhteensä (vuoden {current_year} loppuun):* €{yearly_total:,.2f}"

        # Kokoa viestit n. 3500 merkin paloihin kuukausirajoilla
        max_len = 3500
        current = header
        messages = []
        for block in blocks:
            if len(current) + len(block) > max_len:
                messages.append(current)
                current = block
            else:
                current += block + "\n"
        current += footer
        messages.append(current)

        for msg in messages:
            await update.message.reply_text(msg, parse_mode="Markdown")

    except Exception as e:
        logging.error(f"Virhe dividends-komennossa: {e}")
        await update.message.reply_text(f"⚠️ Virhe: {str(e)[:150]}")

# =============================================
# 16. SUOSITUKSET (BUY/HOLD/SELL)
# =============================================
async def recommend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📊 *Sijoitusanalyysi & suositukset*\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "⚡ 30 päivän hinnanmuutokseen perustuen:\n\n"

    msg += "🪙 *Kryptot*\n"
    crypto_prices = {
        "BTC": get_btc_price(), "ETH": get_eth_price(), "SOL": get_sol_price(),
        "XRP": get_xrp_price(), "BNB": get_bnb_price(), "SUI": get_sui_price(),
        "XLM": get_xlm_price(), "ADA": get_ada_price(), "LINK": get_link_price()
    }
    for name, current in crypto_prices.items():
        if current:
            old = get_crypto_historical(name.lower(), 30)
            rec, detail = get_recommendation(current, old, name)
            msg += f"{rec} *{name}*: €{current:,.0f} ({detail})\n"
        else:
            msg += f"❌ {name}: Ei hintaa\n"

    msg += "\n📈 *ETF:t*\n"
    for etf in ETF_HOLDINGS:
        ticker = etf["symbol"]
        current = get_etf_price(ticker)
        if current:
            old = get_stock_historical(ticker, 30)
            rec, detail = get_recommendation(current, old, etf["name"])
            msg += f"{rec} *{etf['name'][:22]}*: €{current:,.2f} ({detail})\n"
        else:
            msg += f"❌ {etf['name'][:22]}: Ei hintaa\n"

    msg += "\n📊 *Osakkeet*\n"
    for stock in STOCK_HOLDINGS:
        current = get_stock_price(stock["symbol"])
        if current:
            old = get_stock_historical(stock["symbol"], 30)
            rec, detail = get_recommendation(current, old, stock["name"])
            msg += f"{rec} *{stock['name'][:18]}*: ${current:,.2f} ({detail})\n"
        else:
            msg += f"❌ {stock['name'][:18]}: Ei hintaa\n"

    msg += "\n💡 *Selitys:*\n"
    msg += "🟢 BUY = hinta laskenut ≥10% (hyvä ostopaikka)\n"
    msg += "🟡 HOLD = hinta muuttunut alle 10%\n"
    msg += "🔴 SELL = hinta noussut ≥10% (hyvä myydä)"

    await update.message.reply_text(msg, parse_mode="Markdown")

# =============================================
# 17. VIRHEIDENKÄSITTELY
# =============================================
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.error(f"Virhe: {context.error}")
    if update and update.effective_message:
        await update.effective_message.reply_text("⚠️ Jokin meni pieleen. Yritä uudelleen.")

# =============================================
# 18. FLASK
# =============================================
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "🤖 Aydaruus Invest AI bot is running!", 200

def run_flask():
    flask_app.run(host='0.0.0.0', port=PORT, debug=False)

# =============================================
# 19. PÄÄFUNKTIO
# =============================================
def run_bot():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("check", check))
    app.add_handler(CommandHandler("portfolio", portfolio))
    app.add_handler(CommandHandler("etfs", etfs))
    app.add_handler(CommandHandler("stocks", stocks))
    app.add_handler(CommandHandler("crypto", crypto))
    app.add_handler(CommandHandler("testapi", testapi))
    app.add_handler(CommandHandler("news", news))
    app.add_handler(CommandHandler("goal", goal))
    app.add_handler(CommandHandler("dividends", dividends))
    app.add_handler(CommandHandler("recommend", recommend))
    app.add_error_handler(error_handler)
    app.run_polling()

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    run_bot()
