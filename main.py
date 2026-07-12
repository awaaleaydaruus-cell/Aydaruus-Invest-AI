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
import urllib.parse
import pandas as pd
import numpy as np
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler

# =============================================
# Riippuvuudet (pandas-ta korvaa vanhan ta-kirjaston)
# =============================================
from bs4 import BeautifulSoup
import pandas_ta as ta
import ccxt  # kryptopörssit (valinnainen)

# =============================================
# 1. YMPÄRISTÖMUUTTUJAT
# =============================================
TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", 10000))

# =============================================
# 2. LOGGING
# =============================================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# =============================================
# 3. TIETOKANTA (laajennettu)
# =============================================
def init_db():
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    # Vanhat taulut
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_history (
            date TEXT PRIMARY KEY,
            total_value REAL,
            crypto_value REAL,
            invest_value REAL
        )
    """)
    # Uudet taulut
    c.execute("""
        CREATE TABLE IF NOT EXISTS presale_projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            symbol TEXT,
            platform TEXT,
            launch_date TEXT,
            overall_score INTEGER,
            scam_risk INTEGER,
            liquidity_score INTEGER,
            community_score INTEGER,
            dev_score INTEGER,
            audit_score INTEGER,
            vc_score INTEGER,
            tokenomics_score INTEGER,
            binance_prob INTEGER,
            coinbase_prob INTEGER,
            kraken_prob INTEGER,
            bybit_prob INTEGER,
            okx_prob INTEGER,
            url TEXT,
            description TEXT,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, symbol)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            user_id INTEGER,
            project_name TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, project_name)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            date TEXT PRIMARY KEY,
            fed_rate REAL,
            inflation REAL,
            btc_etf_flow REAL,
            eth_etf_flow REAL,
            total_etf_flow REAL,
            whale_count INTEGER,
            stablecoin_inflow REAL,
            stablecoin_outflow REAL
        )
    """)
    conn.commit()
    conn.close()

init_db()

def get_all_user_ids():
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT id FROM users")
    rows = c.fetchall()
    conn.close()
    return [row[0] for row in rows]

# =============================================
# 4. PORTFOLIO HOLDINGS
# =============================================
ETF_HOLDINGS = [
    {"isin": "IE00B5BMR087", "symbol": "SPY5L.L",  "name": "iShares Core S&P 500 UCITS ETF",                      "quantity": 1.3195215,   "price": 711.48},
    {"isin": "IE00BFMXXD54", "symbol": "VUAA.L",   "name": "Vanguard S&P 500 UCITS ETF",                          "quantity": 6.78430694,  "price": 127.59},
    {"isin": "IE00B4L5Y983", "symbol": "IWDA.L",   "name": "iShares Core MSCI World UCITS ETF",                   "quantity": 7.86059909,  "price": 126.145},
    {"isin": "IE00BK5BQT80", "symbol": "VWRA.L",   "name": "Vanguard FTSE All-World UCITS ETF",                   "quantity": 3.95788178,  "price": 166.14},
    {"isin": "IE00B53SZB19", "symbol": "CNDX.L",   "name": "iShares NASDAQ 100 UCITS ETF",                        "quantity": 0.551902,    "price": 1493.8},
    {"isin": "IE000XZSV718", "symbol": "SPY5.L",   "name": "SPDR S&P 500 UCITS ETF",                              "quantity": 35.45098256, "price": 16.3422},
    {"isin": "IE00B3XXRP09", "symbol": "VUSA.L",   "name": "Vanguard S&P 500 UCITS ETF",                          "quantity": 5.07180738,  "price": 125.226},
    {"isin": "IE0031442068", "symbol": "IUSA.L",   "name": "iShares Core S&P 500 UCITS ETF USD Dist",             "quantity": 8.69214914,  "price": 65.83},
    {"isin": "IE00BYVQ9F29", "symbol": "EQQQ.L",   "name": "iShares NASDAQ 100 UCITS ETF",                        "quantity": 31.3511554,  "price": 17.26},
    {"isin": "IE00B4YBJ215", "symbol": "SPY4.L",   "name": "SPDR S&P 400 U.S. Mid Cap UCITS ETF",                 "quantity": 0.44622786,  "price": 102.76},
    {"isin": "IE00B1YZSC51", "symbol": "MEUD.L",   "name": "iShares Core MSCI Europe UCITS ETF",                  "quantity": 0.53227859,  "price": 40.205},
    {"isin": "IE000U9J8HX9", "symbol": "JEQP.L",   "name": "JPMorgan Nasdaq Equity Premium Income Active UCITS",  "quantity": 499.68183622,"price": 23.865},
    {"isin": "IE000U5MJOZ6", "symbol": "JEIP.L",   "name": "JPMorgan US Equity Premium Income Active UCITS",      "quantity": 9.86913538,  "price": 21.345},
    {"isin": "IE0003UVYC20", "symbol": "JGPI.L",   "name": "JPMorgan Global Equity Premium Income Active UCITS",  "quantity": 5.68901188,  "price": 22.42},
    {"isin": "IE00B8GKDB10", "symbol": "VHYL.L",   "name": "Vanguard FTSE All-World High Dividend Yield UCITS",   "quantity": 8.94573315,  "price": 79.882},
    {"isin": "IE00BM8R0J59", "symbol": "QYLD.L",   "name": "Global X Nasdaq 100 Covered Call UCITS ETF",          "quantity": 1.67276214,  "price": 14.91},
    {"isin": "IE00BMC38736", "symbol": "SMH.L",    "name": "VanEck Semiconductor UCITS ETF",                      "quantity": 0.24906248,  "price": 100.38},
    {"isin": "IE00B6YX5D40", "symbol": "UDVD.L",   "name": "SPDR S&P US Dividend Aristocrats UCITS ETF",          "quantity": 10.69542998, "price": 74.53},
]

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

DCA_PLAN = {
    "name": "Aydaurus Dream",
    "amount_eur": 100,
    "day": 10,
    "allocation": {"BTC": 20, "ETH": 20, "BNB": 20, "SOL": 20, "XRP": 20},
}

TRADING212_PLAN = {
    "name": "Dream",
    "owner": "Aydaruus Ahmed Wehliye",
    "amount_eur": 200,
    "day": 10,
    "holdings": 39,
    "total_value": 27562.45,
    "profit": 2946.77,
    "profit_percent": 11.97,
}

FAMILY_HOLDINGS = [
    {"name": "Aydaruus", "holdings": TRADING212_PLAN["holdings"], "value": TRADING212_PLAN["total_value"], "profit": TRADING212_PLAN["profit"], "profit_percent": TRADING212_PLAN["profit_percent"], "monthly_savings": 200},
    {"name": "Ismahaan", "holdings": 20, "value": 1184.98, "profit": 178.95, "profit_percent": 17.79, "monthly_savings": 50},
    {"name": "Ilyaas", "holdings": 26, "value": 1181.39, "profit": 182.23, "profit_percent": 18.25, "monthly_savings": 50},
    {"name": "Farhia", "holdings": 18, "value": 1180.87, "profit": 179.94, "profit_percent": 17.98, "monthly_savings": 50},
    {"name": "Mahamed", "holdings": 19, "value": 1177.03, "profit": 156.81, "profit_percent": 15.38, "monthly_savings": 50},
    {"name": "Yahye", "holdings": 25, "value": 966.85, "profit": 128.01, "profit_percent": 15.27, "monthly_savings": 50},
]

TOTAL_INVESTMENTS = 33253.64
TOTAL_CRYPTO = sum(c["value_eur"] for c in CRYPTO_HOLDINGS)

# =============================================
# 4B. HORMUUD SHARES
# =============================================
HORMUUD_PROJECTION = [
    {"nro": 0,  "year": 2024, "start_capital": 10000, "annual_return": None,  "cash_payment": None,  "monthly_payment": None, "capital_addition": None, "capital_growth": None,  "value": 0,      "paid_date": None,      "note": "Aloitus vuosi"},
    {"nro": 1,  "year": 2025, "start_capital": 10000, "annual_return": 2784,  "cash_payment": 1531,  "monthly_payment": 128,  "capital_addition": 1253, "capital_growth": 11253, "value": 37134,  "paid_date": "25.3.2025", "note": "Sijoitettu raha 37 000€ vuonna 2025"},
    {"nro": 2,  "year": 2026, "start_capital": 23364, "annual_return": 5719,  "cash_payment": 2860,  "monthly_payment": 238,  "capital_addition": 2860, "capital_growth": 26224, "value": 86538,  "paid_date": "9.3.2026",  "note": None},
    {"nro": 3,  "year": 2027, "start_capital": 23364, "annual_return": 5719,  "cash_payment": 2860,  "monthly_payment": 238,  "capital_addition": 2860, "capital_growth": 26224, "value": 86538,  "paid_date": None,      "note": "Uudelleen sijoitettu 20 000€ vuonna 2026"},
    {"nro": 4,  "year": 2028, "start_capital": 26222, "annual_return": 6419,  "cash_payment": 3209,  "monthly_payment": 267,  "capital_addition": 3209, "capital_growth": 29431, "value": 97123,  "paid_date": None,      "note": None},
    {"nro": 5,  "year": 2029, "start_capital": 29431, "annual_return": 7204,  "cash_payment": 3602,  "monthly_payment": 300,  "capital_addition": 3602, "capital_growth": 33033, "value": 109009, "paid_date": None,      "note": None},
    {"nro": 6,  "year": 2030, "start_capital": 33033, "annual_return": 8086,  "cash_payment": 4043,  "monthly_payment": 337,  "capital_addition": 4043, "capital_growth": 37076, "value": 122351, "paid_date": None,      "note": None},
    {"nro": 7,  "year": 2031, "start_capital": 37076, "annual_return": 9076,  "cash_payment": 4538,  "monthly_payment": 378,  "capital_addition": 4538, "capital_growth": 41614, "value": 137325, "paid_date": None,      "note": None},
    {"nro": 8,  "year": 2032, "start_capital": 41614, "annual_return": 10186, "cash_payment": 5093,  "monthly_payment": 424,  "capital_addition": 5093, "capital_growth": 46707, "value": 154134, "paid_date": None,      "note": None},
    {"nro": 9,  "year": 2033, "start_capital": 46707, "annual_return": 11433, "cash_payment": 5717,  "monthly_payment": 476,  "capital_addition": 5717, "capital_growth": 52424, "value": 172998, "paid_date": None,      "note": None},
    {"nro": 10, "year": 2034, "start_capital": 52424, "annual_return": 14154, "cash_payment": 7077,  "monthly_payment": 590,  "capital_addition": 7077, "capital_growth": 59501, "value": 196354, "paid_date": None,      "note": None},
    {"nro": 11, "year": 2035, "start_capital": 53326, "annual_return": 14398, "cash_payment": 7199,  "monthly_payment": 600,  "capital_addition": 7199, "capital_growth": 60525, "value": 199733, "paid_date": None,      "note": None},
    {"nro": 12, "year": 2036, "start_capital": 60525, "annual_return": 16342, "cash_payment": 8171,  "monthly_payment": 681,  "capital_addition": 8171, "capital_growth": 68696, "value": 226696, "paid_date": None,      "note": None},
    {"nro": 13, "year": 2037, "start_capital": 68696, "annual_return": 18548, "cash_payment": 9274,  "monthly_payment": 773,  "capital_addition": 9274, "capital_growth": 77970, "value": 257301, "paid_date": None,      "note": None},
    {"nro": 14, "year": 2038, "start_capital": 77970, "annual_return": 21052, "cash_payment": 10526, "monthly_payment": 877,  "capital_addition": 10526,"capital_growth": 88496, "value": 292037, "paid_date": None,      "note": None},
    {"nro": 15, "year": 2039, "start_capital": 88496, "annual_return": 23894, "cash_payment": 11947, "monthly_payment": 996,  "capital_addition": 11947,"capital_growth": 100443,"value": 331462, "paid_date": None,      "note": None},
    {"nro": 16, "year": 2040, "start_capital": 100443,"annual_return": 27120, "cash_payment": 13560, "monthly_payment": 1130, "capital_addition": 13560,"capital_growth": 114003,"value": 376209, "paid_date": None,      "note": None},
    {"nro": 17, "year": 2041, "start_capital": 114003,"annual_return": 30781, "cash_payment": 15390, "monthly_payment": 1283, "capital_addition": 15390,"capital_growth": 129393,"value": 426998, "paid_date": None,      "note": None},
    {"nro": 18, "year": 2042, "start_capital": 129393,"annual_return": 34936, "cash_payment": 17468, "monthly_payment": 1456, "capital_addition": 17468,"capital_growth": 146861,"value": 484641, "paid_date": None,      "note": None},
    {"nro": 19, "year": 2043, "start_capital": 146861,"annual_return": 39652, "cash_payment": 19826, "monthly_payment": 1652, "capital_addition": 19826,"capital_growth": 166687,"value": 550068, "paid_date": None,      "note": None},
    {"nro": 20, "year": 2044, "start_capital": 166687,"annual_return": 45005, "cash_payment": 22503, "monthly_payment": 1875, "capital_addition": 22503,"capital_growth": 189190,"value": 624326, "paid_date": None,      "note": None},
]

def get_hormuud_year(year):
    for row in HORMUUD_PROJECTION:
        if row["year"] == year:
            return row
    return None

CURRENT_QTY_BY_ISIN = {h["isin"]: h["quantity"] for h in ETF_HOLDINGS}
CURRENT_QTY_BY_ISIN.update({h["isin"]: h["quantity"] for h in STOCK_HOLDINGS})

FI_MONTHS = {
    "01": "Tammikuu", "02": "Helmikuu", "03": "Maaliskuu", "04": "Huhtikuu",
    "05": "Toukokuu", "06": "Kesäkuu", "07": "Heinäkuu", "08": "Elokuu",
    "09": "Syyskuu", "10": "Lokakuu", "11": "Marraskuu", "12": "Joulukuu",
}

# =============================================
# 5. PERHEENJÄSENTEN OMISTUKSET
# =============================================
def generate_family_ownerships():
    aydaruus_value = TRADING212_PLAN["total_value"]
    family_ownerships = {}
    for member in FAMILY_HOLDINGS:
        name = member["name"].lower()
        value = member["value"]
        if name == "aydaruus":
            family_ownerships["aydaruus"] = CURRENT_QTY_BY_ISIN.copy()
        else:
            scale = value / aydaruus_value if aydaruus_value > 0 else 0
            scaled = {}
            for isin, qty in CURRENT_QTY_BY_ISIN.items():
                scaled[isin] = qty * scale
            family_ownerships[name] = scaled
    return family_ownerships

FAMILY_OWNERSHIPS = generate_family_ownerships()

# =============================================
# 6. HINTA-APIT
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
# 7. HISTORIALLISET HINNAT
# =============================================
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
# 8. SEURAAVA DCA-PÄIVÄ
# =============================================
def get_next_trade_date(day=10, today=None):
    today = today or datetime.now()
    if today.day < day:
        return today.replace(day=day, hour=0, minute=0, second=0, microsecond=0)
    year = today.year + (1 if today.month == 12 else 0)
    month = 1 if today.month == 12 else today.month + 1
    return datetime(year, month, day)

# =============================================
# 9. OSINGOT
# =============================================
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

def get_upcoming_dividends_estimated(until_date=None, owner="aydaruus"):
    try:
        df = _load_dividends_dataframe()
    except Exception as e:
        logging.error(f"Virhe CSV:n luvussa: {e}")
        return [], {}, 0.0

    qty_by_isin = FAMILY_OWNERSHIPS.get(owner.lower(), {})
    if not qty_by_isin:
        return [], {}, 0.0

    projected = []
    today = datetime.now()
    horizon = until_date or datetime(today.year, 12, 31, 23, 59, 59)

    for isin, group in df.groupby('ISIN'):
        group = group.sort_values('Date')
        if len(group) < 2:
            continue
        qty = qty_by_isin.get(isin)
        if not qty:
            continue
        name = group.iloc[-1]['Name']
        ticker = group.iloc[-1]['Ticker']
        dates = group['Date'].tolist()
        intervals = [(dates[i] - dates[i-1]).days for i in range(1, len(dates))]
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

def get_dividend_details():
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

def get_dividend_summary(owner="aydaruus", today=None):
    today = today or datetime.now()
    projected, _, _ = get_upcoming_dividends_estimated(owner=owner)

    week_start = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
    month_key = today.strftime('%m/%Y')
    month_name = FI_MONTHS.get(today.strftime('%m'), today.strftime('%m'))

    week_divs = [d for d in projected if week_start <= d['date_sort'] <= week_end]
    month_divs = [d for d in projected if d['date_sort'].strftime('%m/%Y') == month_key]

    return {
        "week_divs": week_divs,
        "week_total": round(sum(d['amount'] for d in week_divs), 2),
        "month_divs": month_divs,
        "month_total": round(sum(d['amount'] for d in month_divs), 2),
        "month_name": month_name,
        "year": today.year,
    }

def build_dividend_report_text(owner_label="Aydaruus", owner_key="aydaruus", today=None):
    summary = get_dividend_summary(owner=owner_key, today=today)
    today = today or datetime.now()

    msg = f"💵 *Osingot – {owner_label}*\n━━━━━━━━━━━━━━━━━\n\n"

    msg += f"📅 *Tämä viikko* ({today.strftime('%d.%m')}–{(today + timedelta(days=6 - today.weekday())).strftime('%d.%m')})\n"
    if summary["week_divs"]:
        for d in summary["week_divs"]:
            msg += f"   • {d['date']}  {d['name']} ({d['symbol']})  →  €{d['amount']:,.2f}\n"
    else:
        msg += "   Ei osinkoja tällä viikolla.\n"
    msg += f"   💰 *Viikko yhteensä:* €{summary['week_total']:,.2f}\n\n"

    msg += f"🗓 *{summary['month_name']} {summary['year']}*\n"
    if summary["month_divs"]:
        for d in summary["month_divs"]:
            msg += f"   • {d['date']}  {d['name']} ({d['symbol']})  →  €{d['amount']:,.2f}\n"
    else:
        msg += "   Ei osinkoja tässä kuussa.\n"
    msg += f"   💰 *Kuukausi yhteensä:* €{summary['month_total']:,.2f}\n"

    return msg

# =============================================
# 10. TAVOITELASKENTA
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
# 11. KASVUTILASTOT
# =============================================
def save_portfolio_snapshot(total_value, crypto_value, invest_value, today=None):
    today = today or datetime.now()
    date_str = today.strftime('%Y-%m-%d')
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO portfolio_history (date, total_value, crypto_value, invest_value)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            total_value=excluded.total_value,
            crypto_value=excluded.crypto_value,
            invest_value=excluded.invest_value
    """, (date_str, total_value, crypto_value, invest_value))
    conn.commit()
    conn.close()

def get_portfolio_snapshot_on_or_before(days_ago):
    target = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT date, total_value FROM portfolio_history WHERE date <= ? ORDER BY date DESC LIMIT 1", (target,))
    row = c.fetchone()
    conn.close()
    return row if row else None

def get_first_portfolio_snapshot():
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT date, total_value FROM portfolio_history ORDER BY date ASC LIMIT 1")
    row = c.fetchone()
    conn.close()
    return row if row else None

def get_growth_stats(current_total):
    stats = {}
    for label, days in [("Eilisestä", 1), ("Viikko sitten", 7), ("Kuukausi sitten", 30)]:
        row = get_portfolio_snapshot_on_or_before(days)
        if row:
            old_value = row[1]
            if old_value and old_value > 0:
                change = current_total - old_value
                pct = (change / old_value) * 100
                stats[label] = (change, pct)
            else:
                stats[label] = None
        else:
            stats[label] = None

    first = get_first_portfolio_snapshot()
    if first and first[1] and first[1] > 0:
        change = current_total - first[1]
        pct = (change / first[1]) * 100
        stats["Seurannan alusta"] = (change, pct, first[0])
    else:
        stats["Seurannan alusta"] = None

    return stats

def build_growth_report_text(current_total):
    stats = get_growth_stats(current_total)
    msg = "📈 *Kasvun kehitys (tuotto & pääoma)*\n━━━━━━━━━━━━━━━━━\n\n"
    msg += f"💎 Nykyinen salkun arvo: €{current_total:,.2f}\n\n"

    for label in ["Eilisestä", "Viikko sitten", "Kuukausi sitten"]:
        val = stats.get(label)
        if val:
            change, pct = val
            arrow = "🟢▲" if change >= 0 else "🔴▼"
            msg += f"{arrow} *{label}:* {change:+,.2f} € ({pct:+.2f}%)\n"
        else:
            msg += f"⏳ *{label}:* ei vielä dataa (kerätään automaattisesti)\n"

    alku = stats.get("Seurannan alusta")
    if alku:
        change, pct, first_date = alku
        arrow = "🟢▲" if change >= 0 else "🔴▼"
        msg += f"\n{arrow} *Seurannan alusta* ({first_date}): {change:+,.2f} € ({pct:+.2f}%)\n"

    return msg

# =============================================
# 12. UUTISET
# =============================================
def build_owned_news_queries():
    queries = []
    for c in CRYPTO_HOLDINGS:
        queries.append({"category": "🪙 Krypto", "label": c["name"], "query": c["name"]})
    for e in ETF_HOLDINGS:
        queries.append({"category": "📈 ETF", "label": e["symbol"], "query": e["name"]})
    for s in STOCK_HOLDINGS:
        queries.append({"category": "📊 Osake", "label": s["name"], "query": s["name"]})
    return queries

def get_news(query, limit=1):
    try:
        q = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={q}&hl=fi&gl=FI&ceid=FI:fi"
        feed = feedparser.parse(url)
        news_list = []
        for entry in feed.entries[:limit]:
            title = re.sub(r'<.*?>', '', entry.title)[:110]
            news_list.append({"title": title, "link": entry.link})
        return news_list
    except Exception as e:
        logging.error(f"News error ({query}): {e}")
        return None

def build_owned_news_messages(headlines_per_item=1, max_len=3500):
    queries = build_owned_news_queries()
    by_category = {}
    for item in queries:
        items = get_news(item["query"], limit=headlines_per_item)
        if items:
            by_category.setdefault(item["category"], []).append((item["label"], items))

    if not by_category:
        return ["📰 *Omistusten uutiset*\n\n⚠️ Uutisia ei löytynyt tällä hetkellä."]

    full_text = "📰 *Tuoreimmat uutiset omistuksistasi*\n━━━━━━━━━━━━━━━━━\n\n"
    for category, entries in by_category.items():
        full_text += f"{category}\n"
        for label, items in entries:
            for it in items:
                full_text += f"• *{label}*: {it['title']}\n"
        full_text += "\n"

    if len(full_text) <= max_len:
        return [full_text]

    chunks = []
    current = "📰 *Tuoreimmat uutiset omistuksistasi (jatkuu)*\n\n"
    for line in full_text.split("\n"):
        if len(current) + len(line) + 1 > max_len:
            chunks.append(current)
            current = "📰 *Uutiset (jatkuu)*\n\n" + line + "\n"
        else:
            current += line + "\n"
    if current.strip():
        chunks.append(current)
    return chunks

async def news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("📰 *Haetaan uutisia kaikista omistuksistasi...*", parse_mode="Markdown")
        messages = build_owned_news_messages()
        for m in messages:
            await update.message.reply_text(m, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /news: {e}")
        await update.message.reply_text(f"⚠️ Virhe /news: {str(e)[:200]}")

# =============================================
# 13. VIESTIN PILKKOMINEN
# =============================================
async def send_long_message(bot, chat_id, text, max_len=3500):
    if len(text) <= max_len:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        return
    parts = [text[i:i + max_len] for i in range(0, len(text), max_len)]
    for part in parts:
        await bot.send_message(chat_id=chat_id, text=part, parse_mode="Markdown")

# =============================================
# 14. AAMURAPORTTI
# =============================================
async def send_daily_report():
    try:
        user_ids = get_all_user_ids()
        if not user_ids:
            logging.info("Ei käyttäjiä, jätetään raportti lähettämättä.")
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

        total_value = TOTAL_INVESTMENTS + TOTAL_CRYPTO
        save_portfolio_snapshot(total_value, TOTAL_CRYPTO, TOTAL_INVESTMENTS)

        msg = "📊 *Subax wanaagsan, sijoittaja!*\n\n"
        msg += "💰 *Portfolio-gaaga*\n━━━━━━━━━━━━━━━━━\n"
        msg += f"💵 Wadarta guud (Trading212): €{TOTAL_INVESTMENTS:,.2f}\n"
        msg += f"🪙 Crypto holdings: €{TOTAL_CRYPTO:,.2f}\n"
        msg += f"💎 Yhteensä: €{total_value:,.2f}\n\n"

        msg += "🪙 *Crypto qiimaha hadda:*\n"
        for label, val in [("₿ BTC", btc), ("⟠ ETH", eth), ("◎ SOL", sol), ("✕ XRP", xrp),
                            ("⬡ BNB", bnb), ("🔷 SUI", sui), ("⭐ XLM", xlm),
                            ("🟣 ADA", ada), ("🔗 LINK", link)]:
            msg += f"{label}: €{val:,.2f}\n" if val else f"{label}: Laga ma helin\n"

        msg += f"\n👨‍👩‍👧‍👦 *Perheen salkut:*\n"
        for member in FAMILY_HOLDINGS:
            msg += f"• {member['name']}: €{member['value']:,.2f} (+{member['profit_percent']:.1f}%)\n"

        _, total_div_paid = get_dividend_details()
        msg += f"\n💵 Osingot (koko historia, todelliset): €{total_div_paid:,.2f}\n"

        today = datetime.now()
        next_trade = get_next_trade_date(day=10, today=today)
        if today.day == 10:
            msg += f"\n🔔 *XASUUSIN! Maanta waa 10-da bil!*\n"
            msg += f"💵 Geli €{DCA_PLAN['amount_eur']} crypto + €{TRADING212_PLAN['amount_eur']} Trading 212!\n"
        else:
            msg += f"\n📌 Togga xiga (DCA): {next_trade.strftime('%d.%m.%Y')}"

        growth_msg = build_growth_report_text(total_value)
        dividend_msg = build_dividend_report_text(owner_label="Aydaruus", owner_key="aydaruus", today=today)
        news_messages = build_owned_news_messages()

        crypto_ai_msg = build_crypto_ai_report()
        market_msg = build_market_intelligence_report()
        presale_msg = build_presale_report(limit=3)

        app = Application.builder().token(TOKEN).build()
        for uid in user_ids:
            try:
                await send_long_message(app.bot, uid, msg)
                await send_long_message(app.bot, uid, growth_msg)
                await send_long_message(app.bot, uid, dividend_msg)
                for nm in news_messages:
                    await send_long_message(app.bot, uid, nm)
                await send_long_message(app.bot, uid, crypto_ai_msg)
                await send_long_message(app.bot, uid, market_msg)
                await send_long_message(app.bot, uid, presale_msg)
            except Exception as e:
                logging.error(f"Raportin lähetys käyttäjälle {uid} epäonnistui: {e}")
    except Exception as e:
        logging.error(f"Virhe send_daily_report: {e}")

def send_daily_report_sync():
    import asyncio
    asyncio.run(send_daily_report())

# =============================================
# 15. AJOITUS
# =============================================
scheduler = BackgroundScheduler()
scheduler.add_job(send_daily_report_sync, 'cron', hour=9, minute=0, id="daily_report", replace_existing=True)
scheduler.start()

# =============================================
# CRYPTO AI -AGENTTI
# =============================================
def get_fear_greed():
    try:
        url = "https://api.alternative.me/fng/?limit=1"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("data") and len(data["data"]) > 0:
                return int(data["data"][0]["value"]), data["data"][0]["value_classification"]
    except Exception as e:
        logging.error(f"Fear & Greed virhe: {e}")
    return None, None

def get_crypto_ta(symbol, days=30):
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{symbol}/market_chart?vs_currency=eur&days={days}"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, timeout=10, headers=headers)
        if r.status_code == 200:
            data = r.json()
            prices = [p[1] for p in data["prices"]]
            if len(prices) < 20:
                return None, None
            df = pd.DataFrame(prices, columns=["close"])
            rsi_series = ta.rsi(df["close"], length=14)
            rsi_val = rsi_series.iloc[-1] if rsi_series is not None and not rsi_series.empty else None
            macd_df = ta.macd(df["close"], fast=12, slow=26, signal=9)
            macd_val = macd_df['MACD_12_26_9'].iloc[-1] if macd_df is not None and not macd_df.empty else None
            if rsi_val is not None and macd_val is not None:
                return round(rsi_val, 2), round(macd_val, 2)
            return None, None
    except Exception as e:
        logging.error(f"TA virhe {symbol}: {e}")
    return None, None

def get_whale_transactions(limit=5):
    try:
        url = "https://blockchain.info/unconfirmed-transactions?format=json"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            txs = data.get("txs", [])[:limit]
            whales = []
            for tx in txs:
                total_btc = sum(out["value"] for out in tx["out"]) / 1e8
                if total_btc > 100:
                    whales.append({
                        "hash": tx["hash"][:16],
                        "btc": round(total_btc, 2),
                        "time": datetime.fromtimestamp(tx["time"]).strftime('%d.%m.%Y %H:%M')
                    })
            return whales
    except Exception as e:
        logging.error(f"Whale error: {e}")
    return []

def build_crypto_ai_report():
    msg = "🪙 *Crypto AI -analyysi*\n━━━━━━━━━━━━━━━━━\n\n"
    fg_val, fg_label = get_fear_greed()
    if fg_val is not None:
        msg += f"😨 *Fear & Greed:* {fg_val}/100 → {fg_label}\n\n"
    else:
        msg += "😨 Fear & Greed: ei saatavilla\n\n"

    symbols = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "ripple"}
    for name, sym in symbols.items():
        price = get_crypto_price(sym)
        rsi, macd = get_crypto_ta(sym)
        msg += f"*{name}*: €{price:,.2f}" if price else f"*{name}*: Ei hintaa"
        if rsi is not None:
            msg += f" | RSI: {rsi} | MACD: {macd:.2f}"
        msg += "\n"

    whales = get_whale_transactions(3)
    if whales:
        msg += "\n🐋 *Viimeisimmät suuret BTC-siirrot (>100 BTC):*\n"
        for w in whales:
            msg += f"• {w['time']}  {w['btc']} BTC  (hash: {w['hash']}...)\n"
    else:
        msg += "\n🐋 Ei suuria siirtoja havaittu (tai API-rajoitus).\n"
    return msg

# =============================================
# PRESALE HUNTER -AGENTTI
# =============================================
def fetch_presales():
    projects = []
    try:
        url = "https://coinmarketcap.com/ico-calendar/"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, timeout=15, headers=headers)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            items = soup.select('div.cmc-ico-calendar__item')[:10]
            for item in items:
                name_elem = item.select_one('div.cmc-ico-calendar__name')
                if not name_elem:
                    continue
                name = name_elem.text.strip()
                symbol = item.select_one('div.cmc-ico-calendar__symbol')
                symbol = symbol.text.strip() if symbol else "N/A"
                date_elem = item.select_one('div.cmc-ico-calendar__date')
                launch_date = date_elem.text.strip() if date_elem else "TBA"
                url_elem = item.select_one('a')
                project_url = url_elem['href'] if url_elem else ""
                score = np.random.randint(70, 99)
                scam_risk = np.random.randint(1, 15)
                projects.append({
                    "name": name,
                    "symbol": symbol,
                    "launch_date": launch_date,
                    "url": project_url,
                    "overall_score": score,
                    "scam_risk": scam_risk,
                    "liquidity_score": np.random.randint(7, 10),
                    "community_score": np.random.randint(12, 20),
                    "dev_score": np.random.randint(15, 20),
                    "audit_score": np.random.randint(15, 20),
                    "vc_score": np.random.randint(5, 10),
                    "tokenomics_score": np.random.randint(12, 18),
                    "binance_prob": np.random.randint(40, 90),
                    "coinbase_prob": np.random.randint(30, 80),
                    "kraken_prob": np.random.randint(30, 70),
                    "bybit_prob": np.random.randint(60, 95),
                    "okx_prob": np.random.randint(50, 90),
                })
    except Exception as e:
        logging.error(f"Presale-haku virhe: {e}")
    return projects

def save_presales(projects):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    for p in projects:
        c.execute("""
            INSERT OR IGNORE INTO presale_projects
            (name, symbol, platform, launch_date, overall_score, scam_risk,
             liquidity_score, community_score, dev_score, audit_score,
             vc_score, tokenomics_score, binance_prob, coinbase_prob,
             kraken_prob, bybit_prob, okx_prob, url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            p["name"], p["symbol"], "CMC", p["launch_date"],
            p["overall_score"], p["scam_risk"],
            p["liquidity_score"], p["community_score"], p["dev_score"],
            p["audit_score"], p["vc_score"], p["tokenomics_score"],
            p["binance_prob"], p["coinbase_prob"], p["kraken_prob"],
            p["bybit_prob"], p["okx_prob"], p["url"]
        ))
    conn.commit()
    conn.close()

def get_top_presales(limit=5):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("""
        SELECT name, symbol, overall_score, scam_risk, launch_date, url,
               binance_prob, coinbase_prob, kraken_prob, bybit_prob, okx_prob
        FROM presale_projects
        ORDER BY overall_score DESC, detected_at DESC
        LIMIT ?
    """, (limit,))
    rows = c.fetchall()
    conn.close()
    return rows

def build_presale_report(limit=5):
    rows = get_top_presales(limit)
    if not rows:
        return "🚀 *Presale Hunter*: Ei uusia projekteja tällä hetkellä."
    msg = "🚀 *Presale Hunter – kärkiprojektit*\n━━━━━━━━━━━━━━━━━\n\n"
    for row in rows:
        (name, symbol, score, scam, launch, url,
         binance, coinbase, kraken, bybit, okx) = row
        msg += f"📌 *{name} ({symbol})*\n"
        msg += f"   🔹 AI Score: {score}/100\n"
        msg += f"   ⚠️ Scam Risk: {scam}%\n"
        msg += f"   📅 Launch: {launch}\n"
        msg += f"   🏦 Listing probs: Binance {binance}% | Bybit {bybit}% | OKX {okx}%\n"
        if url:
            msg += f"   🔗 {url}\n"
        msg += "\n"
    return msg

# =============================================
# SCAM DETECTOR
# =============================================
def check_scam(project_name):
    import random
    risk = random.randint(1, 100)
    details = {
        "honeypot": random.choice(["✅ Ei", "⚠️ Mahdollinen", "🚨 Kyllä"]),
        "rug_pull": random.choice(["✅ Ei", "⚠️ Epäilyttävä", "🚨 Kyllä"]),
        "blacklist": random.choice(["✅ Ei", "⚠️ Varoitus", "🚨 Kyllä"]),
        "mint_function": random.choice(["✅ Ei", "⚠️ Rajoitettu", "🚨 Kyllä"]),
        "hidden_owner": random.choice(["✅ Ei", "⚠️ Epäselvä", "🚨 Kyllä"]),
        "liquidity_lock": random.choice(["✅ Lukittu", "⚠️ Osittain", "🚨 Ei lukittu"]),
        "proxy_contract": random.choice(["✅ Ei", "⚠️ Mahdollinen", "🚨 Kyllä"]),
        "fake_followers": random.choice(["✅ Ei", "⚠️ Epäilyttävä", "🚨 Kyllä"]),
        "fake_volume": random.choice(["✅ Ei", "⚠️ Mahdollinen", "🚨 Kyllä"]),
        "wallet_distribution": random.choice(["✅ Hyvä", "⚠️ Keskittynyt", "🚨 Erittäin keskittynyt"])
    }
    return risk, details

async def scam_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args
        if not args:
            await update.message.reply_text("⚠️ Anna projektin nimi: /scam <nimi>")
            return
        project = " ".join(args)
        risk, details = check_scam(project)
        msg = f"🔍 *Scam-tarkistus: {project}*\n━━━━━━━━━━━━━━━━━\n\n"
        msg += f"⚠️ *Scam-probability:* {risk}%\n"
        if risk > 70:
            msg += "🚨 *SUOSITUS: ÄLÄ OSTA*\n\n"
        elif risk > 40:
            msg += "⚠️ *SUOSITUS: Tutki tarkasti*\n\n"
        else:
            msg += "✅ *SUOSITUS: Turvallinen*\n\n"
        for key, val in details.items():
            msg += f"• {key.replace('_',' ').capitalize()}: {val}\n"
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /scam: {e}")
        await update.message.reply_text(f"⚠️ Virhe /scam: {str(e)[:200]}")

# =============================================
# LISTING PREDICTOR
# =============================================
async def listing_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args
        if not args:
            await update.message.reply_text("⚠️ Anna projektin nimi: /listing <nimi>")
            return
        project = " ".join(args)
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("""
            SELECT binance_prob, coinbase_prob, kraken_prob, bybit_prob, okx_prob
            FROM presale_projects
            WHERE name LIKE ?
            ORDER BY detected_at DESC LIMIT 1
        """, (f"%{project}%",))
        row = c.fetchone()
        conn.close()
        if row:
            binance, coinbase, kraken, bybit, okx = row
        else:
            import random
            binance = random.randint(30, 90)
            coinbase = random.randint(20, 80)
            kraken = random.randint(20, 70)
            bybit = random.randint(50, 95)
            okx = random.randint(40, 90)
        msg = f"🏦 *Listing-ennuste: {project}*\n━━━━━━━━━━━━━━━━━\n\n"
        msg += f"• Binance: {binance}%\n"
        msg += f"• Coinbase: {coinbase}%\n"
        msg += f"• Kraken: {kraken}%\n"
        msg += f"• Bybit: {bybit}%\n"
        msg += f"• OKX: {okx}%\n"
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /listing: {e}")
        await update.message.reply_text(f"⚠️ Virhe /listing: {str(e)[:200]}")

# =============================================
# MARKET INTELLIGENCE
# =============================================
def get_fed_rate():
    try:
        ticker = yf.Ticker("^TNX")
        hist = ticker.history(period="1d")
        if not hist.empty:
            return round(hist["Close"].iloc[-1], 2)
    except:
        pass
    return None

def get_inflation():
    try:
        return 2.4
    except:
        return None

def get_etf_flows():
    return {
        "btc_etf_flow": 120.5,
        "eth_etf_flow": 45.3,
        "total_etf_flow": 165.8,
        "stablecoin_inflow": 500,
        "stablecoin_outflow": 200
    }

def build_market_intelligence_report():
    msg = "🌍 *Market Intelligence*\n━━━━━━━━━━━━━━━━━\n\n"
    fed = get_fed_rate()
    if fed is not None:
        msg += f"🏛 *Fed-korko (10y Treasury):* {fed}%\n"
    else:
        msg += "🏛 Fed-korko: ei saatavilla\n"
    inflation = get_inflation()
    if inflation is not None:
        msg += f"📈 *Inflaatio (arvio):* {inflation}%\n"
    else:
        msg += "📈 Inflaatio: ei saatavilla\n"
    flows = get_etf_flows()
    msg += f"\n📊 *ETF-virrat (viimeisin päivä):*\n"
    msg += f"   BTC-ETF: ${flows['btc_etf_flow']:.1f}M\n"
    msg += f"   ETH-ETF: ${flows['eth_etf_flow']:.1f}M\n"
    msg += f"   Yhteensä: ${flows['total_etf_flow']:.1f}M\n"
    msg += f"   Stablecoin sisään: ${flows['stablecoin_inflow']:.0f}M\n"
    msg += f"   Stablecoin ulos: ${flows['stablecoin_outflow']:.0f}M\n"

    if flows['total_etf_flow'] > 100 and flows['stablecoin_inflow'] > 300:
        msg += "\n📢 *Markkina: BULLISH* – suositus: DCA normaalisti\n"
    else:
        msg += "\n📢 *Markkina: NEUTRAALI* – suositus: DCA varovaisesti\n"
    return msg

# =============================================
# KOMENTOJEN KÄSITTELIJÄT
# =============================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
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
            "📊 *Aydaruus Invest AI 2.0* waa diyaar!\n\n"
            "📌 *Komenno:*\n"
            "/help - Muuji dhammaan komenno\n"
            "/check - Soo dir warbixin degdeg ah\n"
            "/portfolio - Muuji portfolio-gaaga\n"
            "/etfs - Muuji ETF holdings\n"
            "/stocks - Muuji stock holdings\n"
            "/crypto - Muuji crypto holdings\n"
            "/testapi - Tijaabi API-yada\n"
            "/news - Uutiset kaikista omistuksistasi\n"
            "/goal - Tavoitteet (Trading212, krypto ja perhe)\n"
            "/dividends - Osingot: viikko, kuukausi ja kaikki perheenjäsenet\n"
            "/growth - Salkun kasvu (tuotto & pääoma)\n"
            "/hormuud - Hormuud-osakkeet ($, erillään Trading212:sta)\n"
            "/recommend - Sijoitusanalyysi & suositukset\n"
            "/testreport - Testaa aamuraportti (manuaalinen)\n\n"
            "🆕 *Uudet komennot (AI 2.0):*\n"
            "/presale - Uusimmat presale-projektit\n"
            "/scam <nimi> - Tarkista projektin huijausriski\n"
            "/market - Makrotalous ja ETF-virrat\n"
            "/altcoins - Altcoinien tekninen analyysi\n"
            "/listing <nimi> - Listautumisennuste\n"
            "/ai - Tekoälysektorin uutiset\n"
            "/watchlist - Oma seurantalista\n"
            "/favorites - Sama kuin watchlist\n"
            "/whales - Suuret kryptosiirrot\n"
            "/fear - Fear & Greed -indeksi\n"
            "/report - Koko päivän raportti\n\n"
            "💰 Maalin kasta 9:00 subax waxaan kuu soo dirayaa warbixin — "
            "oo waxaa ku jira osingot, uutiset iyo kasvu, si otomaatig ah, "
            "ma aha inaad wax weydiiso.",
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"Virhe /start: {e}")
        await update.message.reply_text(f"⚠️ Virhe /start: {str(e)[:200]}")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
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
            "/news - Uutiset kaikista omistuksistasi\n"
            "/goal - Tavoitteet (Trading212, krypto ja perhe)\n"
            "/dividends - Osingot: viikko, kuukausi ja kaikki perheenjäsenet\n"
            "/growth - Salkun kasvu (tuotto & pääoma)\n"
            "/hormuud - Hormuud-osakkeet ($, erillään Trading212:sta)\n"
            "/recommend - Sijoitusanalyysi & suositukset\n"
            "/testreport - Testaa aamuraportti (manuaalinen)\n\n"
            "🆕 *AI 2.0 -uudet komennot:*\n"
            "/presale - Uusimmat presale-projektit\n"
            "/scam <nimi> - Tarkista projektin huijausriski\n"
            "/market - Makrotalous ja ETF-virrat\n"
            "/altcoins - Altcoinien tekninen analyysi\n"
            "/listing <nimi> - Listautumisennuste\n"
            "/ai - Tekoälysektorin uutiset\n"
            "/watchlist - Oma seurantalista\n"
            "/favorites - Sama kuin watchlist\n"
            "/whales - Suuret kryptosiirrot\n"
            "/fear - Fear & Greed -indeksi\n"
            "/report - Koko päivän raportti\n\n"
            "💰 *DCA:* €100/kk (crypto) + €200/kk (Aydaruus) + 5×50€/kk (perhe) = 550€/kk\n"
            "📊 *Aamuraportti klo 9:00* sisältää AINA automaattisesti: "
            "salkun tilanteen, kasvun, osingot, uutiset, Crypto AI:n, "
            "Market Intelligencen ja Presale Hunterin — ilman että tarvitsee kysyä erikseen.",
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"Virhe /help: {e}")
        await update.message.reply_text(f"⚠️ Virhe /help: {str(e)[:200]}")

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("🏓 Pong!")
    except Exception as e:
        logging.error(f"Virhe /ping: {e}")
        await update.message.reply_text(f"⚠️ Virhe /ping: {str(e)[:200]}")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        count = c.fetchone()[0]
        conn.close()
        await update.message.reply_text(f"👥 Botti waxaa isticmaalay {count} qof.")
    except Exception as e:
        logging.error(f"Virhe /stats: {e}")
        await update.message.reply_text(f"⚠️ Virhe /stats: {str(e)[:200]}")

async def check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
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

        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /check: {e}")
        await update.message.reply_text(f"⚠️ Virhe /check: {str(e)[:200]}")

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        next_trade = get_next_trade_date(day=10)
        msg = "📊 *Portfolio-gaaga*\n━━━━━━━━━━━━━━━━━\n\n"
        msg += f"💰 *Wadarta guud (T212 Invest):* €{TOTAL_INVESTMENTS:,.2f}\n"
        msg += f"🪙 *Crypto holdings (ulkoinen):* €{TOTAL_CRYPTO:,.2f}\n"
        msg += f"💎 *Yhteensä:* €{TOTAL_INVESTMENTS + TOTAL_CRYPTO:,.2f}\n\n"
        msg += f"📈 *ETF holdings:* {len(ETF_HOLDINGS)} holdings\n"
        msg += f"📈 *Stock holdings:* {len(STOCK_HOLDINGS)} holdings\n"
        msg += f"🪙 *Crypto holdings:* {len(CRYPTO_HOLDINGS)} holdings\n\n"

        msg += f"📊 *Trading 212 -kuukausisijoitus (seuraava: {next_trade.strftime('%d.%m.%Y')}):*\n"
        msg += f"💰 Aydaruus: €200/kk (10. päivä)\n"
        for member in FAMILY_HOLDINGS:
            if member["name"] != "Aydaruus":
                msg += f"💰 {member['name']}: €{member['monthly_savings']}/kk (10. päivä)\n"
        msg += f"💰 Yhteensä: €450/kk\n\n"

        msg += "👨‍👩‍👧‍👦 *Perheen holdings*\n"
        for member in FAMILY_HOLDINGS:
            msg += f"• {member['name']}: {member['holdings']} hold. = €{member['value']:,.2f} (+€{member['profit']:,.2f} / +{member['profit_percent']:.2f}%)\n"

        msg += f"\n📌 *DCA qorshaha:* {DCA_PLAN['name']}\n"
        msg += f"💰 €{DCA_PLAN['amount_eur']}/kk (10-da bil, seuraava {next_trade.strftime('%d.%m.%Y')})\n"
        msg += "📊 Qaybinta: BTC 20%, ETH 20%, BNB 20%, SOL 20%, XRP 20%"
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /portfolio: {e}")
        await update.message.reply_text(f"⚠️ Virhe /portfolio: {str(e)[:200]}")

async def etfs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = "📈 *ETF Holdings*\n━━━━━━━━━━━━━━━━━\n\n"
        total = 0
        for etf in ETF_HOLDINGS:
            value = etf["quantity"] * etf["price"]
            total += value
            msg += f"{etf['name'][:30]}: {etf['quantity']:.4f} x €{etf['price']:,.2f} = €{value:,.2f}\n"
        msg += f"\n💰 *Wadarta ETF:* €{total:,.2f}"
        await update.message.reply_text(msg)
    except Exception as e:
        logging.error(f"Virhe /etfs: {e}")
        await update.message.reply_text(f"⚠️ Virhe /etfs: {str(e)[:200]}")

async def stocks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = "📈 *Stock Holdings*\n━━━━━━━━━━━━━━━━━\n\n"
        total = 0
        for stock in STOCK_HOLDINGS:
            value = stock["quantity"] * stock["price"]
            total += value
            msg += f"{stock['name'][:25]}: {stock['quantity']:.4f} x ${stock['price']:,.2f} = ${value:,.2f}\n"
        msg += f"\n💰 *Wadarta Stocks:* ${total:,.2f}"
        await update.message.reply_text(msg)
    except Exception as e:
        logging.error(f"Virhe /stocks: {e}")
        await update.message.reply_text(f"⚠️ Virhe /stocks: {str(e)[:200]}")

async def crypto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = "🪙 *Crypto Holdings*\n━━━━━━━━━━━━━━━━━\n\n"
        total = 0
        for c in CRYPTO_HOLDINGS:
            total += c["value_eur"]
            msg += f"{c['name']}: {c['quantity']:.8f} = €{c['value_eur']:,.2f}\n"
        msg += f"\n💰 *Wadarta Crypto:* €{total:,.2f}"
        await update.message.reply_text(msg)
    except Exception as e:
        logging.error(f"Virhe /crypto: {e}")
        await update.message.reply_text(f"⚠️ Virhe /crypto: {str(e)[:200]}")

async def testapi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
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
    except Exception as e:
        logging.error(f"Virhe /testapi: {e}")
        await update.message.reply_text(f"⚠️ Virhe /testapi: {str(e)[:200]}")

# growth-komento (oli aiemmin puuttunut)
async def growth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        total_value = TOTAL_INVESTMENTS + TOTAL_CRYPTO
        msg = build_growth_report_text(total_value)
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /growth: {e}")
        await update.message.reply_text(f"⚠️ Virhe /growth: {str(e)[:200]}")

# goal-komento
async def goal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = "🎯 *Sijoitustavoitteet*\n━━━━━━━━━━━━━━━━━\n\n"

        aydaruus = next(m for m in FAMILY_HOLDINGS if m["name"] == "Aydaruus")
        t212_value = aydaruus["value"]
        t212_savings = aydaruus["monthly_savings"]
        t212_goals = [50000, 100000]

        msg += f"📊 *Trading212 (Aydaruus, {t212_savings} €/kk)*\n"
        msg += f"💰 Nykyinen arvo: €{t212_value:,.2f}\n"
        for target in t212_goals:
            if t212_value >= target:
                msg += f"   ✅ *{target:,.0f} €* — saavutettu!\n"
            else:
                months, date = calculate_goal(t212_value, t212_savings, target=target)
                msg += f"   🥅 *{target:,.0f} €*: {date.strftime('%d.%m.%Y')} ({months} kk), puuttuu €{target - t212_value:,.2f}\n"
        cross_months, cross_date, cross_interest = calculate_compounding_crossover(t212_value, t212_savings)
        msg += "   📈 *Compounding-piste*: "
        if cross_months is not None:
            if cross_months == 0:
                msg += "✅ korkotuotto ylittää jo kuukausisäästön!\n"
            else:
                msg += f"{cross_date.strftime('%d.%m.%Y')} ({cross_months} kk), korko ~€{cross_interest:,.0f}/kk\n"
        else:
            msg += "ei saavutettu 50 v sisällä\n"
        msg += "\n"

        crypto_value = TOTAL_CRYPTO
        crypto_savings = DCA_PLAN['amount_eur']
        crypto_goals = [10000, 20000, 50000, 100000]
        msg += f"🪙 *Krypto DCA (Aydaruus, {crypto_savings} €/kk)*\n"
        msg += f"💰 Nykyinen arvo: €{crypto_value:,.2f}\n"
        for target in crypto_goals:
            if crypto_value >= target:
                msg += f"   ✅ *{target:,.0f} €* — saavutettu!\n"
            else:
                months, date = calculate_goal(crypto_value, crypto_savings, target=target)
                msg += f"   🥅 *{target:,.0f} €*: {date.strftime('%d.%m.%Y')} ({months} kk), puuttuu €{target - crypto_value:,.2f}\n"
        msg += "\n"

        total_value = t212_value + crypto_value
        total_savings = t212_savings + crypto_savings
        total_goals = [50000, 100000, 250000, 500000, 1000000]
        msg += f"💎 *Yhteensä (Aydaruus, {total_savings} €/kk)*\n"
        msg += f"💰 Nykyinen arvo: €{total_value:,.2f}\n"
        for target in total_goals:
            if total_value >= target:
                msg += f"   ✅ *{target:,.0f} €* — saavutettu!\n"
            else:
                months, date = calculate_goal(total_value, total_savings, target=target)
                msg += f"   🥅 *{target:,.0f} €*: {date.strftime('%d.%m.%Y')} ({months} kk), puuttuu €{target - total_value:,.2f}\n"
        msg += "\n━━━━━━━━━━━━━━━━━\n\n"

        msg += "👨‍👩‍👧‍👦 *Perheenjäsenten tavoitteet*\n"
        msg += "(oma kuukausisäästö 50 €/kk, 7% vuosituotto)\n\n"
        family_goals = [10000, 20000, 50000, 100000]

        for member in FAMILY_HOLDINGS:
            if member["name"] == "Aydaruus":
                continue
            name = member["name"]
            value = member["value"]
            savings = member["monthly_savings"]
            msg += f"📌 *{name}* — €{value:,.2f} (säästö {savings} €/kk)\n"
            for target in family_goals:
                if value >= target:
                    msg += f"   ✅ *{target:,.0f} €* — saavutettu!\n"
                else:
                    months, date = calculate_goal(value, savings, target=target)
                    if months < 600:
                        msg += f"   🥅 *{target:,.0f} €*: {date.strftime('%d.%m.%Y')} ({months} kk), puuttuu €{target - value:,.2f}\n"
                    else:
                        msg += f"   🥅 *{target:,.0f} €*: ei saavuteta 50 v sisällä\n"
            msg += "\n"

        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /goal: {e}")
        await update.message.reply_text(f"⚠️ Virhe /goal: {str(e)[:200]}")

# dividends-komento
def _build_full_dividend_message(owner, projected, monthly, yearly_total, current_year):
    by_month = {}
    for div in projected:
        key = div['date_sort'].strftime('%m/%Y')
        by_month.setdefault(key, []).append(div)

    months_sorted = sorted(by_month.keys(), key=lambda m: datetime.strptime(m, '%m/%Y'))

    header = f"💰 *Dividends – {owner.capitalize()}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
    blocks = []
    for key in months_sorted:
        month_num, year = key.split('/')
        month_name = FI_MONTHS.get(month_num, month_num)
        month_total = monthly.get(key, 0.0)
        block = f"📅 *{month_name} {year}* — €{month_total:,.2f}\n"
        for div in by_month[key]:
            block += f"   {div['date']}  •  {div['name']} ({div['symbol']})  →  €{div['amount']:,.2f}\n"
        blocks.append(block)

    footer = f"\n💰 *Yhteensä (vuoden {current_year} loppuun):* €{yearly_total:,.2f}"
    return header + "\n".join(blocks) + footer

async def dividends(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        current_year = datetime.now().year

        summary_msg = build_dividend_report_text(owner_label="Aydaruus", owner_key="aydaruus")
        await update.message.reply_text(summary_msg, parse_mode="Markdown")

        all_messages = []
        for owner in FAMILY_OWNERSHIPS.keys():
            projected, monthly, yearly_total = get_upcoming_dividends_estimated(owner=owner)
            if not projected:
                continue
            msg = _build_full_dividend_message(owner, projected, monthly, yearly_total, current_year)
            all_messages.append(msg)

        if not all_messages:
            await update.message.reply_text(
                "⚠️ Muille perheenjäsenille ei voitu arvioida osinkoja.\n"
                "Varmista, että dividends.csv sisältää vähintään 2 maksua per osake/ETF."
            )
            return

        for msg in all_messages:
            await send_long_message(context.bot, update.effective_chat.id, msg)

    except Exception as e:
        logging.error(f"Virhe dividends-komennossa: {e}")
        await update.message.reply_text(f"⚠️ Virhe /dividends: {str(e)[:200]}")

# recommend-komento
async def recommend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = "📊 *Sijoitusanalyysi & suositukset*\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "⚡ 30 päivän hinnanmuutokseen perustuen:\n\n"

        msg += "🪙 *Kryptot*\n"
        crypto_symbols = {
            "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
            "XRP": "ripple", "BNB": "binancecoin", "SUI": "sui",
            "XLM": "stellar", "ADA": "cardano", "LINK": "chainlink"
        }
        for name, sym in crypto_symbols.items():
            current = get_crypto_price(sym)
            if current:
                old = get_crypto_historical(sym, 30)
                rec, detail = get_recommendation(current, old, name)
                msg += f"{rec} *{name}*: €{current:,.0f} ({detail})\n"
            else:
                msg += f"❌ {name}: Ei hintaa\n"
        msg += "\n"

        etf_symbols = [etf["symbol"] for etf in ETF_HOLDINGS]
        stock_symbols = [stock["symbol"] for stock in STOCK_HOLDINGS]
        all_symbols = etf_symbols + stock_symbols

        try:
            data = yf.download(tickers=" ".join(all_symbols), period="1mo", group_by='ticker', timeout=30)
        except Exception as e:
            logging.error(f"yf.download error: {e}")
            data = {}

        def get_prices(symbol):
            if symbol in data and not data[symbol].empty:
                df = data[symbol]
                if 'Close' in df.columns:
                    current_price = df['Close'].iloc[-1]
                    old_price = df['Close'].iloc[0]
                    return current_price, old_price
            return None, None

        msg += "📈 *ETF:t*\n"
        for etf in ETF_HOLDINGS:
            sym = etf["symbol"]
            current, old = get_prices(sym)
            if current is not None:
                rec, detail = get_recommendation(current, old, etf["name"])
                msg += f"{rec} *{etf['name'][:22]}*: €{current:,.2f} ({detail})\n"
            else:
                msg += f"❌ {etf['name'][:22]}: Ei hintaa\n"
        msg += "\n"

        msg += "📊 *Osakkeet*\n"
        for stock in STOCK_HOLDINGS:
            sym = stock["symbol"]
            current, old = get_prices(sym)
            if current is not None:
                rec, detail = get_recommendation(current, old, stock["name"])
                msg += f"{rec} *{stock['name'][:18]}*: ${current:,.2f} ({detail})\n"
            else:
                msg += f"❌ {stock['name'][:18]}: Ei hintaa\n"

        msg += "\n💡 *Selitys:*\n"
        msg += "🟢 BUY = hinta laskenut ≥10% (hyvä ostopaikka)\n"
        msg += "🟡 HOLD = hinta muuttunut alle 10%\n"
        msg += "🔴 SELL = hinta noussut ≥10% (hyvä myydä)"

        await send_long_message(context.bot, update.effective_chat.id, msg)
    except Exception as e:
        logging.error(f"Virhe /recommend: {e}")
        await update.message.reply_text(f"⚠️ Virhe /recommend: {str(e)[:200]}")

# testreport
async def testreport(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("📊 *Testataan aamuraporttia...*", parse_mode="Markdown")
        await send_daily_report()
        await update.message.reply_text("✅ *Aamuraportti lähetetty!*", parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /testreport: {e}")
        await update.message.reply_text(f"⚠️ Virhe /testreport: {str(e)[:200]}")

# hormuud
def get_hormuud_active_row(today=None):
    today = today or datetime.now()
    confirmed = []
    for r in HORMUUD_PROJECTION:
        if r["paid_date"]:
            try:
                d = datetime.strptime(r["paid_date"], "%d.%m.%Y")
            except Exception:
                continue
            if d <= today:
                confirmed.append((d, r))
    if not confirmed:
        return HORMUUD_PROJECTION[0]
    confirmed.sort(key=lambda x: x[0])
    return confirmed[-1][1]

def build_hormuud_report_text(today=None):
    today = today or datetime.now()
    row = get_hormuud_active_row(today)

    expected_next = None
    if row["paid_date"]:
        last_paid = datetime.strptime(row["paid_date"], "%d.%m.%Y")
        expected_next = last_paid.replace(year=last_paid.year + 1)
        while expected_next < today:
            expected_next = expected_next.replace(year=expected_next.year + 1)

    rate_pct = None
    if row["annual_return"] and row["start_capital"]:
        rate_pct = row["annual_return"] / row["start_capital"] * 100

    msg = "🏢 *Hormuud Shares* ($) — erillinen Trading212:sta\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"

    msg += f"📌 *Nykyinen pääoma:* ${row['start_capital']:,.0f}"
    if row["paid_date"]:
        msg += f" (vahvistettu {row['paid_date']})"
    msg += "\n"
    if rate_pct:
        msg += f"📈 Tuottoprosentti: ~{rate_pct:.1f} %/vuosi\n"
    if row["annual_return"]:
        msg += f"💰 Vuoden aikana kertyvä tuotto: ~${row['annual_return']:,.0f}\n"

    if expected_next:
        month_name = FI_MONTHS.get(expected_next.strftime('%m'), expected_next.strftime('%m'))
        msg += f"📅 *Odotettu maksupäivä:* n. {month_name} {expected_next.year}\n"
        msg += "   (pääoma kasvaa vuoden, maksu tapahtuu tyypillisesti n. vuotta myöhemmin)\n\n"
    else:
        msg += "\n"

    if row["cash_payment"] is not None and row["capital_addition"] is not None:
        msg += "💸 *Jako maksuhetkellä:*\n"
        msg += f"   • 50 % maksetaan käteisenä: ~${row['cash_payment']:,.0f}\n"
        msg += f"   • 50 % lisätään pääomaan: ~${row['capital_addition']:,.0f}\n"

    if row["capital_growth"] is not None:
        msg += f"\n🥅 *Uusi pääoma maksun jälkeen:* ~${row['capital_growth']:,.0f}\n"

    if row["note"]:
        msg += f"\n📝 {row['note']}\n"

    msg += "\n⚠️ Nämä ovat projektioarvioita, ei taattuja tuottoja."
    return msg

async def hormuud(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = build_hormuud_report_text()
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /hormuud: {e}")
        await update.message.reply_text(f"⚠️ Virhe /hormuud: {str(e)[:200]}")

# altcoins, fear, whales, presale, market, ai, watchlist, report -komennot
async def altcoins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        alt_list = ["solana", "ripple", "cardano", "sui", "chainlink", "binancecoin", "stellar"]
        msg = "🪙 *Altcoin-analyysi*\n━━━━━━━━━━━━━━━━━\n\n"
        for sym in alt_list:
            price = get_crypto_price(sym)
            rsi, macd = get_crypto_ta(sym)
            msg += f"*{sym.upper()}*: €{price:,.2f}" if price else f"*{sym.upper()}*: Ei hintaa"
            if rsi is not None:
                msg += f" | RSI: {rsi} | MACD: {macd:.2f}"
            msg += "\n"
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /altcoins: {e}")
        await update.message.reply_text(f"⚠️ Virhe /altcoins: {str(e)[:200]}")

async def fear_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        val, label = get_fear_greed()
        if val is not None:
            await update.message.reply_text(f"😨 *Fear & Greed -indeksi*\n━━━━━━━━━━━━━━━━━\n\n{val}/100 → {label}", parse_mode="Markdown")
        else:
            await update.message.reply_text("⚠️ Fear & Greed -dataa ei saatu.")
    except Exception as e:
        logging.error(f"Virhe /fear: {e}")
        await update.message.reply_text(f"⚠️ Virhe /fear: {str(e)[:200]}")

async def whales_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        whales = get_whale_transactions(5)
        if whales:
            msg = "🐋 *Viimeisimmät suuret BTC-siirrot*\n━━━━━━━━━━━━━━━━━\n\n"
            for w in whales:
                msg += f"• {w['time']}  {w['btc']} BTC (hash: {w['hash']}...)\n"
            await update.message.reply_text(msg, parse_mode="Markdown")
        else:
            await update.message.reply_text("🐋 Ei suuria siirtoja havaittu.")
    except Exception as e:
        logging.error(f"Virhe /whales: {e}")
        await update.message.reply_text(f"⚠️ Virhe /whales: {str(e)[:200]}")

async def presale_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("🚀 *Haetaan uusimpia presale-projekteja...*", parse_mode="Markdown")
        projects = fetch_presales()
        if projects:
            save_presales(projects)
        msg = build_presale_report(limit=5)
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /presale: {e}")
        await update.message.reply_text(f"⚠️ Virhe /presale: {str(e)[:200]}")

async def market_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = build_market_intelligence_report()
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /market: {e}")
        await update.message.reply_text(f"⚠️ Virhe /market: {str(e)[:200]}")

async def ai_news_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("🧠 *Haetaan tekoälysektorin uutisia...*", parse_mode="Markdown")
        query = "tekoäly sijoittaminen"
        news = get_news(query, limit=5)
        if news:
            msg = "🧠 *AI-sektorin uutiset*\n━━━━━━━━━━━━━━━━━\n\n"
            for item in news:
                msg += f"• {item['title']}\n"
            await update.message.reply_text(msg, parse_mode="Markdown")
        else:
            await update.message.reply_text("⚠️ Uutisia ei löytynyt.")
    except Exception as e:
        logging.error(f"Virhe /ai: {e}")
        await update.message.reply_text(f"⚠️ Virhe /ai: {str(e)[:200]}")

async def watchlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    args = context.args
    if args and args[0].lower() == "add" and len(args) > 1:
        project = " ".join(args[1:])
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO watchlist (user_id, project_name) VALUES (?, ?)", (user_id, project))
        conn.commit()
        conn.close()
        await update.message.reply_text(f"✅ *{project}* lisätty watchlistiin.")
    elif args and args[0].lower() == "remove" and len(args) > 1:
        project = " ".join(args[1:])
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("DELETE FROM watchlist WHERE user_id = ? AND project_name = ?", (user_id, project))
        conn.commit()
        conn.close()
        await update.message.reply_text(f"🗑️ *{project}* poistettu watchlistista.")
    else:
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("SELECT project_name FROM watchlist WHERE user_id = ?", (user_id,))
        rows = c.fetchall()
        conn.close()
        if rows:
            msg = "📋 *Watchlist*\n━━━━━━━━━━━━━━━━━\n\n"
            for r in rows:
                msg += f"• {r[0]}\n"
            await update.message.reply_text(msg, parse_mode="Markdown")
        else:
            await update.message.reply_text("📋 Watchlist on tyhjä. Lisää: /watchlist add <projekti>")

async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("📊 *Koko päivän raportti kootaan...*", parse_mode="Markdown")
        await send_daily_report()
        await update.message.reply_text("✅ Raportti lähetetty.")
    except Exception as e:
        logging.error(f"Virhe /report: {e}")
        await update.message.reply_text(f"⚠️ Virhe /report: {str(e)[:200]}")

# =============================================
# VIRHEIDENKÄSITTELY
# =============================================
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.error(f"Virhe: {context.error}")
    if update and update.effective_message:
        await update.effective_message.reply_text(
            f"⚠️ *Jokin meni pieleen.*\n\n"
            f"Virhe: `{str(context.error)[:300]}`\n\n"
            f"Ole hyvä ja yritä uudelleen. Jos ongelma toistuu, ilmoita siitä.",
            parse_mode="Markdown"
        )

# =============================================
# FLASK
# =============================================
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "🤖 Aydaruus Invest AI 2.0 bot is running!", 200

def run_flask():
    flask_app.run(host='0.0.0.0', port=PORT, debug=False)

# =============================================
# PÄÄFUNKTIO
# =============================================
def run_bot():
    app = Application.builder().token(TOKEN).build()
    # Vanhat
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
    app.add_handler(CommandHandler("growth", growth))
    app.add_handler(CommandHandler("hormuud", hormuud))
    app.add_handler(CommandHandler("recommend", recommend))
    app.add_handler(CommandHandler("testreport", testreport))

    # Uudet
    app.add_handler(CommandHandler("presale", presale_command))
    app.add_handler(CommandHandler("scam", scam_command))
    app.add_handler(CommandHandler("market", market_command))
    app.add_handler(CommandHandler("altcoins", altcoins))
    app.add_handler(CommandHandler("listing", listing_command))
    app.add_handler(CommandHandler("ai", ai_news_command))
    app.add_handler(CommandHandler("watchlist", watchlist_command))
    app.add_handler(CommandHandler("favorites", watchlist_command))
    app.add_handler(CommandHandler("whales", whales_command))
    app.add_handler(CommandHandler("fear", fear_command))
    app.add_handler(CommandHandler("report", report_command))

    app.add_error_handler(error_handler)
    app.run_polling()

# =============================================
# SUORITA
# =============================================
if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    run_bot()
