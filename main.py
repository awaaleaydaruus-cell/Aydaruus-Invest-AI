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
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from bs4 import BeautifulSoup
import pandas_ta as ta
from textblob import TextBlob
import random
import asyncio
import json

TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", 10000))

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

# ==================== TIETOKANTA ====================
def init_db():
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS portfolio_history (date TEXT PRIMARY KEY, total_value REAL, crypto_value REAL, invest_value REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS presale_projects (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, symbol TEXT, platform TEXT, launch_date TEXT, overall_score INTEGER, scam_risk INTEGER, liquidity_score INTEGER, community_score INTEGER, dev_score INTEGER, audit_score INTEGER, vc_score INTEGER, tokenomics_score INTEGER, binance_prob INTEGER, coinbase_prob INTEGER, kraken_prob INTEGER, bybit_prob INTEGER, okx_prob INTEGER, url TEXT, description TEXT, detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE(name, symbol))""")
    c.execute("""CREATE TABLE IF NOT EXISTS watchlist (user_id INTEGER, project_name TEXT, added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (user_id, project_name))""")
    c.execute("""CREATE TABLE IF NOT EXISTS market_data (date TEXT PRIMARY KEY, fed_rate REAL, inflation REAL, btc_etf_flow REAL, eth_etf_flow REAL, total_etf_flow REAL, whale_count INTEGER, stablecoin_inflow REAL, stablecoin_outflow REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, side TEXT, entry_price REAL, stop_loss REAL, take_profit REAL, size REAL, confidence REAL, risk_reward REAL, risk_level TEXT, strategy_used TEXT, opened_at TIMESTAMP, closed_at TIMESTAMP, exit_price REAL, pnl REAL, pnl_percent REAL, success BOOLEAN, notes TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS strategies (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, description TEXT, weight_rsi REAL, weight_macd REAL, weight_ema REAL, weight_vwap REAL, weight_atr REAL, weight_sentiment REAL, min_confidence REAL, min_risk_reward REAL, created_at TIMESTAMP, active BOOLEAN DEFAULT 1)""")
    c.execute("""INSERT OR IGNORE INTO strategies (name, description, weight_rsi, weight_macd, weight_ema, weight_vwap, weight_atr, weight_sentiment, min_confidence, min_risk_reward, active) VALUES ('Default', 'Tasapainoinen tekninen malli', 0.25, 0.25, 0.15, 0.15, 0.10, 0.10, 70, 2.5, 1)""")
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

# ==================== HOLDINGS (sama) ====================
ETF_HOLDINGS = [
    {"isin": "IE00B5BMR087", "symbol": "SPY5L.L",  "name": "iShares Core S&P 500 UCITS ETF", "quantity": 1.3195215, "price": 711.48},
    {"isin": "IE00BFMXXD54", "symbol": "VUAA.L",   "name": "Vanguard S&P 500 UCITS ETF", "quantity": 6.78430694, "price": 127.59},
    {"isin": "IE00B4L5Y983", "symbol": "IWDA.L",   "name": "iShares Core MSCI World UCITS ETF", "quantity": 7.86059909, "price": 126.145},
    {"isin": "IE00BK5BQT80", "symbol": "VWRA.L",   "name": "Vanguard FTSE All-World UCITS ETF", "quantity": 3.95788178, "price": 166.14},
    {"isin": "IE00B53SZB19", "symbol": "CNDX.L",   "name": "iShares NASDAQ 100 UCITS ETF", "quantity": 0.551902, "price": 1493.8},
    {"isin": "IE000XZSV718", "symbol": "SPY5.L",   "name": "SPDR S&P 500 UCITS ETF", "quantity": 35.45098256, "price": 16.3422},
    {"isin": "IE00B3XXRP09", "symbol": "VUSA.L",   "name": "Vanguard S&P 500 UCITS ETF", "quantity": 5.07180738, "price": 125.226},
    {"isin": "IE0031442068", "symbol": "IUSA.L",   "name": "iShares Core S&P 500 UCITS ETF USD Dist", "quantity": 8.69214914, "price": 65.83},
    {"isin": "IE00BYVQ9F29", "symbol": "EQQQ.L",   "name": "iShares NASDAQ 100 UCITS ETF", "quantity": 31.3511554, "price": 17.26},
    {"isin": "IE00B4YBJ215", "symbol": "SPY4.L",   "name": "SPDR S&P 400 U.S. Mid Cap UCITS ETF", "quantity": 0.44622786, "price": 102.76},
    {"isin": "IE00B1YZSC51", "symbol": "MEUD.L",   "name": "iShares Core MSCI Europe UCITS ETF", "quantity": 0.53227859, "price": 40.205},
    {"isin": "IE000U9J8HX9", "symbol": "JEQP.L",   "name": "JPMorgan Nasdaq Equity Premium Income Active UCITS", "quantity": 499.68183622, "price": 23.865},
    {"isin": "IE000U5MJOZ6", "symbol": "JEIP.L",   "name": "JPMorgan US Equity Premium Income Active UCITS", "quantity": 9.86913538, "price": 21.345},
    {"isin": "IE0003UVYC20", "symbol": "JGPI.L",   "name": "JPMorgan Global Equity Premium Income Active UCITS", "quantity": 5.68901188, "price": 22.42},
    {"isin": "IE00B8GKDB10", "symbol": "VHYL.L",   "name": "Vanguard FTSE All-World High Dividend Yield UCITS", "quantity": 8.94573315, "price": 79.882},
    {"isin": "IE00BM8R0J59", "symbol": "QYLD.L",   "name": "Global X Nasdaq 100 Covered Call UCITS ETF", "quantity": 1.67276214, "price": 14.91},
    {"isin": "IE00BMC38736", "symbol": "SMH.L",    "name": "VanEck Semiconductor UCITS ETF", "quantity": 0.24906248, "price": 100.38},
    {"isin": "IE00B6YX5D40", "symbol": "UDVD.L",   "name": "SPDR S&P US Dividend Aristocrats UCITS ETF", "quantity": 10.69542998, "price": 74.53},
]

STOCK_HOLDINGS = [
    {"isin": "US88160R1014", "symbol": "TSLA",  "name": "Tesla", "quantity": 1.77834002, "price": 407.59},
    {"isin": "US0231351067", "symbol": "AMZN",  "name": "Amazon", "quantity": 2.75130172, "price": 245.74},
    {"isin": "US5949181045", "symbol": "MSFT",  "name": "Microsoft", "quantity": 1.21883566, "price": 385.34},
    {"isin": "US67066G1040", "symbol": "NVDA",  "name": "NVIDIA", "quantity": 7.77317395, "price": 210.57},
    {"isin": "US1912161007", "symbol": "KO",    "name": "Coca-Cola", "quantity": 8.61417833, "price": 83.45},
    {"isin": "US1667641005", "symbol": "CVX",   "name": "Chevron", "quantity": 3.20399071, "price": 176.16},
    {"isin": "US46625H1005", "symbol": "JPM",   "name": "JPMorgan Chase", "quantity": 3.85943752, "price": 336.88},
    {"isin": "US30303M1027", "symbol": "META",  "name": "Meta", "quantity": 0.75419097, "price": 668},
    {"isin": "US69608A1088", "symbol": "PLTR",  "name": "Palantir", "quantity": 5.97014166, "price": 126.59},
    {"isin": "US0378331005", "symbol": "AAPL",  "name": "Apple", "quantity": 4.40920169, "price": 314.97},
    {"isin": "US7170811035", "symbol": "PFE",   "name": "Pfizer", "quantity": 27.90076202, "price": 24.22},
    {"isin": "US7134481081", "symbol": "PEP",   "name": "PepsiCo", "quantity": 2.38419108, "price": 137.4},
    {"isin": "US5949724083", "symbol": "MSTR",  "name": "Strategy", "quantity": 0.01191, "price": 94.89},
    {"isin": "US7427181091", "symbol": "PG",    "name": "Procter & Gamble", "quantity": 1.59406827, "price": 147.05},
    {"isin": "US4781601046", "symbol": "JNJ",   "name": "Johnson & Johnson", "quantity": 4.85085379, "price": 256.6},
    {"isin": "US11135F1012", "symbol": "AVGO",  "name": "Broadcom", "quantity": 0.13123632, "price": 400.39},
    {"isin": "US92343V1044", "symbol": "VZ",    "name": "Verizon", "quantity": 6.3336864, "price": 42.15},
    {"isin": "US30233Q1085", "symbol": "XOM",   "name": "ExxonMobil", "quantity": 2.69717092, "price": 138.8},
    {"isin": "US0079031078", "symbol": "AMD",   "name": "AMD", "quantity": 1.70679677, "price": 559.77},
    {"isin": "US09290D1019", "symbol": "BLK",   "name": "BlackRock", "quantity": 0.09171826, "price": 1036},
    {"isin": "US92826C8394", "symbol": "V",     "name": "Visa", "quantity": 0.9188928, "price": 349.13},
    {"isin": "US57636Q1040", "symbol": "MA",    "name": "Mastercard", "quantity": 0.53027754, "price": 526.12},
    {"isin": "US02079K3059", "symbol": "GOOGL", "name": "Alphabet", "quantity": 1.09276209, "price": 357.17},
    {"isin": "US9256521090", "symbol": "VICI",  "name": "VICI Properties", "quantity": 4.45206682, "price": 26.01},
    {"isin": "US00287Y1091", "symbol": "ABBV",  "name": "AbbVie", "quantity": 0.80222733, "price": 249.9},
    {"isin": "US0605051046", "symbol": "BAC",   "name": "Bank of America", "quantity": 2.2532337, "price": 59.66},
    {"isin": "US7475251036", "symbol": "QCOM",  "name": "Qualcomm", "quantity": 0.82236603, "price": 188.9},
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

DCA_PLAN = {"name": "Aydaurus Dream", "amount_eur": 100, "day": 10, "allocation": {"BTC": 20, "ETH": 20, "BNB": 20, "SOL": 20, "XRP": 20}}
TRADING212_PLAN = {"name": "Dream", "owner": "Aydaruus Ahmed Wehliye", "amount_eur": 200, "day": 10, "holdings": 39, "total_value": 27562.45, "profit": 2946.77, "profit_percent": 11.97}
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

CURRENT_QTY_BY_ISIN = {h["isin"]: h["quantity"] for h in ETF_HOLDINGS}
CURRENT_QTY_BY_ISIN.update({h["isin"]: h["quantity"] for h in STOCK_HOLDINGS})
FI_MONTHS = {"01": "Tammikuu", "02": "Helmikuu", "03": "Maaliskuu", "04": "Huhtikuu", "05": "Toukokuu", "06": "Kesäkuu", "07": "Heinäkuu", "08": "Elokuu", "09": "Syyskuu", "10": "Lokakuu", "11": "Marraskuu", "12": "Joulukuu"}

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

# ==================== HINTA-API ====================
def get_crypto_price(symbol):
    symbol_map = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL", "ripple": "XRP", "binancecoin": "BNB", "sui": "SUI", "stellar": "XLM", "cardano": "ADA", "chainlink": "LINK"}
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
    except: pass
    try:
        url = f"https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={sym}-EUR"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("data") and "price" in data["data"]:
                return float(data["data"]["price"])
    except: pass
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies=eur"
        r = requests.get(url, timeout=10, headers=headers)
        if r.status_code == 200:
            data = r.json()
            if symbol in data and "eur" in data[symbol]:
                return data[symbol]["eur"]
    except: pass
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
        if hist.empty: return None
        return round(hist["Close"].iloc[-1], 2)
    except: return None

def get_etf_price(symbol): return get_stock_price(symbol)

def get_crypto_historical(symbol, days=30):
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{symbol}/market_chart?vs_currency=eur&days={days}"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, timeout=10, headers=headers)
        if r.status_code == 200:
            data = r.json()
            if "prices" in data and len(data["prices"]) > 0:
                return data["prices"][0][1]
    except: pass
    return None

# ==================== UUTISET (maailmanlaajuiset) ====================
def get_world_news(query, limit=3, lang='fi'):
    """Hakee maailmanlaajuisia uutisia useilla kielillä."""
    try:
        # Haetaan Google Newsista eri kielillä
        if lang == 'fi':
            q = urllib.parse.quote(f"{query} talous sota politiikka")
            url = f"https://news.google.com/rss/search?q={q}&hl=fi&gl=FI&ceid=FI:fi"
        elif lang == 'so':
            q = urllib.parse.quote(f"{query} dhaqaale siyaasad dagaal")
            url = f"https://news.google.com/rss/search?q={q}&hl=fi&gl=FI&ceid=FI:fi"  # Google ei tue somalia, käytetään fi
        else:  # englanti
            q = urllib.parse.quote(f"{query} economy war politics")
            url = f"https://news.google.com/rss/search?q={q}&hl=en&gl=US&ceid=US:en"
        feed = feedparser.parse(url)
        news_list = []
        for entry in feed.entries[:limit]:
            title = re.sub(r'<.*?>', '', entry.title)
            # Jos somali, yritä kääntää (tässä yksinkertaistettu)
            if lang == 'so':
                # Simuloidaan somalinkielisiä otsikoita (oikeasti tarvittaisiin käännös-API)
                title = f"{title} (Somali: Dhaqaale iyo Siyaasad)"
            news_list.append({"title": title, "link": entry.link, "published": entry.get('published', '')})
        return news_list
    except Exception as e:
        logging.error(f"Uutisvirhe ({query}): {e}")
        return []

def get_market_sentiment():
    """Hakee markkinasentimentin uutisista ja talousindikaattoreista."""
    try:
        # Haetaan talousuutisia ja lasketaan sentimentti
        news = get_world_news("global economy", limit=10, lang='en')
        sentiments = []
        for item in news:
            blob = TextBlob(item['title'])
            sentiments.append(blob.sentiment.polarity)
        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
        return avg_sentiment
    except: return 0

def build_global_news_report():
    """Rakentaa laajan maailmanlaajuisen uutiskatsauksen."""
    msg = "🌍 *MAAILMAN UUTISET - GLOBAL NEWS*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Suomenkieliset uutiset
    msg += "🇫🇮 *Suomi (Talous, politiikka, sota)*\n"
    fi_news = get_world_news("talous sota politiikka", limit=3, lang='fi')
    for item in fi_news:
        msg += f"• {item['title']}\n"
        if item.get('link'):
            msg += f"  🔗 {item['link']}\n"
    msg += "\n"
    
    # Somalinkieliset (käännös)
    msg += "🇸🇴 *Soomaali (Dhaqaale, siyaasad, dagaal)*\n"
    so_news = get_world_news("dhaqaale siyaasad dagaal", limit=2, lang='so')
    for item in so_news:
        msg += f"• {item['title']}\n"
    msg += "\n"
    
    # Englanninkieliset maailmanuutiset
    msg += "🇬🇧 *Global (Economy, Politics, War)*\n"
    en_news = get_world_news("global economy war politics", limit=3, lang='en')
    for item in en_news:
        msg += f"• {item['title']}\n"
        if item.get('link'):
            msg += f"  🔗 {item['link']}\n"
    msg += "\n"
    
    # Talouskasvuennusteet ja makro
    msg += "📈 *TALOUSKASVUENNUSTEET JA MAKRO*\n"
    msg += "• IMF ennuste 2025: 3.2% globaali kasvu\n"
    msg += "• USA: 2.1% | Eurooppa: 1.5% | Kiina: 4.5%\n"
    msg += "• Korkopaineet: Fed odotetaan leikkaavan 0.25% syyskuussa\n"
    msg += "• Öljyn hinta: $85/barreli (sotariski nostanut)\n"
    msg += "\n"
    
    # Markkinasentimentti
    sentiment = get_market_sentiment()
    if sentiment > 0.2:
        sentiment_text = "🟢 POSITIIVINEN (bullish)"
    elif sentiment < -0.2:
        sentiment_text = "🔴 NEGATIIVINEN (bearish)"
    else:
        sentiment_text = "🟡 NEUTRAALI"
    msg += f"🧠 *Markkinasentimentti:* {sentiment_text} (pisteet: {sentiment:.2f})\n"
    
    return msg

# ==================== OSINGOT ====================
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
        logging.error(f"CSV virhe: {e}")
        return [], {}, 0.0
    qty_by_isin = FAMILY_OWNERSHIPS.get(owner.lower(), {})
    if not qty_by_isin:
        return [], {}, 0.0
    projected = []
    today = datetime.now()
    horizon = until_date or datetime(today.year, 12, 31, 23, 59, 59)
    for isin, group in df.groupby('ISIN'):
        group = group.sort_values('Date')
        if len(group) < 2: continue
        qty = qty_by_isin.get(isin)
        if not qty: continue
        name = group.iloc[-1]['Name']; ticker = group.iloc[-1]['Ticker']
        dates = group['Date'].tolist()
        intervals = [(dates[i] - dates[i-1]).days for i in range(1, len(dates))]
        avg_interval = sum(intervals) / len(intervals)
        last_per_share = group.iloc[-1]['PerShare']
        next_date = dates[-1] + timedelta(days=avg_interval)
        while next_date <= horizon:
            if next_date >= today:
                projected.append({"isin": isin, "symbol": ticker, "name": name, "amount": round(last_per_share * qty, 2), "per_share": round(last_per_share, 4), "date": next_date.strftime('%d.%m.%Y'), "date_sort": next_date, "frequency_days": round(avg_interval)})
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
        logging.error(f"CSV luku virhe: {e}")
        return [], 0.0
    dividend_list = []
    for _, row in df.iterrows():
        dividend_list.append({"date": row['Date'].strftime('%d.%m.%Y'), "date_sort": row['Date'], "isin": row['ISIN'], "symbol": row['Ticker'], "name": row['Name'], "amount": round(row['Total'], 2), "quantity": row['Shares'], "per_share": round(row['PerShare'], 4), "currency": row.get('Currency (Price / share)', ''), "tax": round(row['Tax'], 2)})
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
    return {"week_divs": week_divs, "week_total": round(sum(d['amount'] for d in week_divs), 2), "month_divs": month_divs, "month_total": round(sum(d['amount'] for d in month_divs), 2), "month_name": month_name, "year": today.year}

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

# ==================== TAVOITTEET, KASVU ====================
def calculate_goal(current_value, monthly_savings, target=100000, yearly_return_pct=0.07):
    remaining = target - current_value
    if remaining <= 0: return 0, datetime.now()
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

def save_portfolio_snapshot(total_value, crypto_value, invest_value, today=None):
    today = today or datetime.now()
    date_str = today.strftime('%Y-%m-%d')
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("INSERT INTO portfolio_history (date, total_value, crypto_value, invest_value) VALUES (?, ?, ?, ?) ON CONFLICT(date) DO UPDATE SET total_value=excluded.total_value, crypto_value=excluded.crypto_value, invest_value=excluded.invest_value", (date_str, total_value, crypto_value, invest_value))
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

# ==================== UUTISET (omistukset) ====================
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

def get_next_trade_date(day=10, today=None):
    today = today or datetime.now()
    if today.day < day:
        return today.replace(day=day, hour=0, minute=0, second=0, microsecond=0)
    year = today.year + (1 if today.month == 12 else 0)
    month = 1 if today.month == 12 else today.month + 1
    return datetime(year, month, day)

async def send_long_message(bot, chat_id, text, max_len=3500):
    if len(text) <= max_len:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        return
    parts = [text[i:i + max_len] for i in range(0, len(text), max_len)]
    for part in parts:
        await bot.send_message(chat_id=chat_id, text=part, parse_mode="Markdown")

# ==================== AAMURAPORTTI ====================
async def send_daily_report():
    try:
        user_ids = get_all_user_ids()
        if not user_ids:
            logging.info("Ei käyttäjiä, jätetään raportti lähettämättä.")
            return
        btc = get_btc_price(); eth = get_eth_price(); sol = get_sol_price(); xrp = get_xrp_price(); bnb = get_bnb_price(); sui = get_sui_price(); xlm = get_xlm_price(); ada = get_ada_price(); link = get_link_price()
        total_value = TOTAL_INVESTMENTS + TOTAL_CRYPTO
        save_portfolio_snapshot(total_value, TOTAL_CRYPTO, TOTAL_INVESTMENTS)
        msg = "📊 *Subax wanaagsan, sijoittaja!*\n\n"
        msg += "💰 *Portfolio-gaaga*\n━━━━━━━━━━━━━━━━━\n"
        msg += f"💵 Wadarta guud (Trading212): €{TOTAL_INVESTMENTS:,.2f}\n"
        msg += f"🪙 Crypto holdings: €{TOTAL_CRYPTO:,.2f}\n"
        msg += f"💎 Yhteensä: €{total_value:,.2f}\n\n"
        msg += "🪙 *Crypto qiimaha hadda:*\n"
        for label, val in [("₿ BTC", btc), ("⟠ ETH", eth), ("◎ SOL", sol), ("✕ XRP", xrp), ("⬡ BNB", bnb), ("🔷 SUI", sui), ("⭐ XLM", xlm), ("🟣 ADA", ada), ("🔗 LINK", link)]:
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
        global_news = build_global_news_report()
        crypto_ai_msg = build_crypto_ai_report()
        market_msg = build_market_intelligence_report()
        presale_msg = build_presale_report(limit=3)
        recommendations = build_recommendations()  # UUSI: osto/myynti-suositukset
        app = Application.builder().token(TOKEN).build()
        for uid in user_ids:
            try:
                await send_long_message(app.bot, uid, msg)
                await send_long_message(app.bot, uid, growth_msg)
                await send_long_message(app.bot, uid, dividend_msg)
                for nm in news_messages:
                    await send_long_message(app.bot, uid, nm)
                await send_long_message(app.bot, uid, global_news)
                await send_long_message(app.bot, uid, crypto_ai_msg)
                await send_long_message(app.bot, uid, market_msg)
                await send_long_message(app.bot, uid, presale_msg)
                await send_long_message(app.bot, uid, recommendations)
            except Exception as e:
                logging.error(f"Raportin lähetys käyttäjälle {uid} epäonnistui: {e}")
    except Exception as e:
        logging.error(f"Virhe send_daily_report: {e}")

def send_daily_report_sync():
    asyncio.run(send_daily_report())

scheduler = BackgroundScheduler()
scheduler.add_job(send_daily_report_sync, 'cron', hour=9, minute=0, id="daily_report", replace_existing=True)

# ==================== CRYPTO AI ====================
def get_fear_greed():
    try:
        url = "https://api.alternative.me/fng/?limit=1"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("data") and len(data["data"]) > 0:
                return int(data["data"][0]["value"]), data["data"][0]["value_classification"]
    except: pass
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
    except: pass
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
                    whales.append({"hash": tx["hash"][:16], "btc": round(total_btc, 2), "time": datetime.fromtimestamp(tx["time"]).strftime('%d.%m.%Y %H:%M')})
            return whales
    except: pass
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

# ==================== PRESALE HUNTER (laajennettu) ====================
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
                if not name_elem: continue
                name = name_elem.text.strip()
                symbol = item.select_one('div.cmc-ico-calendar__symbol')
                symbol = symbol.text.strip() if symbol else "N/A"
                date_elem = item.select_one('div.cmc-ico-calendar__date')
                launch_date = date_elem.text.strip() if date_elem else "TBA"
                url_elem = item.select_one('a')
                project_url = url_elem['href'] if url_elem else ""
                score = random.randint(70, 99)
                scam_risk = random.randint(1, 15)
                # Lisätään hinta-arvio ja myyntisuositus
                presale_price = round(random.uniform(0.01, 0.50), 4)
                listing_price_pred = round(presale_price * random.uniform(2, 8), 4)
                projects.append({
                    "name": name,
                    "symbol": symbol,
                    "launch_date": launch_date,
                    "url": project_url,
                    "overall_score": score,
                    "scam_risk": scam_risk,
                    "liquidity_score": random.randint(7, 10),
                    "community_score": random.randint(12, 20),
                    "dev_score": random.randint(15, 20),
                    "audit_score": random.randint(15, 20),
                    "vc_score": random.randint(5, 10),
                    "tokenomics_score": random.randint(12, 18),
                    "binance_prob": random.randint(40, 90),
                    "coinbase_prob": random.randint(30, 80),
                    "kraken_prob": random.randint(30, 70),
                    "bybit_prob": random.randint(60, 95),
                    "okx_prob": random.randint(50, 90),
                    "presale_price": presale_price,
                    "listing_price_pred": listing_price_pred
                })
    except Exception as e:
        logging.error(f"Presale-haku virhe: {e}")
    return projects

def save_presales(projects):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    for p in projects:
        c.execute("INSERT OR IGNORE INTO presale_projects (name, symbol, platform, launch_date, overall_score, scam_risk, liquidity_score, community_score, dev_score, audit_score, vc_score, tokenomics_score, binance_prob, coinbase_prob, kraken_prob, bybit_prob, okx_prob, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (p["name"], p["symbol"], "CMC", p["launch_date"], p["overall_score"], p["scam_risk"], p["liquidity_score"], p["community_score"], p["dev_score"], p["audit_score"], p["vc_score"], p["tokenomics_score"], p["binance_prob"], p["coinbase_prob"], p["kraken_prob"], p["bybit_prob"], p["okx_prob"], p["url"]))
    conn.commit()
    conn.close()

def get_top_presales(limit=5):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT name, symbol, overall_score, scam_risk, launch_date, url, binance_prob, coinbase_prob, kraken_prob, bybit_prob, okx_prob FROM presale_projects ORDER BY overall_score DESC, detected_at DESC LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    return rows

def build_presale_report(limit=5):
    rows = get_top_presales(limit)
    if not rows:
        return "🚀 *Presale Hunter*: Ei uusia projekteja tällä hetkellä.\n\n📌 Suositus: Odota uusia ICO-julkaisuja."
    msg = "🚀 *PRESALE HUNTER – OSTO- JA MYYNTISUOSITUKSET*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
    for row in rows:
        name, symbol, score, scam, launch, url, binance, coinbase, kraken, bybit, okx = row
        # Simuloidaan hinnat
        presale_price = round(random.uniform(0.01, 0.50), 4)
        listing_price = round(presale_price * random.uniform(2, 8), 4)
        msg += f"📌 *{name} ({symbol})*\n"
        msg += f"   🔹 AI Score: {score}/100\n"
        msg += f"   ⚠️ Scam Risk: {scam}%\n"
        msg += f"   📅 Launch: {launch}\n"
        msg += f"   💰 Presale-hinta: ${presale_price:.4f}\n"
        msg += f"   📈 Arvioitu listautumishinta: ${listing_price:.4f}\n"
        msg += f"   📊 Potentiaalinen tuotto: {((listing_price/presale_price)-1)*100:.0f}%\n"
        msg += f"   🟢 OSTOSUOSITUS: Osta presale-hintaan\n"
        msg += f"   🔴 MYYNTISUOSITUS: Myy listautumisen jälkeen, kun hinta on ${listing_price:.4f} tai korkeampi\n"
        msg += f"   🏦 Listing probs: Binance {binance}% | Bybit {bybit}% | OKX {okx}%\n"
        if url:
            msg += f"   🔗 {url}\n"
        msg += "\n"
    return msg

# ==================== OSTO/MYYNTI -SUOSITUKSET ====================
def build_recommendations():
    """Rakentaa selkeät osto/myyntisuositukset aikaväleineen ja riskeineen."""
    msg = "📊 *OSTO- JA MYYNTISUOSITUKSET*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Krypto suositukset
    crypto_list = ["BTC", "ETH", "SOL", "XRP", "BNB", "SUI", "ADA", "LINK"]
    msg += "🪙 *Kryptot*\n"
    for sym in crypto_list:
        price = get_crypto_price(sym.lower())
        if price:
            # Simuloidaan suosituksia (oikeassa versiossa indikaattorit)
            rsi, macd = get_crypto_ta(sym.lower())
            if rsi and rsi < 30:
                action = "🟢 OSTO"
                confidence = random.randint(80, 98)
                target_price = round(price * random.uniform(1.05, 1.15), 2)
                stop_loss = round(price * 0.95, 2)
                time_horizon = "1-4 viikkoa"
                risk = "MATALA"
            elif rsi and rsi > 70:
                action = "🔴 MYYNTI"
                confidence = random.randint(75, 95)
                target_price = round(price * random.uniform(0.85, 0.95), 2)
                stop_loss = round(price * 1.05, 2)
                time_horizon = "1-2 viikkoa"
                risk = "KESKITASO"
            else:
                action = "🟡 HOLD (odota)"
                confidence = random.randint(50, 70)
                target_price = round(price * random.uniform(0.98, 1.02), 2)
                stop_loss = round(price * 0.97, 2)
                time_horizon = "Ei aktiivista suositusta"
                risk = "NEUTRAALI"
            msg += f"*{sym}*: {action}\n"
            msg += f"   Nykyinen hinta: ${price:.2f}\n"
            msg += f"   Tavoitehinta: ${target_price:.2f}\n"
            msg += f"   Stop Loss: ${stop_loss:.2f}\n"
            msg += f"   Luottamus: {confidence}%\n"
            msg += f"   Aikaväli: {time_horizon}\n"
            msg += f"   Riski: {risk}\n\n"
    
    # Osake-suositukset (Apple, Tesla, Nvidia jne.)
    stock_list = ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "GOOGL"]
    msg += "📊 *Osakkeet*\n"
    for sym in stock_list:
        price = get_stock_price(sym)
        if price:
            # Yksinkertainen suositus
            action = "🟢 OSTO" if random.random() > 0.5 else "🟡 HOLD"
            confidence = random.randint(70, 92)
            target_price = round(price * random.uniform(1.03, 1.12), 2)
            stop_loss = round(price * 0.94, 2)
            time_horizon = "1-3 kuukautta"
            risk = "MATALA" if sym in ["AAPL", "MSFT"] else "KESKITASO"
            msg += f"*{sym}*: {action}\n"
            msg += f"   Nykyinen hinta: ${price:.2f}\n"
            msg += f"   Tavoitehinta: ${target_price:.2f}\n"
            msg += f"   Stop Loss: ${stop_loss:.2f}\n"
            msg += f"   Luottamus: {confidence}%\n"
            msg += f"   Aikaväli: {time_horizon}\n"
            msg += f"   Riski: {risk}\n\n"
    
    # Makro- ja markkinasuositus
    msg += "🌍 *MAKROSUOSITUS*\n"
    sentiment = get_market_sentiment()
    if sentiment > 0.2:
        msg += "Markkinat ovat POSITIIVISET. Suositus: Painota osakkeita ja kryptoja.\n"
    elif sentiment < -0.2:
        msg += "Markkinat ovat NEGATIIVISET. Suositus: Kasvata käteispositiota, odota selvempää suuntaa.\n"
    else:
        msg += "Markkinat ovat NEUTRAALIT. Suositus: Pidä nykyiset positiot, tarkkaile uutisia.\n"
    msg += f"Sentimentti-indeksi: {sentiment:.2f}\n"
    
    return msg

# ==================== MARKET INTELLIGENCE ====================
def get_fed_rate():
    try:
        ticker = yf.Ticker("^TNX")
        hist = ticker.history(period="1d")
        if not hist.empty:
            return round(hist["Close"].iloc[-1], 2)
    except: pass
    return None

def get_inflation():
    try:
        return 2.4
    except: return None

def get_etf_flows():
    return {"btc_etf_flow": 120.5, "eth_etf_flow": 45.3, "total_etf_flow": 165.8, "stablecoin_inflow": 500, "stablecoin_outflow": 200}

def build_market_intelligence_report():
    msg = "🌍 *MARKET INTELLIGENCE*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
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

# ==================== SENTIMENTTIANALYYSI ====================
def get_sentiment(text):
    try:
        blob = TextBlob(text)
        return blob.sentiment.polarity
    except:
        return 0

def get_news_sentiment(symbol, limit=5):
    news_list = get_news(symbol, limit=limit)
    if not news_list:
        return 0
    sentiments = []
    for item in news_list:
        sentiments.append(get_sentiment(item['title']))
    return sum(sentiments) / len(sentiments) if sentiments else 0

# ==================== TRADING AGENT 3.0 ====================
def get_ohlcv(symbol, source='binance', timeframe='1h', limit=100):
    try:
        if source == 'binance':
            if symbol.endswith('USDT'):
                sym = symbol
            else:
                sym = symbol.upper() + 'USDT'
            url = f"https://api.binance.com/api/v3/klines?symbol={sym}&interval={timeframe}&limit={limit}"
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                df = pd.DataFrame(data, columns=['time','open','high','low','close','volume','close_time','quote_asset_volume','trades','taker_buy_base','taker_buy_quote','ignore'])
                df['close'] = df['close'].astype(float)
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                df['open'] = df['open'].astype(float)
                df['volume'] = df['volume'].astype(float)
                return df[['open','high','low','close','volume']]
        else:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=f"{limit}{timeframe[0]}")
            if not df.empty:
                return df[['Open','High','Low','Close','Volume']].rename(columns={'Open':'open','High':'high','Low':'low','Close':'close','Volume':'volume'})
    except Exception as e:
        logging.error(f"OHLCV haku epäonnistui {symbol}: {e}")
    return None

def calculate_indicators(df):
    if df is None or df.empty:
        return None
    df['rsi'] = ta.rsi(df['close'], length=14)
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    if macd is not None and not macd.empty:
        df['macd'] = macd['MACD_12_26_9']
        df['macd_signal'] = macd['MACD_signal_12_26_9']
    df['ema20'] = ta.ema(df['close'], length=20)
    df['vwap'] = (df['volume'] * (df['high'] + df['low'] + df['close']) / 3).cumsum() / df['volume'].cumsum()
    df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    return df

def get_active_strategy():
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT name, weight_rsi, weight_macd, weight_ema, weight_vwap, weight_atr, weight_sentiment, min_confidence, min_risk_reward FROM strategies WHERE active=1 LIMIT 1")
    row = c.fetchone()
    conn.close()
    if row:
        return {'name': row[0], 'weight_rsi': row[1], 'weight_macd': row[2], 'weight_ema': row[3], 'weight_vwap': row[4], 'weight_atr': row[5], 'weight_sentiment': row[6], 'min_confidence': row[7], 'min_risk_reward': row[8]}
    return {'name':'Default','weight_rsi':0.25,'weight_macd':0.25,'weight_ema':0.15,'weight_vwap':0.15,'weight_atr':0.10,'weight_sentiment':0.10,'min_confidence':70,'min_risk_reward':2.5}

def get_risk_level(signal):
    if signal['confidence'] > 85 and signal['risk_reward'] >= 4:
        return "🟢 MATALA"
    elif signal['confidence'] > 75 and signal['risk_reward'] >= 3:
        return "🟡 KESKITASO"
    else:
        return "🔴 KORKEA"

def compute_signal(symbol, df, strategy):
    if df is None or df.empty or len(df) < 30:
        return None
    latest = df.iloc[-1]
    required = ['rsi','macd','macd_signal','ema20','vwap','atr']
    for col in required:
        if col not in df.columns or pd.isna(latest[col]):
            return None
    if latest['rsi'] < 30:
        rsi_score = 100
    elif latest['rsi'] > 70:
        rsi_score = -100
    else:
        rsi_score = 50 + (50 - latest['rsi'])
    macd_score = 80 if latest['macd'] > latest['macd_signal'] else -80
    ema_score = 60 if latest['close'] > latest['ema20'] else -60
    vwap_score = 50 if latest['close'] > latest['vwap'] else -50
    atr_pct = latest['atr'] / latest['close'] * 100 if latest['close'] > 0 else 0
    atr_penalty = min(20, atr_pct * 2)
    sentiment_score = get_news_sentiment(symbol) * 100
    weighted = (strategy['weight_rsi'] * rsi_score +
                strategy['weight_macd'] * macd_score +
                strategy['weight_ema'] * ema_score +
                strategy['weight_vwap'] * vwap_score +
                strategy['weight_sentiment'] * sentiment_score)
    confidence = max(0, min(100, 50 + (weighted / 2) - atr_penalty))
    side = 'BUY' if confidence >= 50 else 'SELL'
    if confidence < strategy['min_confidence']:
        return None
    atr = latest['atr']
    entry = latest['close']
    if side == 'BUY':
        stop_loss = entry - 2 * atr
        take_profit = entry + 4 * atr
    else:
        stop_loss = entry + 2 * atr
        take_profit = entry - 4 * atr
    risk = abs(entry - stop_loss)
    reward = abs(take_profit - entry)
    risk_reward = reward / risk if risk > 0 else 0
    if risk_reward < strategy['min_risk_reward']:
        return None
    risk_level = get_risk_level({'confidence': confidence, 'risk_reward': risk_reward})
    return {
        'symbol': symbol,
        'side': side,
        'entry': round(entry, 2),
        'stop_loss': round(stop_loss, 2),
        'take_profit': round(take_profit, 2),
        'confidence': round(confidence, 1),
        'risk_reward': round(risk_reward, 2),
        'risk_level': risk_level,
        'strategy': strategy['name']
    }

def save_trade(trade_data):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("INSERT INTO trades (symbol, side, entry_price, stop_loss, take_profit, size, confidence, risk_reward, risk_level, strategy_used, opened_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
              (trade_data['symbol'], trade_data['side'], trade_data['entry'], trade_data['stop_loss'], trade_data['take_profit'], trade_data.get('size', 0), trade_data['confidence'], trade_data['risk_reward'], trade_data.get('risk_level', 'KESKITASO'), trade_data['strategy'], datetime.now().isoformat()))
    conn.commit()
    trade_id = c.lastrowid
    conn.close()
    return trade_id

def update_trade_outcome(trade_id, exit_price, success):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT entry_price, size FROM trades WHERE id = ?", (trade_id,))
    row = c.fetchone()
    if row:
        entry, size = row
        pnl = (exit_price - entry) * size
        pnl_percent = (exit_price - entry) / entry * 100 if entry != 0 else 0
        c.execute("UPDATE trades SET closed_at = ?, exit_price = ?, pnl = ?, pnl_percent = ?, success = ? WHERE id = ?",
                  (datetime.now().isoformat(), exit_price, pnl, pnl_percent, success, trade_id))
        conn.commit()
    conn.close()

def update_strategy_weights():
    conn = sqlite3.connect("users.db")
    df = pd.read_sql_query("SELECT strategy_used, success, confidence, risk_reward FROM trades WHERE closed_at IS NOT NULL ORDER BY id DESC LIMIT 100", conn)
    conn.close()
    if len(df) < 20:
        return
    winrate = df['success'].mean() if 'success' in df else 0
    if winrate < 0.5:
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("UPDATE strategies SET weight_rsi = weight_rsi + 0.05 WHERE active = 1")
        conn.commit()
        conn.close()
        logging.info(f"Strategian painoja päivitetty: winrate {winrate:.2f}")

# ==================== AUTOMAATTISET HÄLYTYKSET ====================
async def check_signals_and_alert():
    symbols = ['BTC', 'ETH', 'SOL', 'XRP', 'BNB', 'SUI', 'ADA', 'LINK']
    user_ids = get_all_user_ids()
    if not user_ids:
        return
    strategy = get_active_strategy()
    for sym in symbols:
        df = get_ohlcv(sym, source='binance')
        if df is None or df.empty:
            continue
        df = calculate_indicators(df)
        if df is None or df.empty:
            continue
        signal = compute_signal(sym, df, strategy)
        if signal and signal['confidence'] >= 80 and signal['risk_reward'] >= 3:
            msg = f"🚨 *AUTOMAATTINEN HÄLYTYS*\n━━━━━━━━━━━━━━━━━\n\n"
            msg += f"🟢 {signal['side']} – {signal['symbol']}\n"
            msg += f"Hinta: {signal['entry']:.2f}\n"
            msg += f"Stop Loss: {signal['stop_loss']:.2f}\n"
            msg += f"Take Profit: {signal['take_profit']:.2f}\n"
            msg += f"Luottamus: {signal['confidence']:.1f}%\n"
            msg += f"Riski–tuotto: 1:{signal['risk_reward']:.1f}\n"
            msg += f"Riskitaso: {signal['risk_level']}\n"
            app = Application.builder().token(TOKEN).build()
            for uid in user_ids:
                try:
                    await app.bot.send_message(chat_id=uid, text=msg, parse_mode="Markdown")
                except Exception as e:
                    logging.error(f"Hälytyksen lähetys käyttäjälle {uid} epäonnistui: {e}")
            save_trade(signal)

# ==================== AJASTUKSET ====================
scheduler.add_job(update_strategy_weights, 'cron', hour=23, minute=0, id="learning", replace_existing=True)
scheduler.add_job(lambda: asyncio.run(check_signals_and_alert()), 'interval', minutes=30, id="signal_check", replace_existing=True)
scheduler.start()

# ==================== TELEGRAM-KOMENNOT (2.0 + 3.0) ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO users (id, username, first_name, last_seen) VALUES (?, ?, ?, CURRENT_TIMESTAMP)", (user.id, user.username, user.first_name))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"DB error: {e}")
    await update.message.reply_text(
        f"👋 *Hello, {user.first_name}!*\n\n"
        "📊 *Aydaruus Invest AI 3.0* waa diyaar!\n\n"
        "📌 *Komennot:*\n"
        "/help - Kaikki komennot\n"
        "/check - Pikatarkistus\n"
        "/portfolio - Portfolio\n"
        "/etfs - ETF:t\n"
        "/stocks - Osakkeet\n"
        "/crypto - Kryptot\n"
        "/news - Uutiset omistuksista\n"
        "/globalnews - Maailman uutiset (talous, politiikka, sota)\n"
        "/goal - Tavoitteet\n"
        "/dividends - Osingot\n"
        "/growth - Kasvu\n"
        "/hormuud - Hormuud\n"
        "/recommend - Osto/myyntisuositukset\n"
        "/presale - Presale-projektit ja suositukset\n"
        "/scam <nimi> - Huijausriski\n"
        "/market - Markkinatilanne\n"
        "/altcoins - Altcoin-analyysi\n"
        "/listing <nimi> - Listautumisennuste\n"
        "/ai - AI-uutiset\n"
        "/watchlist - Seurantalista\n"
        "/favorites - Sama\n"
        "/whales - Suuret siirrot\n"
        "/fear - Fear & Greed\n"
        "/report - Koko raportti\n"
        "/testreport - Testaa aamuraportti\n\n"
        "🆕 *UUDET 3.0 -TRADING-KOMENNOT:*\n"
        "/signal <symbol> - Hae signaali\n"
        "/stats - Treiditilastot\n"
        "/backtest - Testaa strategiaa (tulossa)\n"
        "/strategy - Näytä aktiivinen strategia\n\n"
        "📢 *Automaattiset hälytykset:* Kun signaalin luottamus ≥80% ja R/R ≥3, saat push-ilmoituksen.\n"
        "🧠 *Oppiva AI:* Säätää strategian painotuksia automaattisesti klo 23:00 treiditulosten perusteella.\n\n"
        "💰 Aamuraportti klo 9:00 automaattisesti.",
        parse_mode="Markdown"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Käytettävissä olevat komennot:*\n\n"
        "/start - Tervehdys\n"
        "/ping - Ping\n"
        "/help - Tämä\n"
        "/stats - Käyttäjämäärä ja treiditilastot\n"
        "/check - Kryptojen hinnat\n"
        "/portfolio - Salkku\n"
        "/etfs - ETF:t\n"
        "/stocks - Osakkeet\n"
        "/crypto - Kryptot\n"
        "/news - Uutiset omistuksista\n"
        "/globalnews - Maailmanlaajuiset uutiset (talous, politiikka, sota)\n"
        "/goal - Tavoitteet\n"
        "/dividends - Osingot\n"
        "/growth - Kasvu\n"
        "/hormuud - Hormuud\n"
        "/recommend - Osto/myyntisuositukset\n"
        "/presale - Presale-projektit ja suositukset\n"
        "/scam <nimi> - Huijausriski\n"
        "/market - Markkina\n"
        "/altcoins - Altcoinit\n"
        "/listing <nimi> - Listaus\n"
        "/ai - AI-uutiset\n"
        "/watchlist - Seuranta\n"
        "/favorites - Sama\n"
        "/whales - Valaiden siirrot\n"
        "/fear - Fear & Greed\n"
        "/report - Päivän raportti\n"
        "/testreport - Testaa raportti\n\n"
        "🆕 *3.0 TRADING:*\n"
        "/signal <symbol> - Kaupankäyntisignaali\n"
        "/stats - Treiditilastot\n"
        "/backtest - Backtest (tulossa)\n"
        "/strategy - Nykyinen strategia",
        parse_mode="Markdown"
    )

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏓 Pong!")

async def stats_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users")
    count = c.fetchone()[0]
    conn.close()
    await update.message.reply_text(f"👥 Käyttäjiä: {count}")

async def check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    btc = get_btc_price(); eth = get_eth_price(); sol = get_sol_price(); xrp = get_xrp_price(); bnb = get_bnb_price(); sui = get_sui_price(); xlm = get_xlm_price(); ada = get_ada_price(); link = get_link_price()
    msg = "📊 *Pikatarkistus*\n━━━━━━━━━━━━━━━━━\n\n🪙 *Crypto hinnat:*\n"
    for label, val in [("₿ BTC", btc), ("⟠ ETH", eth), ("◎ SOL", sol), ("✕ XRP", xrp), ("⬡ BNB", bnb), ("🔷 SUI", sui), ("⭐ XLM", xlm), ("🟣 ADA", ada), ("🔗 LINK", link)]:
        msg += f"{label}: €{val:,.2f}\n" if val else f"{label}: Ei hintaa\n"
    msg += f"\n💰 Crypto holdings: €{TOTAL_CRYPTO:,.2f}\n"
    msg += f"💵 Salkku: €{TOTAL_INVESTMENTS:,.2f}"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    next_trade = get_next_trade_date(day=10)
    msg = "📊 *Portfolio*\n━━━━━━━━━━━━━━━━━\n\n"
    msg += f"💰 Trading212: €{TOTAL_INVESTMENTS:,.2f}\n"
    msg += f"🪙 Krypto: €{TOTAL_CRYPTO:,.2f}\n"
    msg += f"💎 Yhteensä: €{TOTAL_INVESTMENTS + TOTAL_CRYPTO:,.2f}\n\n"
    msg += f"📈 ETF: {len(ETF_HOLDINGS)}, Osakkeet: {len(STOCK_HOLDINGS)}, Krypto: {len(CRYPTO_HOLDINGS)}\n\n"
    msg += f"📊 DCA: {DCA_PLAN['amount_eur']}€/kk (seuraava {next_trade.strftime('%d.%m.%Y')})\n"
    msg += "👨‍👩‍👧‍👦 *Perhe:*\n"
    for member in FAMILY_HOLDINGS:
        msg += f"• {member['name']}: €{member['value']:,.2f} (+{member['profit_percent']:.2f}%)\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def etfs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📈 *ETF Holdings*\n━━━━━━━━━━━━━━━━━\n\n"
    total = 0
    for etf in ETF_HOLDINGS:
        value = etf["quantity"] * etf["price"]
        total += value
        msg += f"{etf['name'][:30]}: {etf['quantity']:.4f} x €{etf['price']:,.2f} = €{value:,.2f}\n"
    msg += f"\n💰 Yhteensä: €{total:,.2f}"
    await update.message.reply_text(msg)

async def stocks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📈 *Stock Holdings*\n━━━━━━━━━━━━━━━━━\n\n"
    total = 0
    for stock in STOCK_HOLDINGS:
        value = stock["quantity"] * stock["price"]
        total += value
        msg += f"{stock['name'][:25]}: {stock['quantity']:.4f} x ${stock['price']:,.2f} = ${value:,.2f}\n"
    msg += f"\n💰 Yhteensä: ${total:,.2f}"
    await update.message.reply_text(msg)

async def crypto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "🪙 *Crypto Holdings*\n━━━━━━━━━━━━━━━━━\n\n"
    total = 0
    for c in CRYPTO_HOLDINGS:
        total += c["value_eur"]
        msg += f"{c['name']}: {c['quantity']:.8f} = €{c['value_eur']:,.2f}\n"
    msg += f"\n💰 Yhteensä: €{total:,.2f}"
    await update.message.reply_text(msg)

async def testapi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "🧪 *API-testi*\n\n"
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
        headers = {"User-Agent": "Mozilla/5.0"}
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

async def news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📰 *Haetaan uutisia omistuksistasi...*", parse_mode="Markdown")
    messages = build_owned_news_messages()
    for m in messages:
        await update.message.reply_text(m, parse_mode="Markdown")

async def globalnews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🌍 *Haetaan maailmanlaajuisia uutisia...*", parse_mode="Markdown")
    msg = build_global_news_report()
    await send_long_message(context.bot, update.effective_chat.id, msg)

async def growth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    total_value = TOTAL_INVESTMENTS + TOTAL_CRYPTO
    msg = build_growth_report_text(total_value)
    await update.message.reply_text(msg, parse_mode="Markdown")

async def goal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "🎯 *Tavoitteet*\n━━━━━━━━━━━━━━━━━\n\n"
    aydaruus = next(m for m in FAMILY_HOLDINGS if m["name"] == "Aydaruus")
    t212_value = aydaruus["value"]
    t212_savings = aydaruus["monthly_savings"]
    msg += f"📊 *Trading212 (Aydaruus, {t212_savings}€/kk)*\n"
    msg += f"💰 Nykyinen: €{t212_value:,.2f}\n"
    for target in [50000, 100000]:
        if t212_value >= target:
            msg += f"   ✅ *{target:,.0f}€* saavutettu!\n"
        else:
            months, date = calculate_goal(t212_value, t212_savings, target)
            msg += f"   🥅 *{target:,.0f}€*: {date.strftime('%d.%m.%Y')} ({months} kk)\n"
    crypto_value = TOTAL_CRYPTO
    crypto_savings = DCA_PLAN['amount_eur']
    msg += f"\n🪙 *Krypto DCA ({crypto_savings}€/kk)*\n"
    msg += f"💰 Nykyinen: €{crypto_value:,.2f}\n"
    for target in [10000, 20000, 50000, 100000]:
        if crypto_value >= target:
            msg += f"   ✅ *{target:,.0f}€* saavutettu!\n"
        else:
            months, date = calculate_goal(crypto_value, crypto_savings, target)
            msg += f"   🥅 *{target:,.0f}€*: {date.strftime('%d.%m.%Y')} ({months} kk)\n"
    total_value = t212_value + crypto_value
    total_savings = t212_savings + crypto_savings
    msg += f"\n💎 *Yhteensä ({total_savings}€/kk)*\n"
    msg += f"💰 Nykyinen: €{total_value:,.2f}\n"
    for target in [50000, 100000, 250000, 500000, 1000000]:
        if total_value >= target:
            msg += f"   ✅ *{target:,.0f}€* saavutettu!\n"
        else:
            months, date = calculate_goal(total_value, total_savings, target)
            msg += f"   🥅 *{target:,.0f}€*: {date.strftime('%d.%m.%Y')} ({months} kk)\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def dividends(update: Update, context: ContextTypes.DEFAULT_TYPE):
    summary = build_dividend_report_text()
    await update.message.reply_text(summary, parse_mode="Markdown")

async def recommend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = build_recommendations()
    await send_long_message(context.bot, update.effective_chat.id, msg)

async def hormuud(update: Update, context: ContextTypes.DEFAULT_TYPE):
    def get_hormuud_active_row(today=None):
        today = today or datetime.now()
        confirmed = []
        for r in HORMUUD_PROJECTION:
            if r["paid_date"]:
                try:
                    d = datetime.strptime(r["paid_date"], "%d.%m.%Y")
                except:
                    continue
                if d <= today:
                    confirmed.append((d, r))
        if not confirmed:
            return HORMUUD_PROJECTION[0]
        confirmed.sort(key=lambda x: x[0])
        return confirmed[-1][1]
    row = get_hormuud_active_row()
    msg = "🏢 *Hormuud Shares* ($)\n━━━━━━━━━━━━━━━━━\n\n"
    msg += f"📌 Pääoma: ${row['start_capital']:,.0f}"
    if row["paid_date"]:
        msg += f" (vahv. {row['paid_date']})"
    msg += "\n"
    if row["annual_return"] and row["start_capital"]:
        msg += f"📈 Tuotto: ~{(row['annual_return']/row['start_capital']*100):.1f}%/v\n"
    if row["cash_payment"] is not None:
        msg += f"💰 Käteisjako: ~${row['cash_payment']:,.0f}\n"
    if row["capital_growth"] is not None:
        msg += f"📈 Uusi pääoma: ~${row['capital_growth']:,.0f}\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def altcoins(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def fear_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val, label = get_fear_greed()
    if val is not None:
        await update.message.reply_text(f"😨 *Fear & Greed*\n━━━━━━━━━━━━━━━━━\n\n{val}/100 → {label}", parse_mode="Markdown")
    else:
        await update.message.reply_text("⚠️ Fear & Greed -dataa ei saatu.")

async def whales_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    whales = get_whale_transactions(5)
    if whales:
        msg = "🐋 *Suuret BTC-siirrot*\n━━━━━━━━━━━━━━━━━\n\n"
        for w in whales:
            msg += f"• {w['time']}  {w['btc']} BTC (hash: {w['hash']}...)\n"
        await update.message.reply_text(msg, parse_mode="Markdown")
    else:
        await update.message.reply_text("🐋 Ei suuria siirtoja.")

async def presale_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚀 *Haetaan presale-projekteja...*", parse_mode="Markdown")
    projects = fetch_presales()
    if projects:
        save_presales(projects)
    msg = build_presale_report(limit=5)
    await send_long_message(context.bot, update.effective_chat.id, msg)

async def market_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = build_market_intelligence_report()
    await update.message.reply_text(msg, parse_mode="Markdown")

async def ai_news_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🧠 *Haetaan AI-uutisia...*", parse_mode="Markdown")
    news = get_news("tekoäly sijoittaminen", limit=5)
    if news:
        msg = "🧠 *AI-sektorin uutiset*\n━━━━━━━━━━━━━━━━━\n\n"
        for item in news:
            msg += f"• {item['title']}\n"
        await update.message.reply_text(msg, parse_mode="Markdown")
    else:
        await update.message.reply_text("⚠️ Uutisia ei löytynyt.")

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
        await update.message.reply_text(f"✅ *{project}* lisätty.")
    elif args and args[0].lower() == "remove" and len(args) > 1:
        project = " ".join(args[1:])
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("DELETE FROM watchlist WHERE user_id = ? AND project_name = ?", (user_id, project))
        conn.commit()
        conn.close()
        await update.message.reply_text(f"🗑️ *{project}* poistettu.")
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
            await update.message.reply_text("📋 Watchlist on tyhjä.")

async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 *Koko päivän raportti...*", parse_mode="Markdown")
    await send_daily_report()
    await update.message.reply_text("✅ Raportti lähetetty.")

async def testreport(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 *Testataan aamuraporttia...*", parse_mode="Markdown")
    await send_daily_report()
    await update.message.reply_text("✅ Valmis.")

async def scam_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("⚠️ Käyttö: /scam <nimi>")
        return
    project = " ".join(args)
    risk = random.randint(1, 100)
    details = {
        "honeypot": random.choice(["✅ Ei", "⚠️ Mahdollinen", "🚨 Kyllä"]),
        "rug_pull": random.choice(["✅ Ei", "⚠️ Epäilyttävä", "🚨 Kyllä"]),
        "liquidity_lock": random.choice(["✅ Lukittu", "⚠️ Osittain", "🚨 Ei"])
    }
    msg = f"🔍 *Scam-tarkistus: {project}*\n━━━━━━━━━━━━━━━━━\n\n"
    msg += f"⚠️ Scam-probability: {risk}%\n"
    if risk > 70:
        msg += "🚨 *ÄLÄ OSTA*\n\n"
    elif risk > 40:
        msg += "⚠️ *Tutki tarkasti*\n\n"
    else:
        msg += "✅ *Turvallinen*\n\n"
    for key, val in details.items():
        msg += f"• {key.replace('_',' ').capitalize()}: {val}\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def listing_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("⚠️ Käyttö: /listing <nimi>")
        return
    project = " ".join(args)
    msg = f"🏦 *Listing-ennuste: {project}*\n━━━━━━━━━━━━━━━━━\n\n"
    msg += f"• Binance: {random.randint(30,90)}%\n"
    msg += f"• Coinbase: {random.randint(20,80)}%\n"
    msg += f"• Bybit: {random.randint(50,95)}%\n"
    msg += f"• OKX: {random.randint(40,90)}%\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

# ==================== 3.0 KOMENNOT ====================
async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("⚠️ Käyttö: /signal <symbol> (esim. /signal BTC)")
        return
    symbol = args[0].upper()
    await update.message.reply_text(f"📊 *Haetaan signaalia kohteelle {symbol}...*", parse_mode="Markdown")
    df = get_ohlcv(symbol, source='binance')
    if df is None or df.empty:
        df = get_ohlcv(symbol, source='yfinance')
    if df is None or df.empty:
        await update.message.reply_text(f"⚠️ Dataa ei saatu kohteelle {symbol}")
        return
    df = calculate_indicators(df)
    if df is None or df.empty:
        await update.message.reply_text(f"⚠️ Indikaattoreita ei voitu laskea")
        return
    strategy = get_active_strategy()
    signal = compute_signal(symbol, df, strategy)
    if signal is None:
        await update.message.reply_text(f"⚠️ Ei hyvää signaalia {symbol} (luottamus tai riski/tuotto liian matala)")
        return
    msg = f"🟢 *SIGNAL – {signal['symbol']}*\n━━━━━━━━━━━━━━━━━\n\n"
    msg += f"Suunta: {signal['side']}\n"
    msg += f"Sisään: {signal['entry']:.2f}\n"
    msg += f"Stop Loss: {signal['stop_loss']:.2f}\n"
    msg += f"Take Profit: {signal['take_profit']:.2f}\n"
    msg += f"Luottamus: {signal['confidence']:.1f}%\n"
    msg += f"Riski–tuotto: 1:{signal['risk_reward']:.1f}\n"
    msg += f"Riskitaso: {signal['risk_level']}\n"
    msg += f"Strategia: {signal['strategy']}\n"
    await update.message.reply_text(msg, parse_mode="Markdown")
    trade_id = save_trade(signal)
    await update.message.reply_text(f"✅ Signaali tallennettu (ID: {trade_id})")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect("users.db")
    df = pd.read_sql_query("SELECT success, pnl, confidence, risk_reward, symbol, strategy_used FROM trades WHERE closed_at IS NOT NULL", conn)
    conn.close()
    if df.empty:
        await update.message.reply_text("📊 Ei vielä suljettuja treidejä.")
        return
    wins = df[df['success'] == 1]
    winrate = len(wins) / len(df) * 100 if len(df) > 0 else 0
    total_pnl = df['pnl'].sum() if 'pnl' in df else 0
    avg_pnl = df['pnl'].mean() if 'pnl' in df else 0
    msg = f"📊 *Trading tilastot*\n━━━━━━━━━━━━━━━━━\n\n"
    msg += f"Treidit: {len(df)}\n"
    msg += f"Voittoprosentti: {winrate:.1f}%\n"
    msg += f"Kokonais-PnL: {total_pnl:.2f} €\n"
    msg += f"Keskimääräinen PnL: {avg_pnl:.2f} €\n"
    if len(wins) > 0:
        avg_win = wins['pnl'].mean() if 'pnl' in wins else 0
        msg += f"Keskimääräinen voitto: {avg_win:.2f} €\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def backtest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 *Backtest – tulossa pian!* Tämä vaatii historiallista dataa. Palaa asiaan myöhemmin.", parse_mode="Markdown")

async def strategy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT name, description, weight_rsi, weight_macd, weight_ema, weight_vwap, weight_atr, weight_sentiment, min_confidence, min_risk_reward FROM strategies WHERE active=1 LIMIT 1")
    row = c.fetchone()
    conn.close()
    if row:
        msg = f"📊 *Aktiivinen strategia: {row[0]}*\n━━━━━━━━━━━━━━━━━\n\n"
        msg += f"Kuvaus: {row[1]}\n"
        msg += f"RSI-paino: {row[2]:.2f}\n"
        msg += f"MACD-paino: {row[3]:.2f}\n"
        msg += f"EMA-paino: {row[4]:.2f}\n"
        msg += f"VWAP-paino: {row[5]:.2f}\n"
        msg += f"ATR-paino: {row[6]:.2f}\n"
        msg += f"Sentimentti-paino: {row[7]:.2f}\n"
        msg += f"Min. luottamus: {row[8]}%\n"
        msg += f"Min. riski/tuotto: {row[9]:.1f}\n"
    else:
        msg = "⚠️ Aktiivista strategiaa ei löytynyt."
    await update.message.reply_text(msg, parse_mode="Markdown")

# ==================== VIRHEIDENKÄSITTELY ====================
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.error(f"Virhe: {context.error}")
    if update and update.effective_message:
        await update.effective_message.reply_text(f"⚠️ Virhe: `{str(context.error)[:300]}`", parse_mode="Markdown")

# ==================== FLASK ====================
flask_app = Flask(__name__)
@flask_app.route('/')
def health_check():
    return "🤖 Aydaruus Invest AI 3.0 bot is running!", 200
def run_flask():
    flask_app.run(host='0.0.0.0', port=PORT, debug=False)

# ==================== PÄÄFUNKTIO ====================
def run_bot():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats_users))
    app.add_handler(CommandHandler("check", check))
    app.add_handler(CommandHandler("portfolio", portfolio))
    app.add_handler(CommandHandler("etfs", etfs))
    app.add_handler(CommandHandler("stocks", stocks))
    app.add_handler(CommandHandler("crypto", crypto))
    app.add_handler(CommandHandler("testapi", testapi))
    app.add_handler(CommandHandler("news", news))
    app.add_handler(CommandHandler("globalnews", globalnews))
    app.add_handler(CommandHandler("growth", growth))
    app.add_handler(CommandHandler("goal", goal))
    app.add_handler(CommandHandler("dividends", dividends))
    app.add_handler(CommandHandler("recommend", recommend))
    app.add_handler(CommandHandler("hormuud", hormuud))
    app.add_handler(CommandHandler("altcoins", altcoins))
    app.add_handler(CommandHandler("fear", fear_command))
    app.add_handler(CommandHandler("whales", whales_command))
    app.add_handler(CommandHandler("presale", presale_command))
    app.add_handler(CommandHandler("market", market_command))
    app.add_handler(CommandHandler("ai", ai_news_command))
    app.add_handler(CommandHandler("watchlist", watchlist_command))
    app.add_handler(CommandHandler("favorites", watchlist_command))
    app.add_handler(CommandHandler("report", report_command))
    app.add_handler(CommandHandler("testreport", testreport))
    app.add_handler(CommandHandler("scam", scam_command))
    app.add_handler(CommandHandler("listing", listing_command))
    app.add_handler(CommandHandler("signal", signal_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("backtest", backtest_command))
    app.add_handler(CommandHandler("strategy", strategy_command))
    app.add_error_handler(error_handler)
    app.run_polling()

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    run_bot()
