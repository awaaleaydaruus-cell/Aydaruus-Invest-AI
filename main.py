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
    "next_trade": "2026-08-10"
}

TRADING212_PLAN = {
    "name": "Dream",
    "owner": "Aydaruus Ahmed Wehliye",
    "amount_eur": 200,
    "day": 10,
    "holdings": 39,
    "next_trade": "2026-08-10",
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

CURRENT_QTY_BY_ISIN = {h["isin"]: h["quantity"] for h in ETF_HOLDINGS}
CURRENT_QTY_BY_ISIN.update({h["isin"]: h["quantity"] for h in STOCK_HOLDINGS})

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
# 8. OSINGOT — LUE CSV:STÄ
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

# =============================================
# 9. TAVOITELASKENTA
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
    try:
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
    except Exception as e:
        logging.error(f"Virhe /news: {e}")
        await update.message.reply_text(f"⚠️ Virhe /news: {str(e)[:200]}")

# =============================================
# 11. AAMURAPORTTI
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

        msg = "📊 *Subax wanaagsan, sijoittaja!*\n\n"
        msg += "💰 *Portfolio-gaaga*\n━━━━━━━━━━━━━━━━━\n"
        msg += f"💵 Wadarta guud (Trading212): €{TOTAL_INVESTMENTS:,.2f}\n"
        msg += f"🪙 Crypto holdings: €{TOTAL_CRYPTO:,.2f}\n"
        msg += f"💎 Yhteensä: €{TOTAL_INVESTMENTS + TOTAL_CRYPTO:,.2f}\n\n"

        msg += "🪙 *Crypto qiimaha hadda:*\n"
        for label, val in [("₿ BTC", btc), ("⟠ ETH", eth), ("◎ SOL", sol), ("✕ XRP", xrp),
                            ("⬡ BNB", bnb), ("🔷 SUI", sui), ("⭐ XLM", xlm),
                            ("🟣 ADA", ada), ("🔗 LINK", link)]:
            msg += f"{label}: €{val:,.2f}\n" if val else f"{label}: Laga ma helin\n"

        msg += f"\n👨‍👩‍👧‍👦 *Perheen salkut:*\n"
        for member in FAMILY_HOLDINGS:
            msg += f"• {member['name']}: €{member['value']:,.2f} (+{member['profit_percent']:.1f}%)\n"

        _, total_div = get_dividend_details()
        msg += f"\n💵 Osingot (12 kk, todelliset): €{total_div:,.2f}\n"

        today = datetime.now()
        if today.day == 10:
            msg += f"\n🔔 *XASUUSIN! Maanta waa 10-da bil!*\n"
            msg += f"💵 Geli €{DCA_PLAN['amount_eur']} crypto + €{TRADING212_PLAN['amount_eur']} Trading 212!\n"
        else:
            next_month = today.month + 1 if today.month < 12 else 1
            next_year = today.year if today.month < 12 else today.year + 1
            msg += f"\n📌 Togga xiga: 10-{next_month:02d}-{next_year}"

        app = Application.builder().token(TOKEN).build()
        for uid in user_ids:
            try:
                await app.bot.send_message(chat_id=uid, text=msg, parse_mode="Markdown")
            except Exception as e:
                logging.error(f"Raportin lähetys käyttäjälle {uid} epäonnistui: {e}")
    except Exception as e:
        logging.error(f"Virhe send_daily_report: {e}")

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

def send_daily_report_sync():
    import asyncio
    asyncio.run(send_daily_report())

# =============================================
# 12. AJOITUS
# =============================================
scheduler = BackgroundScheduler()
scheduler.add_job(send_daily_report_sync, 'cron', hour=9, minute=0, id="daily_report", replace_existing=True)
scheduler.start()

# =============================================
# 13. TELEGRAM KOMENNOT
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
            "/goal - Tavoitteet (Trading212, krypto ja perhe)\n"
            "/dividends - Näytä kaikkien perheenjäsenten osingot eriteltynä\n"
            "/recommend - Sijoitusanalyysi & suositukset\n"
            "/testreport - Testaa aamuraportti (manuaalinen)\n\n"
            "💰 Maalin kasta 9:00 subax waxaan kuu soo dirayaa warbixin!",
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"Virhe /start: {e}")
        await update.message.reply_text(f"⚠️ Virhe /start: {str(e)[:200]}")

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("🏓 Pong!")
    except Exception as e:
        logging.error(f"Virhe /ping: {e}")
        await update.message.reply_text(f"⚠️ Virhe /ping: {str(e)[:200]}")

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
            "/news - Uutiset omistuksista\n"
            "/goal - Tavoitteet (Trading212, krypto ja perhe)\n"
            "/dividends - Näytä kaikkien perheenjäsenten osingot eriteltynä\n"
            "/recommend - Sijoitusanalyysi & suositukset\n"
            "/testreport - Testaa aamuraportti (manuaalinen)\n\n"
            "💰 *DCA:* €100/bil (crypto) + €200/kk (Aydaruus) + 5×50€/kk (perhe) = 550€/kk\n"
            "📊 *Warbixin maalinle:* 9:00 subax",
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"Virhe /help: {e}")
        await update.message.reply_text(f"⚠️ Virhe /help: {str(e)[:200]}")

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
        msg = "📊 *Portfolio-gaaga*\n━━━━━━━━━━━━━━━━━\n\n"
        msg += f"💰 *Wadarta guud (T212 Invest):* €{TOTAL_INVESTMENTS:,.2f}\n"
        msg += f"🪙 *Crypto holdings (ulkoinen):* €{TOTAL_CRYPTO:,.2f}\n"
        msg += f"💎 *Yhteensä:* €{TOTAL_INVESTMENTS + TOTAL_CRYPTO:,.2f}\n\n"
        msg += f"📈 *ETF holdings:* {len(ETF_HOLDINGS)} holdings\n"
        msg += f"📈 *Stock holdings:* {len(STOCK_HOLDINGS)} holdings\n"
        msg += f"🪙 *Crypto holdings:* {len(CRYPTO_HOLDINGS)} holdings\n\n"

        msg += f"📊 *Trading 212 -kuukausisijoitus:*\n"
        msg += f"💰 Aydaruus: €200/kk (10. päivä)\n"
        for member in FAMILY_HOLDINGS:
            if member["name"] != "Aydaruus":
                msg += f"💰 {member['name']}: €{member['monthly_savings']}/kk (10. päivä)\n"
        msg += f"💰 Yhteensä: €450/kk\n\n"

        msg += "👨‍👩‍👧‍👦 *Perheen holdings*\n"
        for member in FAMILY_HOLDINGS:
            msg += f"• {member['name']}: {member['holdings']} hold. = €{member['value']:,.2f} (+€{member['profit']:,.2f} / +{member['profit_percent']:.2f}%)\n"

        msg += f"\n📌 *DCA qorshaha:* {DCA_PLAN['name']}\n"
        msg += f"💰 €{DCA_PLAN['amount_eur']}/bil (10-da bil)\n"
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

# =============================================
# 14. GOAL — AYDARUUS + PERHE
# =============================================
async def goal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = "🎯 *Sijoitustavoitteet*\n━━━━━━━━━━━━━━━━━\n\n"

        # Aydaruus: Trading212 (200 €/kk)
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

        # Aydaruus: Krypto DCA (100 €/kk)
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

        # Aydaruus: Yhteensä (200+100=300 €/kk)
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

        # Perheenjäsenet (Ismahaan, Ilyaas, Farhia, Mahamed, Yahye) — 50 €/kk kukin
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

# =============================================
# 15. DIVIDENDS — KAIKKI PERHEENJÄSENET
# =============================================
FI_MONTHS = {
    "01": "Tammikuu", "02": "Helmikuu", "03": "Maaliskuu", "04": "Huhtikuu",
    "05": "Toukokuu", "06": "Kesäkuu", "07": "Heinäkuu", "08": "Elokuu",
    "09": "Syyskuu", "10": "Lokakuu", "11": "Marraskuu", "12": "Joulukuu",
}

def _build_dividend_message(owner, projected, monthly, yearly_total, current_year):
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
        max_len = 3500

        all_messages = []
        for owner in FAMILY_OWNERSHIPS.keys():
            projected, monthly, yearly_total = get_upcoming_dividends_estimated(owner=owner)
            if not projected:
                continue
            msg = _build_dividend_message(owner, projected, monthly, yearly_total, current_year)
            all_messages.append(msg)

        if not all_messages:
            await update.message.reply_text(
                "⚠️ Osinkoja ei voitu arvioida kenellekään perheenjäsenelle.\n"
                "Varmista, että dividends.csv sisältää vähintään 2 maksua per osake/ETF."
            )
            return

        for msg in all_messages:
            if len(msg) > max_len:
                parts = [msg[i:i+max_len] for i in range(0, len(msg), max_len)]
                for part in parts:
                    await update.message.reply_text(part, parse_mode="Markdown")
            else:
                await update.message.reply_text(msg, parse_mode="Markdown")

    except Exception as e:
        logging.error(f"Virhe dividends-komennossa: {e}")
        await update.message.reply_text(f"⚠️ Virhe /dividends: {str(e)[:200]}")

# =============================================
# 16. RECOMMEND
# =============================================
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

        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /recommend: {e}")
        await update.message.reply_text(f"⚠️ Virhe /recommend: {str(e)[:200]}")

# =============================================
# 17. TESTREPORT
# =============================================
async def testreport(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("📊 *Testataan aamuraporttia...*", parse_mode="Markdown")
        await send_daily_report()
        await update.message.reply_text("✅ *Aamuraportti lähetetty!*", parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Virhe /testreport: {e}")
        await update.message.reply_text(f"⚠️ Virhe /testreport: {str(e)[:200]}")

# =============================================
# 18. VIRHEIDENKÄSITTELY
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
# 19. FLASK
# =============================================
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "🤖 Aydaruus Invest AI bot is running!", 200

def run_flask():
    flask_app.run(host='0.0.0.0', port=PORT, debug=False)

# =============================================
# 20. PÄÄFUNKTIO
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
    app.add_handler(CommandHandler("testreport", testreport))
    app.add_error_handler(error_handler)
    app.run_polling()

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    run_bot()
