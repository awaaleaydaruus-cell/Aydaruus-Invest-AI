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
    c.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            price REAL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

init_db()

USER_ID = None

# =============================================
# 4. PORTFOLIO HOLDINGS
# =============================================

# ETFs (18 holdings)
ETF_HOLDINGS = [
    {"isin": "IE00B5BMR087", "name": "iShares Core S&P 500", "quantity": 1.3195, "price": 711.48},
    {"isin": "IE00BFMXXD54", "name": "Vanguard S&P 500", "quantity": 46.7843, "price": 127.59},
    {"isin": "IE00B4L5Y983", "name": "iShares Core MSCI World", "quantity": 7.8606, "price": 126.145},
    {"isin": "IE00BK5BQT80", "name": "Vanguard FTSE All-World", "quantity": 3.9579, "price": 166.14},
    {"isin": "IE00B53SZ819", "name": "iShares NASDAQ 100", "quantity": 0.5519, "price": 1493.8},
    {"isin": "IE00XZSV7183", "name": "SPDR S&P 500", "quantity": 35.4510, "price": 16.3422},
    {"isin": "IE00B3XXRP09", "name": "Vanguard S&P 500", "quantity": 5.0718, "price": 125.226},
    {"isin": "IE0031442068", "name": "iShares Core S&P 500 Dist", "quantity": 8.6921, "price": 65.83},
    {"isin": "IE00BYVQ9F29", "name": "iShares NASDAQ 100", "quantity": 31.3512, "price": 17.26},
    {"isin": "IE00B4YBJ215", "name": "SPDR S&P 400 Mid Cap", "quantity": 0.4462, "price": 102.76},
    {"isin": "IE00B1YZSC51", "name": "iShares Core MSCI Europe", "quantity": 0.5323, "price": 40.205},
    {"isin": "IE00U9J8HX94", "name": "JPMorgan Nasdaq Premium", "quantity": 99.6818, "price": 23.865},
    {"isin": "IE00U5MJOZ6", "name": "JPMorgan US Equity Premium", "quantity": 9.8691, "price": 21.345},
    {"isin": "IE0003UVYC20", "name": "JPMorgan Global Equity Premium", "quantity": 5.6890, "price": 22.42},
    {"isin": "IE00B8GKD810", "name": "Vanguard FTSE All-World High Div", "quantity": 8.9457, "price": 79.882},
    {"isin": "IE00BM8ROJ59", "name": "Global X Nasdaq 100 Covered Call", "quantity": 1.6728, "price": 14.91},
    {"isin": "IE00BMC38736", "name": "VanEck Semiconductor", "quantity": 0.2491, "price": 100.38},
    {"isin": "IE00B6YX5D40", "name": "SPDR S&P US Dividend Aristocrats", "quantity": 10.6954, "price": 74.53}
]

# Stocks (27 holdings)
STOCK_HOLDINGS = [
    {"symbol": "TSLA", "name": "Tesla", "quantity": 1.7783, "price": 407.59},
    {"symbol": "AMZN", "name": "Amazon", "quantity": 2.7513, "price": 245.74},
    {"symbol": "MSFT", "name": "Microsoft", "quantity": 51.2188, "price": 385.34},
    {"symbol": "NVDA", "name": "NVIDIA", "quantity": 7.7732, "price": 210.57},
    {"symbol": "KO", "name": "Coca-Cola", "quantity": 78.6142, "price": 83.45},
    {"symbol": "CVX", "name": "Chevron", "quantity": 53.2040, "price": 176.16},
    {"symbol": "JPM", "name": "JPMorgan Chase", "quantity": 53.8594, "price": 336.88},
    {"symbol": "META", "name": "Meta", "quantity": 70.7542, "price": 668},
    {"symbol": "PLTR", "name": "Palantir", "quantity": 85.9701, "price": 126.59},
    {"symbol": "AAPL", "name": "Apple", "quantity": 54.4092, "price": 314.97},
    {"symbol": "PFE", "name": "Pfizer", "quantity": 527.9008, "price": 24.22},
    {"symbol": "PEP", "name": "PepsiCo", "quantity": 12.3842, "price": 137.4},
    {"symbol": "MSTR", "name": "Strategy", "quantity": 30.0119, "price": 94.89},
    {"symbol": "PG", "name": "Procter & Gamble", "quantity": 11.5941, "price": 147.05},
    {"symbol": "JNJ", "name": "Johnson & Johnson", "quantity": 64.8509, "price": 256.6},
    {"symbol": "AVGO", "name": "Broadcom", "quantity": 20.1312, "price": 400.39},
    {"symbol": "VZ", "name": "Verizon", "quantity": 46.3337, "price": 42.15},
    {"symbol": "XOM", "name": "ExxonMobil", "quantity": 52.6972, "price": 138.8},
    {"symbol": "AMD", "name": "AMD", "quantity": 81.7068, "price": 559.77},
    {"symbol": "BLK", "name": "BlackRock", "quantity": 90.0917, "price": 1036},
    {"symbol": "V", "name": "Visa", "quantity": 0.9189, "price": 349.13},
    {"symbol": "MA", "name": "Mastercard", "quantity": 0.5303, "price": 526.12},
    {"symbol": "GOOGL", "name": "Alphabet", "quantity": 91.0928, "price": 357.17},
    {"symbol": "VICI", "name": "VICI Properties", "quantity": 4.4521, "price": 26.01},
    {"symbol": "ABBV", "name": "AbbVie", "quantity": 10.8022, "price": 249.9},
    {"symbol": "BAC", "name": "Bank of America", "quantity": 62.2532, "price": 59.66},
    {"symbol": "QCOM", "name": "Qualcomm", "quantity": 60.8224, "price": 188.9}
]

# Crypto holdings (9)
CRYPTO_HOLDINGS = [
    {"symbol": "bitcoin", "name": "BTC", "quantity": 0.00674376, "value_eur": 379.43},
    {"symbol": "ethereum", "name": "ETH", "quantity": 0.36953452, "value_eur": 587.15},
    {"symbol": "solana", "name": "SOL", "quantity": 2.0039211, "value_eur": 136.76},
    {"symbol": "ripple", "name": "XRP", "quantity": 276.85936581, "value_eur": 269.02},
    {"symbol": "binancecoin", "name": "BNB", "quantity": 0.17903486, "value_eur": 91.00},
    {"symbol": "sui", "name": "SUI", "quantity": 51.96174103, "value_eur": 33.73},
    {"symbol": "stellar", "name": "XLM", "quantity": 197.60613615, "value_eur": 33.04},
    {"symbol": "cardano", "name": "ADA", "quantity": 206.73380095, "value_eur": 30.70},
    {"symbol": "chainlink", "name": "LINK", "quantity": 3.40208837, "value_eur": 23.86}
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
    "name": "Aydaruus Ahmed Wehliye",
    "amount_eur": 450,
    "day": 10,
    "holdings": 39,
    "next_trade": "2026-08-10",
    "total_value": 27562.45,
    "profit": 2946.77,
    "profit_percent": 11.97
}

# Perheen holdings – isä mukana
FAMILY_HOLDINGS = [
    {"name": "👨 Aydaruus Ahmed Wehliye (Isä)", "holdings": 39, "value": 27562.45, "profit": 2946.77, "profit_percent": 11.97},
    {"name": "Ismahaan Aydaurus", "holdings": 20, "value": 1184.98, "profit": 178.95, "profit_percent": 17.79},
    {"name": "Ilyaas Aydaurus", "holdings": 26, "value": 1181.39, "profit": 182.23, "profit_percent": 18.25},
    {"name": "Farhia Aydaurus", "holdings": 18, "value": 1180.87, "profit": 179.94, "profit_percent": 17.98},
    {"name": "Mahamed Aydaurus", "holdings": 19, "value": 1177.03, "profit": 156.81, "profit_percent": 15.38},
    {"name": "Yahye Aydaurus", "holdings": 25, "value": 966.85, "profit": 128.01, "profit_percent": 15.27}
]

TOTAL_INVESTMENTS = 33253.64
TOTAL_CRYPTO = sum(c["value_eur"] for c in CRYPTO_HOLDINGS)

# =============================================
# 5. API-FUNKTIOIT (EUR)
# =============================================

def get_crypto_price(symbol):
    symbol_map = {
        "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
        "ripple": "XRP", "binancecoin": "BNB", "sui": "SUI",
        "stellar": "XLM", "cardano": "ADA", "chainlink": "LINK"
    }
    sym = symbol_map.get(symbol, symbol.upper())

    # Kraken
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

    # KuCoin
    try:
        url = f"https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={sym}-EUR"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("data") and "price" in data["data"]:
                return float(data["data"]["price"])
    except Exception as e:
        logging.warning(f"KuCoin error {symbol}: {e}")

    # CoinGecko
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
# 6. HISTORIALLISET HINNAT (30 päivää)
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
# 7. OSINGOT – LUE CSV:STÄ (TODELLISET MENNEET)
# =============================================

def get_dividend_details():
    """Lukee todelliset osingot dividends.csv-tiedostosta"""
    dividend_list = []
    total_yearly = 0.0
    
    try:
        df = pd.read_csv('dividends.csv')
        df = df[df['Action'] == 'Dividend (Dividend)']
        
        for _, row in df.iterrows():
            total = float(str(row['Total']).replace(',', '.'))
            total_yearly += total
            
            date_str = row['Time'].split(' ')[0]
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            date_formatted = date_obj.strftime('%d.%m.%Y')
            
            tax = float(str(row['Withholding tax']).replace(',', '.')) if row['Withholding tax'] else 0
            
            dividend_list.append({
                "date": date_formatted,
                "symbol": row['Ticker'],
                "name": row['Name'],
                "amount": total,
                "quantity": float(row['No. of shares']),
                "price": float(row['Price / share']),
                "tax": tax
            })
        
        dividend_list.sort(key=lambda x: x['date'], reverse=True)
        
    except Exception as e:
        logging.error(f"Virhe luettaessa CSV: {e}")
        return [], 0
    
    return dividend_list, round(total_yearly, 2)

# =============================================
# 8. TULEVAT OSINGOT
# =============================================

def get_upcoming_dividends():
    """Hakee tulevat osingot yfinance:stä"""
    upcoming = []
    total_upcoming = 0.0
    today = datetime.now().date()

    for stock in STOCK_HOLDINGS:
        try:
            ticker = yf.Ticker(stock["symbol"])
            info = ticker.info

            div_rate = info.get("dividendRate")
            ex_date_ts = info.get("exDividendDate")
            payout_ts = info.get("dividendDate")

            if ex_date_ts:
                ex_date = datetime.fromtimestamp(ex_date_ts).date()
                if ex_date >= today:
                    quarterly_div = (div_rate / 4) if div_rate else 0
                    if quarterly_div > 0:
                        amount = quarterly_div * stock["quantity"]
                    else:
                        div_hist = ticker.dividends
                        if not div_hist.empty:
                            last_div = div_hist.iloc[-1]
                            amount = last_div * stock["quantity"]
                            quarterly_div = last_div
                        else:
                            continue
                    
                    total_upcoming += amount
                    
                    if payout_ts:
                        payout_date = datetime.fromtimestamp(payout_ts).strftime('%d.%m.%Y')
                    else:
                        payout_est = ex_date + timedelta(days=30)
                        payout_date = payout_est.strftime('%d.%m.%Y') + " (arvio)"

                    upcoming.append({
                        "symbol": stock["symbol"],
                        "name": stock["name"],
                        "type": "Osake",
                        "amount": round(amount, 2),
                        "dividend_per_share": round(quarterly_div, 4),
                        "ex_date": ex_date.strftime('%d.%m.%Y'),
                        "payout_date": payout_date,
                        "quantity": stock["quantity"]
                    })
        except Exception as e:
            logging.error(f"Virhe {stock['symbol']}: {e}")

    # ETF:t (arvio)
    ETF_TICKER_MAP = {
        "iShares Core S&P 500": "SPY",
        "Vanguard S&P 500": "VOO",
        "iShares Core MSCI World": "URTH",
        "Vanguard FTSE All-World": "VWRA",
        "iShares NASDAQ 100": "QQQ",
        "SPDR S&P 500": "SPY5",
        "Vanguard S&P 500": "VUSA",
        "iShares Core S&P 500 Dist": "IUSA",
        "iShares NASDAQ 100": "EQQQ",
        "SPDR S&P 400 Mid Cap": "SPY4",
        "iShares Core MSCI Europe": "MEUD",
        "JPMorgan Nasdaq Premium": "JNQ",
        "JPMorgan US Equity Premium": "JUEQ",
        "JPMorgan Global Equity Premium": "JGEP",
        "Vanguard FTSE All-World High Div": "VHYL",
        "Global X Nasdaq 100 Covered Call": "QYLD",
        "VanEck Semiconductor": "SMH",
        "SPDR S&P US Dividend Aristocrats": "UDVD"
    }

    for etf in ETF_HOLDINGS:
        ticker = ETF_TICKER_MAP.get(etf["name"])
        if ticker:
            try:
                etf_ticker = yf.Ticker(ticker)
                div_hist = etf_ticker.dividends
                if not div_hist.empty:
                    last_div = div_hist.iloc[-1]
                    ex_date = div_hist.index[-1] + timedelta(days=30)
                    if ex_date.date() >= datetime.now().date():
                        amount = last_div * etf["quantity"]
                        total_upcoming += amount
                        upcoming.append({
                            "symbol": ticker,
                            "name": etf["name"],
                            "type": "ETF (arvio)",
                            "amount": round(amount, 2),
                            "dividend_per_share": round(last_div, 4),
                            "ex_date": ex_date.strftime('%d.%m.%Y') + " (arvio)",
                            "payout_date": (ex_date + timedelta(days=30)).strftime('%d.%m.%Y') + " (arvio)",
                            "quantity": etf["quantity"]
                        })
            except Exception as e:
                logging.error(f"Virhe ETF-osinkoa {etf['name']}: {e}")

    upcoming.sort(key=lambda x: x['payout_date'])
    return upcoming, round(total_upcoming, 2)

# =============================================
# 9. TAVOITE (100k)
# =============================================

def calculate_goal(current_value, monthly_savings, yearly_return_pct=0.07):
    target = 100000
    remaining = target - current_value
    if remaining <= 0:
        return 0, datetime.now()
    monthly_return = (1 + yearly_return_pct) ** (1/12) - 1
    months = 0
    value = current_value
    while value < target and months < 600:
        value = value * (1 + monthly_return) + monthly_savings
        months += 1
    return months, datetime.now() + timedelta(days=months*30)

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
    if btc: msg += f"₿ BTC: €{btc:,.0f}\n"
    else: msg += "₿ BTC: Laga ma helin\n"
    if eth: msg += f"⟠ ETH: €{eth:,.0f}\n"
    else: msg += "⟠ ETH: Laga ma helin\n"
    if sol: msg += f"◎ SOL: €{sol:,.0f}\n"
    else: msg += "◎ SOL: Laga ma helin\n"
    if xrp: msg += f"✕ XRP: €{xrp:,.0f}\n"
    else: msg += "✕ XRP: Laga ma helin\n"
    if bnb: msg += f"⬡ BNB: €{bnb:,.0f}\n"
    else: msg += "⬡ BNB: Laga ma helin\n"
    if sui: msg += f"🔷 SUI: €{sui:,.2f}\n"
    else: msg += "🔷 SUI: Laga ma helin\n"
    if xlm: msg += f"⭐ XLM: €{xlm:,.2f}\n"
    else: msg += "⭐ XLM: Laga ma helin\n"
    if ada: msg += f"🟣 ADA: €{ada:,.2f}\n"
    else: msg += "🟣 ADA: Laga ma helin\n"
    if link: msg += f"🔗 LINK: €{link:,.2f}\n"
    else: msg += "🔗 LINK: Laga ma helin\n"

    _, total_div = get_dividend_details()
    months, target_date = calculate_goal(TOTAL_INVESTMENTS, TRADING212_PLAN['amount_eur'] + DCA_PLAN['amount_eur'])
    msg += f"\n🎯 *100k € tavoite*\n"
    msg += f"📈 Puuttuu: €{100000 - TOTAL_INVESTMENTS:,.2f}\n"
    msg += f"📅 Arvio: {target_date.strftime('%d.%m.%Y')} ({months} kk)\n"
    msg += f"💵 Osingot/v (todelliset): €{total_div:,.2f}\n"

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
# 13. TELEGRAM KOMENNOT
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
        "/dividends - Menneet osingot (TODELLISET)\n"
        "/upcoming - Tulevat osingot\n"
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
        "/dividends - Menneet osingot (TODELLISET)\n"
        "/upcoming - Tulevat osingot\n"
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
    except Exception as e:
        await update.message.reply_text("⚠️ Kuma heli karo tirokoobka.")

async def check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    btc = get_btc_price()
    eth = get_eth_price()
    sol = get_sol_price()
    xrp = get_xrp_price()
    bnb = get_bnb_price()
    sui = get_sui_price()
    xlm = get_xlm_price()
    ada = get_ada_price()
    link = get_link_price()

    msg = "📊 *Warbixin degdeg ah*\n━━━━━━━━━━━━━━━━━\n\n"
    msg += "🪙 *Crypto qiimaha hadda:*\n"
    if btc: msg += f"₿ BTC: €{btc:,.0f}\n"
    else: msg += "₿ BTC: Laga ma helin\n"
    if eth: msg += f"⟠ ETH: €{eth:,.0f}\n"
    else: msg += "⟠ ETH: Laga ma helin\n"
    if sol: msg += f"◎ SOL: €{sol:,.0f}\n"
    else: msg += "◎ SOL: Laga ma helin\n"
    if xrp: msg += f"✕ XRP: €{xrp:,.0f}\n"
    else: msg += "✕ XRP: Laga ma helin\n"
    if bnb: msg += f"⬡ BNB: €{bnb:,.0f}\n"
    else: msg += "⬡ BNB: Laga ma helin\n"
    if sui: msg += f"🔷 SUI: €{sui:,.2f}\n"
    else: msg += "🔷 SUI: Laga ma helin\n"
    if xlm: msg += f"⭐ XLM: €{xlm:,.2f}\n"
    else: msg += "⭐ XLM: Laga ma helin\n"
    if ada: msg += f"🟣 ADA: €{ada:,.2f}\n"
    else: msg += "🟣 ADA: Laga ma helin\n"
    if link: msg += f"🔗 LINK: €{link:,.2f}\n"
    else: msg += "🔗 LINK: Laga ma helin\n"

    msg += f"\n💰 *Crypto holdings:* €{TOTAL_CRYPTO:,.2f}\n"
    msg += f"💵 *Wadarta guud:* €{TOTAL_INVESTMENTS:,.2f}"

    months, target_date = calculate_goal(TOTAL_INVESTMENTS, TRADING212_PLAN['amount_eur'] + DCA_PLAN['amount_eur'])
    msg += f"\n\n🎯 *100k €:* puuttuu €{100000 - TOTAL_INVESTMENTS:,.2f}, arvio {target_date.strftime('%d.%m.%Y')} ({months} kk)"

    await update.message.reply_text(msg, parse_mode="Markdown")

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📊 *Portfolio-gaaga*\n━━━━━━━━━━━━━━━━━\n\n"
    msg += f"💰 *Wadarta guud:* €{TOTAL_INVESTMENTS:,.2f}\n"
    msg += f"🪙 *Crypto holdings:* €{TOTAL_CRYPTO:,.2f}\n\n"
    msg += f"📈 *ETF holdings:* {len(ETF_HOLDINGS)} holdings\n"
    msg += f"📈 *Stock holdings:* {len(STOCK_HOLDINGS)} holdings\n"
    msg += f"🪙 *Crypto holdings:* {len(CRYPTO_HOLDINGS)} holdings\n\n"

    msg += f"📊 *Trading 212 -kuukausisijoitus*\n"
    msg += f"👤 *{TRADING212_PLAN['name']}*\n"
    msg += f"💰 €{TRADING212_PLAN['amount_eur']}/kk (10. päivä)\n"
    msg += f"📈 Dream: {TRADING212_PLAN['holdings']} holdingia\n"
    msg += f"💵 Arvo: €{TRADING212_PLAN['total_value']:,.2f}\n"
    msg += f"📈 Voitto: +{TRADING212_PLAN['profit_percent']:.2f}%\n\n"

    msg += "👨‍👩‍👧‍👦 *Perheen holdings*\n"
    for member in FAMILY_HOLDINGS:
        msg += f"• {member['name']}: {member['holdings']} hold. = €{member['value']:,.2f} (+{member['profit_percent']:.2f}%)\n"

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
        msg += f"{etf['name'][:25]}: {etf['quantity']:.2f} x €{etf['price']:,.2f} = €{value:,.2f}\n"
    msg += f"\n💰 *Wadarta ETF:* €{total:,.2f}"
    await update.message.reply_text(msg)

async def stocks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📈 *Stock Holdings*\n━━━━━━━━━━━━━━━━━\n\n"
    total = 0
    for stock in STOCK_HOLDINGS:
        value = stock["quantity"] * stock["price"]
        total += value
        msg += f"{stock['name'][:25]}: {stock['quantity']:.2f} x ${stock['price']:,.2f} = ${value:,.2f}\n"
    msg += f"\n💰 *Wadarta Stocks:* ${total:,.2f}"
    await update.message.reply_text(msg)

async def crypto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "🪙 *Crypto Holdings*\n━━━━━━━━━━━━━━━━━\n\n"
    total = 0
    for crypto in CRYPTO_HOLDINGS:
        total += crypto["value_eur"]
        msg += f"{crypto['name']}: {crypto['quantity']:.4f} = €{crypto['value_eur']:,.2f}\n"
    msg += f"\n💰 *Wadarta Crypto:* €{total:,.2f}"
    await update.message.reply_text(msg)

async def testapi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "🧪 *Tijaabo API (EUR)*\n\n"
    try:
        url = "https://api.kraken.com/0/public/Ticker?pair=BTCEUR"
        r = requests.get(url, timeout=10)
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
        url = "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=BTC-EUR"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("data") and "price" in data["data"]:
                msg += f"✅ KuCoin BTC/EUR: {float(data['data']['price']):,.0f} €\n"
        else:
            msg += f"❌ KuCoin: {r.status_code}\n"
    except Exception as e:
        msg += f"❌ KuCoin error: {e}\n"
    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=eur"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        r = requests.get(url, timeout=10, headers=headers)
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
    months, target_date = calculate_goal(TOTAL_INVESTMENTS, monthly_savings)
    remaining = 100000 - TOTAL_INVESTMENTS

    msg = "🎯 *Tavoite: 100 000 €*\n"
    msg += "━━━━━━━━━━━━━━━━━\n\n"
    msg += f"💰 Nykyinen: €{TOTAL_INVESTMENTS:,.2f}\n"
    msg += f"📈 Puuttuu: €{remaining:,.2f}\n"
    msg += f"📅 Arvioitu saavutus: {target_date.strftime('%d.%m.%Y')} ({months} kk)\n"
    msg += f"📊 Kuukausisäästö: €{monthly_savings:,.0f} (Trading212 + crypto DCA)"
    
    await update.message.reply_text(msg, parse_mode="Markdown")

# =============================================
# 14. MENNEET OSINGOT (CSV)
# =============================================
async def dividends(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        dividend_list, total_yearly = get_dividend_details()

        if not dividend_list:
            await update.message.reply_text("⚠️ Osinkotietoja ei löytynyt. Varmista, että dividends.csv on tallennettu.")
            return

        max_per_msg = 15
        total_items = len(dividend_list)
        sent_count = 0

        while sent_count < total_items:
            chunk = dividend_list[sent_count:sent_count + max_per_msg]
            sent_count += len(chunk)

            msg = "💰 *Menneet osinkomaksut (Trading 212)*\n"
            msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"

            for div in chunk:
                msg += f"📅 *{div['date']}*\n"
                msg += f"🔹 *{div['name']}* ({div['symbol']})\n"
                msg += f"   📦 {div['quantity']:.2f} × €{div['price']:.4f} = *€{div['amount']:,.2f}*\n"
                if div['tax'] > 0:
                    msg += f"   🏦 Lähdevero: €{div['tax']:.2f}\n"
                msg += "\n"

            if sent_count >= total_items:
                msg += f"📊 *Osinkoja yhteensä (12 kk):* €{total_yearly:,.2f}"
                msg += "\nℹ️ *Lähde:* Trading 212 -osinkohistoria"

            await update.message.reply_text(msg, parse_mode="Markdown")

    except Exception as e:
        logging.error(f"Virhe dividends-komennossa: {e}")
        await update.message.reply_text(f"⚠️ Virhe haettaessa osinkoja: {str(e)[:100]}")

# =============================================
# 15. TULEVAT OSINGOT
# =============================================
async def upcoming(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        upcoming_list, total_upcoming = get_upcoming_dividends()

        if not upcoming_list:
            await update.message.reply_text("⚠️ Tulevia osinkoja ei löytynyt tällä hetkellä.")
            return

        # Järjestä maksupäivän mukaan (aikaisin ensin)
        upcoming_list.sort(key=lambda x: datetime.strptime(x['payout_date'].split(' ')[0], '%d.%m.%Y'))

        # Ryhmittele kuukausittain
        monthly = {}
        yearly_total = 0
        for div in upcoming_list:
            payout_clean = div['payout_date'].split(' ')[0]
            month_key = payout_clean[3:5] + "/" + payout_clean[6:10]
            if month_key not in monthly:
                monthly[month_key] = 0
            monthly[month_key] += div['amount']
            yearly_total += div['amount']

        msg = "📅 *TULEVAT OSINGOT*\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"

        for div in upcoming_list:
            msg += f"🔹 *{div['name']}* ({div['symbol']})\n"
            msg += f"   💰 €{div['amount']:,.2f}\n"
            msg += f"   📅 Maksupäivä: {div['payout_date']}\n"
            msg += "\n"

        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "📊 *Kuukausittain:*\n"
        for month, total in sorted(monthly.items()):
            msg += f"   📅 {month}: €{total:,.2f}\n"

        msg += f"\n💰 *Tulevia osinkoja yhteensä:* €{yearly_total:,.2f}"
        msg += "\n📅 *Ajanjakso:* lähimmät 6 kuukautta"

        await update.message.reply_text(msg, parse_mode="Markdown")

    except Exception as e:
        logging.error(f"Virhe upcoming-komennossa: {e}")
        await update.message.reply_text(f"⚠️ Virhe haettaessa tulevia osinkoja: {str(e)[:100]}")

# =============================================
# 16. SUOSITUKSET (BUY/HOLD/SELL)
# =============================================
async def recommend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📊 *Sijoitusanalyysi & suositukset*\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "⚡ 30 päivän hinnanmuutokseen perustuen:\n\n"
    
    # --- Crypto ---
    msg += "🪙 *Kryptot*\n"
    crypto_prices = {
        "BTC": get_btc_price(),
        "ETH": get_eth_price(),
        "SOL": get_sol_price(),
        "XRP": get_xrp_price(),
        "BNB": get_bnb_price(),
        "SUI": get_sui_price(),
        "XLM": get_xlm_price(),
        "ADA": get_ada_price(),
        "LINK": get_link_price()
    }
    for name, current in crypto_prices.items():
        if current:
            old = get_crypto_historical(name.lower(), 30)
            rec, detail = get_recommendation(current, old, name)
            msg += f"{rec} *{name}*: €{current:,.0f} ({detail})\n"
        else:
            msg += f"❌ {name}: Ei hintaa\n"
    
    # --- ETF:t ---
    msg += "\n📈 *ETF:t*\n"
    ticker_map = {
        "iShares Core S&P 500": "SPY",
        "Vanguard S&P 500": "VOO",
        "iShares Core MSCI World": "URTH",
        "Vanguard FTSE All-World": "VWRA",
        "iShares NASDAQ 100": "QQQ",
        "SPDR S&P 500": "SPY5",
        "Vanguard S&P 500": "VUSA",
        "iShares Core S&P 500 Dist": "IUSA",
        "iShares NASDAQ 100": "EQQQ",
        "SPDR S&P 400 Mid Cap": "SPY4",
        "iShares Core MSCI Europe": "MEUD",
        "JPMorgan Nasdaq Premium": "JNQ",
        "JPMorgan US Equity Premium": "JUEQ",
        "JPMorgan Global Equity Premium": "JGEP",
        "Vanguard FTSE All-World High Div": "VHYL",
        "Global X Nasdaq 100 Covered Call": "QYLD",
        "VanEck Semiconductor": "SMH",
        "SPDR S&P US Dividend Aristocrats": "UDVD"
    }
    for etf in ETF_HOLDINGS:
        ticker = ticker_map.get(etf["name"])
        if ticker:
            current = get_etf_price(ticker)
            if current:
                old = get_stock_historical(ticker, 30)
                rec, detail = get_recommendation(current, old, etf["name"])
                msg += f"{rec} *{etf['name'][:20]}*: ${current:,.2f} ({detail})\n"
            else:
                msg += f"❌ {etf['name'][:20]}: Ei hintaa\n"
        else:
            msg += f"⚠️ {etf['name'][:20]}: Ei tickeriä\n"
    
    # --- Osakkeet ---
    msg += "\n📊 *Osakkeet*\n"
    for stock in STOCK_HOLDINGS:
        current = get_stock_price(stock["symbol"])
        if current:
            old = get_stock_historical(stock["symbol"], 30)
            rec, detail = get_recommendation(current, old, stock["name"])
            msg += f"{rec} *{stock['name'][:15]}*: ${current:,.2f} ({detail})\n"
        else:
            msg += f"❌ {stock['name'][:15]}: Ei hintaa\n"
    
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
    app.add_handler(CommandHandler("upcoming", upcoming))
    app.add_handler(CommandHandler("recommend", recommend))
    app.add_error_handler(error_handler)
    app.run_polling()

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    run_bot()
