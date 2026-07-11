import os
import logging
import sqlite3
import requests
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import threading
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
# 4. PORTFOLIO HOLDINGS (PDF-ka ku qoran)
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

# Crypto holdings (kuwaaga sax ah)
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

# DCA qorshaha
DCA_PLAN = {
    "name": "Aydaurus Dream",
    "amount_eur": 100,
    "day": 10,
    "allocation": {"BTC": 20, "ETH": 20, "BNB": 20, "SOL": 20, "XRP": 20},
    "next_trade": "2026-08-10"
}

TOTAL_INVESTMENTS = 33253.64  # € (PDF-ka ku qoran)
TOTAL_CRYPTO = 1585.20  # € (Crypto holdings)

# =============================================
# 5. API-FUNKTIOIT – QIIMAHA HEL
# =============================================

def get_crypto_price(symbol):
    """Hel qiimaha crypto-ga (EUR)"""
    try:
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies=eur"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if symbol in data and "eur" in data[symbol]:
                return data[symbol]["eur"]
        logging.warning(f"CoinGecko fashilantay {symbol}")
        return None
    except Exception as e:
        logging.error(f"Error fetching {symbol}: {e}")
        return None

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
# 6. WARBIXIN MAALINLE AH
# =============================================
async def send_daily_report():
    global USER_ID
    if USER_ID is None:
        return
    
    # Hel qiimaha crypto
    btc = get_crypto_price("bitcoin")
    eth = get_crypto_price("ethereum")
    sol = get_crypto_price("solana")
    xrp = get_crypto_price("ripple")
    bnb = get_crypto_price("binancecoin")
    sui = get_crypto_price("sui")
    xlm = get_crypto_price("stellar")
    ada = get_crypto_price("cardano")
    link = get_crypto_price("chainlink")
    
    msg = "📊 *Subax wanaagsan, Aydaruus!*\n\n"
    msg += "💰 *Portfolio-gaaga*\n"
    msg += "━━━━━━━━━━━━━━━━━\n"
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
    
    today = datetime.now()
    if today.day == 10:
        msg += f"\n🔔 *XASUUSIN! Maanta waa 10-da bil!*\n"
        msg += f"💵 Geli €{DCA_PLAN['amount_eur']}!\n"
        msg += "📊 Qaybinta: BTC 20%, ETH 20%, BNB 20%, SOL 20%, XRP 20%"
    else:
        msg += f"\n📌 Togga xiga: 10-{today.month+1 if today.month < 12 else 1}-{today.year if today.month < 12 else today.year+1}"
    
    try:
        app = Application.builder().token(TOKEN).build()
        await app.bot.send_message(chat_id=USER_ID, text=msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Warbixin maalinle ah waa ay fashilantay: {e}")

# =============================================
# 7. QORSHEYNTA
# =============================================
scheduler = BackgroundScheduler()
scheduler.add_job(send_daily_report, 'cron', hour=9, minute=0, id="daily_report", replace_existing=True)
scheduler.start()

# =============================================
# 8. TELEGRAM KOMENNOT
# =============================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global USER_ID
    user = update.effective_user
    USER_ID = user.id
    
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO users (id, username, first_name, last_seen) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
              (user.id, user.username, user.first_name))
    conn.commit()
    conn.close()
    
    await update.message.reply_text(
        f"👋 *Hello, {user.first_name}!*\n\n"
        "📊 *Aydaruus Invest AI* waa diyaar!\n\n"
        "📌 *Komenno:*\n"
        "/help - Muuji dhammaan komenno\n"
        "/check - Soo dir warbixin degdeg ah\n"
        "/portfolio - Muuji portfolio-gaaga\n"
        "/etfs - Muuji ETF holdings\n"
        "/stocks - Muuji stock holdings\n"
        "/crypto - Muuji crypto holdings\n\n"
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
        "/crypto - Muuji crypto holdings\n\n"
        "💰 *DCA:* €100/bil (10-da bil)\n"
        "📊 *Warbixin maalinle:* 9:00 subax",
        parse_mode="Markdown"
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users")
    count = c.fetchone()[0]
    conn.close()
    await update.message.reply_text(f"👥 Botti waxaa isticmaalay {count} qof.")

async def check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    btc = get_crypto_price("bitcoin")
    eth = get_crypto_price("ethereum")
    sol = get_crypto_price("solana")
    xrp = get_crypto_price("ripple")
    bnb = get_crypto_price("binancecoin")
    sui = get_crypto_price("sui")
    xlm = get_crypto_price("stellar")
    ada = get_crypto_price("cardano")
    link = get_crypto_price("chainlink")
    
    msg = "📊 *Warbixin degdeg ah*\n"
    msg += "━━━━━━━━━━━━━━━━━\n\n"
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
    
    await update.message.reply_text(msg, parse_mode="Markdown")

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📊 *Portfolio-gaaga*\n"
    msg += "━━━━━━━━━━━━━━━━━\n\n"
    msg += f"💰 *Wadarta guud:* €{TOTAL_INVESTMENTS:,.2f}\n"
    msg += f"🪙 *Crypto holdings:* €{TOTAL_CRYPTO:,.2f}\n\n"
    
    msg += f"📈 *ETF holdings:* {len(ETF_HOLDINGS)} holdings\n"
    msg += f"📈 *Stock holdings:* {len(STOCK_HOLDINGS)} holdings\n"
    msg += f"🪙 *Crypto holdings:* {len(CRYPTO_HOLDINGS)} holdings\n\n"
    
    msg += f"📌 *DCA qorshaha:* {DCA_PLAN['name']}\n"
    msg += f"💰 €{DCA_PLAN['amount_eur']}/bil (10-da bil)\n"
    msg += "📊 Qaybinta: BTC 20%, ETH 20%, BNB 20%, SOL 20%, XRP 20%"
    
    await update.message.reply_text(msg, parse_mode="Markdown")

async def etfs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📈 *ETF Holdings*\n"
    msg += "━━━━━━━━━━━━━━━━━\n\n"
    
    total = 0
    for etf in ETF_HOLDINGS:
        value = etf["quantity"] * etf["price"]
        total += value
        msg += f"{etf['name'][:25]}: {etf['quantity']:.2f} x €{etf['price']:,.2f} = €{value:,.2f}\n"
    
    msg += f"\n💰 *Wadarta ETF:* €{total:,.2f}"
    await update.message.reply_text(msg)

async def stocks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "📈 *Stock Holdings*\n"
    msg += "━━━━━━━━━━━━━━━━━\n\n"
    
    total = 0
    for stock in STOCK_HOLDINGS:
        value = stock["quantity"] * stock["price"]
        total += value
        msg += f"{stock['name'][:25]}: {stock['quantity']:.2f} x ${stock['price']:,.2f} = ${value:,.2f}\n"
    
    msg += f"\n💰 *Wadarta Stocks:* ${total:,.2f}"
    await update.message.reply_text(msg)

async def crypto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "🪙 *Crypto Holdings*\n"
    msg += "━━━━━━━━━━━━━━━━━\n\n"
    
    total = 0
    for crypto in CRYPTO_HOLDINGS:
        total += crypto["value_eur"]
        msg += f"{crypto['name']}: {crypto['quantity']:.4f} = €{crypto['value_eur']:,.2f}\n"
    
    msg += f"\n💰 *Wadarta Crypto:* €{total:,.2f}"
    await update.message.reply_text(msg)

# =============================================
# 9. VIRHEIDENKÄSITTELY
# =============================================
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.error(f"Virhe: {context.error}")
    if update and update.effective_message:
        await update.effective_message.reply_text("⚠️ Jokin meni pieleen. Yritä uudelleen.")

# =============================================
# 10. FLASK
# =============================================
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "🤖 Aydaruus Invest AI bot is running!", 200

def run_flask():
    flask_app.run(host='0.0.0.0', port=PORT, debug=False)

# =============================================
# 11. PÄÄFUNKTIO
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
    app.add_error_handler(error_handler)
    app.run_polling()

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    run_bot()
