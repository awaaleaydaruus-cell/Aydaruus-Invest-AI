import os
import logging
import sqlite3
import requests
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
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
# 3. TIETOKANTA (SQLite)
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
# 4. PORTFOLIO-gaaga (SAX AH)
# =============================================
TOTAL_PORTFOLIO_VALUE = 1585.20  # € (kuwaaga sax ah)

# Crypto holdings (kuwaaga sax ah)
CRYPTO_HOLDINGS = [
    {"symbol": "ethereum", "name": "ETH", "quantity": 0.36953452, "value_eur": 587.15},
    {"symbol": "bitcoin", "name": "BTC", "quantity": 0.00674376, "value_eur": 379.43},
    {"symbol": "ripple", "name": "XRP", "quantity": 276.85936581, "value_eur": 269.02},
    {"symbol": "solana", "name": "SOL", "quantity": 2.0039211, "value_eur": 136.76},
    {"symbol": "binancecoin", "name": "BNB", "quantity": 0.17903486, "value_eur": 91.00},
    {"symbol": "sui", "name": "SUI", "quantity": 51.96174103, "value_eur": 33.73},
    {"symbol": "stellar", "name": "XLM", "quantity": 197.60613615, "value_eur": 33.04},
    {"symbol": "cardano", "name": "ADA", "quantity": 206.73380095, "value_eur": 30.70},
    {"symbol": "chainlink", "name": "LINK", "quantity": 3.40208837, "value_eur": 23.86}
]

# DCA qorshaha (kuwaaga sax ah)
DCA_PLAN = {
    "name": "Aydaurus Dream",
    "amount_eur": 100,  # €100/bil
    "day": 10,  # 10-da bil
    "allocation": {
        "BTC": 20,
        "ETH": 20,
        "BNB": 20,
        "SOL": 20,
        "XRP": 20
    },
    "next_trade": "2026-08-10",
    "roi": -15.54  # %
}

# =============================================
# 5. API-FUNKTIOIT – QIIMAHA HEL
# =============================================

def get_crypto_price(symbol):
    try:
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies=eur"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if symbol in data and "eur" in data[symbol]:
                return data[symbol]["eur"]
            else:
                logging.warning(f"Symbol '{symbol}' not found: {data}")
        else:
            logging.warning(f"CoinGecko status {response.status_code} for {symbol}")
        return None
    except Exception as e:
        logging.error(f"Error fetching {symbol}: {e}")
        return None

def get_btc_price():
    return get_crypto_price("bitcoin")

def get_eth_price():
    return get_crypto_price("ethereum")

def get_sol_price():
    return get_crypto_price("solana")

def get_xrp_price():
    return get_crypto_price("ripple")

def get_bnb_price():
    return get_crypto_price("binancecoin")

def get_sui_price():
    return get_crypto_price("sui")

def get_xlm_price():
    return get_crypto_price("stellar")

def get_ada_price():
    return get_crypto_price("cardano")

def get_link_price():
    return get_crypto_price("chainlink")

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
# 6. KAYDI QIIMAHA
# =============================================
def save_price(symbol, price):
    try:
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("INSERT INTO prices (symbol, price) VALUES (?, ?)", (symbol, price))
        conn.commit()
        conn.close()
    except:
        pass

def get_last_price(symbol):
    try:
        conn = sqlite3.connect("users.db")
        c = conn.cursor()
        c.execute("SELECT price FROM prices WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1", (symbol,))
        result = c.fetchone()
        conn.close()
        return result[0] if result else None
    except:
        return None

# =============================================
# 7. WARBIXIN MAALINLE AH
# =============================================
async def send_daily_report():
    global USER_ID
    if USER_ID is None:
        logging.warning("USER_ID ma la dejin, warbixin lama diri karo")
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
    msg += "💰 *Portfolio-gaaga (Crypto)*\n"
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += f"💵 Wadarta: €{TOTAL_PORTFOLIO_VALUE:,.2f}\n\n"
    
    msg += "🪙 *Holdings-kaaga:*\n"
    if btc is not None:
        msg += f"₿ BTC: €{btc:,.0f}\n"
    else:
        msg += "₿ BTC: Laga ma helin\n"
    if eth is not None:
        msg += f"⟠ ETH: €{eth:,.0f}\n"
    else:
        msg += "⟠ ETH: Laga ma helin\n"
    if sol is not None:
        msg += f"◎ SOL: €{sol:,.0f}\n"
    else:
        msg += "◎ SOL: Laga ma helin\n"
    if xrp is not None:
        msg += f"✕ XRP: €{xrp:,.0f}\n"
    else:
        msg += "✕ XRP: Laga ma helin\n"
    if bnb is not None:
        msg += f"⬡ BNB: €{bnb:,.0f}\n"
    else:
        msg += "⬡ BNB: Laga ma helin\n"
    if sui is not None:
        msg += f"🔷 SUI: €{sui:,.2f}\n"
    else:
        msg += "🔷 SUI: Laga ma helin\n"
    if xlm is not None:
        msg += f"⭐ XLM: €{xlm:,.2f}\n"
    else:
        msg += "⭐ XLM: Laga ma helin\n"
    if ada is not None:
        msg += f"🟣 ADA: €{ada:,.2f}\n"
    else:
        msg += "🟣 ADA: Laga ma helin\n"
    if link is not None:
        msg += f"🔗 LINK: €{link:,.2f}\n"
    else:
        msg += "🔗 LINK: Laga ma helin\n"
    
    msg += f"\n📌 *DCA qorshaha:* {DCA_PLAN['name']}\n"
    msg += f"💰 €{DCA_PLAN['amount_eur']}/bil\n"
    msg += f"📅 10-da bil kasta\n"
    
    today = datetime.now()
    if today.day == 10:
        msg += f"\n🔔 *XASUUSIN! Maanta waa 10-da bil!*\n"
        msg += f"💵 Geli €{DCA_PLAN['amount_eur']}!\n"
        msg += f"📊 Qaybinta: BTC 20%, ETH 20%, BNB 20%, SOL 20%, XRP 20%"
    
    msg += f"\n\n📊 *Komenno:* /help"
    
    try:
        app = Application.builder().token(TOKEN).build()
        await app.bot.send_message(chat_id=USER_ID, text=msg, parse_mode="Markdown")
        logging.info("Warbixin maalinle ah waa la diray!")
    except Exception as e:
        logging.error(f"Warbixin maalinle ah waa ay fashilantay: {e}")

# =============================================
# 8. QORSHEYNTA (SCHEDULER)
# =============================================
scheduler = BackgroundScheduler()

def schedule_daily_report():
    scheduler.add_job(
        send_daily_report,
        'cron',
        hour=9,
        minute=0,
        id="daily_report",
        replace_existing=True
    )

scheduler.start()
schedule_daily_report()

# =============================================
# 9. TELEGRAM KOMENNOT
# =============================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global USER_ID
    user = update.effective_user
    USER_ID = user.id
    
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO users (id, username, first_name, last_seen)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, (user.id, user.username, user.first_name))
    conn.commit()
    conn.close()
    
    await update.message.reply_text(
        f"👋 *Hello, {user.first_name}!*\n\n"
        "📊 *Aydaruus Invest AI* waa diyaar!\n\n"
        "📌 *Komenno:*\n"
        "/help - Muuji dhammaan komenno\n"
        "/ping - Ping Pong testi\n"
        "/stats - Muuji tirokoobka isticmaalaha\n"
        "/check - Soo dir warbixin degdeg ah\n"
        "/portfolio - Muuji portfolio-gaaga\n\n"
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
        "/portfolio - Muuji portfolio-gaaga\n\n"
        "💰 *DCA:* €100/bil (10-da bil)\n"
        "🪙 *Crypto:* BTC, ETH, SOL, XRP, BNB, SUI, XLM, ADA, LINK\n"
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
    btc = get_btc_price()
    eth = get_eth_price()
    sol = get_sol_price()
    xrp = get_xrp_price()
    bnb = get_bnb_price()
    sui = get_sui_price()
    xlm = get_xlm_price()
    ada = get_ada_price()
    link = get_link_price()
    
    msg = "📊 *Warbixin degdeg ah*\n"
    msg += "━━━━━━━━━━━━━━━━━\n\n"
    msg += "🪙 *Crypto qiimaha hadda:*\n"
    
    if btc is not None:
        msg += f"₿ BTC: €{btc:,.0f}\n"
    else:
        msg += "₿ BTC: Laga ma helin\n"
    
    if eth is not None:
        msg += f"⟠ ETH: €{eth:,.0f}\n"
    else:
        msg += "⟠ ETH: Laga ma helin\n"
    
    if sol is not None:
        msg += f"◎ SOL: €{sol:,.0f}\n"
    else:
        msg += "◎ SOL: Laga ma helin\n"
    
    if xrp is not None:
        msg += f"✕ XRP: €{xrp:,.0f}\n"
    else:
        msg += "✕ XRP: Laga ma helin\n"
    
    if bnb is not None:
        msg += f"⬡ BNB: €{bnb:,.0f}\n"
    else:
        msg += "⬡ BNB: Laga ma helin\n"
    
    if sui is not None:
        msg += f"🔷 SUI: €{sui:,.2f}\n"
    else:
        msg += "🔷 SUI: Laga ma helin\n"
    
    if xlm is not None:
        msg += f"⭐ XLM: €{xlm:,.2f}\n"
    else:
        msg += "⭐ XLM: Laga ma helin\n"
    
    if ada is not None:
        msg += f"🟣 ADA: €{ada:,.2f}\n"
    else:
        msg += "🟣 ADA: Laga ma helin\n"
    
    if link is not None:
        msg += f"🔗 LINK: €{link:,.2f}\n"
    else:
        msg += "🔗 LINK: Laga ma helin\n"
    
    msg += f"\n💵 Portfolio wadarta: €{TOTAL_PORTFOLIO_VALUE:,.2f}"
    
    await update.message.reply_text(msg, parse_mode="Markdown")

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    btc = get_btc_price()
    eth = get_eth_price()
    sol = get_sol_price()
    xrp = get_xrp_price()
    bnb = get_bnb_price()
    sui = get_sui_price()
    xlm = get_xlm_price()
    ada = get_ada_price()
    link = get_link_price()
    
    msg = "📊 *Portfolio-gaaga*\n"
    msg += "━━━━━━━━━━━━━━━━━\n\n"
    msg += f"💰 *Wadarta:* €{TOTAL_PORTFOLIO_VALUE:,.2f}\n\n"
    
    msg += "🪙 *Crypto holdings-kaaga:*\n"
    if btc is not None:
        msg += f"₿ BTC: €{btc:,.0f}\n"
    else:
        msg += "₿ BTC: Laga ma helin\n"
    if eth is not None:
        msg += f"⟠ ETH: €{eth:,.0f}\n"
    else:
        msg += "⟠ ETH: Laga ma helin\n"
    if sol is not None:
        msg += f"◎ SOL: €{sol:,.0f}\n"
    else:
        msg += "◎ SOL: Laga ma helin\n"
    if xrp is not None:
        msg += f"✕ XRP: €{xrp:,.0f}\n"
    else:
        msg += "✕ XRP: Laga ma helin\n"
    if bnb is not None:
        msg += f"⬡ BNB: €{bnb:,.0f}\n"
    else:
        msg += "⬡ BNB: Laga ma helin\n"
    if sui is not None:
        msg += f"🔷 SUI: €{sui:,.2f}\n"
    else:
        msg += "🔷 SUI: Laga ma helin\n"
    if xlm is not None:
        msg += f"⭐ XLM: €{xlm:,.2f}\n"
    else:
        msg += "⭐ XLM: Laga ma helin\n"
    if ada is not None:
        msg += f"🟣 ADA: €{ada:,.2f}\n"
    else:
        msg += "🟣 ADA: Laga ma helin\n"
    if link is not None:
        msg += f"🔗 LINK: €{link:,.2f}\n"
    else:
        msg += "🔗 LINK: Laga ma helin\n"
    
    msg += "\n📌 *DCA qorshaha:*\n"
    msg += f"💰 €{DCA_PLAN['amount_eur']}/bil (10-da bil)\n"
    msg += "📊 Qaybinta: BTC 20%, ETH 20%, BNB 20%, SOL 20%, XRP 20%"
    
    await update.message.reply_text(msg, parse_mode="Markdown")

# =============================================
# 10. VIRHEIDENKÄSITTELY
# =============================================
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.error(f"Virhe: {context.error}")
    if update and update.effective_message:
        await update.effective_message.reply_text(
            "⚠️ Jokin meni pieleen. Yritä uudelleen."
        )

# =============================================
# 11. FLASK
# =============================================
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "🤖 Aydaruus Invest AI bot is running!", 200

def run_flask():
    flask_app.run(host='0.0.0.0', port=PORT, debug=False)

# =============================================
# 12. PÄÄFUNKTIO
# =============================================
def run_bot():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("check", check))
    app.add_handler(CommandHandler("portfolio", portfolio))
    app.add_error_handler(error_handler)
    app.run_polling()

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    run_bot()
