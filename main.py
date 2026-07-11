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
# 4. PORTFOLIO-gaaga
# =============================================
TOTAL_PORTFOLIO_VALUE = 33253.64  # €

ETF_LIST = [
    {"symbol": "SPY", "name": "iShares Core S&P 500"},
    {"symbol": "VOO", "name": "Vanguard S&P 500"},
    {"symbol": "URTH", "name": "iShares Core MSCI World"},
    {"symbol": "VWRA", "name": "Vanguard FTSE All-World"},
    {"symbol": "QQQ", "name": "iShares NASDAQ 100"},
    {"symbol": "SPY5", "name": "SPDR S&P 500"},
    {"symbol": "VUSA", "name": "Vanguard S&P 500"},
    {"symbol": "IUSA", "name": "iShares Core S&P 500 Dist"},
    {"symbol": "EQQQ", "name": "iShares NASDAQ 100"},
    {"symbol": "SPY4", "name": "SPDR S&P 400 Mid Cap"},
    {"symbol": "MEUD", "name": "iShares Core MSCI Europe"},
    {"symbol": "JNQ", "name": "JPMorgan Nasdaq Premium"},
    {"symbol": "JUEQ", "name": "JPMorgan US Equity Premium"},
    {"symbol": "JGEP", "name": "JPMorgan Global Equity Premium"},
    {"symbol": "VHYL", "name": "Vanguard FTSE All-World High Div"},
    {"symbol": "QYLD", "name": "Global X Nasdaq 100 Covered Call"},
    {"symbol": "SMH", "name": "VanEck Semiconductor"},
    {"symbol": "UDVD", "name": "SPDR S&P US Dividend Aristocrats"}
]

STOCK_LIST = [
    {"symbol": "TSLA", "name": "Tesla"},
    {"symbol": "AMZN", "name": "Amazon"},
    {"symbol": "MSFT", "name": "Microsoft"},
    {"symbol": "NVDA", "name": "NVIDIA"},
    {"symbol": "KO", "name": "Coca-Cola"},
    {"symbol": "CVX", "name": "Chevron"},
    {"symbol": "JPM", "name": "JPMorgan Chase"},
    {"symbol": "META", "name": "Meta"},
    {"symbol": "PLTR", "name": "Palantir"},
    {"symbol": "AAPL", "name": "Apple"},
    {"symbol": "PFE", "name": "Pfizer"},
    {"symbol": "PEP", "name": "PepsiCo"},
    {"symbol": "MSTR", "name": "Strategy"},
    {"symbol": "PG", "name": "Procter & Gamble"},
    {"symbol": "JNJ", "name": "Johnson & Johnson"},
    {"symbol": "AVGO", "name": "Broadcom"},
    {"symbol": "VZ", "name": "Verizon"},
    {"symbol": "XOM", "name": "ExxonMobil"},
    {"symbol": "AMD", "name": "AMD"},
    {"symbol": "BLK", "name": "BlackRock"},
    {"symbol": "V", "name": "Visa"},
    {"symbol": "MA", "name": "Mastercard"},
    {"symbol": "GOOGL", "name": "Alphabet"},
    {"symbol": "VICI", "name": "VICI Properties"},
    {"symbol": "ABBV", "name": "AbbVie"},
    {"symbol": "BAC", "name": "Bank of America"},
    {"symbol": "QCOM", "name": "Qualcomm"}
]

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
    
    msg = "📊 *Subax wanaagsan, Aydaruus!*\n\n"
    msg += "💰 *Portfolio-gaaga*\n"
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += f"💵 Wadarta: €{TOTAL_PORTFOLIO_VALUE:,.2f}\n\n"
    
    msg += "🪙 *Crypto (DCA €100/bil)*\n"
    msg += "━━━━━━━━━━━━━━━━━\n"
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
    
    today = datetime.now()
    if today.day == 10:
        msg += f"\n📌 *🔔 XASUUSIN DCA!*\n"
        msg += "━━━━━━━━━━━━━━━━━\n"
        msg += f"💰 Maanta waa 10-da bil!\n"
        msg += f"💵 Geli €450 (€350 ETF + €100 Crypto)!\n"
        next_month = today.month + 1 if today.month < 12 else 1
        next_year = today.year if today.month < 12 else today.year + 1
        msg += f"📅 Togga xiga: 10-{next_month:02d}-{next_year}"
    else:
        next_month = today.month + 1 if today.month < 12 else 1
        next_year = today.year if today.month < 12 else today.year + 1
        msg += f"\n📌 *Togga xiga DCA:* 10-{next_month:02d}-{next_year}"
    
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
        "💰 *DCA:* €450/bil (10-da bil)\n"
        "🪙 *Crypto:* BTC, ETH, SOL, XRP, BNB\n"
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
    
    msg += f"\n💵 Portfolio wadarta: €{TOTAL_PORTFOLIO_VALUE:,.2f}"
    
    await update.message.reply_text(msg, parse_mode="Markdown")

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    btc = get_btc_price()
    eth = get_eth_price()
    sol = get_sol_price()
    xrp = get_xrp_price()
    bnb = get_bnb_price()
    
    msg = "📊 *Portfolio-gaaga*\n"
    msg += "━━━━━━━━━━━━━━━━━\n\n"
    msg += f"💰 *Wadarta:* €{TOTAL_PORTFOLIO_VALUE:,.2f}\n\n"
    
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
    
    msg += "\n📌 *DCA:* 10-da bil kasta\n"
    msg += "💰 €450/bil (€350 ETF + €100 Crypto)"
    
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
