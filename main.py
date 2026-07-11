import os
import logging
import sqlite3
from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import threading

# -------------------- 1. YMPÄRISTÖMUUTTUJAT --------------------
TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", 10000))

# -------------------- 2. TIETOKANTA --------------------
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

# -------------------- 3. TELEGRAM-KOMENNOT --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    # Tallenna käyttäjä
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO users (id, username, first_name, last_seen)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, (user.id, user.username, user.first_name))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"Hello, {user.first_name}!")

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pong!")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Käytettävissä olevat komennot:*\n\n"
        "/start - Tervehdys\n"
        "/ping - Ping Pong\n"
        "/help - Tämä ohje\n"
        "/stats - Näytä käyttäjämäärä",
        parse_mode="Markdown"
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users")
    count = c.fetchone()[0]
    conn.close()
    await update.message.reply_text(f"👥 Botti on tallentanut {count} käyttäjää.")

# -------------------- 4. VIRHEIDENKÄSITTELY --------------------
logging.basicConfig(level=logging.INFO)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.error(f"Virhe: {context.error}")
    if update and update.effective_message:
        await update.effective_message.reply_text("⚠️ Jokin meni pieleen. Yritä uudelleen.")

# -------------------- 5. FLASK (Renderin Web Serviceä varten) --------------------
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "Bot is running!", 200

def run_flask():
    flask_app.run(host='0.0.0.0', port=PORT)

# -------------------- 6. PÄÄFUNKTIO --------------------
def run_bot():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats))
    app.add_error_handler(error_handler)
    app.run_polling()

if __name__ == "__main__":
    # Käynnistä Flask omassa säikeessään (jotta botti ei odota)
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    # Käynnistä botti
    run_bot()
