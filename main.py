import os
import asyncio
import threading
from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", 10000))  # Render asettaa PORT-muuttujan[reference:8]

# --- Telegram-botti ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Hello, {update.effective_user.first_name}!")

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pong!")

def run_bot():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.run_polling()

# --- Pieni HTTP-palvelin Renderille ---
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "Bot is running!", 200

def run_flask():
    flask_app.run(host='0.0.0.0', port=PORT)

# --- Käynnistä molemmat ---
if __name__ == "__main__":
    # Käynnistä Flask omassa säikeessään
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Käynnistä botti
    run_bot()
