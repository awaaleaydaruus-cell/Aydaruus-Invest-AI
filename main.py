import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.environ["BOT_TOKEN"]

# --- Komennot ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Hello, {update.effective_user.first_name}!")

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pong!")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Käytettävissä olevat komennot:*\n\n"
        "/start - Tervehdysviesti\n"
        "/ping - Ping Pong -testi\n"
        "/help - Tämä ohje",
        parse_mode="Markdown"
    )

# --- Virheidenkäsittely ---
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.error(f"Virhe: {context.error}")
    if update and update.effective_message:
        await update.effective_message.reply_text("⚠️ Jokin meni pieleen. Yritä uudelleen.")

# --- Pääfunktio ---
def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("help", help_command))
    app.add_error_handler(error_handler)
    app.run_polling()

if __name__ == "__main__":
    main()
