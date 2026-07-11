import os
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Lue token ympäristömuuttujasta
TOKEN = os.environ["BOT_TOKEN"]

# Käsittelijä /start-komennolle
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Hello, {update.effective_user.first_name}!"
    )

# Käsittelijä /ping-komennolle (lisätty esimerkkinä)
async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pong!")

def main():
    # Luo sovellus tokenilla
    app = Application.builder().token(TOKEN).build()

    # Lisää handlerit
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))

    # Käynnistä pollaus
    app.run_polling()

if __name__ == "__main__":
    main()
