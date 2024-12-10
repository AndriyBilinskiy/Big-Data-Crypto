import asyncio
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, CallbackContext, MessageHandler, filters

subscriptions = {}
scheduler = BackgroundScheduler()
MAIN_EVENT_LOOP = asyncio.get_event_loop()


def fetch_crypto_data(symbol: str) -> pd.DataFrame:
    """
    Simulate fetching cryptocurrency data.
    Replace this function with actual API calls for real-time data.
    """
    times = pd.date_range(end=datetime.datetime.now(), periods=24, freq="h")
    prices = [50000 + i * 50 for i in range(24)]
    return pd.DataFrame({"event_time": times, "price": prices})


def generate_plot(symbol: str) -> str:
    """
    Generate a scatter plot for cryptocurrency price changes over time.
    """
    df = fetch_crypto_data(symbol)
    plt.figure(figsize=(12, 6))
    plt.scatter(df['event_time'], df['price'], alpha=0.7, label=f"{symbol} Price")
    plt.title(f"Price Change of {symbol} Over Time")
    plt.xlabel("Event Time")
    plt.ylabel("Price")
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plot_path = f"{symbol}_price_plot.png"
    plt.savefig(plot_path)
    plt.close()
    return plot_path


async def send_plot(chat_id: int, symbol: str):
    """
    Send a cryptocurrency price change plot to a user via Telegram.
    """
    print(f"Sending plot to chat_id={chat_id} for symbol={symbol}")
    plot_path = generate_plot(symbol)
    bot = Bot(token='API_TOKEN')
    await bot.send_photo(chat_id=chat_id, photo=open(plot_path, 'rb'), caption=f"{symbol} Price Change Plot")


def schedule_send_plot(chat_id: int, symbol: str):
    """
    Wrapper to schedule the `send_plot` coroutine using APScheduler.
    """
    asyncio.run_coroutine_threadsafe(send_plot(chat_id, symbol), MAIN_EVENT_LOOP)


async def start(update: Update, context: CallbackContext):
    """
    Handle the /start command.
    """
    if update.message:
        await update.message.reply_text("Welcome to the Crypto Price Bot!\nUse /subscribe to get started.")
    else:
        print("Received an update without a message.")


async def subscribe(update: Update, context: CallbackContext):
    """
    Handle the /subscribe command to add a user subscription.
    """
    chat_id = update.message.chat_id
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /subscribe <symbol> <interval>\nExample: /subscribe BTCUSDT minute")
        return

    symbol = context.args[0].upper()
    interval = context.args[1].lower()

    if interval not in ["minute", "hourly", "daily"]:
        await update.message.reply_text("Invalid interval. Use 'minute', 'hourly', or 'daily'.")
        return

    print(f"Subscribing chat_id={chat_id} to {symbol} updates every {interval}")
    subscriptions[chat_id] = {"symbol": symbol, "interval": interval}

    trigger_args = {"minutes": 1} if interval == "minute" else {"hours": 1} if interval == "hourly" else {"days": 1}
    scheduler.add_job(
        schedule_send_plot,
        'interval',
        **trigger_args,
        args=(chat_id, symbol),
        id=str(chat_id),
        replace_existing=True
    )

    await update.message.reply_text(f"Successfully subscribed to {symbol} price updates every {interval}!")


async def unsubscribe(update: Update, context: CallbackContext):
    """
    Handle the /unsubscribe command to remove a user subscription.
    """
    chat_id = update.message.chat_id
    if chat_id in subscriptions:
        del subscriptions[chat_id]
        scheduler.remove_job(str(chat_id))
        await update.message.reply_text("Unsubscribed from price updates.")
    else:
        await update.message.reply_text("You are not subscribed to any updates.")


async def handle_unknown(update: Update, context: CallbackContext):
    """
    Handle unknown or invalid commands.
    """
    await update.message.reply_text("Invalid command. Did you mean /subscribe?")


async def log_update(update: Update, context: CallbackContext):
    """
    Log incoming updates for debugging purposes.
    """
    print(f"Received update: {update}")


def main():
    """
    Main entry point to start the Telegram bot.
    """
    TOKEN = 'API_TOKEN'

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("subscribe", subscribe))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_unknown))
    application.add_handler(MessageHandler(filters.ALL, log_update))

    scheduler.start()
    print("Scheduler started. Bot is running...")

    application.run_polling()


if __name__ == "__main__":
    main()
