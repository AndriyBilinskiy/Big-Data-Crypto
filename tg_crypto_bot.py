import asyncio
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, CallbackContext, MessageHandler, filters
from pyspark.sql import SparkSession
import pytz
import os
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN') if os.getenv('TELEGRAM_BOT_TOKEN') else 'API_TOKEN'

LOCAL_TIMEZONE = pytz.timezone("Europe/Kyiv")
UTC = pytz.utc

subscriptions = {}
scheduler = BackgroundScheduler()
MAIN_EVENT_LOOP = asyncio.get_event_loop()

spark = SparkSession.builder \
    .appName("Crypto Price Bot") \
    .getOrCreate()


def generate_plot(symbol: str, interval: str) -> str:
    """
    Generate a scatter plot for cryptocurrency price changes over a specific interval
    (last minute, hour, or day) from a Parquet dataset.
    """
    now = datetime.datetime.now(LOCAL_TIMEZONE)

    parquet_df = spark.read.parquet("data/part-*.parquet")

    pdf = parquet_df.toPandas()

    pdf['event_time'] = pd.to_datetime(pdf['event_time'], unit='ms', utc=True)
    pdf['event_time'] = pdf['event_time'].dt.tz_convert(LOCAL_TIMEZONE)
    pdf['quantity'] = pd.to_numeric(pdf['quantity'], errors='coerce')
    pdf['price'] = pd.to_numeric(pdf['price'], errors='coerce')
    pdf = pdf.dropna(subset=['quantity', 'price'])

    filtered_pdf = pdf[pdf['symbol'] == symbol]

    if interval == "minute":
        start_time = now - datetime.timedelta(minutes=1)
    elif interval == "hourly":
        start_time = now - datetime.timedelta(hours=1)
    elif interval == "daily":
        start_time = now - datetime.timedelta(days=1)
    else:
        raise ValueError("Invalid interval. Use 'minute', 'hourly', or 'daily'.")

    filtered_pdf = filtered_pdf[filtered_pdf['event_time'] >= start_time]

    if filtered_pdf.empty:
        plt.figure(figsize=(12, 6))
        plt.text(0.5, 0.5, f"No data available for {symbol} in the last {interval}.",
                 fontsize=12, ha='center', va='center')
        plt.title("No Data")
        plt.axis('off')
        plot_path = f"{symbol}_no_data_plot.png"
        plt.savefig(plot_path)
        plt.close()
        return plot_path

    plt.figure(figsize=(12, 6))
    plt.scatter(filtered_pdf['event_time'], filtered_pdf['price'], alpha=0.7, label=f"{symbol} Price")
    plt.title(f"Price Change of {symbol} Over the Last {interval.capitalize()}")
    plt.xlabel("Event Time")
    plt.ylabel("Price")
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()

    plot_path = f"{symbol}_price_plot_{interval}.png"
    plt.savefig(plot_path)
    plt.close()
    return plot_path


async def send_plot(chat_id: int, symbol: str, interval: str):
    """
    Send a cryptocurrency price change plot to a user.
    """
    plot_path = generate_plot(symbol, interval)
    bot = Bot(token=TOKEN)
    logging.info(f"Sending plot to chat_id={chat_id} for symbol={symbol} with interval={interval}")
    await bot.send_photo(chat_id=chat_id, photo=open(plot_path, 'rb'),
                         caption=f"{symbol} Price Change Plot ({interval.capitalize()})")


def schedule_send_plot(chat_id: int, symbol: str, interval: str):
    """
    Wrapper to schedule the `send_plot` coroutine.
    """
    asyncio.run_coroutine_threadsafe(send_plot(chat_id, symbol, interval), MAIN_EVENT_LOOP)


async def start(update: Update, context: CallbackContext):
    """
    Handle the /start command.
    """
    if update.message:
        await update.message.reply_text("Welcome to the Crypto Price Bot!\nUse /subscribe to get started.")
    else:
        logging.info("Received an update without a message.")


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

    logging.info(f"Subscribing chat_id={chat_id} to {symbol} updates every {interval}")

    subscriptions[chat_id] = {"symbol": symbol, "interval": interval}

    trigger_args = {"minutes": 1} if interval == "minute" else {"hours": 1} if interval == "hourly" else {"days": 1}
    scheduler.add_job(
        schedule_send_plot,
        'interval',
        **trigger_args,
        args=(chat_id, symbol, interval),
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
    logging.info(f"Received update: {update}")


def main():
    """
    Main entry point to start the Telegram bot.
    """
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("subscribe", subscribe))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_unknown))
    application.add_handler(MessageHandler(filters.ALL, log_update))

    scheduler.start()
    logging.info("Scheduler started. Bot is running...")

    application.run_polling()


if __name__ == "__main__":
    main()
