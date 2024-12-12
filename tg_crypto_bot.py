import asyncio
import datetime
import random

import pandas as pd
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update, Bot, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, CallbackContext, MessageHandler, filters, CallbackQueryHandler
from pyspark.sql import SparkSession
import pytz
import os
import logging
import mplfinance as mpf
import matplotlib.ticker as ticker
from config import POSITIVE_COMMENTS, NEGATIVE_COMMENTS

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


def read_data_to_pdf(symbol: str, interval: str, chart_type: str):
    now = datetime.datetime.now(LOCAL_TIMEZONE)
    start_time = None

    if interval == "minute":
        if chart_type == "Candlestick":
            start_time = now - datetime.timedelta(minutes=5)
        else:
            start_time = now - datetime.timedelta(minutes=1)
    elif interval == "15_minutes":
        start_time = now - datetime.timedelta(minutes=15)
    elif interval == "hourly":
        start_time = now - datetime.timedelta(hours=1)
    elif interval == "daily":
        start_time = now - datetime.timedelta(days=1)
    else:
        raise ValueError("Invalid interval. Use 'minute', '15_minutes', 'hourly', or 'daily'.")

    start_time_utc = start_time.astimezone(UTC)

    parquet_df = spark.read.parquet("data/part-*.parquet").select("symbol", "event_time", "price", "quantity")

    if chart_type != "Pie":
        filtered_parquet_df = parquet_df.filter(
            (parquet_df['symbol'] == symbol) &
            (parquet_df['event_time'] >= start_time_utc.timestamp() * 1000)
        )
    else:
        filtered_parquet_df = parquet_df.filter(
            (parquet_df['event_time'] >= start_time_utc.timestamp() * 1000)
        )

    pdf = filtered_parquet_df.toPandas()

    pdf['event_time'] = pd.to_datetime(pdf['event_time'], unit='ms', utc=True)
    pdf['event_time'] = pdf['event_time'].dt.tz_convert(LOCAL_TIMEZONE)
    pdf['quantity'] = pd.to_numeric(pdf['quantity'], errors='coerce')
    pdf['price'] = pd.to_numeric(pdf['price'], errors='coerce')
    pdf = pdf.dropna(subset=['quantity', 'price'])

    return pdf


def generate_plot(symbol: str, interval: str, filtered_pdf, chart_type: str) -> str:
    """
    Generate various types of plots for cryptocurrency price changes.
    """

    if filtered_pdf.empty:
        plt.figure(figsize=(12, 6))
        plt.text(0.5, 0.5, f"No data available for {symbol} in the last {interval}.",
                 fontsize=12, ha='center', va='center')
        plt.title("No Data")
        plt.axis('off')
        plot_path = f"{symbol}_no_data_plot.png"
        plt.savefig(plot_path)
        logging.error(f"No data plot to {plot_path}")
        plt.close()
        return plot_path

    if chart_type == "Scatter":
        max_price = filtered_pdf['price'].max()
        min_price = filtered_pdf['price'].min()
        avg_price = filtered_pdf['price'].mean()

        now = datetime.datetime.now(LOCAL_TIMEZONE)

        plt.figure(figsize=(12, 6))
        plt.scatter(filtered_pdf['event_time'], filtered_pdf['price'], alpha=0.7, label=f"{symbol} Price")

        plt.axhline(y=max_price, color='green', linestyle='--', label=f"Max Price: {max_price}")
        plt.axhline(y=min_price, color='red', linestyle='--', label=f"Min Price: {min_price}")
        plt.axhline(y=avg_price, color='blue', linestyle='--', label=f"Avg Price: {avg_price:.2f}")

        max_time = filtered_pdf[filtered_pdf['price'] == max_price]['event_time'].iloc[0] if not filtered_pdf[
            filtered_pdf['price'] == max_price].empty else now
        min_time = filtered_pdf[filtered_pdf['price'] == min_price]['event_time'].iloc[0] if not filtered_pdf[
            filtered_pdf['price'] == min_price].empty else now

        plt.scatter(max_time, max_price, color='green', label="Max Point")
        plt.scatter(min_time, min_price, color='red', label="Min Point")

        plt.title(f"Price Change of {symbol} Over the Last {interval.capitalize()}")
        plt.xlabel("Event Time")
        plt.ylabel("Price")
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()

        plot_path = f"{symbol}_price_plot_{interval}.png"
        plt.savefig(plot_path)
        logging.info(f"Saved plot to {plot_path}")
        plt.close()
        return plot_path

    elif chart_type == "Candlestick":
        filtered_pdf['event_time'] = pd.to_datetime(filtered_pdf['event_time'])
        ohlc = filtered_pdf.groupby(filtered_pdf['event_time'].dt.floor('T')).agg({
            'price': ['first', 'max', 'min', 'last'],
            'quantity': 'sum'
        })
        ohlc.columns = ['open', 'high', 'low', 'close', 'volume']
        ohlc.reset_index(inplace=True)
        ohlc.set_index('event_time', inplace=True)

        plot_path = f"{symbol}_candlestick_plot_{interval}.png"
        mpf.plot(ohlc, type='candle', volume=True, title=f"{symbol} Candlestick Plot ({interval.capitalize()})",
                 style='yahoo', savefig=plot_path)

        return plot_path

    elif chart_type == "Pie":
        quantity_data = filtered_pdf.groupby('symbol').size()
        explode = [0.1 if sym == symbol else 0 for sym in quantity_data.index]

        plot_path = f"{symbol}_pie_chart.png"
        plt.figure(figsize=(8, 8))
        plt.pie(quantity_data, labels=quantity_data.index, autopct='%1.1f%%', startangle=90, explode=explode)
        plt.title(f"{symbol} Symbol Distribution")
        plt.axis('equal')
        plt.savefig(plot_path)
        plt.close()
        return plot_path


def generate_text_statistics(filtered_pdf):
    try:
        statistics = {}

        first_price = filtered_pdf['price'].iloc[0]
        last_price = filtered_pdf['price'].iloc[-1]
        logging.info(f"reached here")

        percent_change = ((last_price - first_price) / first_price) * 100
        statistics["Percent change"] = percent_change
        statistics["Maximum price"] = filtered_pdf['price'].max()
        statistics["Minimum price"] = filtered_pdf['price'].min()
        statistics["Average price"] = filtered_pdf['price'].mean()
        statistics["Price standard deviation"] = filtered_pdf['price'].std()
        statistics["Total volume"] = filtered_pdf['quantity'].sum()
        return statistics
    except Exception as e:
        logging.error(f"Error in generating text statistics {str(e)}")
        return {}


def format_dict_to_text(dictionary: dict) -> str:
    """
    Format a dictionary into a string for display.
    """
    if not dictionary:
        return '\nNo data available.'
    formatted_stats = (
        f"{random.choice(POSITIVE_COMMENTS) if dictionary['Percent change'] > 0 else random.choice(NEGATIVE_COMMENTS)}\n"
        f"Stats Overview:\n"
        f"🔻 Percent change: {dictionary['Percent change']:.2f}%\n"
        f"📈 Max Price: ${dictionary['Maximum price']:.2f}\n"
        f"📉 Min Price: ${dictionary['Minimum price']:.2f}\n"
        f"💹 Avg Price: ${dictionary['Average price']:.2f}\n"
        f"📊 Std Dev: ${dictionary['Price standard deviation']:.2f}\n"
        f"📈 Volume: {dictionary['Total volume']:.2f}"
    )
    return "\n"+formatted_stats


async def send_statistics(chat_id: int, symbol: str, interval: str, chart_type: str):
    """
    Send a cryptocurrency price change plot to a user.
    """

    logging.info(f"Extracting data for symbol={symbol} with interval={interval}")
    filtered_pdf = read_data_to_pdf(symbol, interval, chart_type)
    logging.info(f"Plotting data for symbol={symbol} with interval={interval}")
    plot_path = generate_plot(symbol, interval, filtered_pdf, chart_type)

    bot = Bot(token=TOKEN)
    if chart_type == "Scatter":
        logging.info(f"Generating text statistics for symbol={symbol} with interval={interval}")
        text_statistics = generate_text_statistics(filtered_pdf)
        logging.info(text_statistics)
        logging.info(f"Sending plot to chat_id={chat_id} for symbol={symbol} with interval={interval}")
        await bot.send_photo(chat_id=chat_id, photo=open(plot_path, 'rb'),
                             caption=f"{symbol} Price Change Plot ({interval.capitalize()})\nStatistics:\n{format_dict_to_text(text_statistics)}")
    elif chart_type == "Candlestick":
        logging.info(f"Generating text statistics for symbol={symbol} with interval={interval}")
        text_statistics = generate_text_statistics(filtered_pdf)
        logging.info(text_statistics)
        logging.info(f"Sending plot to chat_id={chat_id} for symbol={symbol} with interval={interval}")
        await bot.send_photo(chat_id=chat_id, photo=open(plot_path, 'rb'),
                             caption=f"{symbol} Price Change Plot (Candlestick) ({interval.capitalize()})\nStatistics:\n{format_dict_to_text(text_statistics)}")
    elif chart_type == "Pie":
        logging.info(f"Sending plot to chat_id={chat_id} for symbol={symbol} with interval={interval}")
        await bot.send_photo(chat_id=chat_id, photo=open(plot_path, 'rb'),
                             caption=f"{symbol} Crypto Distribution ({interval.capitalize()})")


def schedule_send_plot(chat_id: int, symbol: str, interval: str, chart_type: str):
    """
    Wrapper to schedule the `send_statistics` coroutine.
    """
    asyncio.run_coroutine_threadsafe(send_statistics(chat_id, symbol, interval, chart_type), MAIN_EVENT_LOOP)


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

    print(f"Subscribing chat_id={chat_id} to {symbol} updates every {interval}")
    context.user_data['symbol'] = symbol
    context.user_data['interval'] = interval

    keyboard = [
        [InlineKeyboardButton("Pie Chart", callback_data="chart_Pie")],
        [InlineKeyboardButton("Scatter plot", callback_data="chart_Scatter")],
        [InlineKeyboardButton("Candlestick Chart", callback_data="chart_Candlestick")],
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Choose the type of chart:", reply_markup=reply_markup)


async def chart_selection(update: Update, context: CallbackContext):
    """
    Handle the user's chart selection for /subscribe.
    """
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id
    chart_type = query.data.split("_")[1]

    symbol = context.user_data.get('symbol')
    interval = context.user_data.get('interval')

    if not symbol or not interval:
        await query.edit_message_text("Error: Missing subscription details. Please try again.")
        return

    context.user_data['chart_type'] = chart_type

    subscriptions[chat_id] = {"symbol": symbol, "interval": interval}

    trigger_args = {"minutes": 1} if interval == "minute" else {"hours": 1} if interval == "hourly" else {"days": 1}
    scheduler.add_job(
        schedule_send_plot,
        'interval',
        **trigger_args,
        args=(chat_id, symbol, interval, chart_type),
        id=str(chat_id),
        replace_existing=True
    )

    await query.edit_message_text(
        f"Successfully subscribed to {symbol} price updates every {interval} with a {chart_type} chart!")


async def unsubscribe(update: Update, context: CallbackContext):
    """
    Handle the /unsubscribe command to remove a user subscription.
    """
    chat_id = update.message.chat_id
    print(subscriptions)
    if chat_id in subscriptions:
        del subscriptions[chat_id]
        scheduler.remove_job(str(chat_id))
        await update.message.reply_text("Unsubscribed from price updates.")
    else:
        await update.message.reply_text("You are not subscribed to any updates.")


async def get_info(update: Update, context: CallbackContext):
    """
    Handle the /getinfo command to fetch and display data for a specific symbol and interval.
    Usage: /getinfo <symbol> <interval>
    """
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /getinfo <symbol> <interval>\nExample: /getinfo BTCUSDT minute")
        return

    symbol = context.args[0].upper()
    interval = context.args[1].lower()

    if interval not in ["minute", "15_minutes", "hourly", "daily"]:
        await update.message.reply_text("Invalid interval. Use 'minute', '15_minutes', 'hourly', or 'daily'.")
        return

    # Store symbol and interval in user_data for getinfo chart selection
    context.user_data['info_symbol'] = symbol
    context.user_data['info_interval'] = interval

    # Present chart options
    keyboard = [
        [InlineKeyboardButton("Pie Chart", callback_data="getinfo_chart_Pie")],
        [InlineKeyboardButton("Scatter plot", callback_data="getinfo_chart_Scatter")],
        [InlineKeyboardButton("Candlestick Chart", callback_data="getinfo_chart_Candlestick")],
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Choose the type of chart:", reply_markup=reply_markup)


async def getinfo_chart_selection(update: Update, context: CallbackContext):
    """
    Handle the user's chart selection for /getinfo.
    """
    query = update.callback_query
    await query.answer()

    data = query.data.split("_")  # e.g., ['getinfo', 'chart', 'Pie']
    if len(data) != 3:
        await query.edit_message_text("Error: Invalid chart selection.")
        return

    chart_type = data[2]  # Pie, Scatter, or Candlestick
    symbol = context.user_data.get('info_symbol')
    interval = context.user_data.get('info_interval')

    if not symbol or not interval:
        await query.edit_message_text("Error: Missing symbol or interval data. Please try /getinfo again.")
        return

    try:
        filtered_pdf = read_data_to_pdf(symbol, interval, chart_type)
        plot_path = generate_plot(symbol, interval, filtered_pdf, chart_type)
        bot = Bot(token=TOKEN)

        if chart_type == "Scatter":
            text_statistics = generate_text_statistics(filtered_pdf)
            await bot.send_photo(chat_id=query.message.chat_id, photo=open(plot_path, 'rb'),
                                 caption=f"{symbol} Price Change Plot ({interval.capitalize()})\nStatistics:\n{format_dict_to_text(text_statistics)}")
        elif chart_type == "Candlestick":
            text_statistics = generate_text_statistics(filtered_pdf)
            await bot.send_photo(chat_id=query.message.chat_id, photo=open(plot_path, 'rb'),
                                 caption=f"{symbol} Candlestick Plot ({interval.capitalize()})\nStatistics:\n{format_dict_to_text(text_statistics)}")
        elif chart_type == "Pie":
            await bot.send_photo(chat_id=query.message.chat_id, photo=open(plot_path, 'rb'),
                                 caption=f"{symbol} Crypto Distribution ({interval.capitalize()})")

        await query.edit_message_text("Here is your requested chart:")
    except Exception as e:
        logging.error(f"Error in processing getinfo chart selection: {e}")
        await query.edit_message_text("Failed to fetch and display data. Please try again later.")


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
    application.add_handler(CommandHandler("getinfo", get_info))
    application.add_handler(CallbackQueryHandler(chart_selection, pattern="^chart_"))
    application.add_handler(CallbackQueryHandler(getinfo_chart_selection, pattern="^getinfo_chart_"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_unknown))
    application.add_handler(MessageHandler(filters.ALL, log_update))

    scheduler.start()
    logging.info("Scheduler started. Bot is running...")

    application.run_polling()


if __name__ == "__main__":
    main()