import asyncio
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, CallbackContext, MessageHandler, filters
from pyspark.sql import SparkSession
import pytz
from telegram import InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CallbackQueryHandler
import mplfinance as mpf
import matplotlib.ticker as ticker

LOCAL_TIMEZONE = pytz.timezone("Europe/Kyiv")
UTC = pytz.utc

subscriptions = {}
scheduler = BackgroundScheduler()
MAIN_EVENT_LOOP = asyncio.get_event_loop()

spark = SparkSession.builder \
    .appName("Crypto Price Bot") \
    .getOrCreate()


def generate_scatter_plot(pdf: pd.DataFrame, symbol: str, interval: str) -> str:
    filtered_pdf = pdf[pdf['symbol'] == symbol]

    now = datetime.datetime.now(LOCAL_TIMEZONE)
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
    
    max_price = filtered_pdf['price'].max()
    min_price = filtered_pdf['price'].min()
    avg_price = filtered_pdf['price'].mean()

    plt.figure(figsize=(12, 6))
    plt.scatter(filtered_pdf['event_time'], filtered_pdf['price'], alpha=0.7, label=f"{symbol} Price")
    
    plt.axhline(y=max_price, color='green', linestyle='--', label=f"Max Price: {max_price}")
    plt.axhline(y=min_price, color='red', linestyle='--', label=f"Min Price: {min_price}")
    plt.axhline(y=avg_price, color='blue', linestyle='--', label=f"Avg Price: {avg_price:.2f}")

    max_time = filtered_pdf[filtered_pdf['price'] == max_price]['event_time'].iloc[0] if not filtered_pdf[filtered_pdf['price'] == max_price].empty else now
    min_time = filtered_pdf[filtered_pdf['price'] == min_price]['event_time'].iloc[0] if not filtered_pdf[filtered_pdf['price'] == min_price].empty else now

    plt.scatter(max_time, max_price, color='green', label="Max Point")
    plt.scatter(min_time, min_price, color='red', label="Min Point")

    plt.title(f"Price Change of {symbol} Over the Last {interval.capitalize()}")
    plt.xlabel("Event Time")
    plt.ylabel("Price")
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.gca().yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
    plt.legend()
    plt.tight_layout()

    plot_path = f"{symbol}_price_plot_{interval}.png"
    plt.savefig(plot_path)
    plt.close()
    return plot_path

def generate_candlestick_plot(pdf: pd.DataFrame, symbol: str, interval: str) -> str:
    filtered_pdf = pdf[pdf['symbol'] == symbol]

    now = datetime.datetime.now(LOCAL_TIMEZONE)
    if interval == "minute":
        start_time = now - datetime.timedelta(minutes=5)
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


def generate_pie_chart(pdf: pd.DataFrame, symbol: str, interval: str) -> str:
    now = datetime.datetime.now(LOCAL_TIMEZONE)
    if interval == "minute":
        start_time = now - datetime.timedelta(minutes=1)
    elif interval == "hourly":
        start_time = now - datetime.timedelta(hours=1)
    elif interval == "daily":
        start_time = now - datetime.timedelta(days=1)
    else:
        raise ValueError("Invalid interval. Use 'minute', 'hourly', or 'daily'.")

    filtered_pdf = pdf[pdf['event_time'] >= start_time]

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


def generate_plot(symbol: str, interval: str, plot_type: str) -> str:
    """
    Generate a plot (scatter, candlestick, line, or pie chart) for cryptocurrency price changes
    over a specific interval (last minute, hour, or day) from a Parquet dataset.
    """
    parquet_df = spark.read.parquet("data/part-*.parquet")

    pdf = parquet_df.toPandas()

    pdf['event_time'] = pd.to_datetime(pdf['event_time'], unit='ms', utc=True)
    pdf['event_time'] = pdf['event_time'].dt.tz_convert(LOCAL_TIMEZONE)
    pdf['quantity'] = pd.to_numeric(pdf['quantity'], errors='coerce')
    pdf['price'] = pd.to_numeric(pdf['price'], errors='coerce')
    pdf = pdf.dropna(subset=['quantity', 'price'])
    
    if plot_type == "Scatter":
        return generate_scatter_plot(pdf, symbol, interval)
    elif plot_type == "Candlestick":
        return generate_candlestick_plot(pdf, symbol, interval)
    elif plot_type == "Pie":
        return generate_pie_chart(pdf, symbol, interval)
    else:
        raise ValueError("Invalid plot type. Use 'Scatter', 'Candlestick', or 'Pie'.")


async def send_plot(chat_id: int, symbol: str, interval: str, chart_type: str):
    """
    Send a cryptocurrency price change plot to a user.
    """
    print(f"Sending plot to chat_id={chat_id} for symbol={symbol} with interval={interval}")
    plot_path = generate_plot(symbol, interval, chart_type)
    bot = Bot(token='')
    await bot.send_photo(chat_id=chat_id, photo=open(plot_path, 'rb'),
                         caption=f"{symbol} Price Change Plot ({interval.capitalize()})")


def schedule_send_plot(chat_id: int, symbol: str, interval: str, chart_type: str):
    """
    Wrapper to schedule the `send_plot` coroutine.
    """
    asyncio.run_coroutine_threadsafe(send_plot(chat_id, symbol, interval, chart_type), MAIN_EVENT_LOOP)


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
    Handle the user's chart selection.
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

    print(f"User selected chart type: {chart_type}")
    context.user_data['chart_type'] = chart_type

    trigger_args = {"minutes": 1} if interval == "minute" else {"hours": 1} if interval == "hourly" else {"days": 1}
    scheduler.add_job(
        schedule_send_plot,
        'interval',
        **trigger_args,
        args=(chat_id, symbol, interval, chart_type),
        id=str(chat_id),
        replace_existing=True
    )

    await query.edit_message_text(f"Successfully subscribed to {symbol} price updates every {interval} with a {chart_type} chart!")


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
    TOKEN = ''

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("subscribe", subscribe))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe))
    application.add_handler(CallbackQueryHandler(chart_selection, pattern="^chart_"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_unknown))
    application.add_handler(MessageHandler(filters.ALL, log_update))

    scheduler.start()
    print("Scheduler started. Bot is running...")

    application.run_polling()

if __name__ == "__main__":
    main()
