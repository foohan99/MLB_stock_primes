import requests
import mariadb
import logging
import configparser
from datetime import datetime
import pytz
import holidays
import json
from logging.handlers import RotatingFileHandler
import time

# Configure logging with RotatingFileHandler
log_file = 'my_log_file.log'
handler = RotatingFileHandler(
    log_file, maxBytes=10 * 1024 * 1024, backupCount=0  # 10MB max, overwrite the same file
)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set the logging level
logger.addHandler(handler)

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read('/home/ken/MLB_nuft/config.ini')

if 'database' not in config:
    print("❌ [ERROR] 'database' section not found in config.ini")
    print("Sections found:", config.sections())
    raise KeyError("Missing 'database' section")

# Database configuration
DB_CONFIG = {
    'host': config['database']['host'],
    'user': config['database']['user'],
    'password': config['database']['password'],
    'database': config['database']['database']
}

# Retrieve the FMP API key
FMP_API_KEY = config['FMP']['FMP_key']
FMP_API_URL = "https://financialmodelingprep.com/api/v3/quote"

# List containing popular stock symbols
STOCK_SYMBOLS = [
    "AAPL", "GOOGL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "DLB"]

##, "JPM", "NFLX", "DIS",
##     "KO", "PEP", "CSCO", "DLB", "INTU"

API_CALL_COUNT_FILE = 'stock_api_call_count.json'


def initialize_api_call_count():
    """Initialize the API call count file if it doesn't exist."""
    try:
        with open(API_CALL_COUNT_FILE, 'r') as file:
            data = json.load(file)
            logger.debug(f"Loaded API call count data: {data}")
    except (FileNotFoundError, json.JSONDecodeError):
        # Initialize with default values
        data = {
            "daily": {
                "date": datetime.now().strftime('%Y-%m-%d'),
                "count": 0
            },
            "monthly": {
                "month": datetime.now().strftime('%Y-%m'),
                "count": 0
            }
        }
        with open(API_CALL_COUNT_FILE, 'w') as file:
            json.dump(data, file, indent=4)
        logger.info(f"Initialized API call count file with default values: {data}")
    return data


def update_api_call_count():
    """Update the daily and monthly API call counts."""
    data = initialize_api_call_count()
    now = datetime.now()

    # Update daily count
    if data["daily"]["date"] == now.strftime('%Y-%m-%d'):
        data["daily"]["count"] += 1
    else:
        # Reset daily count for a new day
        data["daily"]["date"] = now.strftime('%Y-%m-%d')
        data["daily"]["count"] = 1
        logger.info("Reset daily API call count for a new day.")

    # Update monthly count
    if data["monthly"]["month"] == now.strftime('%Y-%m'):
        data["monthly"]["count"] += 1
    else:
        # Reset monthly count for a new month
        data["monthly"]["month"] = now.strftime('%Y-%m')
        data["monthly"]["count"] = 1
        logger.info("Reset monthly API call count for a new month.")

    # Save the updated counts to the JSON file
    with open(API_CALL_COUNT_FILE, 'w') as file:
        json.dump(data, file, indent=4)
    logger.info(f"Updated API call count file: {data}")


def fetch_stock_data(symbols):
    """Fetch stock data for a list of symbols from the FMP API."""
    try:
        logger.info(f"Fetching stock data for symbols: {symbols}")
        stock_data = []

        for symbol in symbols:
            # Use the correct endpoint for each symbol
            url = f"{FMP_API_URL}/{symbol}"
            params = {"apikey": FMP_API_KEY}
            response = requests.get(url, params=params)
            response.raise_for_status()

            # Update API call count
            update_api_call_count()

            # Removed the debug log for the raw response
            data = response.json()

            if not data:
                logger.warning(f"No data returned for symbol: {symbol}")
            else:
                stock_data.extend(data)  # Append the data to the stock_data list

        if not stock_data:
            logger.error(f"No data returned for symbols: {symbols}")
            return None

        logger.info(f"Successfully fetched stock data for symbols: {symbols}")
        return stock_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching stock data: {e}")
        return None


def write_to_database(cursor, stock_data):
    """Write stock data to the database."""
    try:
        for stock in stock_data:
            # Parse and reformat the earningsAnnouncement field
            earnings_announcement = stock.get('earningsAnnouncement')
            if earnings_announcement:
                try:
                    # Convert ISO 8601 to 'YYYY-MM-DD HH:MM:SS'
                    earnings_announcement = datetime.strptime(
                        earnings_announcement.split('.')[0], "%Y-%m-%dT%H:%M:%S"
                    )
                except ValueError:
                    logger.warning(f"Invalid datetime format for earningsAnnouncement: {earnings_announcement}")
                    earnings_announcement = None
            else:
                earnings_announcement = None

            # Use the raw timestamp directly from the API response
            raw_timestamp = stock.get('timestamp')

            query = """
            INSERT INTO nuStockTracker (
                symbol, name, price, changes_percentage, change_value,
                day_low, day_high, year_high, year_low, market_cap,
                price_avg_50, price_avg_200, exchange, volume, avg_volume,
                open, previous_close, eps, pe, earnings_announcement,
                shares_outstanding, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                price = VALUES(price),
                changes_percentage = VALUES(changes_percentage),
                change_value = VALUES(change_value),
                day_low = VALUES(day_low),
                day_high = VALUES(day_high),
                year_high = VALUES(year_high),
                year_low = VALUES(year_low),
                market_cap = VALUES(market_cap),
                price_avg_50 = VALUES(price_avg_50),
                price_avg_200 = VALUES(price_avg_200),
                exchange = VALUES(exchange),
                volume = VALUES(volume),
                avg_volume = VALUES(avg_volume),
                open = VALUES(open),
                previous_close = VALUES(previous_close),
                eps = VALUES(eps),
                pe = VALUES(pe),
                earnings_announcement = VALUES(earnings_announcement),
                shares_outstanding = VALUES(shares_outstanding),
                timestamp = VALUES(timestamp)
            """
            data = (
                stock['symbol'],
                stock['name'],
                stock['price'],
                stock['changesPercentage'],
                stock['change'],
                stock['dayLow'],
                stock['dayHigh'],
                stock['yearHigh'],
                stock['yearLow'],
                stock['marketCap'],
                stock['priceAvg50'],
                stock['priceAvg200'],
                stock['exchange'],
                stock['volume'],
                stock['avgVolume'],
                stock['open'],
                stock['previousClose'],
                stock['eps'],
                stock['pe'],
                earnings_announcement,  # Reformatted datetime
                stock['sharesOutstanding'],
                raw_timestamp  # Raw timestamp from the API
            )
            cursor.execute(query, data)
        logger.info("Successfully wrote stock data to the database.")
    except mariadb.Error as e:
        logger.error(f"Error writing stock data to the database: {e}")


def is_market_open():
    """Check if the U.S. stock market is open and return a status message."""
    # Define U.S. Eastern Time and Pacific Time
    eastern = pytz.timezone('US/Eastern')
    pacific = pytz.timezone('US/Pacific')
    now_eastern = datetime.now(eastern)
    now_pacific = now_eastern.astimezone(pacific)

    # Market hours: 9:30 AM to 4:00 PM ET, Monday to Friday
    market_open_time = now_eastern.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = now_eastern.replace(hour=16, minute=0, second=0, microsecond=0)

    # Check if today is a weekday and within market hours
    if now_eastern.weekday() < 5 and market_open_time <= now_eastern <= market_close_time:
        return True, (
            f"[{now_pacific.strftime('%Y-%m-%d %H:%M:%S %Z')}] Stock Market is OPEN "
            f"(Eastern Time: {now_eastern.strftime('%Y-%m-%d %H:%M:%S %Z')})"
        )
    return False, (
        f"[{now_pacific.strftime('%Y-%m-%d %H:%M:%S %Z')}] Stock Market is CLOSED "
        f"(Eastern Time: {now_eastern.strftime('%Y-%m-%d %H:%M:%S %Z')})"
    )


def connect_to_database():
    """Connect to the database with retry logic."""
    config = configparser.ConfigParser()
    config.read('/home/ken/dashv6/config.ini')

    db_config = {
        "host": config["database"]["host"],
        "user": config["database"]["user"],
        "password": config["database"]["password"],
        "database": config["database"]["database"]
    }

    max_retries = 5
    retry_delay = 10

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempting to connect to the database (Attempt {attempt}/{max_retries})...")
            connection = mariadb.connect(**db_config)
            logger.info("✅ Successfully connected to the database.")
            return connection
        except mariadb.Error as e:
            logger.error(f"❌ [ERROR] Failed to connect to the database: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("❌ [ERROR] Maximum retry attempts reached. Exiting.")
                raise


def get_pacific_now():
    return datetime.now(pytz.timezone('US/Pacific'))


def run_nuft():
    """Run the main logic of the nuft program."""
    market_open, status_message = is_market_open()
    print(status_message)  # Display the market status in the terminal with a timestamp
    logger.info(status_message)  # Log the market status

    if not market_open:
        logger.info("Skipping API calls because the market is closed.")
        return

    try:
        # Connect to the database
        conn = connect_to_database()
        cursor = conn.cursor()

        # Fetch stock data
        stock_data = fetch_stock_data(STOCK_SYMBOLS)
        if stock_data:
            write_to_database(cursor, stock_data)

        conn.commit()
        logger.info("All stock data committed to the database.")
    except mariadb.Error as e:
        logger.error(f"Database connection error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    try:
        while True:
            logger.info("Starting nuft loop iteration.")
            run_nuft()
            logger.info("nuft loop iteration complete. Sleeping for 1200 seconds.\n")
            time.sleep(1800)
    except KeyboardInterrupt:
        logger.info("Graceful shutdown requested. Exiting nuft loop.")
        print("Graceful shutdown requested. Exiting nuft loop.")
