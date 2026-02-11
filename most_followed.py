import threading
import time
import yfinance as yf
from datetime import datetime, timedelta
import os
from insert_function import insert_most_followed_stock
from database_config import get_curser

server_config = os.getenv("server_config")

# Custom ticker mappings for Yahoo Finance
custom_mappings = {
    'FCX': 'FCX',
    'SCCO': 'SCCO',
    'BHP': 'BHP',
    'RIO': 'RIO',
    'ANTO': 'ANTO.L',  # Antofagasta on London Stock Exchange
    'TECK': 'TECK',
    'IVN': 'IVN.TO',  # Ivanhoe Mines
    'LUN': 'LUN.TO',  # Lundin Mining
    'FM': 'FM.TO',   # First Quantum Minerals
    'COPX': 'COPX',  # Global X Copper Miners ETF
    'CS': 'CS.TO',   # Capstone Copper
    'ERO': 'ERO.TO', # Ero Copper
    'HBM': 'HBM.TO', # Hudbay Minerals
    'TKO': 'TKO.TO', # Taseko Mines
    'ASCU': 'ASCU.TO', # Arizona Sonoran Copper
    'WRN': 'WRN.TO', # Western Copper and Gold
    'IE': 'IE.TO',   # Ivanhoe Electric
    'SLS': 'SLS.TO', # Solaris Resources
    'NGEX': 'NGEX.TO', # NGEx Minerals
    'FOM': 'FOM.TO', # Foran Mining
    'ZIJMF': 'ZIJMF', # Zijin Mining Group
    'JIXAY': 'JIXAY', # Jiangxi Copper
    'MMG': '1208.HK', # MMG Limited on Hong Kong Exchange
    'CMCL': '3993.HK', # CMOC Group on Hong Kong Exchange
    'MARI': 'MARI.TO', # Marimaca Copper
    'NCX': 'NCX.V',  # NorthIsle Copper and Gold
    'PERU': 'PERU.V', # Chakana Copper
    'WCU': 'WCU.V',  # World Copper
    'INFI': 'INFI.V', # Infinitum Copper
    'COPP': 'COPP.TO' # Horizons Copper Producers Index ETF
}

# Exchange suffix mappings for Yahoo Finance
exchange_mappings = {
    'TSXV': '.V',
    'TSX.V': '.V',
    'TSX': '.TO',
    'NYSE': '',
    'NYSE Arca': '',
    'LSE': '.L',
    'LONDON': '.L',
    'ASX': '.AX',
    'CNE': '.CN',
    'BRUSSELS': '.BR',
    'TOKYO': '.T',
    'HKEX': '.HK',
    'OTC': ''
}

# Column 1: Most Watched
most_watched = [
    {"Name": "Freeport-McMoRan", "Country": "United States", "Ticker": "FCX", "tv_ticker": "FCX", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Southern Copper Corporation", "Country": "United States", "Ticker": "SCCO", "tv_ticker": "SCCO", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "BHP Group", "Country": "Australia", "Ticker": "BHP", "tv_ticker": "BHP", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Rio Tinto", "Country": "United Kingdom", "Ticker": "RIO", "tv_ticker": "RIO", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Antofagasta", "Country": "United Kingdom", "Ticker": "ANTO", "tv_ticker": "ANTO", "Stock exchange": "LSE", "stock_exchange_tv": "LSE"},
    {"Name": "Teck Resources", "Country": "Canada", "Ticker": "TECK", "tv_ticker": "TECK", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Ivanhoe Mines", "Country": "Canada", "Ticker": "IVN", "tv_ticker": "IVN", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "Lundin Mining", "Country": "Sweden", "Ticker": "LUN", "tv_ticker": "LUN", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "First Quantum Minerals", "Country": "Canada", "Ticker": "FM", "tv_ticker": "FM", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "Global X Copper Miners ETF", "Country": "United States", "Ticker": "COPX", "tv_ticker": "COPX", "Stock exchange": "NYSE Arca", "stock_exchange_tv": "AMEX"}
]

# Column 2: North American Leaders
north_american_leaders = [
    {"Name": "Capstone Copper", "Country": "Canada", "Ticker": "CS", "tv_ticker": "CS", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "Ero Copper", "Country": "Canada", "Ticker": "ERO", "tv_ticker": "ERO", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "Hudbay Minerals", "Country": "Canada", "Ticker": "HBM", "tv_ticker": "HBM", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "Taseko Mines", "Country": "Canada", "Ticker": "TKO", "tv_ticker": "TKO", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "Arizona Sonoran Copper", "Country": "United States", "Ticker": "ASCU", "tv_ticker": "ASCU", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "Western Copper and Gold", "Country": "Canada", "Ticker": "WRN", "tv_ticker": "WRN", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "Ivanhoe Electric", "Country": "United States", "Ticker": "IE", "tv_ticker": "IE", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "Solaris Resources", "Country": "Ecuador", "Ticker": "SLS", "tv_ticker": "SLS", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "NGEx Minerals", "Country": "Canada", "Ticker": "NGEX", "tv_ticker": "NGEX", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "Foran Mining", "Country": "Canada", "Ticker": "FOM", "tv_ticker": "FOM", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"}
]

# Column 3: Global Market Leaders
global_market_leaders = [
    {"Name": "Zijin Mining Group", "Country": "China", "Ticker": "ZIJMF", "tv_ticker": "ZIJMF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "Jiangxi Copper", "Country": "China", "Ticker": "JIXAY", "tv_ticker": "JIXAY", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "MMG Limited", "Country": "China", "Ticker": "MMG", "tv_ticker": "MMG", "Stock exchange": "HKEX", "stock_exchange_tv": "HKEX"},
    {"Name": "CMOC Group", "Country": "China", "Ticker": "CMCL", "tv_ticker": "CMCL", "Stock exchange": "HKEX", "stock_exchange_tv": "HKEX"},
    {"Name": "Marimaca Copper", "Country": "Chile", "Ticker": "MARI", "tv_ticker": "MARI", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "NorthIsle Copper and Gold", "Country": "Canada", "Ticker": "NCX", "tv_ticker": "NCX", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "Chakana Copper", "Country": "Peru", "Ticker": "PERU", "tv_ticker": "PERU", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "World Copper", "Country": "Canada", "Ticker": "WCU", "tv_ticker": "WCU", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "Infinitum Copper", "Country": "Canada", "Ticker": "INFI", "tv_ticker": "INFI", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "Horizons Copper Producers Index ETF", "Country": "Canada", "Ticker": "COPP", "tv_ticker": "COPP", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"}
]

# Combine all stocks for processing
most_followed_stocks = most_watched + north_american_leaders + global_market_leaders


def get_yahoo_ticker(ticker, exchange):
    """
    Get the correct Yahoo Finance ticker using custom mappings and exchange suffixes.
    """
    # First check if there's a custom mapping
    if ticker in custom_mappings:
        return custom_mappings[ticker]
    
    # If no custom mapping, use exchange mapping
    base_ticker = ticker.split('.')[0]  # Remove any existing suffix
    
    # Map exchange to suffix
    suffix = exchange_mappings.get(exchange, '')
    
    return f"{base_ticker}{suffix}"


def process_stock_data(cursor, connection, stockdata):
    """
    Inserts or updates a single stock data into the database using the upsert function.
    """
    print("DOne 3")
    try:
        insert_most_followed_stock(
            cursor=cursor,
            connection=connection,
            name=stockdata.get("name"),
            ticker=stockdata.get("ticker"),
            open_price=stockdata.get("open_price"),
            close_price=stockdata.get("close_price"),
            intraday_percentage=stockdata.get("intraday_percentage"),
            current_price=stockdata.get("current_price"),
            intraday_change=stockdata.get("intraday_change"),
            seven_day_change=stockdata.get("seven_day_change"),
            seven_day_percentage=stockdata.get("seven_day_percentage"),
            volume=stockdata.get("volume"),
            country=stockdata.get("country"),
            stock_exchange=stockdata.get("stock_exchange"),
            stock_type=stockdata.get("stocks_type"),
        )
    except Exception as e:
        print(f"Error processing stock data for {stockdata.get('ticker')}: {e}")


def calculate_percentage_change(start, end):
    """Calculate percentage change between two values."""
    if start and end and start != 0:
        return ((end - start) / start) * 100
    return None

stock_data = []

def get_stock_data_from_yfinance(ticker):
    """Get stock data using yfinance library."""
    try:
        # Let yfinance handle the session - don't pass custom session
        data = yf.Ticker(ticker)
        
        # Get current market data
        current_info = data.info
        
        # Get historical data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)  # 7-day range
        hist = data.history(start=start_date, end=end_date)
        
        if hist.empty:
            print(f"No historical data available for {ticker}")
            return None
        
        # Extract data
        open_price = hist['Open'].iloc[-1] if 'Open' in hist.columns and len(hist['Open']) > 0 else None
        close_price = hist['Close'].iloc[-1] if 'Close' in hist.columns and len(hist['Close']) > 0 else None
        current_price = close_price  # Use close price as current price
        
        first_close = hist['Close'].iloc[0] if 'Close' in hist.columns and len(hist['Close']) > 0 else None
        volume = hist['Volume'].iloc[-1] if 'Volume' in hist.columns and len(hist['Volume']) > 0 else None
        
        # Calculate changes
        intraday_change = current_price - open_price if open_price and current_price else None
        intraday_percentage = calculate_percentage_change(open_price, current_price)
        seven_day_change = current_price - first_close if first_close and current_price else None
        seven_day_percentage = calculate_percentage_change(first_close, current_price)
        
        return {
            "price": current_price,
            "open_price": open_price,
            "close_price": close_price,
            "intraday_change": intraday_change,
            "intraday_percentage": intraday_percentage,
            "seven_day_change": seven_day_change,
            "seven_day_percentage": seven_day_percentage,
            "volume": volume
        }
        
    except Exception as e:
        print(f"Error getting data for {ticker}: {e}")
        return None


def process_stock_category(cursor, connection, stocks, category_name):
    global stock_data
    for stock in stocks:
        ticker = stock["Ticker"]
        exchange = stock["Stock exchange"]
        
        # Get the correct Yahoo Finance ticker
        yahoo_ticker = get_yahoo_ticker(ticker, exchange)
        
        try:
            # Get stock data from yfinance using the mapped ticker
            stock_info_data = get_stock_data_from_yfinance(yahoo_ticker)
            
            if stock_info_data:
                stock_info = {
                    "name": stock["Name"],
                    "ticker": stock["Ticker"],
                    "open_price": round(stock_info_data["open_price"], 2) if stock_info_data["open_price"] else None,
                    "close_price": stock_info_data["close_price"] if stock_info_data["close_price"] else None,
                    "current_price": round(stock_info_data["price"], 2) if stock_info_data["price"] else None,
                    "intraday_change": round(stock_info_data["intraday_change"], 2) if stock_info_data["intraday_change"] else None,
                    "intraday_percentage": round(stock_info_data["intraday_percentage"], 2) if stock_info_data["intraday_percentage"] else None,
                    "seven_day_change": round(stock_info_data["seven_day_change"], 2) if stock_info_data["seven_day_change"] else None,
                    "seven_day_percentage": round(stock_info_data["seven_day_percentage"], 2) if stock_info_data["seven_day_percentage"] else None,
                    "volume": stock_info_data["volume"],
                    "country": stock["Country"],
                    "stock_exchange": stock["Stock exchange"],
                    "stocks_type": category_name
                }

                process_stock_data(cursor, connection, stock_info)
                stock_data.append(stock_info)
            else:
                print(f"No data available for {yahoo_ticker} (original: {ticker})")
        except Exception as e:
            print(f"Error processing stock {stock['Name']} ({yahoo_ticker}): {str(e)}")

def get_most_followed_data():
    # Get database connection
    connection, cursor = get_curser()
    
    try:
        # Process all categories
        process_stock_category(cursor, connection, most_watched, "most_watched")
        process_stock_category(cursor, connection, north_american_leaders, "north_american_leaders")
        process_stock_category(cursor, connection, global_market_leaders, "global_market_leaders")

        print("Scraped Data:")
        print(stock_data)

        return stock_data
    
    finally:
        # Close database connection
        cursor.close()
        connection.close()
