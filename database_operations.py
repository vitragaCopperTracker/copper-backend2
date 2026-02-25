import uuid
import logging
from datetime import datetime

def insert_most_followed_stock(cursor, connection, name, ticker, open_price, close_price, intraday_percentage, current_price, intraday_change, seven_day_change, seven_day_percentage, volume, country, stock_exchange, stock_type):
    """
    Inserts a single record into the most_followed_stocks table in PostgreSQL 
    after deleting any existing data for the given ticker and stock_type.
    
    Parameters:
    name (str): Name of the stock.
    ticker (str): Ticker symbol of the stock.
    open_price (float): Opening price of the stock.
    close_price (float): Closing price of the stock.
    intraday_percentage (float): Intraday percentage change of the stock.
    current_price (float): Current price of the stock.
    intraday_change (float): Intraday price change of the stock.
    seven_day_change (float): Change in price over the last 7 days.
    seven_day_percentage (float): Percentage change in price over the last 7 days.
    volume (float): Volume of the stock traded.
    country (str): Country of the stock.
    stock_exchange (str): Stock exchange where the stock is listed.
    stock_type (str): Type of stock.
    """
    try:
        # Convert all necessary values to standard Python types, if necessary
        open_price = float(open_price) if open_price is not None else None
        close_price = float(close_price) if close_price is not None else None
        intraday_percentage = float(intraday_percentage) if intraday_percentage is not None else None
        current_price = float(current_price) if current_price is not None else None
        intraday_change = float(intraday_change) if intraday_change is not None else None
        seven_day_change = float(seven_day_change) if seven_day_change is not None else None
        seven_day_percentage = float(seven_day_percentage) if seven_day_percentage is not None else None
        volume = float(volume) if volume is not None else None

        # First, delete any existing records for the given ticker and stock_type
        delete_query = """
        DELETE FROM api_app_mostfollowedstocks 
        WHERE ticker = %s AND stock_type = %s;
        """
        cursor.execute(delete_query, (ticker, stock_type))

        # SQL query to insert data into the most_followed_stocks table
        insert_query = """
        INSERT INTO api_app_mostfollowedstocks (
            id, name, ticker, open_price, close_price, intraday_percentage, 
            current_price, intraday_change, seven_day_change, seven_day_percentage, 
            volume, country, stock_exchange, stock_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Data to be inserted (parameters passed to the function)
        stock_data = (
            str(uuid.uuid4()),  # id (UUID)
            name,               # name
            ticker,             # ticker
            open_price,         # open_price
            close_price,        # close_price
            intraday_percentage,# intraday_percentage
            current_price,      # current_price
            intraday_change,    # intraday_change
            seven_day_change,   # seven_day_change
            seven_day_percentage,# seven_day_percentage
            volume,             # volume
            country,            # country
            stock_exchange,     # stock_exchange
            stock_type          # stock_type
        )

        # Execute the SQL query with the data
        cursor.execute(insert_query, stock_data)
        
        # Commit the transaction
        connection.commit()
        print(f"Data for {ticker} inserted successfully!")

    except Exception as e:
        # Rollback the transaction in case of an error
        connection.rollback()
        print(f"Error inserting data for {ticker}: {e}")

def update_process_status(cursor, connection, process_name):
    """Update the current process status"""
    cursor.execute("DELETE FROM process_python2")
    cursor.execute("INSERT INTO process_python2 (current_process) VALUES (%s)", (process_name,))
    connection.commit()

def insert_metal_price(cursor, connection, price_data):
    """Insert metal price data into the database"""
    try:
        # First, delete any existing records for the same metal
        delete_query = """
        DELETE FROM api_app_metalprices 
        WHERE metal_name = %s;
        """
        cursor.execute(delete_query, (price_data['metal_name'],))

        # SQL query to insert data into the metal_prices table
        insert_query = """
        INSERT INTO api_app_metalprices (
            id, metal_name, symbol, price, price_change, price_change_percent, 
            currency, exchange, last_updated, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Data to be inserted
        metal_data = (
            str(uuid.uuid4()),  # id (UUID)
            price_data['metal_name'],
            price_data['symbol'],
            price_data['price'],
            price_data['price_change'],
            price_data['price_change_percent'],
            price_data['currency'],
            price_data['exchange'],
            price_data['last_updated'],
            datetime.now(),  # created_at
            datetime.now()   # updated_at
        )

        # Execute the SQL query with the data
        cursor.execute(insert_query, metal_data)
        
        # Commit the transaction
        connection.commit()
        print(f"Metal price data for {price_data['metal_name']} inserted successfully!")

    except Exception as e:
        # Rollback the transaction in case of an error
        connection.rollback()
        print(f"Error inserting metal price data for {price_data['metal_name']}: {e}")
def insert_cme_copper_price(cursor, connection, copper_data):
    """Insert CME copper spot price data into the database"""
    try:
        # First, delete any existing records for the same globex code from today
        delete_query = """
        DELETE FROM api_app_cmecopperspot 
        WHERE globex_code = %s AND DATE(scraped_at) = CURRENT_DATE;
        """
        cursor.execute(delete_query, (copper_data['globex_code'],))

        # SQL query to insert data into the cme copper spot table
        insert_query = """
        INSERT INTO api_app_cmecopperspot (
            id, globex_code, last_price, price_change, price_change_percent, 
            volume, is_decrease, source, scraped_at, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Data to be inserted
        cme_data = (
            str(uuid.uuid4()),  # id (UUID)
            copper_data['globex_code'],
            copper_data['last_price'],
            copper_data['price_change'],
            copper_data['price_change_percent'],
            copper_data['volume'],
            copper_data['is_decrease'],
            copper_data['source'],
            copper_data['scraped_at'],
            datetime.now(),  # created_at
            datetime.now()   # updated_at
        )

        # Execute the SQL query with the data
        cursor.execute(insert_query, cme_data)
        
        # Commit the transaction
        connection.commit()
        print(f"CME copper data for {copper_data['globex_code']} inserted successfully!")

    except Exception as e:
        # Rollback the transaction in case of an error
        connection.rollback()
        print(f"Error inserting CME copper data for {copper_data['globex_code']}: {e}")
        raise

def insert_insider_transaction(cursor, connection, transaction_data):
    """Insert insider transaction data into the database"""
    try:
        # First, delete any existing records for the same ticker and transaction date
        delete_query = """
        DELETE FROM api_app_insidertransactions 
        WHERE ticker = %s AND transaction_date = %s AND insider_name = %s;
        """
        cursor.execute(delete_query, (
            transaction_data['ticker'],
            transaction_data['transaction_date'],
            transaction_data['insider_name']
        ))

        # SQL query to insert data into the insider transactions table
        insert_query = """
        INSERT INTO api_app_insidertransactions (
            id, transaction_date, ticker, company_name, insider_name, title, 
            trade_type, price, qty, owned, value, country, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Data to be inserted
        insider_data = (
            str(uuid.uuid4()),  # id (UUID)
            transaction_data['transaction_date'],
            transaction_data['ticker'],
            transaction_data['company_name'],
            transaction_data['insider_name'],
            transaction_data['title'],
            transaction_data['trade_type'],
            transaction_data['price'],
            transaction_data['qty'],
            transaction_data['owned'],
            transaction_data['value'],
            transaction_data['country'],
            datetime.now(),  # created_at
            datetime.now()   # updated_at
        )

        # Execute the SQL query with the data
        cursor.execute(insert_query, insider_data)
        
        # Commit the transaction
        connection.commit()

    except Exception as e:
        # Rollback the transaction in case of an error
        connection.rollback()
        logging.error(f"Error inserting insider transaction for {transaction_data['ticker']}: {e}")
        raise