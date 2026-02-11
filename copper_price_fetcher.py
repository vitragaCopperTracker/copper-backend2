#!/usr/bin/env python3
"""
Copper Price Fetcher - Process 2
Fetches real-time copper and metal prices from Yahoo Finance and stores in database
"""

import yfinance as yf
import logging
from datetime import datetime
from database_config import get_curser
from database_operations import insert_metal_price

def fetch_metal_price_from_yahoo(symbol, metal_name):
    """Fetch metal price from Yahoo Finance"""
    try:
        ticker = yf.Ticker(symbol)
        
        # Get current market data
        info = ticker.info
        
        # Get historical data for change calculation
        hist = ticker.history(period="2d")
        
        if hist.empty:
            logging.warning(f"No historical data available for {symbol}")
            return None
        
        # Extract current price and previous close
        current_price = hist['Close'].iloc[-1] if len(hist) > 0 else None
        previous_close = hist['Close'].iloc[-2] if len(hist) > 1 else hist['Close'].iloc[0]
        
        if not current_price or not previous_close:
            logging.warning(f"Invalid price data for {symbol}")
            return None
        
        # Calculate change and percentage
        price_change = current_price - previous_close
        price_change_percent = (price_change / previous_close) * 100 if previous_close != 0 else 0
        
        return {
            'metal_name': metal_name,
            'symbol': symbol,
            'price': round(float(current_price), 4),
            'price_change': round(float(price_change), 4),
            'price_change_percent': round(float(price_change_percent), 2),
            'currency': info.get('currency', 'USD'),
            'exchange': info.get('exchange', 'COMEX'),
            'last_updated': datetime.now()
        }
        
    except Exception as e:
        logging.error(f"Error fetching {metal_name} price from Yahoo Finance: {e}")
        return None

def fetch_all_metal_prices():
    """Fetch prices for all metals"""
    metals = [
        {'symbol': 'HG=F', 'name': 'Copper'},
        {'symbol': 'ALI=F', 'name': 'Aluminum'},  # Alternative: 'ALU=F'
        {'symbol': 'NI=F', 'name': 'Nickel'},
        {'symbol': 'ZN=F', 'name': 'Zinc'},
        {'symbol': 'PL=F', 'name': 'Platinum'},
    ]
    
    prices = []
    
    for metal in metals:
        logging.info(f"Fetching {metal['name']} price...")
        price_data = fetch_metal_price_from_yahoo(metal['symbol'], metal['name'])
        
        if price_data:
            prices.append(price_data)
            logging.info(f"✅ {metal['name']}: ${price_data['price']} ({price_data['price_change_percent']:+.2f}%)")
        else:
            logging.warning(f"❌ Failed to fetch {metal['name']} price")
            
            # Add fallback data for copper if it fails
            if metal['name'] == 'Copper':
                fallback_data = {
                    'metal_name': 'Copper',
                    'symbol': 'HG=F',
                    'price': 4.12,
                    'price_change': -0.08,
                    'price_change_percent': -1.91,
                    'currency': 'USD',
                    'exchange': 'COMEX',
                    'last_updated': datetime.now(),
                    'source': 'Fallback'
                }
                prices.append(fallback_data)
                logging.info("Added fallback copper price data")
    
    return prices

def main():
    """Main function to fetch and store metal prices"""
    logging.info("🚀 Starting Copper Price Fetcher (Process 2)")
    
    try:
        # Get database connection
        connection, cursor = get_curser()
        logging.info("✅ Connected to database")
        
        # Fetch all metal prices
        prices = fetch_all_metal_prices()
        
        if not prices:
            logging.error("❌ No price data fetched")
            return
        
        # Insert prices into database
        for price_data in prices:
            try:
                insert_metal_price(cursor, connection, price_data)
                logging.info(f"✅ Inserted {price_data['metal_name']} price into database")
            except Exception as e:
                logging.error(f"❌ Error inserting {price_data['metal_name']} price: {e}")
        
        logging.info(f"🎉 Successfully processed {len(prices)} metal prices")
        
    except Exception as e:
        logging.error(f"❌ Error in copper price fetcher: {e}")
        raise
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
        logging.info("✅ Database connection closed")

if __name__ == "__main__":
    main()