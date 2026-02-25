#!/usr/bin/env python3
"""
Insider Transactions Fetcher - Process 4
Fetches insider transaction data for copper stocks from US and Canadian sources
"""

import requests
from bs4 import BeautifulSoup
import logging
import re
import csv
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from database_config import get_curser
from database_operations import insert_insider_transaction

def standardize_trade_type(trade_type, price=None):
    """Standardize trade type based on text and optionally price"""
    if not trade_type:
        return "Other"
    
    trade_type = trade_type.lower()
    
    # Check price if provided
    if price:
        price_value = re.findall(r'[-+]?[\d,]*\.?\d+', str(price))
        if price_value:
            try:
                price_num = float(price_value[0].replace(',', ''))
                if price_num < 0:
                    return "Sale"
                elif price_num > 0:
                    return "Purchase"
            except ValueError:
                pass
    
    # Standardize based on transaction type text
    if "10 - acquisition or disposition in the public market" in trade_type:
        return "Market"
    elif "11 - acquisition or disposition carried out privately" in trade_type:
        return "Private"
    elif "56 - grant of rights" in trade_type:
        return "Grant"
    elif "57 - exercise of rights" in trade_type:
        return "Exercise"
    elif "30 - acquisition or disposition under a purchase/ownership plan" in trade_type:
        return "Plan"
    elif "purchase" in trade_type:
        return "Purchase"
    elif "sale" in trade_type or "disposition" in trade_type:
        return "Sale"
    elif "acquisition" in trade_type:
        return "Purchase"
    else:
        return "Other"

def shorten_title(title):
    """Shorten insider title to standard format"""
    title = title.lower()
    
    if "director" in title or "4 - director of issuer" in title:
        return "Director"
    elif "ceo" in title or "chief executive officer" in title:
        return "CEO"
    elif "cfo" in title or "chief financial officer" in title:
        return "CFO"
    elif "coo" in title or "chief operating officer" in title:
        return "COO"
    elif "president" in title:
        return "Pres"
    elif "vice president" in title or "vp" in title:
        return "VP"
    elif "senior officer" in title or "5 - senior officer of issuer" in title:
        return "Officer"
    elif "10% security holder" in title or "3 - 10% security holder of issuer" in title:
        return "10% Owner"
    else:
        return "Other"

def get_us_insider_data(ticker, company_name):
    """Fetch insider transactions data for a US ticker from OpenInsider using requests"""
    url = f"http://openinsider.com/screener?s={ticker}&o=&pl=&ph=&ll=&lh=&fd=365&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&xs=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=1000&page=1"
    
    all_data = []
    
    try:
        logging.info(f"🌐 Fetching US insider data for {ticker}")
        
        # Add headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'tinytable'})
        
        if table:
            rows = table.find_all('tr')[1:]  # Skip the header row
            
            for row in rows[:5]:  # Limit to 5 most recent transactions
                cols = row.find_all('td')
                if len(cols) >= 16:
                    trade_type = cols[6].text.strip()
                    if "purchase" in trade_type.lower():
                        trade_type = "Purchase"
                    elif "sale" in trade_type.lower():
                        trade_type = "Sale"
                    
                    all_data.append({
                        'transaction_date': cols[2].text.strip(),
                        'ticker': ticker,
                        'company_name': company_name,
                        'insider_name': cols[4].text.strip(),
                        'title': shorten_title(cols[5].text.strip()),
                        'trade_type': trade_type,
                        'price': cols[7].text.strip(),
                        'qty': cols[8].text.strip(),
                        'owned': cols[9].text.strip(),
                        'value': cols[11].text.strip(),
                        'country': 'US'
                    })
            
            logging.info(f"✅ Found {len(all_data)} US transactions for {ticker}")
        else:
            logging.info(f"No insider data table found for {ticker} (may have no recent transactions)")
            
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error fetching US data for {ticker}: {e}")
    except Exception as e:
        logging.error(f"❌ Error processing US data for {ticker}: {e}")
    
    return all_data

def get_canadian_insider_data(ticker, company_name):
    """Fetch and parse insider transactions for a Canadian ticker from CEO.ca API"""
    url = "https://new-api.ceo.ca/api/sedi/filings"
    params = {
        "symbol": ticker,
        "amount": "",
        "transaction": "",
        "insider": ""
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        datas = response.json()
        
        if not datas:
            logging.info(f"No data returned for Canadian ticker {ticker}")
            return []
        
        all_data = []
        
        for data in datas[:5]:  # Limit to 5 most recent transactions
            datab = data.get("datab", {})
            insider_name = datab.get("Insider Name")
            issuer_name = datab.get("Issuer Name")
            filling_date = datab.get("Date of filing")
            transaction_date = datab.get("Date of transaction")
            
            # Use issuer name from API if available, otherwise use provided company_name
            if issuer_name:
                company_name = issuer_name
            
            # Convert date formats to YYYY-MM-DD
            def format_date(date_str):
                try:
                    return datetime.strptime(date_str[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    return None
            
            filling_date = format_date(filling_date)
            transaction_date = format_date(transaction_date)
            
            # Try multiple price fields
            price = datab.get("Conversion or exercise price", 0)
            if not price:
                price = datab.get("Unit price or exercise price", 0)
            
            # Try multiple title fields
            title = datab.get("Insider's Relationship to Issuer")
            if not title:
                title = datab.get("Ownership type")
            
            trade_type = datab.get("Nature of transaction")
            qty = datab.get("Number or value acquired or disposed of")
            
            # Try multiple owned fields
            owned = datab.get("Balance of securities held as of transaction date")
            if owned is None:
                owned = datab.get("Closing balance of equivalent number or value of underlying securities", 0)
            
            # Helper function to safely convert values
            def safe_float(value):
                try:
                    return float(str(value).replace(",", "").strip()) if value else 0.0
                except ValueError:
                    return 0.0
            
            def safe_int(value):
                try:
                    val_str = str(value).replace(",", "").strip() if value else "0"
                    # Remove leading + or - signs for parsing
                    if val_str.startswith("+"):
                        return int(val_str[1:])
                    elif val_str.startswith("-"):
                        return -int(val_str[1:])
                    return int(val_str)
                except ValueError:
                    return 0
            
            # Convert price, qty, and owned safely
            price = safe_float(price)
            qty = safe_int(qty)
            owned = safe_int(owned)
            
            # Calculate the value from price and quantity
            value = abs(float(price) * float(qty)) if price and qty else 0.0
            
            # Format the values for display
            price_str = f"${price:.2f}" if price else "$0.00"
            qty_str = f"{qty}" if qty else "0"
            value_str = f"${value:.2f}" if value else "$0.00"
            
            all_data.append({
                'transaction_date': transaction_date,
                'company_name': company_name,
                'ticker': ticker,
                'insider_name': insider_name,
                'title': shorten_title(title) if title else "Other",
                'trade_type': standardize_trade_type(trade_type, price_str) if trade_type else "Other",
                'price': price_str,
                'qty': qty_str,
                'owned': f"{owned:,}" if owned else "0",
                'value': value_str,
                'country': 'Canada'
            })
        
        logging.info(f"✅ Found {len(all_data)} Canadian transactions for {ticker}")
        return all_data
        
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error fetching Canadian data for {ticker}: {e}")
        return []
    except ValueError as e:
        logging.error(f"❌ JSON parsing error for {ticker}: {e}")
        return []
    except Exception as e:
        logging.error(f"❌ Error processing Canadian data for {ticker}: {e}")
        return []

def load_tickers_from_csv():
    """Load tickers from copper_stocks_complete.csv"""
    tickers_data = []
    csv_path = os.path.join(os.path.dirname(__file__), 'copper_stocks_complete.csv')
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                ticker = row.get('Ticker', '').strip()
                company_name = row.get('Company Name', '').strip()
                exchange = row.get('Stock Exchange', '').strip()
                domiciled = row.get('Domiciled', '').strip()
                
                if ticker and company_name:
                    # Determine country based on exchange and domicile
                    if exchange in ['TSX', 'TSX.V', 'TSXV'] or domiciled == 'Canada':
                        country = 'Canada'
                    else:
                        country = 'US'
                    
                    tickers_data.append({
                        'ticker': ticker,
                        'company_name': company_name,
                        'country': country
                    })
        
        logging.info(f"✅ Loaded {len(tickers_data)} tickers from CSV")
        return tickers_data
        
    except FileNotFoundError:
        logging.error(f"❌ CSV file not found: {csv_path}")
        return []
    except Exception as e:
        logging.error(f"❌ Error loading CSV: {e}")
        return []

def fetch_insider_data_for_ticker(ticker_data):
    """Fetch insider data for a single ticker"""
    ticker = ticker_data['ticker']
    company_name = ticker_data['company_name']
    country = ticker_data['country']
    
    try:
        if country == 'Canada':
            return get_canadian_insider_data(ticker, company_name)
        else:
            return get_us_insider_data(ticker, company_name)
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return []

def check_transaction_exists(cursor, ticker, transaction_date, insider_name):
    """Check if a transaction already exists in the database"""
    try:
        query = """
        SELECT COUNT(*) FROM api_app_insidertransactions 
        WHERE ticker = %s AND transaction_date = %s AND insider_name = %s
        """
        cursor.execute(query, (ticker, transaction_date, insider_name))
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        logging.error(f"❌ Error checking duplicate: {e}")
        return False

def main():
    """Main function to fetch and store insider transactions"""
    logging.info("🚀 Starting Insider Transactions Fetcher (Process 4)")
    
    connection = None
    cursor = None
    total_inserted = 0
    total_skipped = 0
    
    try:
        # Get database connection
        connection, cursor = get_curser()
        logging.info("✅ Connected to database")
        
        # Load tickers from CSV
        tickers_data = load_tickers_from_csv()
        
        if not tickers_data:
            logging.error("❌ No tickers loaded from CSV")
            return
        
        logging.info(f"📊 Processing {len(tickers_data)} tickers")
        
        # Separate US and Canadian tickers
        us_tickers = [t for t in tickers_data if t['country'] == 'US']
        canadian_tickers = [t for t in tickers_data if t['country'] == 'Canada']
        
        logging.info(f"📊 US tickers: {len(us_tickers)}, Canadian tickers: {len(canadian_tickers)}")
        
        # Process US tickers SEQUENTIALLY (Selenium can't handle parallel well in Docker)
        for ticker_data in us_tickers:
            try:
                transactions = get_us_insider_data(ticker_data['ticker'], ticker_data['company_name'])
                if transactions:
                    logging.info(f"✅ {ticker_data['ticker']}: Found {len(transactions)} transactions")
                    
                    # Insert immediately after fetching
                    inserted_for_ticker = 0
                    for transaction in transactions:
                        try:
                            # Check for duplicates
                            if check_transaction_exists(cursor, transaction['ticker'], 
                                                       transaction['transaction_date'], 
                                                       transaction['insider_name']):
                                total_skipped += 1
                                continue
                            
                            insert_insider_transaction(cursor, connection, transaction)
                            total_inserted += 1
                            inserted_for_ticker += 1
                        except Exception as e:
                            logging.error(f"❌ Error inserting transaction for {ticker_data['ticker']}: {e}")
                    
                    if inserted_for_ticker > 0:
                        logging.info(f"💾 {ticker_data['ticker']}: Inserted {inserted_for_ticker} new transactions")
            except Exception as e:
                logging.error(f"❌ Error processing {ticker_data['ticker']}: {e}")
        
        # Process Canadian tickers in parallel (API calls are fine)
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_ticker = {
                executor.submit(get_canadian_insider_data, ticker_data['ticker'], ticker_data['company_name']): ticker_data 
                for ticker_data in canadian_tickers
            }
            
            for future in as_completed(future_to_ticker):
                ticker_data = future_to_ticker[future]
                try:
                    transactions = future.result()
                    if transactions:
                        logging.info(f"✅ {ticker_data['ticker']}: Found {len(transactions)} transactions")
                        
                        # Insert immediately after fetching
                        inserted_for_ticker = 0
                        for transaction in transactions:
                            try:
                                # Check for duplicates
                                if check_transaction_exists(cursor, transaction['ticker'], 
                                                           transaction['transaction_date'], 
                                                           transaction['insider_name']):
                                    total_skipped += 1
                                    continue
                                
                                insert_insider_transaction(cursor, connection, transaction)
                                total_inserted += 1
                                inserted_for_ticker += 1
                            except Exception as e:
                                logging.error(f"❌ Error inserting transaction for {ticker_data['ticker']}: {e}")
                        
                        if inserted_for_ticker > 0:
                            logging.info(f"💾 {ticker_data['ticker']}: Inserted {inserted_for_ticker} new transactions")
                except Exception as e:
                    logging.error(f"❌ Error processing {ticker_data['ticker']}: {e}")
        
        logging.info(f"🎉 Process completed: {total_inserted} inserted, {total_skipped} skipped (duplicates)")
        
    except Exception as e:
        logging.error(f"❌ Error in insider transactions fetcher: {e}")
        raise
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        logging.info("✅ Database connection closed")

if __name__ == "__main__":
    main()
