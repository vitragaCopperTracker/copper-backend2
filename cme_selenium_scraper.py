#!/usr/bin/env python3
"""
CME Group Copper Spot Price Scraper with Selenium
Scrapes real-time copper futures data from CME Group website using Selenium WebDriver
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging
import re
import time
from datetime import datetime
from database_config import get_curser
from database_operations import insert_cme_copper_price

def setup_chrome_driver():
    """Setup Chrome WebDriver with appropriate options"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    # For Docker environment
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        logging.error(f"❌ Failed to setup Chrome driver: {e}")
        return None

def scrape_cme_copper_price_selenium():
    """Scrape copper price data from CME Group website using Selenium"""
    url = "https://www.cmegroup.com/markets/metals/base/copper.quotes.html"
    driver = None
    
    try:
        logging.info("🚀 Setting up Chrome WebDriver...")
        driver = setup_chrome_driver()
        if not driver:
            return None
        
        logging.info(f"🌐 Navigating to CME Group copper page...")
        driver.get(url)
        
        # Wait for page to load with longer timeout
        wait = WebDriverWait(driver, 30)
        
        # Wait for the main content to load - try multiple strategies
        try:
            # Strategy 1: Wait for datarow
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "datarow")))
            logging.info("✅ Page loaded successfully - datarow found")
        except TimeoutException:
            try:
                # Strategy 2: Wait for any price element
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".last-value, .price, [class*='price']")))
                logging.info("✅ Page loaded successfully - price element found")
            except TimeoutException:
                logging.error("❌ Timeout waiting for page elements to load")
                return None
        
        # Add more wait time for JavaScript to load
        logging.info("⏳ Waiting for JavaScript to load...")
        time.sleep(5)
        
        # Try multiple strategies to find the data
        copper_data = None
        
        # Strategy 1: Look for datarow class with more specific selectors
        try:
            data_rows = driver.find_elements(By.CSS_SELECTOR, "div.datarow, .datarow, [class*='datarow']")
            if data_rows:
                logging.info(f"✅ Found {len(data_rows)} datarow elements")
                for row in data_rows:
                    copper_data = extract_data_from_row(row)
                    if copper_data:
                        logging.info("✅ Successfully extracted data from datarow")
                        break
            else:
                logging.info("🔍 No datarow elements found")
        except Exception as e:
            logging.error(f"❌ Error in datarow strategy: {e}")
        
        # Strategy 2: Look for specific price elements with broader selectors
        if not copper_data:
            try:
                # Try different combinations of selectors
                selectors_to_try = [
                    (".last-value", ".change-value", ".volume-value", ".globex"),
                    ("[class*='last']", "[class*='change']", "[class*='volume']", "[class*='globex']"),
                    (".price", ".change", ".vol", ".symbol"),
                    ("[data-field='last']", "[data-field='change']", "[data-field='volume']", "[data-field='symbol']")
                ]
                
                for last_sel, change_sel, vol_sel, globex_sel in selectors_to_try:
                    try:
                        last_value = driver.find_element(By.CSS_SELECTOR, last_sel)
                        change_value = driver.find_element(By.CSS_SELECTOR, change_sel)
                        volume_value = driver.find_element(By.CSS_SELECTOR, vol_sel)
                        globex_value = driver.find_element(By.CSS_SELECTOR, globex_sel)
                        
                        copper_data = extract_data_from_elements(last_value, change_value, volume_value, globex_value)
                        if copper_data:
                            logging.info(f"✅ Found data using selectors: {last_sel}, {change_sel}")
                            break
                    except NoSuchElementException:
                        continue
                        
                if copper_data:
                    logging.info("✅ Successfully found data using element selectors")
                        
            except Exception as e:
                logging.error(f"❌ Error in element strategy: {e}")
        
        # Strategy 3: Parse page text for price patterns (more sophisticated)
        if not copper_data:
            try:
                page_text = driver.page_source
                logging.info("🔍 Analyzing page source for copper price patterns...")
                
                # Look for copper-specific patterns in the HTML
                import re
                
                # Look for price patterns with context
                price_patterns = [
                    r'copper.*?(\d+\.\d{2,4})',  # "copper" followed by price
                    r'HG[A-Z]\d.*?(\d+\.\d{2,4})',  # Globex code followed by price
                    r'last.*?(\d+\.\d{2,4})',  # "last" followed by price
                    r'(\d+\.\d{4})',  # Any 4-decimal price
                ]
                
                for pattern in price_patterns:
                    matches = re.findall(pattern, page_text, re.IGNORECASE)
                    if matches:
                        for price_str in matches:
                            try:
                                price = float(price_str)
                                if 3.0 <= price <= 15.0:  # Reasonable copper price range
                                    copper_data = {
                                        'globex_code': 'HG_SCRAPED',
                                        'last_price': price,
                                        'price_change': 0.0,
                                        'price_change_percent': 0.0,
                                        'volume': 0,
                                        'is_decrease': False,
                                        'scraped_at': datetime.now(),
                                        'source': 'CME Group (Text Parse)'
                                    }
                                    logging.info(f"✅ Found price via text parsing: ${price}")
                                    break
                            except ValueError:
                                continue
                        if copper_data:
                            break
                            
            except Exception as e:
                logging.error(f"❌ Error in text parsing strategy: {e}")
        
        # Strategy 4: If no real data found, return None (no mock data)
        if not copper_data:
            logging.error("❌ Could not scrape real data from CME website")
            return None
        
        return copper_data
        
    except Exception as e:
        logging.error(f"❌ Error in Selenium scraper: {e}")
        # Return None instead of mock data
        return None
    
    finally:
        if driver:
            driver.quit()
            logging.info("✅ WebDriver closed")

def extract_data_from_row(data_row):
    """Extract copper data from a data row element"""
    try:
        # Extract Globex Code
        try:
            globex_element = data_row.find_element(By.CSS_SELECTOR, "span.globex")
            globex_code = globex_element.text.strip()
        except NoSuchElementException:
            globex_code = "HG_UNKNOWN"
        
        # Extract Last Price
        last_element = data_row.find_element(By.CSS_SELECTOR, "div.last-value")
        last_price = float(last_element.text.strip())
        
        # Extract Change and Percentage
        change_element = data_row.find_element(By.CSS_SELECTOR, "div.change-value")
        change_text = change_element.text.strip()
        
        # Parse change text like "-0.2220 (-3.65%)"
        change_match = re.match(r'([+-]?\d+\.?\d*)\s*\(([+-]?\d+\.?\d*)%\)', change_text)
        if change_match:
            price_change = float(change_match.group(1))
            price_change_percent = float(change_match.group(2))
        else:
            price_change = 0.0
            price_change_percent = 0.0
        
        # Extract Volume
        try:
            volume_element = data_row.find_element(By.CSS_SELECTOR, "div.volume-value")
            volume_text = volume_element.text.strip().replace(',', '')
            volume = int(volume_text) if volume_text.isdigit() else 0
        except (NoSuchElementException, ValueError):
            volume = 0
        
        # Determine if price is decreasing
        is_decrease = 'decrease' in change_element.get_attribute('class')
        
        return {
            'globex_code': globex_code,
            'last_price': last_price,
            'price_change': price_change,
            'price_change_percent': price_change_percent,
            'volume': volume,
            'is_decrease': is_decrease,
            'scraped_at': datetime.now(),
            'source': 'CME Group (Selenium)'
        }
        
    except Exception as e:
        logging.error(f"❌ Error extracting data from row: {e}")
        return None

def extract_data_from_elements(last_value, change_value, volume_value, globex_value):
    """Extract copper data from individual elements"""
    try:
        # Extract values
        last_price = float(last_value.text.strip())
        globex_code = globex_value.text.strip()
        
        # Parse change
        change_text = change_value.text.strip()
        change_match = re.match(r'([+-]?\d+\.?\d*)\s*\(([+-]?\d+\.?\d*)%\)', change_text)
        if change_match:
            price_change = float(change_match.group(1))
            price_change_percent = float(change_match.group(2))
        else:
            price_change = 0.0
            price_change_percent = 0.0
        
        # Parse volume
        volume_text = volume_value.text.strip().replace(',', '')
        volume = int(volume_text) if volume_text.isdigit() else 0
        
        # Check if decreasing
        is_decrease = 'decrease' in change_value.get_attribute('class')
        
        return {
            'globex_code': globex_code,
            'last_price': last_price,
            'price_change': price_change,
            'price_change_percent': price_change_percent,
            'volume': volume,
            'is_decrease': is_decrease,
            'scraped_at': datetime.now(),
            'source': 'CME Group (Selenium)'
        }
        
    except Exception as e:
        logging.error(f"❌ Error extracting data from elements: {e}")
        return None

def extract_data_from_text(page_text):
    """Extract copper data from page text using regex"""
    try:
        # Look for price patterns in the text
        price_pattern = r'(\d+\.\d{4})'
        prices = re.findall(price_pattern, page_text)
        
        if prices:
            # Use the first reasonable price found
            for price_str in prices:
                price = float(price_str)
                if 3.0 <= price <= 10.0:  # Reasonable copper price range
                    return {
                        'globex_code': 'HG_TEXT',
                        'last_price': price,
                        'price_change': 0.0,
                        'price_change_percent': 0.0,
                        'volume': 0,
                        'is_decrease': False,
                        'scraped_at': datetime.now(),
                        'source': 'CME Group (Text Parse)'
                    }
        
        return None
        
    except Exception as e:
        logging.error(f"❌ Error extracting data from text: {e}")
        return None

def main():
    """Main function to scrape and store CME copper data using Selenium"""
    logging.info("🚀 Starting CME Copper Selenium Scraper")
    
    try:
        # Get database connection
        connection, cursor = get_curser()
        logging.info("✅ Connected to database")
        
        # Scrape copper data
        copper_data = scrape_cme_copper_price_selenium()
        
        if not copper_data:
            logging.error("❌ No real copper data could be scraped from CME website")
            logging.error("❌ Skipping database insertion - only real data allowed")
            return
        
        # Insert data into database
        insert_cme_copper_price(cursor, connection, copper_data)
        logging.info("✅ CME copper data inserted into database")
        
        logging.info(f"🎉 CME Selenium scraping completed: ${copper_data['last_price']} ({copper_data['price_change']:+.4f}, {copper_data['price_change_percent']:+.2f}%)")
        
    except Exception as e:
        logging.error(f"❌ Error in CME Selenium scraper: {e}")
        raise
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
        logging.info("✅ Database connection closed")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    main()