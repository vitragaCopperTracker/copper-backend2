#!/usr/bin/env python3
"""
Main Application - Copper Stock Data Collection Pipeline
Process-based execution: 
- process1 = most followed stocks fetcher
- process2 = copper price fetcher  
- process3 = CME copper spot price scraper

IMPORTANT: Process is updated BEFORE execution to prevent getting stuck on failures
"""

import logging
import sys
import time
from datetime import datetime

# Import our modules
from most_followed import get_most_followed_data
from copper_price_fetcher import main as run_copper_price_fetcher
from cme_selenium_scraper import main as run_cme_scraper
from database_config import get_curser
from database_operations import update_process_status

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)


def get_current_process():
    """Get current process from database"""
    try:
        connection, cursor = get_curser()
        cursor.execute("SELECT current_process FROM process_python2 LIMIT 1")
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        return result[0] if result else "process1"
    except:
        return "process1"

def main():
    """Main pipeline function - runs based on current process"""
    current_process = get_current_process()
    logging.info(f"🔍 DEBUG: Current process from DB: '{current_process}'")
    logging.info(f"🔍 DEBUG: Type: {type(current_process)}")
    logging.info(f"🔍 DEBUG: Length: {len(current_process)}")
    logging.info(f"🔍 DEBUG: Repr: {repr(current_process)}")
    
    # Strip any whitespace just in case
    current_process = current_process.strip()
    
    logging.info(f"🔍 DEBUG: After strip: '{current_process}'")
    logging.info(f"🔍 DEBUG: Equals 'process1': {current_process == 'process1'}")
    logging.info(f"🔍 DEBUG: Equals 'process2': {current_process == 'process2'}")
    logging.info(f"🔍 DEBUG: Equals 'process3': {current_process == 'process3'}")
    
    if current_process == "process1":
        # Most followed stocks fetcher
        logging.info("🚀 STARTING PROCESS 1: MOST FOLLOWED STOCKS FETCHER")
        
        # Update to next process FIRST (before execution)
        connection, cursor = get_curser()
        logging.info("✅ Updating to process2 BEFORE execution")
        update_process_status(cursor, connection, "process2")
        cursor.close()
        connection.close()
        
        try:
            get_most_followed_data()
            logging.info("✅ Process 1 completed successfully")
        except Exception as e:
            logging.error(f"❌ Error in process1: {e}")
            
    elif current_process == "process2":
        # Copper price fetcher
        logging.info("🚀 STARTING PROCESS 2: COPPER PRICE FETCHER")
        
        # Update to next process FIRST (before execution)
        connection, cursor = get_curser()
        logging.info("✅ Updating to process3 BEFORE execution")
        update_process_status(cursor, connection, "process3")
        cursor.close()
        connection.close()
        
        try:
            run_copper_price_fetcher()
            logging.info("✅ Process 2 completed successfully")
        except Exception as e:
            logging.error(f"❌ Error in process2: {e}")
            
    elif current_process == "process3":
        # CME copper spot price scraper
        logging.info("🚀 STARTING PROCESS 3: CME COPPER SPOT PRICE SCRAPER")
        
        # Update to next process FIRST (before execution) - cycling back to process1
        connection, cursor = get_curser()
        logging.info("✅ Updating to process1 BEFORE execution (cycling back)")
        update_process_status(cursor, connection, "process1")
        cursor.close()
        connection.close()
        
        try:
            logging.info("🌐 About to run CME scraper...")
            run_cme_scraper()
            logging.info("✅ Process 3 completed successfully")
        except Exception as e:
            logging.error(f"❌ Error in process3: {e}")
            raise
    
    else:
        # Default to process1 if unknown process
        logging.warning(f"❌ Unknown process: '{current_process}', defaulting to process1")
        connection, cursor = get_curser()
        update_process_status(cursor, connection, "process1")
        cursor.close()
        connection.close()
        
        # Run process1 as fallback
        try:
            get_most_followed_data()
            logging.info("✅ Fallback process 1 completed")
        except Exception as e:
            logging.error(f"❌ Error in fallback process1: {e}")

if __name__ == "__main__":
    main()