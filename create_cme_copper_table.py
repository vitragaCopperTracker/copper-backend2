#!/usr/bin/env python3
"""
Create CME Copper Spot Price Table
"""

import logging
import sys
from database_config import get_curser

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def create_cme_copper_table():
    """Create the api_app_cmecopperspot table"""
    try:
        connection, cursor = get_curser()
        logging.info("✅ Connected to database")
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS api_app_cmecopperspot (
            id VARCHAR(255) PRIMARY KEY,
            globex_code VARCHAR(20) NOT NULL,
            last_price DECIMAL(10, 4) NOT NULL,
            price_change DECIMAL(10, 4),
            price_change_percent DECIMAL(10, 2),
            volume INTEGER DEFAULT 0,
            is_decrease BOOLEAN DEFAULT FALSE,
            source VARCHAR(50) DEFAULT 'CME Group',
            scraped_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        logging.info("✅ Created api_app_cmecopperspot table successfully")
        
        # Create index for better performance
        index_query = """
        CREATE INDEX IF NOT EXISTS idx_cmecopperspot_scraped_at 
        ON api_app_cmecopperspot (scraped_at DESC);
        """
        cursor.execute(index_query)
        connection.commit()
        logging.info("✅ Created index on scraped_at")
        
        # Create index on globex_code
        globex_index_query = """
        CREATE INDEX IF NOT EXISTS idx_cmecopperspot_globex_code 
        ON api_app_cmecopperspot (globex_code);
        """
        cursor.execute(globex_index_query)
        connection.commit()
        logging.info("✅ Created index on globex_code")
        
        cursor.close()
        connection.close()
        logging.info("✅ Database connection closed")
        
    except Exception as e:
        logging.error(f"❌ Error creating api_app_cmecopperspot table: {e}")
        raise

if __name__ == "__main__":
    create_cme_copper_table()