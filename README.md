# copper_python2

Copper price and most followed stocks data collection pipeline - scrapes CME copper spot prices, tracks most followed copper stocks, and fetches insider transactions.

## Features
- Process 1: Most followed stocks tracker
- Process 2: Copper price fetcher from multiple sources
- Process 3: CME copper spot price scraper (using Selenium)
- Process 4: Insider transactions fetcher (US & Canadian stocks)

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Configure `.env` with database credentials
3. Create tables: 
   - `python create_cme_copper_table.py`
   - `python create_insider_transactions_table.py`
4. Run `python set_process.py` to select process
5. Run `python app.py` to start scraping

## Database
Uses PostgreSQL on Railway.app to store copper prices, stock tracking data, and insider transactions.

## Docker
Includes Dockerfile.selenium for running Selenium-based scrapers in containerized environment.

## Process Cycle
The application runs in a continuous cycle:
1. Process 1 → Process 2 → Process 3 → Process 4 → (back to Process 1)
2. Each process updates to the next before execution to prevent getting stuck on failures
