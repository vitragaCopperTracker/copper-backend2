# copper_python2

Copper price and most followed stocks data collection pipeline - scrapes CME copper spot prices and tracks most followed copper stocks.

## Features
- Process 1: CME copper spot price scraper (using Selenium)
- Process 2: Most followed stocks tracker
- Process 3: Copper price fetcher from multiple sources

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Configure `.env` with database credentials
3. Run `python set_process.py` to select process
4. Run `python app.py` to start scraping

## Database
Uses PostgreSQL on Railway.app to store copper prices and stock tracking data.

## Docker
Includes Dockerfile.selenium for running Selenium-based scrapers in containerized environment.
