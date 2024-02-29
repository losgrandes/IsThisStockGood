#!/usr/bin/env python3
from src.Database import SQLite
from IsThisStockGood.src.DataFetcher import fetchDataForTickerSymbol
import sys

if __name__ == "__main__":
  print("Connecting to database...")
  db = SQLite()
  print("Adding stocks...")
  table_name = "stocks"
  if len(sys.argv) == 2:
    ticker = sys.argv[1]
    db.insertDataIntoTableForTicker(table_name, ticker, fetchDataForTickerSymbol(ticker,None))
  else:
    stock_list = db.get_stocks_to_be_fetched()
    for ticker_exchange in stock_list:
      db.insertDataIntoTableForTicker(table_name, ticker_exchange[0], fetchDataForTickerSymbol(ticker_exchange[0], ticker_exchange[1]))