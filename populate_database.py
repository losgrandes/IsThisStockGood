#!/usr/bin/env python3
from src.Database import SQLite
from IsThisStockGood.src.DataFetcher import fetchDataForTickerSymbol
import sys

def worthInserting(data):
  values = [
    data['roic'][0],
    data['eps'][0],
    data['equity'][0],
    data['roic'][1],
    data['roic'][2]
  ]
  if all([value is not None and value > 10 for value in values])\
  and data['margin_of_safety_price'] is not None\
  and data['current_price'] is not None\
  and data['margin_of_safety_price'] > data['current_price']:
    return True
  else:
    return False

if __name__ == "__main__":
  print("Connecting to database...")
  db = SQLite()
  print("Adding stocks...")
  table_name = "stocks"
  if len(sys.argv) == 2:
    ticker = sys.argv[1]
    data = fetchDataForTickerSymbol(ticker, None)
    db.insertDataIntoTableForTicker(table_name, ticker, data)
  else:
    stock_list = db.get_stocks_to_be_fetched()
    for ticker_exchange in stock_list:
      data = fetchDataForTickerSymbol(ticker_exchange[0], ticker_exchange[1])
      if worthInserting(data):
        print(f"Inserting {ticker_exchange[0]}")
        db.insertDataIntoTableForTicker(table_name, ticker_exchange[0], data)
      else:
        print(f"Not worth inserting {ticker_exchange[0]}")