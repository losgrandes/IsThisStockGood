#!/usr/bin/env python3
import sys
import logging
import os
from IsThisStockGood.src.DataFetcher import fetchDataForTickerSymbol
logging.basicConfig(level=logging.DEBUG)

ticker = sys.argv[1]
exchange = None
if len(sys.argv) == 3:
    exchange = sys.argv[2]

r = fetchDataForTickerSymbol(ticker, exchange)
print(r)
