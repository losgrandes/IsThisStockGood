"""Tests for the DataFetcher.py functions."""


import os
import sys
import unittest

app_path = os.path.join(os.path.dirname(__file__), "..", 'src')
sys.path.append(app_path)

from src.YahooFinance import YahooFinanceQuoteSummary, YahooFinanceQuoteSummaryModule
from src.DataFetcher import DataFetcher
from src.StockRow import StockRowKeyStats

class DataFetcherTest(unittest.TestCase):

  def test_roic_should_return_1_3_from_yahoo_and_the_rest_from_stockrow(self):
    df = DataFetcher()
    df.stockrow_key_stats = StockRowKeyStats('DUMMY')
    df.stockrow_key_stats.roic_average_growth_rates = [11, 22, 33, 44]
    modules = [
        YahooFinanceQuoteSummaryModule.incomeStatementHistory,
        YahooFinanceQuoteSummaryModule.balanceSheetHistory
    ]
    df.yahoo_finance_quote_summary = YahooFinanceQuoteSummary('DUMMY', modules)
    df.yahoo_finance_quote_summary.module_data = {
      'incomeStatementHistory' : {
        'incomeStatementHistory' : [
          {
              'netIncome' : { 'raw' : 1 },
          },
          {
              'netIncome' : { 'raw' : 2 },
          },
          {
              'netIncome' : { 'raw' : 3 },
          }
        ]
      },
      'balanceSheetHistory' : {
          'balanceSheetStatements' : [
              {
                  'cash' : { 'raw' : 2},
                  'longTermDebt' : { 'raw' : 2},
                  'totalStockholderEquity' : { 'raw' : 10 }
              },
              {
                  'cash' : { 'raw' : 2},
                  'longTermDebt' : { 'raw' : 2},
                  'totalStockholderEquity' : { 'raw' : 10 }
              },
              {
                  'cash' : { 'raw' : 2},
                  'longTermDebt' : { 'raw' : 2},
                  'totalStockholderEquity' : { 'raw' : 10 }
              }
          ]
      }
    }
    df.yahoo_finance_quote_summary.parse_roic_growth_rates()
    df.sources = [df.stockrow_key_stats, df.yahoo_finance_quote_summary]
    roic_avgs = df.get_growth_rates('roic_average')
    self.assertEqual(roic_avgs, [10.0, 20.0, 33, 44])

  def test_roic_should_return_1_from_yahoo_and_the_rest_from_stockrow(self):
    df = DataFetcher()
    df.stockrow_key_stats = StockRowKeyStats('DUMMY')
    df.stockrow_key_stats.roic_average_growth_rates = [11, 22, 33, 44]
    modules = [
        YahooFinanceQuoteSummaryModule.incomeStatementHistory,
        YahooFinanceQuoteSummaryModule.balanceSheetHistory
    ]
    df.yahoo_finance_quote_summary = YahooFinanceQuoteSummary('DUMMY', modules)
    df.yahoo_finance_quote_summary.module_data = {
      'incomeStatementHistory' : {
        'incomeStatementHistory' : [
          {
              'netIncome' : { 'raw' : 1 },
          },
        ]
      },
      'balanceSheetHistory' : {
          'balanceSheetStatements' : [
              {
                  'cash' : { 'raw' : 2},
                  'longTermDebt' : { 'raw' : 2},
                  'totalStockholderEquity' : { 'raw' : 10 }
              },
          ]
      }
    }
    df.yahoo_finance_quote_summary.parse_roic_growth_rates()
    df.sources = [df.stockrow_key_stats, df.yahoo_finance_quote_summary]
    roic_avgs = df.get_growth_rates('roic_average')
    self.assertEqual(roic_avgs, [10.0, 22, 33, 44])

  def test_roic_should_return_all_from_stockrow_if_nothing_in_yahoo(self):
    df = DataFetcher()
    df.stockrow_key_stats = StockRowKeyStats('DUMMY')
    df.stockrow_key_stats.roic_average_growth_rates = [11, 22, 33, 44]
    df.sources = [df.stockrow_key_stats]
    roic_avgs = df.get_growth_rates('roic_average')
    self.assertEqual(roic_avgs, [11, 22, 33, 44])

  def test_roic_should_return_all_it_has_from_stockrow_if_nothing_in_yahoo(self):
    df = DataFetcher()
    df.stockrow_key_stats = StockRowKeyStats('DUMMY')
    df.stockrow_key_stats.roic_average_growth_rates = [11, 22]
    df.sources = [df.stockrow_key_stats]
    roic_avgs = df.get_growth_rates('roic_average')
    self.assertEqual(roic_avgs, [11, 22, None, None])
