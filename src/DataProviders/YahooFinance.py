from enum import Enum
import json
import logging
import re
import src.RuleOneInvestingCalculations as RuleOne
from src.DataProviders.Base import Base


class YahooAutocomplete(Base):
  URL_TEMPLATE = 'https://query2.finance.yahoo.com/v1/finance/search?q={}&lang=en-US&region=US&quotesCount=10&listsCount=2&enableFuzzyQuery=false&quotesQueryId=tss_match_phrase_query'

  def extract_stock_id(self, content, exchange):
    data = json.loads(content)
    quotes = data.get('quotes', [])
    possible_symbols = []
    for quote in quotes:
      if quote.get('exchange', '') == exchange and self.ticker_symbol in quote.get('symbol'):
        possible_symbols.append(quote.get('symbol'))
    if len(possible_symbols) == 1:
      return possible_symbols[0]
    elif self.ticker_symbol in possible_symbols:
      return self.ticker_symbol
    else:
      logging.error(f"Possible tickers {possible_symbols}")
      raise ValueError(f"Couldn't fetch Yahoo id for {self.ticker_symbol}")

class YahooFinanceQuote(Base):
  # Expects the ticker symbol as the only argument.
  # This can theoretically request multiple comma-separated symbols.
  # This could theoretically be trimmed down by using `fields=` parameter.
  URL_TEMPLATE = 'https://query1.finance.yahoo.com/v7/finance/quote?symbols={}&crumb=I5tk0M/.aU1'
  HEADERS = {
    'Cookie': "GUC=AQABCAFl2lxmCUIdXgRo&s=AQAAAO9SKe8M&g=ZdkWCA; A1=d=AQABBP4V2WUCEM4K8ATJVNHBrxFMGKpODv0FEgABCAFc2mUJZu-bb2UBAiAAAAcI-hXZZYlifGg&S=AQAAAhS2F7cp4fVj56u1G6VqibQ; A3=d=AQABBP4V2WUCEM4K8ATJVNHBrxFMGKpODv0FEgABCAFc2mUJZu-bb2UBAiAAAAcI-hXZZYlifGg&S=AQAAAhS2F7cp4fVj56u1G6VqibQ; cmp=t=1709241055&j=1&u=1---&v=15; PRF=t%3DANET%252BAKAM%26newChartbetateaser%3D1; EuConsent=CP6bsAAP6bsAAAOACKENApEgAAAAAAAAACiQAAAAAAAA; A1S=d=AQABBP4V2WUCEM4K8ATJVNHBrxFMGKpODv0FEgABCAFc2mUJZu-bb2UBAiAAAAcI-hXZZYlifGg&S=AQAAAhS2F7cp4fVj56u1G6VqibQ"
  } 

  def parse(self, content, *args, **kwargs):
    data = json.loads(content)
    results = data.get('quoteResponse', {}).get('result', [])
    if not results:
      return False
    success = self._parse_current_price(results)
    success = success and self._parse_market_cap(results)
    success = success and self._parse_name(results)
    success = success and self._parse_average_volume(results)
    success = success and self._parse_ttm_eps(results)
    return success

  def _parse_current_price(self, results):
    if results:
      self.current_price = results[0].get('regularMarketPrice', None)
    return True if self.current_price else False

  def _parse_market_cap(self, results):
    if results:
      self.market_cap = results[0].get('marketCap', None)
    return True if self.market_cap else False

  def _parse_name(self, results):
    if results:
      self.name = results[0].get('longName', None)
    return True if self.name else False

  def _parse_average_volume(self, results):
    if results:
      regularMarketVolume = results[0].get('regularMarketVolume', -1)
      averageDailyVolume3Month = results[0].get('averageDailyVolume3Month', -1)
      averageDailyVolume10Day = results[0].get('averageDailyVolume10Day', -1)
      self.average_volume = max(0, min(regularMarketVolume, averageDailyVolume3Month, averageDailyVolume10Day))
    return True if self.average_volume else False
    
  def _parse_ttm_eps(self, results):
    if results:
      self.ttm_eps = results[0].get('epsTrailingTwelveMonths', None)
    return True if self.ttm_eps else False


class YahooFinanceQuoteSummaryModule(Enum):
    assetProfile = 1
    incomeStatementHistory = 2
    incomeStatementHistoryQuarterly = 3
    balanceSheetHistory = 4
    balanceSheetHistoryQuarterly = 5
    cashFlowStatementHistory = 6
    cashFlowStatementHistoryQuarterly = 7
    defaultKeyStatistics = 8
    financialData = 9
    calendarEvents = 10
    secFilings = 11
    recommendationTrend = 12
    upgradeDowngradeHistory = 13
    institutionOwnership = 14
    fundOwnership = 15
    majorDirectHolders = 16,
    majorHoldersBreakdown = 17
    insiderTransactions = 18
    insiderHolders = 19
    netSharePurchaseActivity = 20
    earnings = 21
    earningsHistory = 22
    earningsTrend = 23
    industryTrend = 24
    indexTrend = 26
    sectorTrend = 27


## (unofficial) API documentation: https://observablehq.com/@stroked/yahoofinance
class YahooFinanceQuoteSummary(Base):
  # Expects the ticker symbol as the first format string, and a comma-separated list
  # of `QuotesummaryModules` strings for the second argument.
  URL_TEMPLATE = 'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{}?modules={}&crumb=I5tk0M/.aU1'
  HEADERS = {
    'Cookie': "GUC=AQABCAFl2lxmCUIdXgRo&s=AQAAAO9SKe8M&g=ZdkWCA; A1=d=AQABBP4V2WUCEM4K8ATJVNHBrxFMGKpODv0FEgABCAFc2mUJZu-bb2UBAiAAAAcI-hXZZYlifGg&S=AQAAAhS2F7cp4fVj56u1G6VqibQ; A3=d=AQABBP4V2WUCEM4K8ATJVNHBrxFMGKpODv0FEgABCAFc2mUJZu-bb2UBAiAAAAcI-hXZZYlifGg&S=AQAAAhS2F7cp4fVj56u1G6VqibQ; cmp=t=1709241055&j=1&u=1---&v=15; PRF=t%3DANET%252BAKAM%26newChartbetateaser%3D1; EuConsent=CP6bsAAP6bsAAAOACKENApEgAAAAAAAAACiQAAAAAAAA; A1S=d=AQABBP4V2WUCEM4K8ATJVNHBrxFMGKpODv0FEgABCAFc2mUJZu-bb2UBAiAAAAcI-hXZZYlifGg&S=AQAAAhS2F7cp4fVj56u1G6VqibQ"
  } 
  # A list of modules that can be used inside of `QUOTE_SUMMARY_URL_TEMPLATE`.
  # These should be passed as a comma-separated list.
  _MODULES = {
    YahooFinanceQuoteSummaryModule.assetProfile: "assetProfile",  # Company info/background
    YahooFinanceQuoteSummaryModule.incomeStatementHistory: "incomeStatementHistory",
    YahooFinanceQuoteSummaryModule.incomeStatementHistoryQuarterly: "incomeStatementHistoryQuarterly",
    YahooFinanceQuoteSummaryModule.balanceSheetHistory: "balanceSheetHistory",  # Current cash/equivalents
    YahooFinanceQuoteSummaryModule.balanceSheetHistoryQuarterly: "balanceSheetHistoryQuarterly",
    YahooFinanceQuoteSummaryModule.cashFlowStatementHistory: "cashFlowStatementHistory",
    YahooFinanceQuoteSummaryModule.cashFlowStatementHistoryQuarterly: "cashFlowStatementHistoryQuarterly",
    YahooFinanceQuoteSummaryModule.defaultKeyStatistics: "defaultKeyStatistics",
    YahooFinanceQuoteSummaryModule.financialData: "financialData",
    YahooFinanceQuoteSummaryModule.calendarEvents: "calendarEvents",  # Contains ex-dividend date
    YahooFinanceQuoteSummaryModule.secFilings: "secFilings",  # SEC filing links
    YahooFinanceQuoteSummaryModule.recommendationTrend: "recommendationTrend",
    YahooFinanceQuoteSummaryModule.upgradeDowngradeHistory: "upgradeDowngradeHistory",
    YahooFinanceQuoteSummaryModule.institutionOwnership: "institutionOwnership",
    YahooFinanceQuoteSummaryModule.fundOwnership: "fundOwnership",
    YahooFinanceQuoteSummaryModule.majorDirectHolders: "majorDirectHolders",
    YahooFinanceQuoteSummaryModule.majorHoldersBreakdown: "majorHoldersBreakdown",
    YahooFinanceQuoteSummaryModule.insiderTransactions: "insiderTransactions",
    YahooFinanceQuoteSummaryModule.insiderHolders: "insiderHolders",
    YahooFinanceQuoteSummaryModule.netSharePurchaseActivity: "netSharePurchaseActivity",
    YahooFinanceQuoteSummaryModule.earnings: "earnings",
    YahooFinanceQuoteSummaryModule.earningsHistory: "earningsHistory",
    YahooFinanceQuoteSummaryModule.earningsTrend: "earningsTrend",
    YahooFinanceQuoteSummaryModule.industryTrend: "industryTrend",
    YahooFinanceQuoteSummaryModule.indexTrend: "indexTrend",
    YahooFinanceQuoteSummaryModule.sectorTrend: "sectorTrend"
  }

  _SELECTED_MODULES = [
      YahooFinanceQuoteSummaryModule.incomeStatementHistory,
      YahooFinanceQuoteSummaryModule.balanceSheetHistory,
      YahooFinanceQuoteSummaryModule.financialData,
      YahooFinanceQuoteSummaryModule.earningsTrend,
      YahooFinanceQuoteSummaryModule.assetProfile
  ]

  def get_url(self, ticker_symbol=None):
    modulesString = self._construct_modules_string(self.modules)
    return self.URL_TEMPLATE.format(ticker_symbol or self.ticker_symbol, modulesString)

  # A helper method to return a formatted modules string.
  @classmethod
  def _construct_modules_string(cls, modules):
    modulesString = modules[0]
    for module in modules[1:]:
      modulesString = modulesString + ',' + module
    return modulesString

  # Accepts the ticker symbol followed by a list of
  # `YahooFinanceQuoteSummaryModule` enum values.
  def __init__(self, ticker_symbol):
    super().__init__(ticker_symbol)
    self.modules = [self._MODULES[module] for module in self._SELECTED_MODULES]
    self.url = self.get_url(ticker_symbol)
    self.module_data = {}
    self.analyst_estimated_growth_rate = None

  def parse(self, content):
    """Parses all the of the module responses from the json into a top-level dictionary."""
    data = json.loads(content)
    results = data.get('quoteSummary', {}).get('result', None)
    if not results:
      logging.error('Could not parse response for url: ' + self.url)
      return False
    for module in self.modules:
      for result in results:
        if module in result:
          self.module_data[module] = result[module]
          break
    self.parse_long_term_debt()
    self.parse_roic_growth_rates()
    self.parse_latest_free_cash_flow()
    self.parse_analyst_estimated_growth_rate()
    self.parse_company_summary()
    return True

  def get_balance_sheet_history(self, key):
    history = []
    for stmt in self.module_data.get('balanceSheetHistory', {}).get('balanceSheetStatements', []):
      history.append(stmt.get(key, {}).get('raw', None))
    return history

  def get_income_statement_history(self, key):
    history = []
    for stmt in self.module_data.get('incomeStatementHistory', {}).get('incomeStatementHistory', []):
      history.append(stmt.get(key, {}).get('raw', None))
    return history

  def parse_latest_free_cash_flow(self):
    # Setting default free_cash_flow to 1 because it's small and doesn't play role then
    self.latest_free_cash_flow = self.module_data.get('financialData', {}).get('freeCashflow', {}).get('raw', None)

  def parse_analyst_estimated_growth_rate(self):
    trends = self.module_data.get('earningsTrend', {}).get('trend', [])
    for trend in trends:
      if trend.get('period', '') == '+5y':
        growth = trend.get('growth', {}).get('raw', None)
        if growth:
          self.analyst_estimated_growth_rate = growth*100
        else:
          self.analyst_estimated_growth_rate = growth
  
  def parse_company_summary(self):
    self.summary = self.module_data.get('assetProfile', {}).get('longBusinessSummary','')
    return True
  
  def parse_long_term_debt(self):
    self.long_term_debt = self.get_balance_sheet_history('longTermDebt')[0]

  def parse_roic_growth_rates(self):
    """
    Calculate ROIC averages for 1,3,5 and Max years
    StockRow averages aren't accurate, so we're getting avgs for 1y and 3y from Yahoo
    by calculating these by ouselves. The rest is from StockRow to at least have some (even
    a bit inaccurate values), cause Yahoo has data for 4 years only.
    """
    self.roic_average_growth_rates.append(self._get_roic_average(years=1))
    self.roic_average_growth_rates.append(self._get_roic_average(years=3))

  def _get_roic_history(self):
    """
    Calculates ROIC historial values based on annual financial statements.

    net_income_history: Net Income (starts from the last annual statement)
    cash_history: Cash (starts from the last annual statement)
    long_term_debt_history: Long Term Debt (starts from the last annual statement)
    stockholder_equity_history: Stockholder Equity (starts from the last annual statement)
    """
    net_income_history = self.get_income_statement_history('netIncome')
    cash_history = self.get_balance_sheet_history('cash')
    long_term_debt_history = self.get_balance_sheet_history(
       'longTermDebt'
    )
    stockholder_equity_history = self.get_balance_sheet_history(
       'totalStockholderEquity'
    )
    roic_history = []
    for i in range(0, len(net_income_history)):
      if (
        net_income_history[i] is not None
        and cash_history[i] is not None
        and long_term_debt_history[i] is not None
        and stockholder_equity_history[i] is not None
      ):
        logging.debug(f"Calculating (Yahoo) ROIC based on Net Income {net_income_history[i]}, Cash {cash_history[i]} LongDebt {long_term_debt_history[i]}, StockholderEquity {stockholder_equity_history[i]}")
        roic_history.append(
          RuleOne.calculate_roic(
            net_income_history[i], cash_history[i],
            long_term_debt_history[i], stockholder_equity_history[i]
          )
        )
      else:
        roic_history.append(None)

    return roic_history

  def _get_roic_average(self, years):
    history = self._get_roic_history()
    if len(history[0:years]) < years or any([roic is None for roic in history[0:years]]):
      return None
    return round(sum(history[0:years]) / years, 2)
