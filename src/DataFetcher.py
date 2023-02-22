import json
import random
import logging
import sys
import src.RuleOneInvestingCalculations as RuleOne
from requests_futures.sessions import FuturesSession
from requests import Session
from src.MSNMoney import MSNMoneyKeyRatios, MSNMoneyKeyStats
from src.StockRow import StockRowKeyStats
from src.YahooFinance import YahooFinanceQuote,\
YahooFinanceQuoteSummary, YahooFinanceQuoteSummaryModule,\
YahooAutocomplete
from threading import Lock

def fetchDataForTickerSymbol(ticker):
  """Fetches and parses all of the financial data for the `ticker`.

    Args:
      ticker: The ticker symbol string.

    Returns:
      Returns a dictionary of all the processed financial data. If
      there's an error, return None.

      Keys include:
        'roic',
        'eps',
        'sales',
        'equity',
        'cash',
        'long_term_debt',
        'free_cash_flow',
        'debt_payoff_time',
        'debt_equity_ratio',
        'margin_of_safety_price',
        'current_price'
  """
  if not ticker:
    return None

  data_fetcher = DataFetcher()
  data_fetcher.ticker_symbol = ticker

  # Make all network request asynchronously to build their portion of
  # the json results.
  data_fetcher.fetch_stockrow_key_stats()
  data_fetcher.fetch_msn_key_stats()
  data_fetcher.fetch_msn_ratios()
  data_fetcher.fetch_yahoo_ticker()
  data_fetcher.fetch_yahoo_finance_quote()
  data_fetcher.fetch_yahoo_finance_quote_summary()
  # Wait for each RPC result before proceeding.
  for rpc in data_fetcher.rpcs:
    rpc.result()
  margin_of_safety_price, sticker_price = _calculate_mos_and_sticker(
    data_fetcher.get_min('max_equity_growth_rate'),
    data_fetcher.get_min('pe_high'), data_fetcher.get_min('pe_low'),
    data_fetcher.get_analyst_estimated_growth_rate(),
    data_fetcher.get_min('ttm_eps')
    )
  payback_time = _calculate_payback_time(
    data_fetcher.get_last_year_net_income(),
    data_fetcher.get_min('max_equity_growth_rate'),
    data_fetcher.get_market_cap(),
    data_fetcher.get_analyst_estimated_growth_rate()
  )
  if data_fetcher.get_max('long_term_debt') is not None and data_fetcher.get_latest_free_cash_flow() is not None:
    debt_payoff_time = data_fetcher.get_max('long_term_debt') / data_fetcher.get_latest_free_cash_flow()
  else:
    debt_payoff_time = None
  template_values = {
    'ticker' : ticker,
    'name' : data_fetcher.get_company_name(),
    'roic': data_fetcher.get_growth_rates('roic_average'),
    'eps': data_fetcher.get_growth_rates('eps'),
    'sales': data_fetcher.get_growth_rates('revenue'),
    'equity': data_fetcher.get_growth_rates('equity'),
    'cash': data_fetcher.get_growth_rates('free_cash_flow'),
    'long_term_debt' : data_fetcher.get_max('long_term_debt'),
    'free_cash_flow' : data_fetcher.get_latest_free_cash_flow(),
    'debt_payoff_time' : debt_payoff_time,
    'debt_equity_ratio' : data_fetcher.get_max('debt_equity_ratio'),
    'margin_of_safety_price' : margin_of_safety_price,
    'current_price' : data_fetcher.get_current_price(),
    'sticker_price' : sticker_price,
    'payback_time' : payback_time,
    'average_volume' : data_fetcher.get_average_volume(),
    'ttm_net_income' : data_fetcher.get_last_year_net_income()
  }
  return template_values

def _calculate_mos_and_sticker(max_equity_growth_rate, pe_high, pe_low, analyst_estimated_growth_rate, eps_ttm):
  logging.debug(f'Growth rates. Analyst\'s: {analyst_estimated_growth_rate}, Max equity growth: {max_equity_growth_rate}')
  growth_rate = get_min_growth_rate(analyst_estimated_growth_rate, max_equity_growth_rate)
  logging.debug(f'Calculating prices based on TTM EPS {eps_ttm} PE low {pe_low} PE High {pe_high} and 5y estimated growth {growth_rate}')
  if (
    eps_ttm is None or eps_ttm <=0
    or growth_rate is None or growth_rate <=0
    or pe_low is None
    or pe_high is None
  ):
    return None, None
  # Divide the growth rate by 100 to convert from percent to decimal.
  growth_rate = growth_rate / 100.0
  margin_of_safety_price, sticker_price = \
      RuleOne.margin_of_safety_price(float(eps_ttm), growth_rate,
                                     float(pe_low), float(pe_high))
  return margin_of_safety_price, sticker_price

def _calculate_payback_time(last_year_net_income, latest_equity_growth_rate, market_cap, estimated_growth_rate):
  if not latest_equity_growth_rate or not market_cap or not estimated_growth_rate:
    return None

  if not estimated_growth_rate or not latest_equity_growth_rate:
    return None
  growth_rate = min(float(estimated_growth_rate),
                    float(latest_equity_growth_rate))
  # Divide the growth rate by 100 to convert from percent to decimal.
  growth_rate = growth_rate / 100.0

  # TODO: Figure out how to get TTM net income instead of previous year net income.
  if not last_year_net_income or not market_cap:
    return None
  payback_time = RuleOne.payback_time(market_cap, last_year_net_income, growth_rate)
  return payback_time

def get_min_growth_rate(analyst_estimated_growth_rate, max_equity_growth_rate):
  if analyst_estimated_growth_rate and max_equity_growth_rate:
    return min(float(analyst_estimated_growth_rate), float(max_equity_growth_rate))
  else:
    return analyst_estimated_growth_rate or max_equity_growth_rate


class DataFetcher():
  """A helper class that syncronizes all of the async data fetches."""

  USER_AGENT_LIST = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
  ]

  GROWTH_RATES_COUNT = 4

  def __init__(self,):
    self.lock = Lock()
    self.rpcs = []
    self.ticker_symbol = ''
    self.stockrow_key_stats = None
    self.msn_key_stats = None
    self.msn_ratios = None
    self.yahoo_finance_quote = None
    self.yahoo_finance_quote_summary = None
    self.yahoo_autocomplete = None
    self.error = False
    self.sources = []

  def _create_session(self):
    session = FuturesSession()
    session.headers.update({
      'User-Agent' : random.choice(DataFetcher.USER_AGENT_LIST)
    })
    return session

  def fetch_msn_key_stats(self):

    self.msn_key_stats = MSNMoneyKeyStats(self.ticker_symbol)
    session = self._create_session()
    rpc = session.get(self.msn_key_stats.get_ticker_autocomplete_url(), allow_redirects=True, hooks={
       'response': self.continue_fetching_msn_key_stats,
    })
    self.rpcs.append(rpc)

  def continue_fetching_msn_key_stats(self, response, *args, **kwargs):

    msn_stock_id = self.msn_key_stats.extract_stock_id(response.text)
    session = self._create_session()
    rpc = session.get(self.msn_key_stats.get_url(msn_stock_id), allow_redirects=True, hooks={
       'response': self.parse_msn_key_stats,
    })
    self.rpcs.append(rpc)

  def parse_msn_key_stats(self, response, *args, **kwargs):
    self.lock.acquire()
    if not self.msn_key_stats:
      self.lock.release()
      return
    success = self.msn_key_stats.parse(response.content)
    if not success:
      self.msn_key_stats = None
    else:
      self.sources.append(self.msn_key_stats)
    self.lock.release()

  def fetch_stockrow_key_stats(self):
    self.stockrow_key_stats = StockRowKeyStats(self.ticker_symbol)
    session = self._create_session()
    key_stat_rpc = session.get(self.stockrow_key_stats.key_stat_url, hooks={
       'response': self.parse_stockrow_key_stats,
    })
    self.rpcs.append(key_stat_rpc)

  # Called asynchronously upon completion of the URL fetch from
  # `fetch_stockrow_key_stats`.
  def parse_stockrow_key_stats(self, response, *args, **kwargs):
    self.lock.acquire()
    if not self.stockrow_key_stats:
      self.lock.release()
      return
    success = self.stockrow_key_stats.parse_json_data(response.content)
    if not success:
      self.stockrow_key_stats = None
    else:
      self.sources.append(self.stockrow_key_stats)
    self.lock.release()

  def fetch_msn_ratios(self):
    """
    Fetching PE Ratios to calculate Sticker Price and Safety Margin Price
    First we need to get an internal MSN stock id for a ticker
    and then fetch PE Ratios.
    """
    self.msn_ratios = MSNMoneyKeyRatios(self.ticker_symbol)
    session = self._create_session()
    rpc = session.get(self.msn_ratios.get_ticker_autocomplete_url(), allow_redirects=True, hooks={
       'response': self.continue_fetching_msn_ratios,
    })
    self.rpcs.append(rpc)

  def continue_fetching_msn_ratios(self, response, *args, **kwargs):
    """
    After msn_stock_id was fetched in fetch_pe_ratios method
    we can now get the financials
    """
    msn_stock_id = self.msn_ratios.extract_stock_id(response.text)
    session = self._create_session()
    rpc = session.get(self.msn_ratios.get_url(msn_stock_id), allow_redirects=True, hooks={
       'response': self.parse_msn_ratios,
    })
    self.rpcs.append(rpc)

  # Called asynchronously upon completion of the URL fetch from
  # `fetch_pe_ratios` and `continue_fetching_pe_ratios`.
  def parse_msn_ratios(self, response, *args, **kwargs):
    if response.status_code != 200:
      return
    if not self.msn_ratios:
      return
    result = response.text
    success = self.msn_ratios.parse_msn_ratios(result)
    if not success:
      self.msn_ratios = None
    else:
      self.sources.append(self.msn_ratios)

  def fetch_yahoo_ticker(self):
    session = Session()
    session.headers.update({
      'User-Agent' : random.choice(DataFetcher.USER_AGENT_LIST)
    })
    self.yahoo_autocomplete = YahooAutocomplete(self.ticker_symbol)
    response = session.get(self.yahoo_autocomplete.url)
    if response.status_code != 200:
      return
    data = json.loads(response.text)
    quotes = data.get('quotes', [])
    if len(quotes) == 1 and not quotes[0].get('symbol', None):
      self.yahoo_autocomplete.ticker_symbol = quotes[0].get('symbol', None)
    else:
      # TODO make it smarter to deal with multi suggestions from Yahoo autocomplete
      self.yahoo_autocomplete.ticker_symbol = self.ticker_symbol

  def fetch_yahoo_finance_quote(self):
    self.yahoo_finance_quote = YahooFinanceQuote(self.yahoo_autocomplete.ticker_symbol)
    session = self._create_session()
    rpc = session.get(self.yahoo_finance_quote.url, allow_redirects=True, hooks={
       'response': self.parse_yahoo_finance_quote,
    })
    self.rpcs.append(rpc)

  # Called asynchronously upon completion of the URL fetch from
  # `fetch_yahoo_finance_quote`.
  def parse_yahoo_finance_quote(self, response, *args, **kwargs):
    if response.status_code != 200:
      return
    if not self.yahoo_finance_quote:
      return
    result = response.text
    success = self.yahoo_finance_quote.parse_quote(result)
    if not success:
      self.yahoo_finance_quote = None
    else:
      self.sources.append(self.yahoo_finance_quote)

  def fetch_yahoo_finance_quote_summary(self):
    modules = [
        YahooFinanceQuoteSummaryModule.incomeStatementHistory,
        YahooFinanceQuoteSummaryModule.balanceSheetHistory,
        YahooFinanceQuoteSummaryModule.financialData,
        YahooFinanceQuoteSummaryModule.earningsTrend
    ]
    self.yahoo_finance_quote_summary = YahooFinanceQuoteSummary(self.yahoo_autocomplete.ticker_symbol, modules)
    session = self._create_session()
    rpc = session.get(self.yahoo_finance_quote_summary.url, allow_redirects=True, hooks={
       'response': self.parse_yahoo_finance_quote_summary,
    })
    self.rpcs.append(rpc)

  # Called asynchronously upon completion of the URL fetch from
  # `fetch_yahoo_finance_quote_summary`.
  def parse_yahoo_finance_quote_summary(self, response, *args, **kwargs):
    if response.status_code != 200:
      return
    if not self.yahoo_finance_quote_summary:
      return
    result = response.text
    success = self.yahoo_finance_quote_summary.parse_modules(result)
    if not success:
      self.yahoo_finance_quote_summary = None
    else:
      self.sources.append(self.yahoo_finance_quote_summary)

  def get_growth_rates(self, key):
    working_sources = [source for source in self.sources if hasattr(source, key+"_growth_rates")]
    logging.debug(f'Growth rates for {key}: {[(source.__class__.__name__, getattr(source, key+"_growth_rates", [])) for source in working_sources]}')
    working_rates = [iter(getattr(source, key+"_growth_rates", [])) for source in working_sources]
    final_growth_rates = []
    for i in range(0, self.GROWTH_RATES_COUNT):
      source_rates = list(filter(lambda x: x is not None, [next(rate, None) for rate in working_rates]))
      if source_rates:
        final_growth_rates.append(min(source_rates))
      else:
        final_growth_rates.append(None)
    logging.debug(f'Found {key}_growth_rates: {final_growth_rates}')
    return final_growth_rates

  def get_max(self, key):
    working_sources = [source for source in self.sources if hasattr(source, key) and getattr(source, key) is not None]
    working_sources_sorted = sorted(
      working_sources,
      key=lambda x: getattr(x, key), reverse=True
    )
    if not working_sources_sorted:
      logging.error(f'No working sources for {key}')
      return None
    logging.debug(
      f'Found maximum {key} of {getattr(working_sources_sorted[0], key)} in {working_sources_sorted[0].__class__.__name__}'
    )
    logging.debug(
      f"""Available sources were {
        [
          working_source.__class__.__name__+" ("+str(getattr(working_source, key))+")"
          for working_source in working_sources_sorted
        ]
      }"""
    )
    return getattr(working_sources_sorted[0], key)

  def get_min(self, key):
    working_sources = [source for source in self.sources if hasattr(source, key) and getattr(source, key) is not None]
    working_sources_sorted = sorted(
      working_sources,
      key=lambda x: getattr(x, key)
    )
    if not working_sources_sorted:
      logging.error(f'No working sources for {key}')
      return None
    logging.debug(
      f'Found minimum {key} of {getattr(working_sources_sorted[0], key)} in {working_sources_sorted[0].__class__.__name__}'
    )
    logging.debug(
      f"""Available sources were {
        [
          working_source.__class__.__name__+" ("+str(getattr(working_source, key))+")"
          for working_source in working_sources_sorted
        ]
      }"""
    )
    return getattr(working_sources_sorted[0], key)

  def get_latest_free_cash_flow(self):
    if self.yahoo_finance_quote_summary.get_latest_free_cash_flow() and getattr(self.stockrow_key_stats, 'recent_free_cash_flow', None):
      return max(self.yahoo_finance_quote_summary.get_latest_free_cash_flow(), self.stockrow_key_stats.recent_free_cash_flow)
    else:
      return self.yahoo_finance_quote_summary.get_latest_free_cash_flow() or getattr(self.stockrow_key_stats, 'recent_free_cash_flow', None)

  def get_analyst_estimated_growth_rate(self):
    return self.yahoo_finance_quote_summary.get_analyst_estimated_growth_rate()

  def get_current_price(self):
    return getattr(getattr(self, 'yahoo_finance_quote', None), 'current_price', None)

  def get_company_name(self):
    return getattr(getattr(self, 'yahoo_finance_quote', None), 'name', None)

  def get_average_volume(self):
    return getattr(getattr(self, 'yahoo_finance_quote', None), 'average_volume', None)

  def get_market_cap(self):
    return getattr(getattr(self, 'yahoo_finance_quote', None), 'market_cap', None)

  def get_last_year_net_income(self):
    return getattr(getattr(self, 'msn_key_stats', None), 'last_year_net_income', None)