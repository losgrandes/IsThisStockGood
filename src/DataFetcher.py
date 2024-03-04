import random
import logging
import sys
import src.RuleOneInvestingCalculations as RuleOne
from requests_futures.sessions import FuturesSession
from requests import Session
from src.DataProviders.MSNMoney import MSNMoneyKeyRatios, MSNMoneyKeyStats, MSNQuote
from src.DataProviders.StockRow import StockRowKeyStats, StockRowPrice, StockRowName
from src.DataProviders.YahooFinance import YahooFinanceQuote,\
YahooFinanceQuoteSummary, YahooFinanceQuoteSummaryModule,\
YahooAutocomplete
from src.Database import SQLite
from threading import Lock

def fetchDataForTickerSymbol(ticker, exchange=None):
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
  db = SQLite()
  (data_fetcher.exchange, data_fetcher.mic_code) = db.get_market_for_ticker(ticker, exchange)
  # Make all network request asynchronously to build their portion of
  # the json results.
  data_fetcher.fetch_yahoo_ticker()
  data_fetcher.fetch_with_autocomplete(MSNMoneyKeyStats(data_fetcher.ticker_symbol))
  data_fetcher.fetch_with_autocomplete(MSNMoneyKeyRatios(data_fetcher.ticker_symbol))
  data_fetcher.fetch_with_autocomplete(MSNQuote(data_fetcher.ticker_symbol))
  data_fetcher.fetch(YahooFinanceQuote(data_fetcher.yahoo_autocomplete.ticker_symbol))
  data_fetcher.fetch(YahooFinanceQuoteSummary(data_fetcher.yahoo_autocomplete.ticker_symbol))
  if data_fetcher.exchange in ('NMS','NYQ'):
    pass
    #data_fetcher.fetch(StockRowKeyStats(data_fetcher.ticker_symbol))
    #data_fetcher.fetch(StockRowPrice(data_fetcher.ticker_symbol))
    #data_fetcher.fetch(StockRowName(data_fetcher.ticker_symbol))
  # Wait for each RPC result before proceeding.
  for rpc in data_fetcher.rpcs:
    rpc.result()
  margin_of_safety_price, sticker_price = _calculate_mos_and_sticker(
    data_fetcher.get_min('max_equity_growth_rate'),
    data_fetcher.get_min('pe_high'), data_fetcher.get_min('pe_low'),
    data_fetcher.get_min('analyst_estimated_growth_rate'),
    data_fetcher.get_min('ttm_eps')
  )
  payback_time = _calculate_payback_time(
    data_fetcher.get_min('last_year_net_income'),
    data_fetcher.get_min('max_equity_growth_rate'),
    data_fetcher.get_min('market_cap'),
    data_fetcher.get_min('analyst_estimated_growth_rate')
  )
  if data_fetcher.get_max('long_term_debt') is not None and data_fetcher.get_min('latest_free_cash_flow') is not None:
    debt_payoff_time = data_fetcher.get_max('long_term_debt') / data_fetcher.get_min('latest_free_cash_flow')
  else:
    debt_payoff_time = None
  template_values = {
    'ticker' : ticker,
    'name' : data_fetcher.get_company_details('name'),
    'roic': data_fetcher.get_growth_rates('roic_average'),
    'eps': data_fetcher.get_growth_rates('eps'),
    'sales': data_fetcher.get_growth_rates('revenue'),
    'equity': data_fetcher.get_growth_rates('equity'),
    'cash': data_fetcher.get_growth_rates('free_cash_flow'),
    'long_term_debt' : data_fetcher.get_max('long_term_debt'),
    'free_cash_flow' : data_fetcher.get_min('latest_free_cash_flow'),
    'debt_payoff_time' : debt_payoff_time,
    'debt_equity_ratio' : data_fetcher.get_max('debt_equity_ratio'),
    'margin_of_safety_price' : margin_of_safety_price,
    'current_price' : data_fetcher.get_max('current_price'),
    'sticker_price' : sticker_price,
    'payback_time' : payback_time,
    'average_volume' : data_fetcher.get_min('average_volume'),
    'ttm_net_income' : data_fetcher.get_min('last_year_net_income'),
    'summary' : data_fetcher.get_company_details('summary'),
    'pe_low' : data_fetcher.get_min('pe_low'),
    'pe_high' : data_fetcher.get_min('pe_high'),
    'analyst_estimated_growth_rate' : data_fetcher.get_min('analyst_estimated_growth_rate'),
    'max_equity_growth_rate' : data_fetcher.get_min('max_equity_growth_rate'),
    'ttm_eps' : data_fetcher.get_min('ttm_eps')
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
    self.exchange = None
    self.mic_code = None

  def _create_session(self):
    session = FuturesSession()
    session.headers.update({
      'User-Agent' : random.choice(DataFetcher.USER_AGENT_LIST)
    })
    return session

  # TODO continue moving other fetches like this 
  def fetch(self, module):
    self.lock.acquire()
    self.sources.append(module)
    self.lock.release()
    session = self._create_session()
    if module.get_headers():
      session.headers.update(module.get_headers())
    rpc = session.get(module.get_url(), allow_redirects=True, hooks={
       'response': [self.parse(module=module)],
    })
    self.rpcs.append(rpc)

  def fetch_with_autocomplete(self, module):
    self.lock.acquire()
    self.sources.append(module)
    self.lock.release()
    session = self._create_session()
    rpc = session.get(module.get_ticker_autocomplete_url(), allow_redirects=True, hooks={
       'response': [self.continue_fetching_with_autocomplete(data_fetcher=self, module=module, mic_code=self.mic_code)],
    })
    self.rpcs.append(rpc)

  def continue_fetching_with_autocomplete(*factory_args, **factory_kwargs):
    def _continue_fetching(response, *args, **kwargs):
      try:
        stock_id = factory_kwargs['module'].extract_stock_id(response.text, factory_kwargs['mic_code'])
      except ValueError:
        return
      session = factory_kwargs['data_fetcher']._create_session()
      rpc = session.get(factory_kwargs['module'].get_url(stock_id), allow_redirects=True, hooks={
        'response': [factory_kwargs['data_fetcher'].parse(module=factory_kwargs['module'])],
      })
      factory_kwargs['data_fetcher'].rpcs.append(rpc)
    return _continue_fetching

  def parse(*factory_args, **factory_kwargs):
    def _parse(response, *request_args, **request_kwargs):
      if response.status_code != 200:
        return
      try:
        success = factory_kwargs['module'].parse(response.text)
      except Exception:
        return None
      return None
    return _parse

  def fetch_yahoo_ticker(self):
    session = Session()
    session.headers.update({
      'User-Agent' : random.choice(DataFetcher.USER_AGENT_LIST)
    })
    self.yahoo_autocomplete = YahooAutocomplete(self.ticker_symbol)
    response = session.get(self.yahoo_autocomplete.get_url())
    if response.status_code != 200:
      return
    try:
      self.yahoo_autocomplete.ticker_symbol = self.yahoo_autocomplete.extract_stock_id(response.text, self.exchange)
    except ValueError:
      self.yahoo_autocomplete.ticker_symbol = None

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
      f"""Available sources = {
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
      f"""Available sources = {
        [
          working_source.__class__.__name__+" ("+str(getattr(working_source, key))+")"
          for working_source in working_sources_sorted
        ]
      }"""
    )
    return getattr(working_sources_sorted[0], key)

  # TODO More smart than picking the first source?
  def get_company_details(self, key):
    logging.debug(f"Getting company {key}")
    working_sources = [source for source in self.sources if hasattr(source, key) and getattr(source, key) is not None]
    logging.debug(
      f"""Available sources = {
        [
          working_source.__class__.__name__+" ("+str(getattr(working_source, key))+")"
          for working_source in working_sources
        ]
      }"""
    )
    if working_sources:
      return getattr(working_sources[0], key, None)
    return None
