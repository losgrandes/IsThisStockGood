import json
import sys
import logging
from src.Base import Base
import src.RuleOneInvestingCalculations as RuleOne

class MSNMoney(Base):
  AUTOCOMPLETE_URL = 'https://services.bingapis.com/contentservices-finance.csautosuggest/api/v1/Query?query={}&market=en-us'

  def extract_stock_id(self, content, mic_code):
    data = json.loads(content)
    possible_symbols = []
    for ticker in data.get('data', {}).get('stocks', []):
        js = json.loads(ticker)
        if js.get('RT00S', '').upper() == self.ticker_symbol.upper() and js.get('ExMicCode', '').upper() == mic_code:
            possible_symbols.append(js.get('SecId', ''))
    if len(possible_symbols) == 1:
      return possible_symbols[0]
    else:
      logging.error(f"Possible tickers {possible_symbols}")
      raise ValueError(f"Couldn't fetch MSN id for {self.ticker_symbol}")

  def get_ticker_autocomplete_url(self):
    return self.AUTOCOMPLETE_URL.format(self.ticker_symbol)

  def _average(self, rates):
    if len(rates) == 0 or any([isinstance(rate, str) for rate in rates]):
      return None
    return round(sum(rates) / len(rates), 2)

class MSNMoneyKeyRatios(MSNMoney):

  URL_TEMPLATE = 'https://services.bingapis.com/contentservices-finance.financedataservice/api/v1/KeyRatios?stockId={}'
  KEY_RATIOS_YEAR_SPAN = 5

  def parse_msn_ratios(self, content):
    self._parse_annual_ratios(json.loads(content))
    return True

  def _parse_annual_ratios(self, data):
    self.annual_key_ratios = sorted(
      [x for x in data['companyMetrics']
      if x['fiscalPeriodType'] == 'Annual'],
      key=lambda x: x['year'],
      reverse=True
    )[0:self.KEY_RATIOS_YEAR_SPAN]
    recent_pe_ratios = self._get_ratios('priceToEarningsRatio')
    logging.debug(f'Recent PE Ratios (MSN) {recent_pe_ratios}')
    try:
      self.pe_high = max(recent_pe_ratios)
      self.pe_low = min(recent_pe_ratios)
    except ValueError:
        pass
    free_cash_flow_growth_rates = self._get_ratios('freeCashFlowGrowthRate')
    self.free_cash_flow_growth_rates = [
      next(iter(free_cash_flow_growth_rates), None),
      self._average(free_cash_flow_growth_rates[0:3]),
      self._average(free_cash_flow_growth_rates[0:5])
    ]  
    equity_growth_rates = self._get_ratios('bookValueGrowthRate')
    self.equity_growth_rates = [
      next(iter(equity_growth_rates), None),
      self._average(equity_growth_rates[0:3]),
      self._average(equity_growth_rates[0:5])
    ]
    self.max_equity_growth_rate = self.equity_growth_rates[-1]
    revenue_growth_rates = self._get_ratios('revenueGrowthRate')
    self.revenue_growth_rates = [
      next(iter(revenue_growth_rates), None),
      self._average(revenue_growth_rates[0:3]),
      self._average(revenue_growth_rates[0:5])
    ]  
    eps_growth_rates = self._get_ratios('earningsGrowthRate')
    self.eps_growth_rates = [
      next(iter(eps_growth_rates), None),
      self._average(eps_growth_rates[0:3]),
      self._average(eps_growth_rates[0:5])
    ]
    # TODO Get debt_equity_ratio from the last quarter
    debt_equity_ratio = (
      self.annual_key_ratios[0].get('debtToEquityRatio', None)
      or data.get('companyAverage3Years', {}).get('debtToEquityRatio', None)
    )
    self.debt_equity_ratio = debt_equity_ratio / 100 if debt_equity_ratio is not None else None
    return True

  def _get_ratios(self, key):
    ratios = [
      year.get(key)
      for year in self.annual_key_ratios
      if year.get(key, None) is not None and not isinstance(year.get(key, None), str)
    ]
    return ratios

class MSNMoneyKeyStats(MSNMoney):

  URL_TEMPLATE = 'https://assets.msn.com/service/Finance/Equities/financialstatements?apikey=0QfOX3Vn51YCzitbLaRkTTBadtWpgTN8NZLW0C1SEM&activityId=BB1AD374-918B-4E88-8F0E-26246BC4DBA5&ocid=finance-utils-peregrine&cm=en-us&$filter=_p%20eq%20%27{}%27&$top=200&wrapodata=false'

  def parse(self, content):
    self._parse(json.loads(content))
    self._parse_roic_growth_rates(json.loads(content))
    return True

  def _parse(self, data):
    balance_sheets_all = sorted([sheet.get('balanceSheets', {}) for sheet in data],key=lambda x: x.get('endDate'), reverse=True)
    income_statements_annual = sorted([sheet.get('incomeStatement', {}) for sheet in data if sheet.get('type','') == 'annual'],key=lambda x: x.get('endDate'), reverse=True)
    if balance_sheets_all:
      self.long_term_debt = balance_sheets_all[0].get('currentLiabilities',{}).get('totalLongTermDebt', None)
    if income_statements_annual:
      self.last_year_net_income = income_statements_annual[0].get('income',{}).get('netIncomeBeforeExtraItems', None)
    return True

  def _get_roic_history(self, data):
    """
    Calculate ROIC averages for 1,3,5 and Max years
    StockRow averages aren't accurate, so we're getting avgs for 1y and 3y from Yahoo
    by calculating these by ouselves. The rest is from StockRow to at least have some (even
    a bit inaccurate values), cause Yahoo has data for 4 years only.
    """
    balance_sheets_annual = sorted([sheet.get('balanceSheets', {}) for sheet in data if sheet.get('type','') == 'annual'],key=lambda x: x.get('endDate'), reverse=True)
    income_statements_annual = sorted([sheet.get('incomeStatement', {}) for sheet in data if sheet.get('type','') == 'annual'],key=lambda x: x.get('endDate'), reverse=True)
    income_statements_annual = [stmt.get('income') for stmt in income_statements_annual]
    cash_flow_annual = sorted([sheet.get('cashFlow', {}) for sheet in data if sheet.get('type','') == 'annual'],key=lambda x: x.get('endDate'), reverse=True)
    cash_flow_annual = [stmt.get('operating') for stmt in cash_flow_annual]
    net_income_history = self._get_history(income_statements_annual, 'incomeAvailableToComInclExtraOrd')
    cash_history = self._get_history(cash_flow_annual, 'netCashEndingBalance')
    long_term_debt_history = self._get_history(
      [stmt.get('currentLiabilities') for stmt in balance_sheets_annual],
      'longTermDebt'
    )
    # if data is empty 
    if None in long_term_debt_history[0:2]:
      long_term_debt_history = self._get_history(
        [stmt.get('currentLiabilities') for stmt in balance_sheets_annual],
        'totalLongTermDebt'
      ) 
    stockholder_equity_history = self._get_history(
      [stmt.get('equity') for stmt in balance_sheets_annual],
      'totalEquity'
    )
    roic_history = []
    for i in range(
      0,
      min(
        len(net_income_history), len(cash_history),
        len(long_term_debt_history), len(stockholder_equity_history)
      )
    ):
      if (
        net_income_history[i] is not None
        and cash_history[i] is not None
        and long_term_debt_history[i] is not None
        and stockholder_equity_history[i] is not None
      ):
        logging.debug(f"Calculating (MSN) ROIC based on Net Income {net_income_history[i]}, Cash {cash_history[i]} LongDebt {long_term_debt_history[i]}, StockholderEquity {stockholder_equity_history[i]}")
        roic_history.append(
          RuleOne.calculate_roic(
            net_income_history[i], cash_history[i],
            long_term_debt_history[i], stockholder_equity_history[i]
          )
        )
      else:
        roic_history.append(None)
    return roic_history

  def _get_history(self, data, key):
    history = []
    for stmt in data:
      if stmt is not None:
        history.append(stmt.get(key, None))
    return history

  def _get_roic_average(self, data, years):
    history = self._get_roic_history(data)
    if len(history[0:years]) < years or any([roic is None for roic in history[0:years]]):
      return None
    return round(sum(history[0:years]) / years, 2)

  def _parse_roic_growth_rates(self, data):
    """
    Calculate ROIC averages for 1,3,5 and Max years
    StockRow averages aren't accurate, so we're getting avgs for 1y and 3y from Yahoo
    by calculating these by ouselves. The rest is from StockRow to at least have some (even
    a bit inaccurate values), cause Yahoo has data for 4 years only.
    """
    self.roic_average_growth_rates.append(self._get_roic_average(data, years=1))
    self.roic_average_growth_rates.append(self._get_roic_average(data, years=3))
    self.roic_average_growth_rates.append(self._get_roic_average(data, years=5))
    self.roic_average_growth_rates.append(self._get_roic_average(data, years=10))
    

  
