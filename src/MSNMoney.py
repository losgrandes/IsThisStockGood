import json
import logging
from src.Base import Base

class MSNMoney(Base):
  AUTOCOMPLETE_URL = 'https://services.bingapis.com/contentservices-finance.csautosuggest/api/v1/Query?query={}&market=en-us'

  def extract_stock_id(self, content):
    data = json.loads(content)
    for ticker in data.get('data', {}).get('stocks', []):
        js = json.loads(ticker)
        if js.get('RT00S', '').upper() == self.ticker_symbol.upper():
            return js.get('SecId', '')

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
      year.get(key, None)
      for year in self.annual_key_ratios
    ]
    if any([ratio is None or isinstance(ratio, str) for ratio in ratios]):
      return []
    else:
      return ratios

class MSNMoneyKeyStats(MSNMoney):

  URL_TEMPLATE = 'https://assets.msn.com/service/Finance/Equities/financialstatements?apikey=0QfOX3Vn51YCzitbLaRkTTBadtWpgTN8NZLW0C1SEM&activityId=BB1AD374-918B-4E88-8F0E-26246BC4DBA5&ocid=finance-utils-peregrine&cm=en-us&$filter=_p%20eq%20%27{}%27&$top=200&wrapodata=false'

  def parse(self, content):
    return self._parse(json.loads(content))

  def _parse(self, data):
    balance_sheets = sorted([sheet.get('balanceSheets', {}) for sheet in data],key=lambda x: x.get('endDate'), reverse=True)
    income_statements = sorted([sheet.get('incomeStatement', {}) for sheet in data if sheet.get('type','') == 'annual'],key=lambda x: x.get('endDate'), reverse=True)
    if balance_sheets:
      self.long_term_debt = balance_sheets[0].get('currentLiabilities',{}).get('totalLongTermDebt', None)
    if income_statements:
      self.last_year_net_income = income_statements[0].get('income',{}).get('netIncomeBeforeExtraItems', None)
    return True
    

  
