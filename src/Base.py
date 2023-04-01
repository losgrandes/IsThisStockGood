class Base:

    def __init__(self, ticker_symbol):
        self.ticker_symbol = ticker_symbol
        self.roic = []  # Return on invested capital
        self.roic_average_growth_rates = []
        self.equity = []  # Equity or BVPS (book value per share)
        self.equity_growth_rates = []
        self.latest_equity_growth_rate = None
        self.free_cash_flow = []  # Free Cash Flow
        self.free_cash_flow_growth_rates = []
        self.revenue_growth_rates = []  # Revenue
        self.eps_growth_rates = []  # Earnings per share
        self.last_year_net_income = None
        self.total_debt = None
        self.long_term_debt = None
        self.recent_free_cash_flow = None
        self.debt_payoff_time = 0
        self.debt_equity_ratio = None
        self.pe_high = None
        self.pe_low = None
        self.eps_ttm = None

    def get_url(self, ticker_symbol=None):
        return self.URL_TEMPLATE.format(ticker_symbol or self.ticker_symbol)