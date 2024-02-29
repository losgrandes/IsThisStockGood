import sqlite3 as sql

class SQLite(object):
  
  def __init__(self):
    self.conn = sql.connect("stocks.db")
    self.db = self.conn.cursor()

  def get_market_for_ticker(self, ticker, exchange):
    query = f"""
    SELECT p.exchange, mic_code
    from markets m
    left join stocks_payload p on p.exchange = m.exchange
    where p.ticker='{ticker}'
    """
    if exchange:
        query += f" and p.exchange='{exchange}'"
    res = self.db.execute(query)
    rows = res.fetchall()
    if len(rows) > 1:
        raise ValueError("Too many markets for "+ticker)
    elif not rows:
        raise ValueError("Couldn't find market for "+ticker)
    else:
       return (rows[0][0], rows[0][1])
    
  def get_stocks_to_be_fetched(self):
     query = "select * from stocks_payload"
     stock_list = self.db.execute(query).fetchall()
     return stock_list


  def setValueForFieldWithName(self, table_name, ticker, field, value):
    if value is None:
        value = 'NULL'
    self.db.execute("UPDATE " + table_name + " SET " + field + "=" + str(value) + " WHERE ticker = '" + ticker + "'")

  def setValuesForFieldRangeWithName(self, table_name, ticker, field, values):
    if not values or not len(values):
        return
    suffixes = ['1', '3', '5', 'max']
    for i in range(len(values)):
        self.setValueForFieldWithName(table_name, ticker, field + '_' + suffixes[i], values[i] if values[i] else "NULL")

  def insertDataIntoTableForTicker(self, table_name, ticker, data):
    if not data:
        print("Empty data for ticker: " + ticker)
        return
    self.db.execute('REPLACE INTO ' + table_name + ' (ticker, name) VALUES ("' + ticker + '", "' + (data['name'] or "NULL") + '")')
    try:
        self.setValuesForFieldRangeWithName(table_name, ticker, 'roic', data.get('roic', []))
        self.setValuesForFieldRangeWithName(table_name, ticker, 'eps', data.get('eps', []))
        self.setValuesForFieldRangeWithName(table_name, ticker, 'sales', data.get('sales', []))
        self.setValuesForFieldRangeWithName(table_name, ticker, 'equity', data.get('equity', []))
        self.setValuesForFieldRangeWithName(table_name, ticker, 'cash', data.get('cash', []))
        self.setValueForFieldWithName(table_name, ticker, 'long_term_debt', data.get('long_term_debt', "NULL"))
        self.setValueForFieldWithName(table_name, ticker, 'free_cash_flow', data.get('free_cash_flow', "NULL"))
        self.setValueForFieldWithName(table_name, ticker, 'debt_payoff_time', data.get('debt_payoff_time', "NULL"))
        self.setValueForFieldWithName(table_name, ticker, 'debt_equity_ratio', data.get('debt_equity_ratio', "NULL"))
        self.setValueForFieldWithName(table_name, ticker, 'ttm_net_income', data.get('ttm_net_income', "NULL"))
        self.setValueForFieldWithName(table_name, ticker, 'margin_of_safety_price', data.get('margin_of_safety_price', "NULL"))
        self.setValueForFieldWithName(table_name, ticker, 'sticker_price', data.get('sticker_price', "NULL"))
        self.setValueForFieldWithName(table_name, ticker, 'current_price', data.get('current_price', "NULL"))
        self.setValueForFieldWithName(table_name, ticker, 'payback_time', data.get('payback_time', "NULL"))
        self.conn.commit()
    except Exception as e:
        print(f"Error inserting data into db: {str(e)}")