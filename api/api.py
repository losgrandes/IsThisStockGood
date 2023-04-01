import mysql.connector
import json
from flask import Flask, jsonify
from flask_cors import CORS
from collections import OrderedDict

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
app.config['JSON_SORT_KEYS'] = False

def mysql_wrapper(func):
    def wrapper():
        cnx = mysql.connector.connect(user='isthisstockgood', database='isthisstockgood', password='ruleone')
        cursor = cnx.cursor()
        func(cursor)
        row_headers=[x[0] for x in cursor.description] #this will extract row headers
        rv = cursor.fetchall()
        cursor.close()
        cnx.close()
        json_data=[]
        for result in rv:
            json_data.append(OrderedDict(zip(row_headers,result)))
        return jsonify(json_data)
    wrapper.__name__ = func.__name__
    return wrapper

@app.route("/my")
@mysql_wrapper
def my(cursor):
    cursor.execute(
"""
SELECT s.ticker, p.exchange, name,
roic_1,roic_3,roic_5,roic_max,
eps_1,eps_3,eps_5,eps_max,
sales_1,sales_3,sales_5,sales_max,
equity_1,equity_3,equity_5,equity_max,
cash_1,cash_3,cash_5,cash_max,
current_price, margin_of_safety_price
FROM stocks s
LEFT JOIN stocks_payload p on p.ticker=s.ticker
WHERE s.ticker in ('ANET','TSLA','DQ', 'AKAM', 'INPST', 'CPRT')
and fetch_date > now() - interval 1 day
""")

@app.route("/belowmos")
@mysql_wrapper
def belowmos(cursor):
    cursor.execute(
"""
SELECT s.ticker, p.exchange, name,
roic_1,roic_3,roic_5,roic_max,
eps_1,eps_3,eps_5,eps_max,
sales_1,sales_3,sales_5,sales_max,
equity_1,equity_3,equity_5,equity_max,
cash_1,cash_3,cash_5,cash_max,
current_price, margin_of_safety_price
FROM stocks s
LEFT JOIN stocks_payload p on p.ticker=s.ticker
WHERE s.current_price <= s.margin_of_safety_price
and fetch_date > now() - interval 1 day
""")

@app.route("/almostmos")
@mysql_wrapper
def almostmos(cursor):
    cursor.execute(
"""
SELECT s.ticker, p.exchange, name,
roic_1,roic_3,roic_5,roic_max,
eps_1,eps_3,eps_5,eps_max,
sales_1,sales_3,sales_5,sales_max,
equity_1,equity_3,equity_5,equity_max,
cash_1,cash_3,cash_5,cash_max,
current_price, margin_of_safety_price
FROM stocks s
LEFT JOIN stocks_payload p on p.ticker=s.ticker
WHERE s.current_price < s.margin_of_safety_price * 1.2
and s.current_price > s.margin_of_safety_price
and fetch_date > now() - interval 1 day
""")

@app.route("/all")
@mysql_wrapper
def all(cursor):
    cursor.execute(
"""
SELECT a.ticker,p.exchange, name,
roic_1,roic_3,roic_5,roic_max,
eps_1,eps_3,eps_5,eps_max,
sales_1,sales_3,sales_5,sales_max,
equity_1,equity_3,equity_5,equity_max,
cash_1,cash_3,cash_5,cash_max,
current_price, margin_of_safety_price
from (
select
*,
@nulls:=0,
@nulls := if(roic_1 is null,@nulls := @nulls +1, @nulls),
@nulls := if(roic_3 is null, @nulls := @nulls +1, @nulls),
@nulls := if(roic_5 is null,@nulls := @nulls +1, @nulls),
@nulls := if(eps_1 is null,@nulls := @nulls +1, @nulls),
@nulls := if(eps_3 is null, @nulls := @nulls +1, @nulls),
@nulls := if(eps_5 is null,@nulls := @nulls +1, @nulls),
@nulls := if(sales_1 is null,@nulls := @nulls +1, @nulls),
@nulls := if(sales_3 is null, @nulls := @nulls +1, @nulls),
@nulls := if(sales_5 is null,@nulls := @nulls +1, @nulls),
@nulls := if(equity_1 is null,@nulls := @nulls +1, @nulls),
@nulls := if(equity_3 is null, @nulls := @nulls +1, @nulls),
@nulls := if(equity_5 is null,@nulls := @nulls +1, @nulls),
@nulls := if(cash_1 is null,@nulls := @nulls +1, @nulls),
@nulls := if(cash_3 is null, @nulls := @nulls +1, @nulls),
@nulls := if(cash_5 is null,@nulls := @nulls +1, @nulls),
@nulls nulls
FROM stocks
where fetch_date > now() - interval 7 day
) a
LEFT JOIN stocks_payload p on p.ticker=a.ticker
where a.nulls < 5
""")

@app.route("/big5")
@mysql_wrapper
def big5(cursor):
    cursor.execute(
"""
SELECT stocks.ticker, p.exchange, name,
roic_1,roic_3,roic_5,roic_max,
eps_1,eps_3,eps_5,eps_max,
sales_1,sales_3,sales_5,sales_max,
equity_1,equity_3,equity_5,equity_max,
cash_1,cash_3,cash_5,cash_max,
current_price, margin_of_safety_price
FROM stocks
LEFT JOIN stocks_payload p on p.ticker=stocks.ticker
WHERE (stocks.roic_1 IS NOT NULL AND stocks.roic_1 >= 10)
AND (stocks.roic_3 IS NOT NULL AND stocks.roic_3 >= 10)
AND (stocks.roic_5 IS NOT NULL AND stocks.roic_5 >= 10)
AND (stocks.roic_max IS NOT NULL AND stocks.roic_max >= 10)
AND (stocks.eps_1 IS NOT NULL AND stocks.eps_1 >= 10)
AND (stocks.eps_3 IS NOT NULL AND stocks.eps_3 >= 10)
AND (stocks.eps_5 IS NOT NULL AND stocks.eps_5 >= 10)
AND (stocks.eps_max IS NOT NULL AND stocks.eps_max >= 10)
AND (stocks.sales_1 IS NOT NULL AND stocks.sales_1 >= 10)
AND (stocks.sales_3 IS NOT NULL AND stocks.sales_3 >= 10)
AND (stocks.sales_5 IS NOT NULL AND stocks.sales_5 >= 10)
AND (stocks.sales_max IS NOT NULL AND stocks.sales_max >= 10)
AND (stocks.equity_1 IS NOT NULL AND stocks.equity_1 >= 10)
AND (stocks.equity_3 IS NOT NULL AND stocks.equity_3 >= 10)
AND (stocks.equity_5 IS NOT NULL AND stocks.equity_5 >= 10)
AND (stocks.equity_max IS NOT NULL AND stocks.equity_max >= 10)
AND (stocks.cash_1 IS NOT NULL AND stocks.cash_1 >= 10)
AND (stocks.cash_3 IS NOT NULL AND stocks.cash_3 >= 10)
AND (stocks.cash_5 IS NOT NULL AND stocks.cash_5 >= 10)
AND (stocks.cash_max IS NOT NULL AND stocks.cash_max >= 10)
and fetch_date > now() - interval 1 day
""")

@app.route("/ruleone")
@mysql_wrapper
def ruleone(cursor):
    cursor.execute(
    """
SELECT stocks.ticker, p.exchange, name,
roic_1,roic_3,roic_5,roic_max,
eps_1,eps_3,eps_5,eps_max,
sales_1,sales_3,sales_5,sales_max,
equity_1,equity_3,equity_5,equity_max,
cash_1,cash_3,cash_5,cash_max,
current_price, margin_of_safety_price
FROM stocks
LEFT JOIN stocks_payload p on p.ticker=stocks.ticker
WHERE (stocks.roic_1 IS NOT NULL AND stocks.roic_1 >= 10)
AND (stocks.roic_3 IS NOT NULL AND stocks.roic_3 >= 10)
AND (stocks.roic_5 IS NOT NULL AND stocks.roic_5 >= 10)
AND (stocks.roic_max IS NOT NULL AND stocks.roic_max >= 10)
AND (stocks.eps_1 IS NOT NULL AND stocks.eps_1 >= 10)
AND (stocks.eps_3 IS NOT NULL AND stocks.eps_3 >= 10)
AND (stocks.eps_5 IS NOT NULL AND stocks.eps_5 >= 10)
AND (stocks.eps_max IS NOT NULL AND stocks.eps_max >= 10)
AND (stocks.sales_1 IS NOT NULL AND stocks.sales_1 >= 10)
AND (stocks.sales_3 IS NOT NULL AND stocks.sales_3 >= 10)
AND (stocks.sales_5 IS NOT NULL AND stocks.sales_5 >= 10)
AND (stocks.sales_max IS NOT NULL AND stocks.sales_max >= 10)
AND (stocks.equity_1 IS NOT NULL AND stocks.equity_1 >= 10)
AND (stocks.equity_3 IS NOT NULL AND stocks.equity_3 >= 10)
AND (stocks.equity_5 IS NOT NULL AND stocks.equity_5 >= 10)
AND (stocks.equity_max IS NOT NULL AND stocks.equity_max >= 10)
AND (stocks.cash_1 IS NOT NULL AND stocks.cash_1 >= 10)
AND (stocks.cash_3 IS NOT NULL AND stocks.cash_3 >= 10)
AND (stocks.cash_5 IS NOT NULL AND stocks.cash_5 >= 10)
AND (stocks.cash_max IS NOT NULL AND stocks.cash_max >= 10)
AND current_price <= margin_of_safety_price
and fetch_date > now() - interval 1 day
    """
    )


@app.route("/fetch_stats")
@mysql_wrapper
def fetch_stats(cursor):
    cursor.execute(
"""
SELECT
sum(if(fetch_date >= now() - interval 1 day, 1, 0)) successful_fetches, 
sum(if(fetch_date >= now() - interval 1 day and a.nulls = 0, 1, 0)) without_nulls,
count(*) all_tickers 
FROM (
select
*,
@nulls:=0,
@nulls := if(roic_1 is null,@nulls := @nulls +1, @nulls),
@nulls := if(roic_3 is null, @nulls := @nulls +1, @nulls),
@nulls := if(roic_5 is null,@nulls := @nulls +1, @nulls),
@nulls := if(eps_1 is null,@nulls := @nulls +1, @nulls),
@nulls := if(eps_3 is null, @nulls := @nulls +1, @nulls),
@nulls := if(eps_5 is null,@nulls := @nulls +1, @nulls),
@nulls := if(sales_1 is null,@nulls := @nulls +1, @nulls),
@nulls := if(sales_3 is null, @nulls := @nulls +1, @nulls),
@nulls := if(sales_5 is null,@nulls := @nulls +1, @nulls),
@nulls := if(equity_1 is null,@nulls := @nulls +1, @nulls),
@nulls := if(equity_3 is null, @nulls := @nulls +1, @nulls),
@nulls := if(equity_5 is null,@nulls := @nulls +1, @nulls),
@nulls := if(cash_1 is null,@nulls := @nulls +1, @nulls),
@nulls := if(cash_3 is null, @nulls := @nulls +1, @nulls),
@nulls := if(cash_5 is null,@nulls := @nulls +1, @nulls),
@nulls := if(margin_of_safety_price is null, @nulls := @nulls +1, @nulls),
@nulls := if(sticker_price is null,@nulls := @nulls +1, @nulls),
@nulls := if(free_cash_flow is null,@nulls := @nulls +1, @nulls),
@nulls nulls
FROM stocks
) a
""")
