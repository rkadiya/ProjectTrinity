from __future__ import print_function
from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import time
import urllib2
import os

from pymongo.errors import DuplicateKeyError

count = 0

log = open("./error_tickers_update", "w+")
client = MongoClient()
db = client.equitiesdb
stocks = db.stocks

for stock in stocks.find({}, {"_id":0}):
    if count > 1800:
        time.sleep(60)  # Quandl has a limit of 2000 queries per 10 mins.
        count = 0
    ticker = stock[u'dataset'][u'dataset_code']
    print(ticker)
    end_date = stock[u'dataset'][u'end_date']
    next_date = (datetime.strptime(end_date , '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        count += 1
        response = urllib2.urlopen("https://www.quandl.com/api/v3/datasets/YAHOO/" + ticker + ".json?auth_token=sBEu_6B6oDDp2uBRdpNL&start_date=" + next_date)
        data = json.load(response)

        stocklist = stock[u'dataset'][u'data']
        datalist = data[u'dataset'][u'data']
        datalist.extend(stocklist)

        result = stocks.update_one(
            {"dataset.dataset_code": ticker},
            {"$set": {"dataset.end_date": data[u'dataset'][u'end_date'], "dataset.newest_available_date":data[u'dataset'][u'newest_available_date'], "dataset.data": datalist}}
        )

        print(ticker)
    except urllib2.HTTPError, err:
        if err.code == 404:
            print("Page not found!+" + ticker,file=log)
        elif err.code == 403:
            print ("Access denied!+" + ticker,file=log)
        else:
            print ("Something happened! Error code!+" + ticker,file=log), err.code
    except urllib2.URLError, err:
        print ("Some other error happened!+" + ticker, file.log), err.reason

    except ValueError as e:
        print (e.message + "+" + ticker,file=log)

    except DuplicateKeyError as dke:
        print (dke.message + "+" + ticker,file=log)

log.close()


