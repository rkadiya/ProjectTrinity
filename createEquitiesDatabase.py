from __future__ import print_function
from pymongo import MongoClient
import datetime
import json
import time
import urllib2
import os

from pymongo.errors import DuplicateKeyError

count = 0
log = open("./error_tickers", "w+")

client = MongoClient()
db = client.equitiesdb
stocks = db.stocks
stocks.ensure_index("dataset.dataset_code", unique=True)

exchanges = ["AMEX","NYSE","Nasdaq"]
#exchanges = ["RAVI"]
for exchange in exchanges:
        with open("./Tickers/" + exchange + "_tickerlist.txt") as f1:
            for line in f1:
                if count > 1800:
                    time.sleep(60)  # Quandl has a limit of 2000 queries per 10 mins.
                    count = 0
                ticker = line.rstrip()

                try:
                    count += 1
                    response = urllib2.urlopen("https://www.quandl.com/api/v3/datasets/YAHOO/" + ticker + ".json?auth_token=sBEu_6B6oDDp2uBRdpNL")
                    data = json.load(response)
                    stocks.insert_one(data)
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
