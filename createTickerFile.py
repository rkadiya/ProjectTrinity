from __future__ import print_function

with open('./Files/AMEX_companylist.csv') as f1:
    with open('./Files/AMEX_tickerlist.txt', 'w+') as f2:
        next(f1)
        for line in f1:
            print(line.split(",")[0].strip("\"").strip().replace("^", "-").replace(".","-"))
            print(line.split(",")[0].strip("\"").strip().replace("^", "-").replace(".","-"), file=f2)


