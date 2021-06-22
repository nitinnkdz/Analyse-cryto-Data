# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime
import json
import csv

apikey = 'GET YOUR API FROM CRYTOCOMPARE.COM'
#attach to end of URLstring
url_api_part = '&api_key=' + apikey

connection = psycopg2.connect(host= 'localhost',database =  'crytodb',user = 'postgres', password = 'password')
cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)
#2. Populate list of all coin names
#URL to get a list of coins from cryptocompare API
URLcoinslist = 'https://min-api.cryptocompare.com/data/all/coinlist'

#Get list of cryptos with their symbols
res1 = requests.get(URLcoinslist)
res1_json = res1.json()
data1 = res1_json['Data']
symbol_array = []
cryptoDict = dict(data1)

#write to CSV
with open('coin_names.csv','w',encoding='utf-8') as f:
    f_writer = csv.writer(f,
                                     delimiter = ',',
                                     quotechar = '"',
                                    quoting=csv.QUOTE_MINIMAL)
    for coin in cryptoDict.values():
       name = coin['Name']
       symbol = coin['Symbol']
       symbol_array.append(symbol)
       coin_name = coin['CoinName']
       full_name = coin['FullName']
       entry = [symbol, coin_name]
       f_writer.writerow(entry)
print('Done getting crypto names and symbols. See coin_names.csv for result')


#3. Populate historical price for each crypto in BTC

#Note: this part might take a while to run since we're populating data for 4k+ coins
#counter variable for progress made
progress = 0
num_cryptos = str(len(symbol_array))
for symbol in symbol_array:
   # get data for that currency
   URL = 'https://min-api.cryptocompare.com/data/histoday?fsym=' +symbol +'&tsym=BTC&allData=true' +url_api_part
   res = requests.get(URL)
   res_json = res.json()
   data = res_json['Data']
   # write required fields into csv
   with open('crypto_prices.csv','w',encoding='utf-8') as f:
       f_writer = csv.writer(f,
                                     delimiter = ',',
                                     quotechar = '"',
                                     quoting=csv.QUOTE_MINIMAL)
      
       for day in data:
           rawts = day['time']
           ts = datetime.utcfromtimestamp(rawts).strftime('%Y-%m-%d %H:%M:%S')
           o = day['open']
           h = day['high']
           l = day['low']
           c = day['close']
           vfrom = day['volumefrom']
           vto = day['volumeto']
           entry = [ts, o, h, l, c, vfrom, vto, symbol]
           f_writer.writerow(entry)
   progress = progress + 1
   print('Processed ' + str(symbol))
   print(str(progress) + ' currencies out of ' +  num_cryptos + ' written to csv')
print('Done getting price data for all coins. See crypto_prices.csv for result')

#4. Populate BTC prices in different fiat currencies

# List of fiat currencies we want to query
# You can expand this list, but CryptoCompare does not have
# a comprehensive fiat list on their site
fiatList = ['AUD', 'CAD', 'CNY', 'EUR', 'GBP', 'GOLD', 'HKD',
'ILS', 'INR', 'JPY', 'KRW', 'PLN', 'RUB', 'SGD', 'UAH', 'USD', 'ZAR']

#counter variable for progress made
progress2 = 0
for fiat in fiatList:
   # get data for bitcoin price in that fiat
   URL = 'https://min-api.cryptocompare.com/data/histoday?fsym=BTC&tsym=' +fiat +'&allData=true' +url_api_part
   res = requests.get(URL)
   res_json = res.json()
   data = res_json['Data']
   # write required fields into csv
   with open('btc_prices.csv','w',encoding='utf-8') as f:
       f_writer = csv.writer(f,
                                     delimiter = ',',
                                     quotechar = '"',
                                     quoting=csv.QUOTE_MINIMAL)
       for day in data:
           rawts = day['time']
           ts = datetime.utcfromtimestamp(rawts).strftime('%Y-%m-%d %H:%M:%S')
           o = day['open']
           h = day['high']
           l = day['low']
           c = day['close']
           vfrom = day['volumefrom']
           vto = day['volumeto']
           entry = [ts, o, h, l, c, vfrom, vto, fiat]
           f_writer.writerow(entry)
   progress2 = progress2 + 1
   print('processed ' + str(fiat))
   print(str(progress2) + ' currencies out of  17 written')
print('Done getting price data for btc. See btc_prices.csv for result')

#5. Populate ETH prices in different fiat currencies

#counter variable for progress made
progress3 = 0
for fiat in fiatList:
   # get data for bitcoin price in that fiat
   URL = 'https://min-api.cryptocompare.com/data/histoday?fsym=ETH&tsym=' +fiat +'&allData=true' +url_api_part
   res = requests.get(URL)
   res_json = res.json()
   data = res_json['Data']
   # write required fields into csv
   with open('eth_prices.csv','w',encoding='utf-8') as f:
        f_writer = csv.writer(f,
                                     delimiter = ',',
                                     quotechar = '"',
                                     quoting=csv.QUOTE_MINIMAL)
        for day in data:
           rawts = day['time']
           ts = datetime.utcfromtimestamp(rawts).strftime('%Y-%m-%d %H:%M:%S')
           o = day['open']
           h = day['high']
           l = day['low']
           c = day['close']
           vfrom = day['volumefrom']
           vto = day['volumeto']
           entry = [ts, o, h, l, c, vfrom, vto, fiat]
           f_writer.writerow(entry)
   progress3 = progress3 + 1
   print('processed ' + str(fiat))
   print(str(progress3) + ' currencies out of  17 written')
print('Done getting price data for eth. See eth_prices.csv for result')