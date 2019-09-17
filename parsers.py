import re
import numpy as np
import datetime as dt
from numba import njit
from time import time
from apache.databases.client import DBClient



class QuoteTemplate:

    """
        Template for orderbook standardization across all exchange formats
    """

    @staticmethod
    def main(symbol: str, exchange: str, exchange_time: str, px: float, size: float):

        unix_stamp = time()
        unix_string = re.sub(r'\.', '', str(unix_stamp))

        try:

            query = {'SYMBOL' : symbol, 'EXCHANGE' : exchange, 'UNIX-TIME' : time(), 'PX' : px, 'SIZE' : size,
                     'TIMESTAMP' : exchange_time}

            db = str(exchange).upper() + '-BOOK'
            coll = str(symbol).upper() + '.QUOTE'

            print(query)

            DBClient().client[db][coll].insert(query)

        except KeyError as DroppedQuote:
            error = DroppedQuote

        except Exception as Error:
            error = Error




class BinanceQuote(object):

    @staticmethod
    def on_quote(msg):
        symbol = str(msg['s']) + '.BNB'
        QuoteTemplate.main(symbol, 'BINANCE', str(msg['T']), float(msg['p']), float(msg['q']))



class CoinbaseQuote(object):

    @staticmethod
    def on_quote(msg):
        try:
            symbol = re.sub(r'-', '', str(msg['product_id'])) + '.CB'
            QuoteTemplate.main(symbol, 'COINBASE', str(msg['time']), float(msg['price']), float(msg['size']))

        except Exception: pass



class Digifinex(object):

    @staticmethod
    def on_quote(msg):
        try:
            data = msg['params']
            symbol = re.sub(r'_', '', str(data[2])) + '.DFX'
            QuoteTemplate.main(symbol, 'DIGIFINEX', str(data[1][0]['time']), float(data[1][0]['price']),
                               float(data[1][0]['amount']))

        except Exception: pass


class KrakenQuote(object):

    @staticmethod
    def on_quote(msg):

        if len(msg) == 4:
            try:
                if len(msg[1]) == 1:
                    symbol = re.sub('/', '', str(msg[-1])) + '.KRN'
                    QuoteTemplate.main(symbol, 'KRAKEN', str(msg[1][0][2]), float(msg[1][0][0]), float(msg[1][0][1]))

            except Exception as NullError:
                pass


class OkexQuote(object):

    @staticmethod
    def on_quote(msg):
        try:
            symbol = str(re.sub('-', '', str(msg['instrument_id']))) + '.OKX'
            QuoteTemplate.main(symbol, 'OKEX', msg['timestamp'], msg['last'], None)

        except Exception as NullError:
            pass



