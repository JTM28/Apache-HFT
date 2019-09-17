import numpy as np
import asyncio
import pika
import json
from time import time
from numba import njit
from apache.databases.client import DBClient


@njit('f8(f8, f8)')
def Add(x1, x2):

    return x1 + x2


@njit('f8(f8, f8)')
def Subtract(x1, x2):

    return x1 - x2


@njit('f8(f8, f8)')
def Multiply(x1, x2):

    return x1 * x2


@njit('f8(f8, f8)')
def Divide(x1, x2):

    return x1 * x2


@njit('f8(f8, f8)')
def ROC(x1, x2):

    return ((x1 - x2) / x2) * 100


@njit('f8(f8[:])')
def MEAN(x_array):
    n = len(x_array)
    _sum = 0

    for i in range(n):
        _sum += x_array[i]

    return _sum / n


@njit('f8(f8, f8)')
def DEV(x1, x2):

    return np.log(x1 / x2)



class Reversion(object):

    def __init__(self, database, symbol, exchange=''):
        self.symbol = symbol
        self.AMT = 75
        self.open_position = False
        self.db = DBClient().client[database][symbol]
        self.hold = False

        self.quote_stack = [[], [], []]         # Struct: [ [EXCH-TIME], [PRICE], [SIZE] ]
        self.signal_stack = [[], [], [], []]    # Struct: [ [DEV(Px, EMA)], [ROLLSUM(DEV)], [IFT(ROLLSUM)], [SUM(IFT] ]


    @staticmethod
    def PublishOrder(order):

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', heartbeat=600, blocked_connection_timeout=500))

        channel = connection.channel()
        channel.exchange_declare(exchange='TRADE-ENGINE', exchange_type='direct')
        channel.basic_publish(
            exchange='TRADE-ENGINE',
            routing_key='T',
            body=json.dumps(order),
            properties=pika.BasicProperties(delivery_mode=1))
        connection.close()


    def StartTimer(self):
        self.start_timer = time()


    async def Stream(self):

        PIPELINE = [{
            '$match': {'fullDocument.Symbol': str(self.symbol)}}]

        with self.db.watch(pipeline=PIPELINE, max_await_time_ms=20) as stream:
            for update in stream:  # Pulls Each New Update From MongoDB Change streams Pipeline

                try:
                    doc = update['fullDocument']
                    self.quote_stack[0].insert(0, np.int(doc['EXCH-TIME']))
                    self.quote_stack[1].insert(0, np.float(doc['Px']))
                    self.quote_stack[2].insert(0, np.float(doc['Size']))



                except Exception as err:
                    print(str(err))


def Main(symbol):
    database = 'BINANCE-QUOTES'
    loop = asyncio.get_event_loop()
    loop.create_task(Reversion(database, symbol, exchange='BINANCE').Stream())
    loop.run_forever()




def Momentum(df):
    df['ROC'] = np.multiply(np.divide(np.subtract(df['Px'], df['Px'].shift(35)), df['Px'].shift(35)), 100)
    df['ROC-SMA-1'] = df['ROC'].rolling(10).mean()
    df['ROC-SMA-2'] = df['ROC'].rolling(20).mean()
    df['ROC-SMA-DIFF'] = np.log(np.divide(df['ROC-EMA-1'], df['ROC-EMA-2']))
    df = df.dropna().round(decimals=4)

    df['Signal'] = df['ROC-EMA-DIFF']

    return df