import numpy as np
import asyncio
import pandas as pd
import pika
import json
import datetime as dt
from time import time
from multiprocessing import Pool
from numba import njit, float64
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
def Delta(x1, x1_offset):

    return np.log(x1 / x1_offset) * 100


@njit('f8(f8)')
def IFT(x):

    return (np.exp(x * 2) - 1) / (np.exp(x * 2) + 1)


@njit('f8(f8, f8, i4)')
def Derive(x1, x2, n):

    return (x1 - x2) / n


@njit('f8(f8, f8)')
def Deviation(x, mean_x):

    return (np.log(x / mean_x)) * 100


@njit('f8(f8[:])')
def SUM(x_array):
    _sum = 0
    n = len(x_array)

    for i in range(n):
        _sum += x_array[i]


    return _sum


@njit("f8(f8[:])")
def MEAN(x_array):
    _mean = 0
    n = len(x_array)

    for i in range(n):
        _mean += x_array[i]

    _mean = (_mean / n)

    return _mean


@njit("f8[:](f8[:], i4)")
def EMA(x, n):
    ewma = []
    avg = SUM(x[0:n]) / n
    k = 2 / (1 + n)
    ema = (x[n] - avg) * k + avg

    for i in range(n, len(x)):
        ema = (x[i] - ema) * k + ema
        ewma.append(ema)

    return np.array(ewma, dtype=float64)


@njit('f8(f8)')
def ProfitTarget(px):

    return px * 1.002


@njit('f8(f8)')
def StopLoss(px):

    return px * 0.9910


@njit('f8(f8, f8)')
def LotSize(amt, px):

    return amt / px







class Reversion(object):

    def __init__(self, database, symbol, exchange=''):
        self.symbol = symbol
        self.AMT = 75
        self.open_position = False
        self.db = DBClient().client[database][symbol]
        self.hold = False

        self.quote_stack =  [[], [], []]         # Struct: [ [EXCH-TIME], [PRICE], [SIZE] ]
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

                    if len(self.quote_stack[0]) > 200:    # Controll Size Of Quote Stack
                        ser = pd.Series(self.quote_stack[1][::-1])
                        ema = ser.ewm(span=150).mean()
                        ema = ema.iloc[-1]
                        self.signal_stack[0].insert(
                            0, Deviation(np.float(self.quote_stack[1][0]), np.float(ema)))

                        if len(self.signal_stack[0]) > 20:
                            self.signal_stack[1].insert(
                                0, SUM(np.array(self.signal_stack[0], dtype=np.float)))

                            if len(self.signal_stack[1]) > 5:
                                self.signal_stack[2].insert(0, IFT(self.signal_stack[1][0]))

                                if len(self.signal_stack[2]) > 10:
                                    self.signal_stack[3].insert(
                                        0, SUM(np.array(self.signal_stack[2][0:5], dtype=np.float)))

                                    if (self.signal_stack[3][0] > self.signal_stack[3][1] >
                                            self.signal_stack[3][2] > -4.5 > self.signal_stack[3][3]):

                                        px = np.float(self.quote_stack[1][0])
                                        pt = ProfitTarget(px)
                                        sl = StopLoss(px)
                                        size = LotSize(np.float(self.AMT), px)

                                        Order = {
                                            'ROUTE'    : np.str('NEW-ORDER'),
                                            'SYMBOL'   : np.str(self.symbol),
                                            'EXCHANGE' : np.str('BINANCE'),

                                            'INFO'     : {
                                                'SIDE'     : np.str('BUY'),
                                                'TYPE'     : np.str('LIMIT'),
                                                'TIME'     : np.str('GTC'),
                                                'LEN'      : np.int(45)},

                                            'DATA'     : {
                                                'PX'       : np.float(px),
                                                'PT'       : np.float(pt),
                                                'SL'       : np.float(sl),
                                                'SIZE'     : np.float(size),
                                                'TTL'      : np.int(300)}}

                                        if self.hold is False:
                                            self.hold = True
                                            self.StartTimer()
                                            self.PublishOrder(Order)
                                            print('--NEW ORDER PLACED: [Symbol: %s | Time: %s]' %
                                                  (str(self.symbol), str(dt.datetime.utcnow())))

                                        else:
                                            if time() - self.start_timer > 30:
                                                self.hold = False


                                    if len(self.signal_stack[3]) > 10:
                                        self.signal_stack[3].pop(10)

                                    self.signal_stack[2].pop(10)

                                self.signal_stack[1].pop(5)

                            self.signal_stack[0].pop(20)

                        self.quote_stack[0].pop(200)
                        self.quote_stack[1].pop(200)
                        self.quote_stack[2].pop(200)



                except Exception as err:
                    print(str(err))


def Main(symbol):
    database = 'BINANCE-QUOTES'
    loop = asyncio.get_event_loop()
    loop.create_task(Reversion(database, symbol, exchange='BINANCE').Stream())
    loop.run_forever()



if __name__ == '__main__':


    symbols = ['BTCUSDT.BNB', 'XRPUSDT.BNB', 'XLMUSDT.BNB', 'LTCUSDT.BNB', 'BNBUSDT.BNB', 'ETHUSDT.BNB']

    with Pool(8) as cluster:
        cluster.map_async(Main, symbols)
        cluster.close()
        cluster.join()


