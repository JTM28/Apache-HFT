import asyncio
import re
import datetime as dt
import numpy as np
import sys
from numba import njit
from multiprocessing import Pool, Process
from timeit import default_timer as SW
from apache.databases.client import DBClient



@njit('f8(f8, f8, i4)')
def _1stDeriv(x1, x2, n):

    return ((x1 - x2) / n) * 100


class FlashCrash(object):

    def __init__(self, symbol, crypto_exchange, exchange_key):
        self.EXCHANGE = str(crypto_exchange)
        self.KEY = str(exchange_key)
        self.px_stack = []
        self.delta_stack = []
        self.std_stack = [[], []]          # [ [STDEV @ X], [dY(STDEV)] ]
        self.delta_sum = [[], []]          # [ [DELTA @ X], [dY(DELTA)] ]
        self.size_stack = [[], []]         # [ [SIZE  @ X], [SUM(SIZE)] ]
        self.updown_stack = [[], []]
        self.high_volatility = False
        self.high_momentum = False
        self.trending_up = False
        self.trending_down = False
        self.up_count = 0
        self.down_count = 0

        if re.search(r'\.' + self.KEY, str(symbol)):
            self.symbol = symbol

        else:
            self.symbol = symbol + '.' + self.KEY



    def TimeStart(self):
        self.start_time = dt.datetime.utcnow()
        self.start_clock = SW()


    async def Stream(self):

        with DBClient().client[self.EXCHANGE][self.symbol].watch(pipeline=None, max_await_time_ms=20) as stream:

            for update in stream:  # Pulls Each New Update From MongoDB Change streams Pipeline

                doc = update['fullDocument']
                self.px_stack.insert(0, np.float(doc['Px']))
                self.size_stack[0].insert(0, np.float(doc['Size']))

                if len(self.size_stack[0]) > 50:
                    self.size_stack[0].pop(50)
                    self.size_stack[1].insert(
                        0, np.round(np.sum(self.size_stack[0]), 5))

                    if len(self.size_stack[1]) > 50:
                        self.size_stack[1].pop(50)

                if len(self.px_stack) > 1:
                    self.delta_stack.insert(
                        0, np.round(np.multiply(
                            np.divide(np.subtract(self.px_stack[0], self.px_stack[1]), self.px_stack[1]), 100), 5))

                    if len(self.delta_stack) > 15:
                        self.delta_sum[0].insert(
                            0, np.round(np.sum(self.delta_stack[0:]), 5))
                        self.delta_stack.pop(15)

                        if len(self.delta_sum[0]) > 50:
                            self.delta_sum[0].pop(50)
                            self.delta_sum[1].insert(
                                0, np.round(_1stDeriv(
                                    np.float(self.delta_sum[0][0]), np.float(self.delta_sum[0][-1]), 50), 5))

                            if len(self.delta_sum[1]) > 50:
                                self.delta_sum[1].pop(50)

                        self.std_stack[0].insert(
                            0, np.std(self.delta_stack))

                        if len(self.std_stack[0]) > 50:
                            self.std_stack[0].pop(50)
                            self.std_stack[1].insert(
                                0, np.round(_1stDeriv(
                                    np.float(self.std_stack[0][0]), np.float(self.std_stack[0][-1]), 50), 5))

                            if len(self.std_stack[1]) > 50:
                                self.std_stack[1].pop(50)

                    if self.px_stack[0] > self.px_stack[1]:
                        self.up_count += 1
                        self.down_count = 0
                        self.updown_stack[0].insert(0, self.up_count)

                    elif self.px_stack[0] < self.px_stack[1]:
                        self.down_count -= 1
                        self.up_count = 0
                        self.updown_stack[0].insert(0, self.down_count)


                if len(self.updown_stack[0]) > 50:
                    self.updown_stack[0].pop(50)
                    self.updown_stack[1].insert(
                        0, np.sum(self.updown_stack[0]))

                    if len(self.updown_stack[1]) > 50:
                        self.updown_stack[1].pop(50)


                if len(self.px_stack) > 75:
                    self.px_stack.pop(75)


                print('SIZE: ', self.size_stack[0])
                print('SUM(SIZE): ', self.size_stack[1])
                print('PRICE: ', self.px_stack)
                print('\n')


                #
                # print('STDEV: ', self.std_stack[0])
                # print('dY(STDEV): ', self.std_stack[1])
                # print('DELTA: ', self.delta_sum[0])
                # print('dY(DELTA): ', self.delta_sum[1])
                # print('\n')










def Main(symbol):
    EXCHANGE = ''
    KEY = ''

    if re.search(r'\.BNB', str(symbol)):
        EXCHANGE = 'BINANCE-QUOTES'
        KEY = 'BNB'

        loop = asyncio.get_event_loop()
        loop.create_task(FlashCrash(symbol, EXCHANGE, KEY).Stream())
        loop.run_forever()



if __name__ == '__main__':
    _args = sys.argv[1]
    symbols_bnb_1 = ['LTCUSDT.BNB', 'ETHUSDT.BNB', 'XRPUSDT.BNB']
    symbols_bnb_2 = ['XLMUSDT.BNB', 'BNBUSDT.BNB', 'BTCUSDT.BNB']

    if _args == 'BNB1':

        with Pool(3) as cluster:
            cluster.map_async(Main, list(symbols_bnb_1))
            cluster.close()
            cluster.join()

    elif _args == 'BNB2':

        with Pool(3) as cluster:
            cluster.map_async(Main, list(symbols_bnb_2))
            cluster.close()
            cluster.join()







