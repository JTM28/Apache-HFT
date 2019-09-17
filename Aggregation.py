import asyncio
import re
import datetime as dt
import numpy as np
from multiprocessing import Pool, Process
from timeit import default_timer as SW
from apache.databases.client import DBClient



class StreamStats(object):

    def __init__(self, symbol, crypto_exchange, exchange_key):
        self.EXCHANGE = str(crypto_exchange)
        self.KEY = str(exchange_key)
        self.px_stack = []
        self.size_stack = []
        self.quote_count = 0
        self.db = DBClient().client[self.EXCHANGE + '-STATS']
        self.queries = []

        if re.search(r'\.' + self.KEY, str(symbol)):
            self.symbol = symbol
            self.insert = re.sub(r'\.' + self.KEY, '-STAT.' + self.KEY, str(symbol))

        else:
            self.symbol = symbol + '.' + self.KEY
            self.insert = symbol + '-STAT' + self.KEY

        self.db = self.db[self.insert]

    def TimeStart(self):
        self.start_time = dt.datetime.utcnow()
        self.start_clock = SW()


    async def Block30s(self, px, size):
        px = np.array(px, dtype=np.float)
        size = np.array(size, dtype=np.float)

        Query = {
            'BLOCK-CODE'  : int(30),
            'Block-Start' : str(self.start_time),
            'Block-End'   : str(dt.datetime.utcnow()),
            'QUOTE-COUNT' : int(self.quote_count),
            'AVG-PX'      : float(np.mean(px)),
            'STDEV-PX'    : float(np.std(px)),
            'OPEN'        : float(px[-1]),
            'HIGH'        : float(np.amax(px)),
            'LOW'         : float(np.amin(px)),
            'CLOSE'       : float(px[0]),
            'VOLUME-SUM'  : float(np.sum(size)),
            'AVG-VOLUME'  : float(np.mean(size)),
            'DELTA-TICK'  : float(np.mean(np.diff(px))),
            'VOLATILITY'  : float(np.multiply(np.log(np.divide(np.amax(px), np.amin(px))), 100))}

        self.db.insert(Query)
        self.queries.insert(0, Query)


    async def Stream(self):
        self.TimeStart()
        is_running = False

        with DBClient().client[self.EXCHANGE][self.symbol].watch(pipeline=None, max_await_time_ms=20) as stream:

            for update in stream:  # Pulls Each New Update From MongoDB Change streams Pipeline

                if is_running is False:
                    print('--Starting Aggregation Stream')
                    is_running = True

                self.quote_count += 1

                try:
                    doc = update['fullDocument']
                    self.px_stack.insert(0, np.float(doc['PX']))
                    self.size_stack.insert(0, np.float(doc['SIZE']))

                except Exception as error:
                    print('--ERROR IN BINANCE STAT AGGREGATOR: [%s]' % str(error))


                if SW() - self.start_clock > 30:

                    if len(self.px_stack) > 1 and len(self.size_stack) > 1:

                        await self.Block30s(self.px_stack, self.size_stack)
                        self.px_stack.clear()
                        self.size_stack.clear()
                        self.quote_count = 0
                        self.TimeStart()

                    else:
                        raise Exception('--NO QUOTES AGGREGATED IN CURRENT BLOCK FOR SYMBOL: %s' % str(self.symbol))



def Main(symbol):
    EXCHANGE = 'BINANCE-QUOTES'
    KEY = 'BNB'
    loop = asyncio.get_event_loop()
    task = asyncio.run_coroutine_threadsafe(StreamStats(symbol, EXCHANGE, KEY).Stream(), loop)
    loop.run_forever()



if __name__ == '__main__':
    symbols = ['BTCUSDT.BNB', 'XRPUSDT.BNB', 'XLMUSDT.BNB', 'LTCUSDT.BNB', 'BNBUSDT.BNB', 'ETHUSDT.BNB']

    with Pool() as cluster:
        cluster.map_async(Main, symbols)
        cluster.close()
        cluster.join()

