import numpy as np
import asyncio
import re
from time import time
from apache.crypto.compute.base_compute import ComputeEngine
from apache.databases.client import DBClient




class EventProcessor(object):

    def __init__(self, doc):
        self.doc = doc
        self.weight = 0


    def Filter(self):
        pass






class Strategy(ComputeEngine):

    def __init__(self):
        super().__init__()
        self.INTERVAL = 15
        self.ARRAY_SIZE = 250
        self.MIN_LEN = 12
        self.MAX_STACK = 100


        self.dbclient = DBClient().client

        self.shape = [0 for i in np.arange(250)]               # Initial Shape Of The Arrays
        self.px = np.array(self.shape, dtype=np.float)
        self.size = np.array(self.shape, dtype=np.float)
        self.timestamp = np.array(self.shape, dtype=np.float)

        '''
            Stack Layout
            
            OHLC - The Open, High, Low, Close Arrays Contain Each Resampled Period's OHLC Values
            
            DATA - This Contains All Signals And Recomputed Calculations Run On The Stack OHLC Values
            
                   TPP-STANDARD - The Normal Typical Price Point Using (H + L + C) / 3
                   
                   TPP-ADJUSTED - A Weighted Price Point W/ Emphasis On The O/C Values Holding 75% Of The Weight
                   
                   LOG-RANGE    - A Volatility Constant Measure Finding ln(High / Low)
                                
        '''

        self.Stack = {
            'OPEN'    :  [],
            'HIGH'    :  [],
            'LOW'     :  [],
            'CLOSE'   :  [],
            'VOLUME'  :  [],

            'DATA'  : {

                'TPP-STANDARD' : [],
                'TPP-ADJUSTED' : [],
                'LOG-RANGE'    : [],

                'DELTAS' : {
                    'CLOSE>OPEN' : [],       # C[i] > O[i]
                    'OPEN>'      : [],       # O[i] > O[i-1]
                    'HIGH>'      : [],       # H[i] > H[i-1]
                    'LOW>'       : []        # L[i] > L[i-1]
                },

                'GRADIENTS' : [[], [], []]   # [ dY(H[i-n:i]), dY(L[i-n:i]), dY(C[i-n:i])


            }}


    async def ResetArray(self):   # Resets The Arrays After Each Resample Period
        self.shape = [0 for i in np.arange(250)]
        self.px = np.array(self.shape, dtype=np.float)
        self.size = np.array(self.shape, dtype=np.float)
        self.timestamp = np.array(self.shape, dtype=np.float)


    async def Recompute(self):

        async def Deltas():
            self.Stack['DATA']['DELTAS']['CLOSE>OPEN'].insert(
                0, self.TestBool(self.Stack['CLOSE'], self.Stack['OPEN']))

        async def Gradients():
            self.Stack['DATA']['GRADIENTS'][0].insert(0, self.SLOPE(self.Stack['HIGH']))
            self.Stack['DATA']['GRADIENTS'][1].insert(0, self.SLOPE(self.Stack['LOW']))
            self.Stack['DATA']['GRADIENTS'][2].insert(0, self.SLOPE(self.Stack['CLOSE']))

        if len(self.Stack['CLOSE']) > 10:
            await Gradients()
        await Deltas()


        self.Stack['DATA']['TPP-STANDARD'].insert(
            0, self.NormTPP(
                self.Stack['OPEN'][0], self.Stack['HIGH'][0], self.Stack['LOW'][0]))

        self.Stack['DATA']['TPP-ADJUSTED'].insert(
            0, self.WeightedTPP(
                self.Stack['OPEN'][0], self.Stack['HIGH'][0], self.Stack['LOW'][0], self.Stack['CLOSE'][0]))

        self.Stack['DATA']['LOG-RANGE'].insert(
            0, self.LOGDIVIDE(self.Stack['HIGH'][0], self.Stack['LOW'][0]))


        if len(self.Stack['DATA']['LOG-RANGE']) > 50:
            self.Stack['DATA']['TPP-STANDARD'].pop(50)
            self.Stack['DATA']['TPP-ADJUSTED'].pop(50)
            self.Stack['DATA']['LOG-RANGE'].pop(50)


    async def Resample(self):
        px = [x for x in self.px if x != 0]
        size = [s for s in self.size if s != 0]
        _px = np.array(px, dtype=np.float)
        _size = np.array(size, dtype=np.float)

        self.Stack['OPEN'].insert(0, _px[0])
        self.Stack['HIGH'].insert(0, np.amax(_px))
        self.Stack['LOW'].insert(0, np.amin(_px))
        self.Stack['CLOSE'].insert(0, _px[-1])
        self.Stack['VOLUME'].insert(0, self.SUM(_size))

        print(self.Stack)

        await self.Recompute()

        if len(self.Stack['OPEN']) > self.MAX_STACK:
            self.Stack['OPEN'].pop(self.MAX_STACK)
            self.Stack['HIGH'].pop(self.MAX_STACK)
            self.Stack['LOW'].pop(self.MAX_STACK)
            self.Stack['CLOSE'].pop(self.MAX_STACK)

        await self.ResetArray()


    def Timer(self):
        self.timer = time()


    async def InsertArray(self, doc):
        self.px = np.insert(self.px[1:], -1, doc['PX'])
        self.size = np.insert(self.size[1:], -1, doc['SIZE'])
        self.timestamp = np.insert(self.timestamp[1:], -1, doc['TIMESTAMP'])



class Listener(Strategy):

    def __init__(self, symbol):
        super().__init__()
        self.endpoint = symbol


        # This Pipeline Will Pull The Quotes and Risk Events When Applicable
        self.PIPELINE = [{

            '$match': {

                '$or' : [{

                    '$and' : [

                        {'ns.db': 'BINANCE-QUOTES'},

                        {'fullDocument.SYMBOL' : str(symbol)}]},

                    {'$and' : [

                        {'ns.db'   : 'RISK-EVENTS'},

                        {'ns.coll' : 'SYSTEM-ALERT'}]}]}}]


    async def AwaitQuote(self, doc):
        await self.InsertArray(doc)



    async def WatchStream(self):
        self.Timer()

        with self.dbclient.watch(pipeline=self.PIPELINE, max_await_time_ms=20) as stream:

            for update in stream:  # Pulls Each New Update From MongoDB Change streams Pipeline

                try:
                    doc = update['fullDocument']

                    if re.search('SYSTEM-ALERT', str(doc)):  # Changes System State To Prevent Trades
                        self.InitFreezeState()

                    await self.AwaitQuote(doc)

                    if time() - self.timer > self.INTERVAL:
                        await self.Resample()
                        self.Timer()

                except Exception as err:
                    print(str(err))


def Run(symbol):

    loop = asyncio.get_event_loop()
    loop.create_task(Listener(symbol).WatchStream())
    loop.run_forever()


from multiprocessing import Pool

if __name__ == '__main__':

    symbol_list = ['BTCUSDT.BNB', 'XRPUSDT.BNB', 'XLMUSDT.BNB', 'ETHUSDT.BNB']

    with Pool(4) as cluster:
        cluster.map_async(Run, list(symbol_list))
        cluster.close()
        cluster.join()







