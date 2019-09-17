import asyncio
import numpy as np
from apache.databases.client import DBClient
from apache.crypto.compute.base_compute import ComputeEngine



'''  Frequency Based Signals  '''

class Stack(ComputeEngine):

    def __init__(self):
        super().__init__()
        self.dbclient = DBClient().client
        self.MAX_STACK = 256



        self.Stack = {

                'PX'   : [],
                'SIZE' : [],
                'UNIX' : [],

                'SIGNAL' : {}
        }


    def InsertDoc(self, doc):
        self.Stack['PX'].insert(0, np.float(doc['PX']))
        self.Stack['SIZE'].insert(0, np.float(doc['SIZE']))
        self.Stack['UNIX'].insert(0, doc['TIMESTAMP'])

        if len(self.Stack['PX']) > self.MAX_STACK:
            self.Stack['PX'].pop(self.MAX_STACK)
            self.Stack['SIZE'].pop(self.MAX_STACK)
            self.Stack['UNIX'].pop(self.MAX_STACK)



    def ResetQuery(self):
        self.Query = {
            'SYMBOL'   : 'BTCUSDT',
            'EXCHANGE' : '',
            'DATA'     : {
                'SMA'  : 0.0,
                'STD'  : 0.0}
        }


    async def Compute(self):
        self.ResetQuery()
        self.Query['DATA']['RANGE'] = np.log(np.divide(np.amax(self.Stack['PX']), np.amin(self.Stack['PX'])))
        self.Query['DATA']['SMA'] = self.MEAN(self.Stack['PX'])
        self.Query['DATA']['STD'] = self.STDEV(self.Stack['PX'])
        self.Query['DATA']['ROC'] = self.ROC(self.Stack['PX'])

        print(self.Query)





class AsyncClient(Stack):

    def __init__(self):
        super().__init__()


    async def AwaitCompute(self):
        await self.Compute()



    async def AwaitQuote(self, doc):
        key = doc['EXCHANGE']
        self.InsertDoc(doc)

        if len(self.Stack['PX']) > 5:
            await self.AwaitCompute()




class Listener(AsyncClient):

    def __init__(self):
        super().__init__()
        self.PX_1 = 0
        self.PX_2 = 0

    async def WatchStream(self):

        with self.dbclient['BINANCE-QUOTES']['BTCUSDT.BNB'].watch(pipeline=None, max_await_time_ms=20) as stream:
            for update in stream:  # Pulls Each New Update From MongoDB Change streams Pipeline

                try:
                    doc = update['fullDocument']
                    await self.AwaitQuote(doc)

                except Exception as err:
                    print(str(err))


def Run(symbol):

    SIGNAL_CONFIG = {

        'ENDPOINTS' : int(2),
        'MAX-STACK' : int(25),

        'PIPELINE': [{

            '$match':   {'fullDocument.SYMBOL': str(symbol) + '.BNB'}}]}

    loop = asyncio.get_event_loop()
    loop.create_task(Listener().WatchStream())
    loop.run_forever()


Run('BTCUSDT.BNB')




