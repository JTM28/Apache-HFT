import asyncio
import numpy as np
import re
from apache.databases.client import DBClient
from apache.crypto.compute.base_compute import ComputeEngine




class Stack(ComputeEngine):

    def __init__(self):
        super().__init__()
        self.dbclient = DBClient().client
        self.MAX_STACK = 10


        self.Stack = {}


    async def NewTrade(self, key):
        lmt = self.Stack[key]['PX'][0] * 1.0001
        print(key, lmt)



    def InsertDoc(self, doc, key):

        if re.search(key, str(self.Stack.keys())):
            pass

        else:
            self.Stack[key] = {'PX' : [], 'SIZE' : [], 'UNIX' : []}

        self.Stack[key]['PX'].insert(0, np.float(doc['PX']))
        self.Stack[key]['SIZE'].insert(0, np.float(doc['SIZE']))
        self.Stack[key]['UNIX'].insert(0, doc['EXCH-TIMESTAMP'])

        if len(self.Stack[key]['PX']) > self.MAX_STACK:
            self.Stack[key]['PX'].pop(self.MAX_STACK)
            self.Stack[key]['SIZE'].pop(self.MAX_STACK)
            self.Stack[key]['UNIX'].pop(self.MAX_STACK)



    async def Compute(self, key):

        if len(self.Stack[key]['PX']) > 3:
            px = np.array(self.Stack[key]['PX'], dtype=np.float)
            pct_1 = self.PCT(px[0], px[1])
            pct_2 = self.PCT(px[1], px[2])

            if pct_1 + pct_2 < -0.15:
                await self.NewTrade(key)






class AsyncClient(Stack):

    def __init__(self):
        super().__init__()


    async def AwaitCompute(self, key):
        await self.Compute(key)



    async def AwaitQuote(self, doc):
        key = doc['SYMBOL']
        self.InsertDoc(doc, key)            # Inserts The Document Into The Stack Using The Symbol As The Key

        if len(self.Stack[key]['PX']) > 5:
            await self.AwaitCompute(key)    # Awaits The Insert Operation For Next Computation




class Listener(AsyncClient):

    def __init__(self):
        super().__init__()
        self.PX_1 = 0
        self.PX_2 = 0

    async def WatchStream(self):

        with self.dbclient['DIGIFINEX-QUOTES'].watch(pipeline=None, max_await_time_ms=20) as stream:
            for update in stream:  # Pulls Each New Update From MongoDB Change streams Pipeline

                try:
                    doc = update['fullDocument']
                    await self.AwaitQuote(doc)   # Calls The AwaitQuote In AsyncClient Class

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




