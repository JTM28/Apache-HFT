import numpy as np
from apache.databases.client import DBClient
from apache.crypto.compute.base_compute import ComputeEngine




class Stack(ComputeEngine):

    def __init__(self, config):
        super().__init__()
        self.dbclient = DBClient().client
        self.attrs = config
        self.MAX_STACK = self.attrs['MAX-STACK']

        self.Signals = {
            'BINANCE'  : [],
            'COINBASE' : []}


        self.Stack = {
            'BINANCE'  : {

                'PX'   : [],
                'SIZE' : [],
                'UNIX' : []},

            'COINBASE' : {

                'PX'   : [],
                'SIZE' : [],
                'UNIX' : []},

            'DIGIFINEX' : {

                'PX'   : [],
                'SIZE' : [],
                'UNIX' : []}
            }


    def InsertDoc(self, doc, key):
        self.Stack[key]['PX'].insert(0, np.float(doc['PX']))
        self.Stack[key]['SIZE'].insert(0, np.float(doc['SIZE']))
        self.Stack[key]['UNIX'].insert(0, doc['TIMESTAMP'])

        if len(self.Stack[key]['PX']) > self.MAX_STACK:
            self.Stack[key]['PX'].pop(self.MAX_STACK)
            self.Stack[key]['SIZE'].pop(self.MAX_STACK)
            self.Stack[key]['UNIX'].pop(self.MAX_STACK)



    def ResetQuery(self):
        self.Query = {
            'SYMBOL'   : 'BTCUSDT',
            'EXCHANGE' : '',
            'DATA'     : {
                'SMA'  : 0.0,
                'STD'  : 0.0}
        }


    async def Compute(self, key):
        self.ResetQuery()
        self.Query['EXCHANGE'] = str(key)
        self.Query['DATA']['SMA'] = self.MEAN(self.Stack[key]['PX'])
        self.Query['DATA']['STD'] = self.STDEV(self.Stack[key]['PX'])
        self.Query['DATA']['ROC'] = self.ROC(self.Stack[key]['PX'])

        print(self.Query)
        self.Signals[key].insert(0, self.Query)





class AsyncClient(Stack):

    def __init__(self, config):
        super().__init__(config)


    async def AwaitCompute(self, key):
        await self.Compute(key)



    async def AwaitQuote(self, doc):
        key = doc['EXCHANGE']
        self.InsertDoc(doc, key)

        if len(self.Stack[key]['PX']) > 5:
            await self.AwaitCompute(key)




