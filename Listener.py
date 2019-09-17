import asyncio
from apache.crypto.trade_bot.streams import AsyncClient




class Listener(AsyncClient):

    def __init__(self, config):
        super().__init__(config)
        self.attrs = config
        self.PX_1 = 0
        self.PX_2 = 0

    async def WatchStream(self):

        with self.dbclient.watch(pipeline=self.attrs['PIPELINE'], max_await_time_ms=20) as stream:
            for update in stream:  # Pulls Each New Update From MongoDB Change streams Pipeline

                try:
                    doc = update['fullDocument']
                    await self.AwaitQuote(doc)

                except Exception as err:
                    print(str(err))




class CrossExchange(Listener):

    def __init__(self, config):
        super().__init__(config)



def Run(symbol):


    ARB_CONFIG_BASE = {

        'ENDPOINTS' : int(2),
        'MAX-STACK' : int(25),

        'PIPELINE': [{

            '$match':   {'$or': [

                        {'fullDocument.SYMBOL': str(symbol) + '.BNB'},

                        {'fullDocument.SYMBOL': str(symbol)[:-1] + '.CB'},

                        {'fullDocument.SYMBOL' : str(symbol) + '.DFX'},

                        {'fullDocument.SYMBOL' : str(symbol) + '.KRN'}]}}]

    }

    loop = asyncio.get_event_loop()
    loop.create_task(CrossExchange(ARB_CONFIG_BASE).WatchStream())
    loop.run_forever()


import sys
if __name__ == '__main__':
    _arg = sys.argv[1]

    Run(_arg)







