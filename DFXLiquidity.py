import numpy as np
import asyncio
from apache.databases.client import DBClient


class DFXBot:

    def __init__(self, db='', coll=''):
        self.Stream = DBClient().client['DIGIFINEX-QUOTES']['USDTBTC.DFX']
        self.quote_stack = [[], [], []]
        self.targets = [[], [], []]

    async def WatchStream(self):

        with self.Stream.watch(pipeline=None, max_await_time_ms=20) as stream:

            for update in stream:  # Pulls Each New Update From MongoDB Change streams Pipeline

                try:
                    doc = update['fullDocument']
                    self.quote_stack[0].insert(0, np.float(doc['BID-PX']))
                    self.quote_stack[1].insert(0, np.float(doc['LAST-PX']))
                    self.quote_stack[2].insert(0, np.float(doc['ASK-PX']))

                    self.targets[0].insert(
                        0, np.log(self.quote_stack[1][0] / self.quote_stack[0][0]))

                    self.targets[1].insert(
                        0, np.log(self.quote_stack[2][0] / self.quote_stack[1][0]))

                    self.targets[2].insert(
                        0, np.log(self.targets[0][0] / self.targets[1][0]) * 100)

                    if self.targets[2][0] > 0:
                        lmt_entry = (self.quote_stack[0][0] + self.quote_stack[1][0]) * 0.5
                        target = np.float(self.quote_stack[1][0]) * 1.000025

                        print(lmt_entry)
                        print(self.quote_stack[1][0])
                        print(target)
                        print('\n')






                except Exception as err:
                    print(str(err))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(DFXBot().WatchStream())
    loop.run_forever()


