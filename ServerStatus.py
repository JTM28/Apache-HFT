import asyncio
import re
import json
from apache.databases.client import DBClient




async def Stream():


    with DBClient().client['SERVER-LOGS'].watch(pipeline=None, max_await_time_ms=20) as stream:

        for update in stream:  # Pulls Each New Update From MongoDB Change streams Pipeline
                doc = update['fullDocument']
                print(doc)



def Main():
    loop = asyncio.get_event_loop()
    loop.create_task(Stream())
    loop.run_forever()



if __name__ == '__main__':
    Main()
