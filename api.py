import asyncio
from apache.crypto.aggregators.agg_stream import Stream



class AggregationAPI:

    def __init__(self):
        self.params = {}


    def start_timeseries(self, **kwargs):
        pass




def main():
    loop = asyncio.get_event_loop()
    loop.create_task(Stream(_type='timeseries', resample=15).__call__())
    loop.run_forever()


if __name__ == '__main__':
    main()