import numpy as np
from time import time
from apache.crypto.aggregators import BaseAggregator


class TimeAggregator(BaseAggregator):

    def __init__(self, **kwargs):
        BaseAggregator.__init__(self, **kwargs)


    async def aggregator(self, symbol):
        """
            Performs OHLC/V + Quote Count + TPP and returns all locals() to the insert_quote() method.

                quotes = list(Imagine Quotes Here In The List)

               * open, high, low, close = quotes[0], max(list(quotes)), min(list(quotes)), quotes[-1]

               * high, low = max[:], min[:]

               All

        :param symbol: type(str) - The symbol retrieved from the quote stamp to act as first layer key

        :return: (open, high, low, close, volume, quotes, tpp) * In locals() dict form and sent to insert_stack()
        """

        try:
            open, close = self.quotes[symbol]['price'][0], self.quotes[symbol]['price'][-1]
            high, low = np.amax(self.quotes[symbol]['price']), np.amin(self.quotes[symbol]['price'])
            volume = self.SUM(np.array(self.quotes[symbol]['size'], dtype=np.float))
            quotes, tpp = len(self.quotes[symbol]['price']), self.NORMTPP(high, low, close)
            timestamp = time()
            self.mutable_dicts('quote', symbol)
            self.insert_stack(locals())

        except Exception as TimeseriesError:
            print('TimeseriesAggregatorError: %s' % str(TimeseriesError))


    async def resample(self):

        for key in self.ohlc.keys():
            await self.aggregator(key)



    async def on_quote(self, msg):
        """
            Calls the async resample() method which kicks off the timeseries aggregator

        :return: None - awaits self.resample() method
        """
        self.insert_quote(msg)



class StatAggregator(object):

    def __init__(self):
        pass

