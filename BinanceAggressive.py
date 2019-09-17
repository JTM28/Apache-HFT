import asyncio
import numpy as np
import re
import pandas as pd
from numba import njit
from apache.databases.client import DBClient
from exchanges.Binance import Client



@njit("f8(f8, f8, f8, i4)")
def Deviation(x1, x2, x3, swap_direction):   # Bought/Sold Are Inverse Inputs
    amt = 100
    val_1 = amt / x1

    if swap_direction == 0:

        val_2 = val_1 * x2
        val_3 = val_2 * x3

        return val_3

    else:
        val_2 = val_1 / x2
        val_3 = val_2 * x3

        return val_3


def MatchQuery(symbol):
    collection = pd.DataFrame(list(DBClient().client['CRYPTO-EXCHANGES']['BINANCE'].find({'SYMBOL' : str(symbol)})))
    precision = collection['PRECISION']

    return int(precision)



class Triangular(object):

    def __init__(self, symbol_1, swap, symbol_3):
        self.symbol_1 = str(symbol_1).upper()
        self.swap_symbol = str(swap).upper()
        self.symbol_3 = str(symbol_3).upper()
        self.px_1 = 0
        self.px_swap = 0
        self.px_3 = 0
        self.client = Client()
        self.open_order = False
        self.order_cancels = 0
        self.count = 0
        self.prec_1 = MatchQuery(self.symbol_1)
        self.prec_swap = MatchQuery(self.swap_symbol)
        self.prec_3 = MatchQuery(self.symbol_3)


    async def AwaitOrder(self, trade_1, swap, trade_3):

        if self.open_order is False:
            self.open_order = True

            if trade_1 == self.symbol_1:
                size = np.around(np.divide(40, self.px_1), decimals=self.prec_1)

                try:
                    order = self.client.BuyMarket(symbol=trade_1, quantity=size)
                    print(order)

                    order2 = self.client.SellMarket(
                        symbol=swap, quantity=np.around(np.float(order['executedQty']), self.prec_swap))
                    print(order2)

                    order3 = self.client.SellMarket(
                        symbol=trade_3, quantity=np.around(np.float(order2['cummulativeQuoteQty']), self.prec_3))
                    print(order3)

                    self.open_order = False



                except Exception as error:
                    print(str(error))


            elif trade_1 == self.symbol_3:
                size = np.around(np.divide(40, self.px_3), decimals=self.prec_3)

                try:
                    order = self.client.BuyMarket(symbol=trade_1, quantity=size)
                    print(order)

                    order2 = self.client.BuyMarket(
                        symbol=swap, quantity=np.around(np.float(order['cummulativeQuoteQty']), self.prec_swap))
                    print(order2)

                    order3 = self.client.SellMarket(
                        symbol=trade_3, quantity=np.around(np.float(order2['executedQty']), self.prec_1))
                    print(order3)

                    self.open_order = False





                except Exception as error:
                    print(str(error))



    async def UpdateStream(self):
        with DBClient().client['BINANCE-QUOTES'].watch(pipeline=None, max_await_time_ms=1) as stream:
            for update in stream:   # Pulls Each New Update From MongoDB Change streams Pipeline
                doc = update['fullDocument']
                symbol = str(re.sub(r'\.BNB', '', str(doc['Symbol'])))

                if re.search(
                        str(self.symbol_1)+'|'+str(self.swap_symbol)+'|'+str(self.symbol_3), str(symbol)):

                    if symbol == str(self.symbol_1):
                        self.px_1 = np.float(doc['Px'])

                    elif symbol == str(self.swap_symbol):
                        self.px_swap = doc['Px']

                    elif symbol == str(self.symbol_3):
                        self.px_3 = np.float(doc['Px'])

                    if self.px_1 > 0 and self.px_swap > 0 and self.px_3 > 0:
                        dev1 = Deviation(np.float(self.px_1), np.float(self.px_swap), np.float(self.px_3), 0)
                        dev2 = Deviation(np.float(self.px_3), np.float(self.px_swap), np.float(self.px_1), 1)

                        if dev1 > 100.30:
                            await self.AwaitOrder(self.symbol_1, self.swap_symbol, self.symbol_3)

                        elif dev2 > 100.30:
                            await self.AwaitOrder(self.symbol_3, self.swap_symbol, self.symbol_1)


if __name__ == '__main__':
    strat = Triangular('XRPUSDT', 'XRPBNB', 'BNBUSDT')
    loop = asyncio.get_event_loop()
    loop.create_task(strat.UpdateStream())
    loop.run_forever()














