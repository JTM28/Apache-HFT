import numpy as np
import pandas as pd
from time import time
from apache.crypto.compute.base_compute import ComputeEngine
from apache.databases.client import DBClient
from apache.crypto.trade_bot.clients.binance_client import BinanceClient


class BinanceBot(BinanceClient, ComputeEngine):

    def __init__(self, order):
        super().__init__()

        self.STATE = ''
        self.attrs = order
        self.MAX_ENTRY_WAIT = 20
        self.MAX_EXIT_WAIT = 90
        self.UPDATED_PX = 0.000
        self.ENTRY_PX = order['DATA']['PX']

        self.__call__()

    def matcher(self, symbol):
        collection = pd.DataFrame(
            list(DBClient().client['CRYPTO-EXCHANGES']['BINANCE'].find({'SYMBOL': str(symbol)})))
        precision = collection['PRECISION']

        return int(precision)


    def price_filter(self):
        px = self.attrs['DATA']['PX']

        if px > 1:
            PRECISION = 2
            self.precision = PRECISION

        else:
            PRECISION = 4
            self.precision = PRECISION

        return PRECISION


    def size_filter(self):
        pass


    def order_timer(self):
        self.start_time = time()


    def __call__(self):
        self.on_new_order()


    def on_new_order(self):
        self.place_entry()


    def place_entry(self):
        self.STATE = 'ENTRY'
        print('--PLACED ENTRY ORDER: [Symbol: %s | Price: %s]' %
              (str(self.attrs['SYMBOL']), str(self.attrs['DATA']['PX'])))

        if self.attrs['INFO']['TYPE'] == 'LIMIT':
            entry_order = self.BuyLimit(symbol=self.attrs['SYMBOL'],
                                        timeInForce='GTC',
                                        price=np.around(self.attrs['DATA']['PX'], 3),
                                        quantity=np.around(
                                            self.attrs['DATA']['SIZE'], self.matcher(self.attrs['SYMBOL'])))
            self.order_timer()
            self.monitor_entry(entry_order)


    def monitor_entry(self, order):
        order_amt = np.float(order['origQty'])
        fill_amt = 0

        while time() - self.start_time < self.MAX_ENTRY_WAIT:
            _order = self.GetOrder(symbol=order['symbol'], orderId=order['orderId'])
            fill_amt += np.float(_order['executedQty'])

            if fill_amt >= (order_amt * 0.995):
                self.on_entry_fill(fill_amt)

            else:

                if time() - self.start_time > 5.5 and self.start_time < 20:

                    if fill_amt == 0.0:
                        self.update_px()
                        pct_diff = self.PCT(self.attrs['DATA']['PX'], self.UPDATED_PX)

                        if pct_diff > 0.0025:
                            self.remove_order(order['orderId'])

                        else:
                            if pct_diff > 0.00025:
                                self.replace_order(order['orderId'])

                else:
                    if time() - self.start_time > 20:

                        if fill_amt == 0.0:
                            self.remove_order(order['orderId'])

                            break

                        else:
                            self.on_entry_fill(fill_amt)


    def on_entry_fill(self, fill_amt):
        self.place_exit(fill_amt)


    def place_exit(self, size):
        print('--PLACED EXIT ORDER: [Symbol: %s | Price: %s]' %
              (str(self.attrs['SYMBOL']), str(self.attrs['DATA']['PT'])))

        self.STATE = 'EXIT'
        order = self.SellLimit(symbol=self.attrs['SYMBOL'],
                               timeInForce='GTC',
                               price=self.attrs['DATA']['PT'],
                               quantity=np.around(size, self.matcher(self.attrs['SYMBOL'])))

        self.monitor_exit(order)


    def monitor_exit(self, order):
        order_amt = np.float(order['origQty'])
        fill_amt = 0

        while time() - self.start_time < self.MAX_EXIT_WAIT:
            _order = self.GetOrder(symbol=order['symbol'], orderId=order['orderId'])
            fill_amt += np.float(_order['executedQty'])

            if fill_amt >= (order_amt * 0.995):
                self.on_exit(_order)

            else:
                if time() - self.start_time > self.MAX_EXIT_WAIT * 0.25:
                    new_amt = self.SUBTRACT(order_amt, fill_amt)
                    self.replace_order(_order['orderId'], new_amt)


    def on_exit(self, order):
        print(order)


    def replace_order(self, orderID, amt):
        old_order = self.CancelOrder(symbol=self.attrs['SYMBOL'], orderId=orderID)
        self.recomp_limit(amt)

    def recomp_limit(self, amt):
        self.update_px()
        self.TARGET_PX = self.SplitSpread(self.UPDATED_PX, self.ENTRY_PX)
        self.place_exit(amt)


    def remove_order(self, orderID):
        print('--REMOVED ORDER: [Symbol: %s | Price: %s]' %
              (str(self.attrs['SYMBOL']), str(self.attrs['DATA']['PX'])))

        order = self.CancelOrder(symbol=self.attrs['SYMBOL'], orderId=orderID)
        self.on_removal(order)


    def on_removal(self, order):
        pass


    def update_px(self):
        x = self.GetLastPx(symbol=self.attrs['SYMBOL'])
        self.UPDATED_PX = np.round(np.float(x['price']), self.price_filter())







Order = {

    'ROUTE'    : np.str('ORDER'),
    'SYMBOL'   : np.str('BTCUSDT'),
    'EXCHANGE' : np.str('BINANCE'),

    'INFO': {
        'SIDE' : np.str('BUY'),
        'TYPE' : np.str('LIMIT'),
        'TIME' : np.str('GTC'),
        'LEN'  : np.int(45)},

    'DATA': {
        'PX'   : np.float(1010.00),
        'PT'   : np.float(1050.00),
        'SL'   : np.float(9900.00),
        'SIZE' : np.float(0.005),
        'TTL'  : np.int(300)}}

BinanceBot(Order)






