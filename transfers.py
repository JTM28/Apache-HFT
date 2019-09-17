import requests
import json
from apache.crypto.trade_bot import CBProAuth
from apache.crypto.trade_bot.clients.binance_client import BinanceClient

MAP = {
    'COINBASE' : {
        'BTC' : '3BNP4x6aXiZMeAxXRk2kmfMrkjvtm3YEvF',
        'XRP' : ['rw2ciyaNshpHe7bCHo4bRWq6pqqynnWKQg', '2235177642'],
        'EOS' : ['coinbasebase', '688985969']},

    'BINANCE'  : {
        'BTC' : '1LHM8pKVXcutHC8tdLUgzLoLwpxdXDc9Yb',
        'XRP' : ['rEb8TK3gBgk5auZkwc6sHnwrGVJH8DuaLh', '107801222'],
        'EOS' : ['binancecleos', '106055137']},

    'KRAKEN'   : {
        'XRP' : ['rLHzPsX6oXkzU2qL12kHCH8G8cnZv1rBJh', '6485015'],
        'EOS' : ['krakenkraken', '1945921758']
    }}


class TransferBot(object):

    def __init__(self, order):
        self.EXCHANGE_1 = order['EXCHANGE-1']
        self.SYMBOL = order['SYMBOL']
        self.ASSET = str(self.SYMBOL)[0:3]
        self.EXCHANGE_2 = order['EXCHANGE-2']
        self.MAP = MAP
        self.attrs = order




        self.__call__()
    def __call__(self):
        self.on_buy()

    def on_buy(self):

        for key in self.MAP.keys():

            if key == self.EXCHANGE_2:
                for _key in self.MAP[key]:
                    if _key == self.SYMBOL:
                        if type(self.MAP[key][_key]) == list:
                            addr, memo = self.MAP[key][_key]

                        else:
                            addr = self.MAP[key][_key]
                            memo = None

                        if self.EXCHANGE_1 == 'BINANCE':
                            self.withdraw_binance(addr, memo)

                        elif self.EXCHANGE_1 == 'COINBASE':
                            self.withdraw_coinbase(addr, memo)


    def withdraw_binance(self, addr, memo=None):

        try:
            if memo is not None:
                BinanceClient().Withdraw(asset=self.ASSET, address=addr, addressTag=memo, amount=self.attrs['AMOUNT'])

            else:
                BinanceClient().Withdraw(asses=self.ASSET, address=addr, amount=self.attrs['AMOUNT'])

        except Exception as WithdrawError:
            print('--WithdrawError: [%s]' % str(WithdrawError))




    def withdraw_kraken(self, addr, memo):
        pass



    def withdraw_coinbase(self, addr, memo=None):
        URL = 'https://api.pro.coinbase.com/' + 'withdrawals/crypto'

        params = {'amount' : self.attrs['AMOUNT'], 'currency' : self.ASSET, 'crypto_address' : addr}

        if memo is not None:
            params['destination_tag'] = memo

        req = requests.post(URL, json.dumps(params), auth=CBProAuth())

        print(req.text)




CONFIG = {
    'EXCHANGE-1' : 'COINBASE',
    'EXCHANGE-2' : 'BINANCE',
    'SYMBOL'     : 'BTC',
    'AMOUNT'     : 0.0075}


TransferBot(CONFIG)



