import ast
from apache.crypto.trade_bot.clients import digifinex_client, binance_client
from apache.databases.client import DBClient




class BinanceInfo(binance_client.BinanceClient):

    def __init__(self):
        pass



    def GetExchangeInfo(self):
        client = self.ExchangeInfo()
        tz = client['timezone']
        server_time = client['serverTime']
        rate_limits = client['rateLimits']
        symbol_info = client['symbols']

        n = len(symbol_info)

        for i in range(n):
            x = symbol_info[i]
            symbol = x['symbol']
            status = x['status']
            base_asset = x['baseAsset']
            quote_asset = x['quoteAsset']
            filters = x['filters']

            # Price Filters
            price_filter = filters[0]
            min_price = price_filter['minPrice']
            max_price = price_filter['maxPrice']
            tick_size = price_filter['tickSize']

            # Lotsize Filters
            lotsize_filter = filters[2]
            min_qty = lotsize_filter['minQty']
            max_qty = lotsize_filter['maxQty']
            step_size = float(lotsize_filter['stepSize'])

            notional_filters = filters[3]
            min_notional = notional_filters['minNotional']
            precision = 0.0

            if step_size == 1.0:
                precision = 0

            elif step_size == 0.1:
                precision = 1

            elif step_size == 0.01:
                precision = 2

            elif step_size == 0.001:
                precision = 3

            elif step_size == 0.0001:
                precision = 4

            elif step_size == 0.00001:
                precision = 5

            elif step_size == 0.000001:
                precision = 6

            elif step_size == 0.0000001:
                precision = 7


            Query = {
                'SYMBOL'     : str(symbol),
                'EXCHANGE'   : str('BINANCE'),
                'MIN-PX'     : float(min_price),
                'MAX-PX'     : float(max_price),
                'TICK-SIZE'  : float(tick_size),
                'MIN-QTY'    : float(min_qty),
                'MAX-QTY'    : float(max_qty),
                'PRECISION'  : int(precision),
                'MIN-NOTION' : float(min_notional)}


        # DBClient().client['CRYPTO-EXCHANGES']['BINANCE'].insert(Query)



class DigifinexInfo(digifinex_client.DigifinexClient):

    def __init__(self):
        super().__init__()
        self.db = DBClient().client['CRYPTO-EXCHANGES']['DIGIFINEX']



    def InfoQuery(self, key, data):
        key_head, key_tail = str(key).split('_')
        key = str(key_tail + key_head).upper() + '.DFX'

        Query = {
            'SYMBOL'   : str(key),
            'AMT-PREC' : data[0],
            'PX-PREC'  : data[1],
            'MIN-AMT'  : data[2],
            'MIN-CASH' : data[3]}

        self.db.insert(Query)





    def RetrieveInfo(self):
        req = self.GET(self.URL_ATTACH['TRADE-INFO'])
        req_dict = ast.literal_eval(req)
        data = req_dict['data']

        for key in data.keys():
            self.InfoQuery(key, data[key])

        print('--INSERTED ALL EXCHANGE INFO')

