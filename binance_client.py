import hashlib
import hmac
import requests
import time
from operator import itemgetter



def ConvertTime(time_str, conversion_unit):
    time_str = str(time_str).split(':')
    hours = int(time_str[0]) * 60 * 60   # Convert Hours To Seconds
    minutes = int(time_str[1]) * 60   # Convert Minutes To Seconds
    seconds = float(time_str[2])

    if str(conversion_unit).upper() == 'S':

        return hours + minutes + seconds

    elif str(conversion_unit) == 'micro' or str(conversion_unit).upper() == 'US':

        return (hours + minutes + seconds) * 1e6



class BinanceAPIException(Exception):

    def __init__(self, response):
        self.code = 0
        try:
            json_res = response.json()
        except ValueError:
            self.message = 'Invalid JSON error message from Binance: {}'.format(response.text)
        else:
            self.code = json_res['code']
            self.message = json_res['msg']
        self.status_code = response.status_code
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):  # pragma: no cover
        return 'APIError(code=%s): %s' % (self.code, self.message)


class BinanceRequestException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'BinanceRequestException: %s' % self.message


class BinanceOrderException(Exception):

    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return 'BinanceOrderException(code=%s): %s' % (self.code, self.message)


class BinanceOrderMinAmountException(BinanceOrderException):

    def __init__(self, value):
        message = "Amount must be a multiple of %s" % value
        super(BinanceOrderMinAmountException, self).__init__(-1013, message)


class BinanceOrderMinPriceException(BinanceOrderException):

    def __init__(self, value):
        message = "Price must be at least %s" % value
        super(BinanceOrderMinPriceException, self).__init__(-1013, message)


class BinanceOrderMinTotalException(BinanceOrderException):

    def __init__(self, value):
        message = "Total must be at least %s" % value
        super(BinanceOrderMinTotalException, self).__init__(-1013, message)


class BinanceOrderUnknownSymbolException(BinanceOrderException):

    def __init__(self, value):
        message = "Unknown symbol %s" % value
        super(BinanceOrderUnknownSymbolException, self).__init__(-1013, message)


class BinanceOrderInactiveSymbolException(BinanceOrderException):

    def __init__(self, value):
        message = "Attempting to trade an inactive symbol %s" % value
        super(BinanceOrderInactiveSymbolException, self).__init__(-1013, message)


class BinanceWithdrawException(Exception):
    def __init__(self, message):
        if message == u'参数异常':
            message = 'Withdraw to this address through the flaskr first'
        self.message = message

    def __str__(self):
        return 'BinanceWithdrawException: %s' % self.message


class BinanceExceptions(BinanceWithdrawException,
                        BinanceOrderInactiveSymbolException,
                        BinanceRequestException,
                        BinanceAPIException):

    def __init__(self):
        pass



class BinanceClient(BinanceExceptions):
    API_URL = 'https://api.binance.com/api'
    WITHDRAW_API_URL = 'https://api.binance.com/wapi'
    WEBSITE_URL = 'https://www.binance.com'
    PUBLIC_API_VERSION = 'v1'
    PRIVATE_API_VERSION = 'v3'
    WITHDRAW_API_VERSION = 'v3'


    def __init__(self):
        self.API_KEY = 'ISlq2A0xmV0wRfcVHLzpr7CHc5kq6NnvvutWVv1CrGUZGaqLRcTv3aRMhgYF58b0'
        self.API_SECRET = 'veJPlhDTFdZerlAFl1bDKtPFR1wrW7taNeI2243nLeeO4C7wjQJXme4RKu2LBXkH'
        self.session = self._init_session()
        self._requests_params = None
        self.PING()  # Initial Ping Of Binance servers



    def _init_session(self):
        session = requests.session()
        session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'binance/python',
            'X-MBX-APIKEY': self.API_KEY})

        return session


    def _create_api_uri(self, path, signed=True, version=PUBLIC_API_VERSION):
        v = self.PRIVATE_API_VERSION if signed else version
        return self.API_URL + '/' + v + '/' + path


    def _create_withdraw_api_uri(self, path):
        return self.WITHDRAW_API_URL + '/' + self.WITHDRAW_API_VERSION + '/' + path


    def _create_website_uri(self, path):
        return self.WEBSITE_URL + '/' + path


    def _generate_signature(self, data):

        ordered_data = self._order_params(data)
        query_string = '&'.join(["{}={}".format(d[0], d[1]) for d in ordered_data])
        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()


    @staticmethod
    def _order_params(data):
        has_signature = False
        params = []

        for key, value in data.items():

            if key == 'signature':
                has_signature = True

            else:
                params.append((key, value))
        params.sort(key=itemgetter(0))       # Sort Params By Key

        if has_signature:
            params.append(('signature', data['signature']))

        return params

    def _request(self, method, uri, signed, force_params=False, **kwargs):
        kwargs['timeout'] = 10     # Default Request Timeout Val

        if self._requests_params:  # Global Request Params
            kwargs.update(self._requests_params)

        data = kwargs.get('data', None)

        if data and isinstance(data, dict):
            kwargs['data'] = data

            if 'requests_params' in kwargs['data']:
                kwargs.update(kwargs['data']['requests_params'])

                del(kwargs['data']['requests_params'])

        if signed:  # Generate Signed Hash Request
            kwargs['data']['timestamp'] = int(time.time() * 1000) - 1000 - 1000
            kwargs['data']['signature'] = self._generate_signature(kwargs['data'])

        if data:  # Sort The Post Request Params
            kwargs['data'] = self._order_params(kwargs['data'])

        # if get request assign data array to params value for requests lib
        if data and (method == 'get' or force_params):
            kwargs['params'] = kwargs['data']
            del(kwargs['data'])


        response = getattr(self.session, method)(uri, **kwargs)
        return self._handle_response(response)

    def _request_api(self, method, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        uri = self._create_api_uri(path, signed, version)

        return self._request(method, uri, signed, **kwargs)

    def _request_withdraw_api(self, method, path, signed=False, **kwargs):
        uri = self._create_withdraw_api_uri(path)

        return self._request(method, uri, signed, True, **kwargs)

    def _request_website(self, method, path, signed=False, **kwargs):

        uri = self._create_website_uri(path)

        return self._request(method, uri, signed, **kwargs)


    def _handle_response(self, response):

        if not str(response.status_code).startswith('2'):

            raise BinanceAPIException(response)

        try:
            return response.json()

        except ValueError:

            raise BinanceRequestException('Invalid Response: %s' % response.text)


    def GET(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):

        return self._request_api('get', path, signed, version, **kwargs)


    def POST(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):

        return self._request_api('post', path, signed, version, **kwargs)


    def PUT(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):

        return self._request_api('put', path, signed, version, **kwargs)


    def DELETE(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):

        return self._request_api('delete', path, signed, version, **kwargs)


    #------------------------------------------------------------------------------------------------------------------
    # API Endpoints For Exchange Related Calls
    #------------------------------------------------------------------------------------------------------------------
    def get_products(self):
        products = self._request_website('get', 'exchange/public/product')

        return products


    def GetSymbols(self, **params):

        return self.GET('ticker/price', data=params, version=self.PRIVATE_API_VERSION)


    def ExchangeInfo(self):

        return self.GET('exchangeInfo')


    def GetSymbolInfo(self, symbol):
        res = self.GET('exchangeInfo')

        for item in res['symbols']:

            if item['symbol'] == symbol.upper():

                return item

        return None


    def PING(self):

        return self.GET('ping')


    def ServerTime(self):

        return self.GET('time')


    def get_trades(self, **params):
        return self.GET('trades', version=self.PUBLIC_API_VERSION, data=params)


    def GetAllTickers(self):

        return self.GET('ticker/allPrices')


    # ------------------------------------------------------------------------------------------------------------------
    # Account Historical Activity Endpoints
    # ------------------------------------------------------------------------------------------------------------------
    def CreateOrder(self, **params):

        return self.POST('order', True, data=params)


    def CancelOrder(self, **params):

        return self.DELETE('order', True, data=params)


    def LimitOrder(self, timeInForce='GTC', **params):
        params.update({'type': 'LIMIT', 'timeInForce': timeInForce})

        return self.CreateOrder(**params)


    def BuyLimit(self, timeInForce='', **params):
        params.update({'side': 'BUY'})

        return self.LimitOrder(timeInForce=timeInForce, **params)


    def SellLimit(self, timeInForce='', **params):
        params.update({'side': 'SELL'})

        return self.LimitOrder(timeInForce=timeInForce, **params)


    def MarketOrder(self, **params):
        params.update({'type': 'MARKET'})

        return self.CreateOrder(**params)


    def BuyMarket(self, **params):
        params.update({'side': 'BUY'})

        return self.MarketOrder(**params)


    def SellMarket(self, **params):
        params.update({'side': 'SELL'})

        return self.MarketOrder(**params)


    def GetOrder(self, **params):

        return self.GET('order', True, data=params)


    def GetAllOrders(self, **params):

        return self.GET('allOrders', True, data=params)


    def GetOpenOrders(self, **params):

        return self.GET('openOrders', True, data=params)

    # ------------------------------------------------------------------------------------------------------------------
    # Account Values / Status Endpoints
    # ------------------------------------------------------------------------------------------------------------------
    def GetAccount(self, **params):

        return self.GET('account', True, data=params)


    def AssetValues(self, asset, **params):
        res = self.GetAccount(**params)

        if "balances" in res:

            for bal in res['balances']:

                if bal['asset'].lower() == asset.lower():

                    return bal

        return None


    def TradeHistory(self, **params):

        return self.GET('myTrades', True, data=params)


    def GetSystemStatus(self):

        return self._request_withdraw_api('get', 'systemStatus.html')


    def GetAccountStatus(self, **params):
        res = self._request_withdraw_api('get', 'accountStatus.html', True, data=params)

        if not res['success']:

            raise BinanceWithdrawException(res['msg'])

        return res


    def GetDustLog(self, **params):
        res = self._request_withdraw_api('get', 'userAssetDribbletLog.html', True, data=params)

        if not res['success']:

            raise BinanceWithdrawException(res['msg'])

        return res


    def GetFees(self, **params):
        res = self._request_withdraw_api('get', 'tradeFee.html', True, data=params)

        if not res['success']:

            raise BinanceWithdrawException(res['msg'])

        return res


    def GetAssetDetails(self, **params):
        res = self._request_withdraw_api('get', 'assetDetail.html', True, data=params)

        if not res['success']:

            raise BinanceWithdrawException(res['msg'])

        return res


    def Withdraw(self, **params):

        if 'asset' in params and 'name' not in params:
            params['name'] = params['asset']

        res = self._request_withdraw_api('post', 'withdraw.html', True, data=params)

        if not res['success']:

            raise BinanceWithdrawException(res['msg'])

        return res


    def GetLastPx(self, **params):

        return self.GET('ticker/price', data=params, version=self.PRIVATE_API_VERSION)

    # ------------------------------------------------------------------------------------------------------------------
    # Account Historical Activity Endpoints
    # ------------------------------------------------------------------------------------------------------------------
    def DepositHistory(self, **params):

        return self._request_withdraw_api('get', 'depositHistory.html', True, data=params)


    def WithdrawHistory(self, **params):

        return self._request_withdraw_api('get', 'withdrawHistory.html', True, data=params)


    def GetDepositAddress(self, **params):

        return self._request_withdraw_api('get', 'depositAddress.html', True, data=params)


    def GetWithdrawFee(self, **params):  # Requires Asset And RecvWindow

        return self._request_withdraw_api('get', 'withdrawFee.html', True, data=params)

    #------------------------------------------------------------------------------------------------------------------
    # Account Websocket Endpoints
    #------------------------------------------------------------------------------------------------------------------
    def StartAcctStream(self):  # Generates A Listen Key
        res = self.POST('userDataStream', False, data={})

        return res['listenKey']


    def PingAcctStream(self, listenKey):  # Requires A Listen Key
        params = {'listenKey': listenKey}

        return self.PUT('userDataStream', False, data=params)


    def CloseAcctStream(self, listenKey):  # Requires A Listen Key
        params = {'listenKey': listenKey}

        return self.DELETE('userDataStream', False, data=params)





