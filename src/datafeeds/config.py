from pprint import pprint
from src.backends._mongo.mongo_client import DBClient


"""
    Datastream Configuration Format
    
    ID: type(str) - Should always be the exchange id followed by data ex.(EXCHANGE-DATA)
    
    WEBSOCKET: type(dict) - Contains all of the information for running the websocket
    
          Mandatory Parameters
            -- URL: type(str) - The URL that the data will be received from
            
          Optional Parameters
            -- URL-ITERABLE: type(str) - For URL's that need an attachment on the end. For example 
                                         this is the main url for the binance exchange...        
                                         wss://stream.binance.com:9443/ws/ however simply connecting
                                         to this will not return any data. So instead, you need to add
                                         the /ws/btcusdt@trade/ for example to receive quotes for btcusdt.
                                         So if you want to do that with multiple symbols without creating 
                                         multiple URLs, Insert the iterable attachment along with the
                                         iterable key ex.(If you want to do this with symbols, the value 
                                         of the iterable key should be the same as the key holding the symbols.
                                         
            -- ITERABLE-KEY: type(str) - Explained above and here is an example.
                                         {'SYMBOLS' : [BTCUSDT, XRPUSDT, EOSUSDT], 'ITERABLE-KEY' : 'SYMBOLS'}
                                             
"""


BIBOX_CONFIG = {
    'ID' : str('BIBOX-DATA'),

    'WEBSOCKET' : {
        'URL'         : str('wss://push.bibox.com/'),
        'PARAMS'      : {'event' : 'addChannel', 'channel' : 'bibox_sub_spot_ALL_ALL_market'}},


    'CLUSTER' : {
        'EXCHANGE'    : str('BIBOX-EXCHANGE'),
        'QUEUE'       : str('BIBOX-QUEUE'),
        'NODES': int(2)}}


BINANCE_CONFIG = {
    'ID' : str('BINANCE-DATA'),

    'WEBSOCKET' : {
        'URL'         : str('wss://stream.binance.com:9443/ws/'),
        'URL-ITERABLE': {'STR' : str('@trade/'), 'POSITION' : -1, 'KEY' : 'SYMBOLS'},
        'SYMBOLS'     : [
            'btcusdt', 'ethusdt', 'xrpusdt', 'bnbusdt', 'xlmusdt', 'ltcusdt', 'fetusdt', 'eosusdt', 'tusdusdt',
            'usdcusdt', 'ontusdt', 'neousdt', 'paxusdt', 'nulsusdt']},

    'CLUSTER' : {
        'EXCHANGE'    : str('BINANCE-EXCHANGE'),
        'QUEUE'       : str('BINANCE-QUEUE'),
        'NODES': int(4)}}


BITSTAMP_CONFIG = {
    'ID'           : str('BITSTAMP-QUOTE'),
    'URL'          : str("wss://ws.bitstamp.net"),
    'URL-ITERABLE' : str('live_trades_'),
    'ITERABLE-KEY' : str('SYMBOLS'),
    'PARAMS'       : {'event': str('bts:subscribe'), 'data': {'channel': ''}},
    'NODES'        : int(2),
    'SEARCH-KEY'   : str('match'),
    'CLUSTER'      : str('BITSTAMP'),
    'QUEUE'        : str('BTS-QUEUE'),
    'ROUTE-KEY'    : str('BTS'),
    'SYMBOLS': ['btcusd', 'ethusd', 'xrpusd', 'xrpbtc', 'ethbtc']}


COINBASE_CONFIG = {
    'ID' : str('COINBASE-DATA'),

    'WEBSOCKET' : {
        'URL'        : str("wss://ws-feed.pro.coinbase.com"),
        'SEARCH-KEY' : str('match'),
        'PARAMS'     : {'type': str('subscribe'), 'channels' : [{'name' : 'ticker',
                        'product_ids' : ['BTC-USD',
                                         'ETH-USD',
                                         'XRP-USD',
                                         'EOS-USD',
                                         'ETH-BTC',
                                         'EOS-BTC',
                                         'XRP-BTC',
                                         'LTC-BTC']}]}},
    'CLUSTER' : {
        'EXCHANGE'   : str('COINBASE-EXCHANGE'),
        'QUEUE'      : str('COINBASE-QUEUE'),
        'NODES'      : int(4)}}

COINTIGER_CONFIG = {
    'ID' : str('COINTIGER-DATA'),

    'WEBSOCKET' : {
        'URL'        : str("wss://api.cointiger.com/exchange-market/ws"),
        'SEARCH-KEY' : str('BYTE'),
        'PARAMS'     : {'event': str('sub'), 'params' : {
            "channel" : "market_btcusdt_trade_ticker",
            "cb_id"   : "customize",
            "asks"    : 150,
            "bids" : 150}}},
    'CLUSTER' : {
        'EXCHANGE'   : str('COINBASE-EXCHANGE'),
        'QUEUE'      : str('COINBASE-QUEUE'),
        'NODES'      : int(4)}}


DIGIFINEX_CONFIG = {
    'ID' : str('DIGIFINEX-DATA'),

    'WEBSOCKET' : {
        'URL'        : str("wss://openapi.digifinex.com/ws/v1/"),
        'SEARCH-KEY' : 'BYTE',
        'DEFLATE'    : 'zlib',
        'PARAMS'     : {'id' : '12312', 'method' : 'trades.subscribe', 'params' : ['ETH_USDT', 'BTC_USDT']}},

    'CLUSTER' : {
        'EXCHANGE'   : str('DIGIFINEX-EXCHANGE'),
        'QUEUE'      : str('DIGIFINEX-QUEUE'),
        'NODES'      : int(4)}}


HITBTC_CONFIG = {
    'ID': str('HITBTC-DATA'),

    'WEBSOCKET': {
        'URL': str('wss://api.hitbtc.com/api/2/ws'),
        'PARAMS' : {'method' : 'subscribeTrades', 'params' : {'symbol' : 'ETHBTC', 'limit' : 100}, 'id' : 123}
    },


    'CLUSTER': {
        'EXCHANGE': str('HITBTC-EXCHANGE'),
        'QUEUE': str('HITBTC-QUEUE'),
        'NODES': int(2)}}


IDAX_CONFIG = {
    'ID': str('IDAX-DATA'),

    'WEBSOCKET': {
        'URL': str('wss://openws.idax.pro/ws'),
        'SEARCH-KEY' : 'BYTE',
        'DEFLATE'    : 'gzip',
        'PARAMS': [
            {'event' : 'addChannel', 'channel' : 'idax_sub_eth_btc_ticker'},
            {'event' : 'addChannel', 'channel' : 'idax_sub_btc_usdt_ticker'},
            {'event' : 'addChannel', 'channel' : 'idax_sub_eth_usdt_ticker'}
        ]},


    'CLUSTER': {
        'EXCHANGE': str('IDAX-EXCHANGE'),
        'QUEUE': str('IDAX-QUEUE'),
        'NODES': int(2)}}


class WebsocketConfig:

    DB = DBClient().client['NETWORKS']['datafeeds']

    def check_name(self, name):
        doc = self.DB.find_one({'ID' : name})

        if doc:

            return True

        return False


    def add_config(self, config):

        assert self.check_name(config['ID']) is False, 'The ID has already been selected'

        self.DB.insert_one(config)
        print('--Added Config')


    def update_config(self, doc):
        _id = doc['ID']
        self.DB.find_one_and_update(filter={'ID' : _id}, update={'$set' : doc}, upsert=True)
        print('--Updated Config: ')


    def view_datafeeds(self):
        docs = self.DB.find()
        for doc in docs:
            for key in doc.keys():
                pprint(doc[key])

            print('\n')



