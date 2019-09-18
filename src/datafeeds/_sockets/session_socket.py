import requests
import json
import ast
import re
from threading import Thread



class HTTPSocket(object):

    """
        The HTTPSocket class is used for handling datastreams from exchanges and feed sources
        that do not support long lived TCP connections into a WebSocketServer. For configuring
        the socket, see the configuration file for more info.

        This class will only be inherited into the BaseWebsocket class if the configuration file
        contains the field HTTP and it is set to True {"HTTP" : True}.
    """


    def __init__(self, attrs):
        self.attrs = attrs
        self.attr_keys = []
        self.threaded_urls = []
        self.__getattr__()


    def __getattr__(self):
        """
            Set attributes for all configuration keys as instance variables

                * Any dict with > 3 layers will have the 3 layer key set as instance.

        :return: None - Starts a thread to the method start_session()
        """

        for key in self.attrs.keys():
            if isinstance(self.attrs[key], dict):
                for _key in self.attrs[key].keys():
                    if isinstance(self.attrs[key][_key], dict):
                        for __key in self.attrs[key][_key].keys():
                            self.attr_keys.append(__key)
                            setattr(HTTPSocket, str(__key), str(self.attrs[key][_key][__key]))

                    self.attr_keys.append(_key)
                    setattr(HTTPSocket, str(_key), str(self.attrs[key][_key]))
            else:
                self.attr_keys.append(key)
                setattr(HTTPSocket, str(key), str(self.attrs[key]))

        self.start_session()

    def start_session(self):

        if hasattr(HTTPSocket, 'THREAD-URL'):
            if hasattr(HTTPSocket, 'THREAD-KEY'):

                for key in self.attrs.keys():
                    if getattr(HTTPSocket, 'THREAD-KEY') == key:
                        self.thread_keys = self.attrs[key]

                    else:
                        if isinstance(self.attrs[key], dict):

                            for _key in self.attrs[key]:
                                if getattr(HTTPSocket, 'THREAD-KEY') == _key:
                                    self.thread_keys = self.attrs[key][_key]

                                    if hasattr(HTTPSocket, 'URL-ATTACH'):
                                        if isinstance(self.thread_keys, list):

                                            for keyval in self.thread_keys:
                                                if hasattr(HTTPSocket, 'ADD-ADJ'):
                                                    adj = ast.literal_eval(getattr(HTTPSocket, 'ADD-ADJ'))
                                                    _key_, _type_ = adj['ADJ-KEY'], adj['ADJ-TYPE']

                                                    if int(_type_) == int(-1):
                                                        if re.search(r'_', str(keyval)):
                                                            adj_key = str(keyval).split('_')
                                                            _adj = adj_key[1] + adj_key[0]

                                                            url = getattr(HTTPSocket, 'URL') + str(keyval) + \
                                                                  getattr(HTTPSocket, 'URL-ATTACH')

                                                            t = Thread(target=self.create_session(url, _adj))
                                                            t.start()


    def create_session(self, url, symbol):

        """
            Creates a new session using HTTP 1.1 request to mimic a tcp websocket connection.
            Since 99% of the time any exchange feed that requires you to make API requests for
            data will most likely have it set up so you can only request one symbol, it is
            assumed you will need a threaded socket to stream multiple symbols, however if
            it is not required you can skip all of the configuring for threaded connections.

        :param url: type(str) - The COMPLETED url you will make the http request to
                        * This means if you need to add something to the url do it before here
        :param symbol: type(str) - The symbol that

        :return: None - Calls the on_open() method
        """

        print('--HTTP WEBSOCKET: [Creating HTTPS 1.1 Websocket Session]')
        self.session = requests.Session()
        self.on_open(url, symbol)


    def on_open(self, url, symbol):
        print('--HTTP WEBSOCKET: [Opening Digifinex Websocket]')
        self.keep_alive(url, symbol)


    def on_error(self, error, url, symbol):
        print('--HTTP WEBSOCKET ERROR: [Digifinex Websocket For Symbol]')
        print('--ERROR: [%s]' % str(error))

        def GO():
            try:
                self.keep_alive(url, symbol)

            except Exception as null:
                pass

        try:
            self.keep_alive(url, symbol)

        except Exception as error:
            GO()


    def keep_alive(self, url, symbol):
        self.running = True

        while self.running is True:

            try:
                self.ws = json.loads(self.session.get(url=url).text)
                data = self.ws

                print(data)
                # if re.search('bids', str(data)):
                #     self.new_orderbook(symbol, data)
                #
                # elif re.search(r'data', str(data.keys())):
                #     data = data['data']
                #     data = re.sub(r'(\[{)', '{', str(data))
                #     data = re.sub(r'(}\])', '}', str(data))
                #     data = ast.literal_eval(data)
                #     data['symbol'] = symbol
                #     data['ROUTE'] = 'QUOTE|' + self.attrs['SERVER-EXCHANGE']
                #
                #     publish(
                #         exchange=getattr(HTTPSocket, 'EXCHANGE'),
                #         route_key=balancer(getattr(HTTPSocket, 'ROUTE-KEY'), getattr(HTTPSocket, 'NODES')),
                #         msg=json.dumps(data))
                #
                # sleep(0.1)

            except Exception as error:
                self.on_error(str(error), url, symbol)

    def new_orderbook(self, symbol, data):
        bids = data['bids']
        asks = data['asks']
        doc = {'s' : symbol, 'b' : bids, 'a' : asks}
