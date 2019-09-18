import re
import gzip
import asyncio
import json
import zlib
from time import sleep, time
from threading import Thread
from websocket._core import create_connection
from src.backends._mongo.mongo_client import DBClient


class CoinTiger(object):

    @staticmethod
    def on_quote(msg):
        try:
            symbol = str(msg['channel']).split('_')[1].upper()

        except Exception as Null:
            pass

        # symbol = re.sub(r'-', '', str(msg['product_id'])) + '.CB'
        # QuoteTemplate.main(symbol, 'COINBASE', str(msg['time']), float(msg['price']), float(msg['size']))


class HitBTC(object):

    @staticmethod
    def on_quote(msg):
        try:
            data = msg['params']['data'][0]
            unix = time()
            px = data['price']
            q = data['quantity']
            print(unix, px, q)

        except Exception: pass


class Digifinex(object):

    @staticmethod
    def on_quote(msg):
        try:
            data = msg['params']
            symbol = re.sub(r'_', '', str(data[2])) + '.DFX'
            px = data[1][0]['price']
            size = data[1][0]['amount']
            timestamp = data[1][0]['time']
            buysell = data[1][0]['type']

        except Exception: pass


class WebsocketStates(object):

    """
        ON_STATE - Should be called as soon as a connection has been confirmed through the on_open() method

        OFF_STATE - Should be the state as soon as the websocket exits main loop called from on_close() method

        ERROR_STATE - Should be the state of the socket during an error. The websocket can have 2 states at once if one
                      of the states if ERROR_STATE
    """

    def __init__(self, config):

        if isinstance(config, dict):
            for key in config.keys():
                if isinstance(config[key], dict):
                    for _key in config[key].keys():
                        setattr(self, str(_key), config[key][_key])

                else:
                    setattr(self, str(key), config[key])

        self.ON_STATE = False
        self.OFF_STATE = False
        self.ERROR_STATE = False

    def on_open(self):
        """
            State Change --> ON_STATE
        """
        self.ON_STATE = True
        self.OFF_STATE = False


    def on_close(self):
        """
            State Change --> OFF_STATE
        """
        self.OFF_STATE = True
        self.ON_STATE = False


    def on_error(self, error):
        """
            State Change --> ERROR_STATE

        :param error: type(str) - Error that occured within the websocket

        :return: type(JSON/dict) - Sends message through CallbackSocket updating main server on error
        """
        self.ERROR_STATE = True


    @staticmethod
    def send_callback(data):
        msg = {'type' : 'cb', 'action' : 'datafeed_cb', 'data' : {}}

        if isinstance(data, dict):
            for key in data.keys():
                msg['data'][str(key)] = data[key]

        else:
            msg['data']['payload'] = data



class WebsocketResolver(WebsocketStates):

    def __init__(self, config):
        super().__init__(config)


    def getattr(self, name):

        if hasattr(self, str(name)):
            return True

        else:
            return False

    def check_type(self, name, _type):

        if self.getattr(name) is True:
            if isinstance(getattr(self, str(name)), _type):

                return True
        return False


    def resolver(self):
        print(locals())

        if not hasattr(self, 'URL'):
            raise Exception('--Websocket missing required param: {URL}')

        if self.getattr('URL') is False:
            raise self.resolver_error('URL')

        setattr(self, 'ROUTE-KEY', 'XXXX')
        if self.getattr('EXCHANGE') is False:
            setattr(self, 'EXCHANGE', 'DEFAULT-EXCHANGE')

        if re.search(r'-', str(getattr(self, 'EXCHANGE'))):
            setattr(self, 'EXCHANGE-ROUTE', str(getattr(self, 'EXCHANGE')).split('-')[0])

        if self.getattr('QUEUE') is False:
            setattr(self, 'QUEUE', 'DEFAULT-QUEUE')

        if self.getattr('NODES') is False:
            setattr(self, 'NODES', 2)

        if self.getattr('SEARCH-KEY') is False:
            setattr(self, 'SEARCH-KEY', None)

        elif getattr(self, 'SEARCH-KEY') == 'BYTE':
            if self.getattr('DEFLATE') is True:
                lib = str(getattr(self, 'DEFLATE')).lower()

                if lib == 'zlib' or lib == 'gzip':
                    setattr(self, 'deflate_type', str(lib))


        if self.getattr('PARAMS') is False:
            setattr(self, 'PARAMS', None)


        if self.check_type('URL-ITERABLE', dict) is True:
            iter_params = getattr(self, 'URL-ITERABLE')

            if 'STR' in iter_params.keys() and 'POSITION' in iter_params.keys() and 'KEY' in iter_params.keys():
                url_iter, iter_position, iter_key = iter_params['STR'], iter_params['POSITION'], iter_params['KEY']
                url_attach = ''

                try:
                    for x in getattr(self, iter_key):
                        if str(iter_position) == 'END' or int(iter_position) == -1:
                            url_attach += str(x) + url_iter

                        elif iter_position == 'BEG' or iter_position == 0:
                            url_attach += url_iter + str(x)

                    url = getattr(self, 'URL') + url_attach

                    try:
                        url = re.sub(r' ', '', str(url))

                    finally:
                        self.URL = url

                except Exception as Error:
                    self.resolver_error('UNKOWN')

            else:
                self.resolver_error('URL-ITERABLE')

    def resolver_error(self, error):
        print('Missing Required Field: %s' % str(error))



class BuildStream(WebsocketResolver):

    """
        The BaseWebsocket class is a partially abstracted implementation of a websocket for datastreaming

        Configurable args:

            URL - The url that a client connection will initiate handshake with for a long lived tcp connection
            EXCHANGE - The name of the RabbitMQ Cluster Exchange that quotes will be published to
            NODES/QUEUE_COUNT - The number of nodes aka queues that messages will be distributed across of
    """

    def __init__(self, config):
        super().__init__(config)
        self.params = {}

    async def register_stream(self):
        """
            Invoke the websocket class and start 2 threads.

                Thread-1: The main thread that will be handling the datafeeds
                Thread-2: The thread that will send routine pings to datafeed servers

            Both thread instances are initialized here, but only the main_thread is started, the ping_thread
            is started as soon as the listen() method has been called

        :return: None - self.main_thread.start() calls the connect method
        """
        self.stop = False
        self.main_thread = Thread(target=self.connect, args=(), )
        self.ping_thread = Thread(target=self.ping, args=(), )
        self.main_thread.start()


    def connect(self):   # Connect To Exchange Raw Feed Socket
        self.ws = create_connection(getattr(self, 'URL'), enable_multithread=True)  # Create The Connections
        self.on_open()

        if getattr(self, 'PARAMS') is not None:
            try:
                self.ws.send(json.dumps(getattr(self, 'PARAMS')))   # Send The Socket Parameters

            except Exception as err:
                print(str(err))

        self.listen()


    def ping(self, msg=time(), ping_interval=5):

        try:
            while True:
                self.ws.ping(msg)  # Ping Response To websockets Server
                sleep(ping_interval)

        except Exception as err:
            print(str(err))


    def listen(self):   # Listen For Events In The Event Loop

        """
            The main loop of the websocket. Once this method is called, will not stop unless manually stopped or
            a fatal error occurs while running.

        :return: Error Message/code on failure
        """

        self.ping_thread.start()  # Start The Thread Responsible For Pings In The self.start() Method

        while True:

            try:
                data = self.ws.recv()

            except ValueError:
                self.on_error(ValueError)

            except TypeError:
                self.on_error(TypeError)

            except Exception as err:
                self.on_error(str(err))

            else:
                self.on_message(data)

    def on_message(self, data):

        """
            Called from the listen() method once a new message has been properly received through the client

        :param data: type(bytes) - The data being received directly from the client
            *Do not attempt to convert from json before sending to this method

        :return: Calls the send_message() to send new quote to a queue for a consumer instance
        """

        if getattr(self, 'SEARCH-KEY') == 'BYTE':
            self.on_binary_message(data)

        else:
            msg = json.loads(data)

            if not getattr(self, 'SEARCH-KEY') is None:
                if re.search(str(getattr(self, 'SEARCH-KEY')), str(msg)):
                    self.send_message(msg)

            else:
                self.send_message(msg)

    def send_message(self, msg):
        print(msg)
        #
        # try:
        #     msg['ROUTE'] = 'QUOTE|' + getattr(self, 'EXCHANGE-ROUTE')
        #
        # except Exception as err:
        #     msg = {'ROUTE': 'QUOTE|' + getattr(self, 'EXCHANGE-ROUTE'), 'DATA': msg}
        #
        # finally:
        #     publish(exchange=getattr(self, 'EXCHANGE'),
        #             route_key=balancer(getattr(self, 'ROUTE-KEY'), getattr(self, 'NODES')),
        #             msg=json.dumps(msg))


    def on_binary_message(self, data):
        """
            Handle quotes in binary form using zlib to decompress the message
            * Make sure you do not attempt to load binary messages using json.loads() as it will cause an error

        :param data: type(Binary/Bytes) - The message received through the client tcp connection
        :return: Calls the send_message(msg) method to publish the new message to a queue
        """

        if str(getattr(self, 'deflate_type', '')).lower() == 'zlib':
            msg = json.loads(zlib.decompress(data))
            self.send_message(msg)

        elif str(getattr(self, 'deflate_type', '')).lower() == 'gzip':
            msg = gzip.decompress(bytes(data))
            self.send_message(msg)


    def disconnect(self):
        """
            Check to see if the websocket is still running and if True disconnect it before restarting

        :return: Calls the self.connect() method above
        """

        if self.ON_STATE is True:

            try:
                if self.ws:
                    self.ws.close()

            except Exception as ClosedSocket:
                print(ClosedSocket)

            finally:
                self.on_close()
                sleep(5)
                self.connect()

class WebsocketMonitor(BuildStream):

    def __init__(self, config):

        if isinstance(config, dict):
            super().__init__(config)

        else:
            try:

                if isinstance(config, dict):
                    super().__init__(config)

            except ConnectionError:
                self.on_connection_error(config)


    @staticmethod
    def get_config(name):

        return DBClient().client['NETWORKS']['datafeeds'].find_one()


    def on_connection_error(self, name):
        retries = 0

        while retries < 5:
            try:
                doc = self.get_config(name)

            except ConnectionError:
                sleep(5)
                retries += 1

def run(feed_name):
    doc = DBClient().client['NETWORKS']['datafeeds'].find_one(filter={'ID' : feed_name})
    ws = WebsocketMonitor(doc)
    ws.resolver()
    loop = asyncio.get_event_loop()
    loop.create_task(ws.register_stream())
    loop.run_forever()















