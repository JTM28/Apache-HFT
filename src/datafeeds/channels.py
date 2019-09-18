import re
import ast
import sys
from time import time
from socket import SHUT_RDWR
from apache.databases.client import DBClient
from apache.crypto.datafeeds.mongo_pipes import get_pipe


NoneType = type(None)


class WSSResolver(object):

    def __init__(self, attrs):
        self.MAX_PING_INTERVAL = 30
        for key in attrs.keys():
            if not isinstance(attrs[key], dict):
                setattr(self, str(key), attrs[key])

            else:
                for _key in attrs[key].keys():
                    setattr(self, str(_key), attrs[key][_key])


    def check_attr(self, name, _type):
        if hasattr(self, str(name)):
            if isinstance(getattr(self, name), _type):
                return True

        return False

    def resolver(self):

        if self.check_attr('api_key', str) is not True:
            return 'Missing API Key'
        doc = DBClient().client['CUSTOMERS']['users'].find_one({'api-key' : getattr(self, 'api_key')})

        if not isinstance(doc, dict):
            return 'Invalid API Key'

        if self.check_attr('stream_type', str) is True:
            if getattr(self, 'stream_type') == 'quotes':
                self.resolve_quote()

        return None

    def resolve_quote(self):

        if self.check_attr('exchanges', list) is not False:
            self.PIPE = get_pipe(getattr(self, 'exchanges'), 'quotes')


class WSS(WSSResolver):
    """
        The StreamResolver class is used for resolving the requests made to the streaming servers. The
        AttributeChain class is inherited instead of the DeepChain for clarity (meaning this Resolver
        is not for handling a variable amount of arguments per say, but intended to receive a message
        in JSON/dict format. The AttributeChain will set all attributes upon initialization (so dont
        plan on using this in a super class that is waiting for messages, because the instance is
        intended to be created once a stream request event has taken place.
    """

    def __init__(self, conn, attrs):
        super().__init__(attrs)
        self.resolved = self.resolver()

        if self.resolved is not None:
            conn.send(str({'type' : 'resp', 'action' : 'failed-auth', 'reason' : self.resolved}).encode('utf-8'))
            self.on_close(conn)

        else:
            self.__call__(conn)

    def __call__(self, conn):
        self.on_open(conn)


    def on_open(self, conn):
        conn.send(str('SUBSCRIBED: [CODE: 200, TYPE: ' + str(getattr(self, 'stream_type')) + ']').encode('utf-8'))
        self.start_timer()
        self.on_stream(conn)

    def start_timer(self):
        self.timer = time()


    def on_stream(self, conn):

        with DBClient().client.watch(pipeline=self.PIPE, max_await_time_ms=20) as stream:
            for update in stream:

                if time() - self.timer < self.MAX_PING_INTERVAL:
                    try:
                        doc = str(update['fullDocument']).encode('utf-8')
                        byte_size = sys.getsizeof(doc)
                        conn.sendall(doc[0:byte_size])

                        data = conn.recv(1024)

                        if data:
                            resp = ast.literal_eval(data.decode('utf-8'))

                            if isinstance(resp, dict):
                                if re.search(r'ping', str(resp)):
                                    self.pong_frame()

                                elif 'action' in resp.keys() and resp['action'] == 'unsub':
                                    self.on_unsub(conn)

                    except BlockingIOError: pass

                    except ConnectionRefusedError:
                        self.on_error(conn, ConnectionRefusedError)

                    except ConnectionResetError:
                        self.on_error(conn, ConnectionResetError)

                    except ConnectionAbortedError:
                        self.on_error(conn, ConnectionAbortedError)

                    except BrokenPipeError:
                        self.on_error(conn, BrokenPipeError)

                    except ConnectionError:
                        self.on_error(conn, ConnectionError)

                    except Exception: pass


    def on_error(self, conn, error): pass


    def pong_frame(self):
        self.interval = time() - self.timer
        self.start_timer()


    def on_unsub(self, conn):
        print('--Client Connection [Event: Unsubscribe]')
        unsub = 'Unsubscribed From Stream Socket'
        conn.send(str(unsub).encode())
        self.on_close(conn)


    def on_close(self, conn):

        try:
            conn.send(SHUT_RDWR)
            conn.close()

        except Exception: pass

        finally: pass









