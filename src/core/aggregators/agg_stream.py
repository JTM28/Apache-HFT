import socket
import yaml
import ast
import re
from time import time
from apache.crypto.aggregators.agg_series import TimeAggregator


RESAMPLE = 15


class Stream(TimeAggregator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.HOST = '35.232.115.119'
        self.PORT = 5000
        self.sub_msg = {'type': 'req', 'action': 'subscribe', 'dtype': 'quotes', 'exchange': ['binance', 'coinbase'],
                        'symbols': []}

    async def __call__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.HOST, self.PORT))
        self.socket.sendall(str(self.sub_msg).encode())
        self.start_timer()

        while True:
            try:
                data = self.socket.recv(1024)
                msg = data.decode('utf-8')

                if re.search('}{', str(msg)):

                    try:
                        msg1, msg2 = re.split('}{', str(msg))
                        msg1 = '"' + msg1 + '}"'
                        msg2 = '"{' + msg2 + '"'

                        msg1 = ast.literal_eval('"' + yaml.load(msg1) + '"')
                        msg2 = ast.literal_eval('"' + yaml.load(msg2) + '"')
                        await self.on_quote(msg1)
                        await self.on_quote(msg2)

                    except Exception:
                        pass

                else:
                    msg = yaml.load(msg)
                    if len(msg) > 0:
                        await self.on_quote(msg)

                if time() - self.timer > RESAMPLE:
                    self.reset_timer()
                    await self.resample()  # Calls The Resample Async Function Found In TimeAggregator Class

            except Exception as error:
                self.on_error(error)


    def start_timer(self):
        self.timer = time()


    def reset_timer(self):
        self.start_timer()


    def on_error(self, error):
        self.error_msg = error
