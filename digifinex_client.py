import requests
import base64
import re
import ast
import hashlib
import datetime as dt
from apache.databases.client import DBClient


class DigifinexClient(object):

    def __init__(self):
        self.API_KEY = '15d0a619fc83bb'
        self.BASE_URL = 'https://openapi.digifinex.vip/v2/'
        self.URL_ATTACH = {
            'TRADE-INFO' : 'trade_pairs?'}
        self.API_SECRET = 'da522de1e5c1be32c0c986c3b78f7f4605d0a619f'


    def Payload(self, api_key, req_timestamp, sign):
        payload = {'timestamp' : int(req_timestamp), 'apiKey' : self.API_KEY, 'sign' : sign}
        self.payload = payload


    def Signature(self):
        m = hashlib.md5()
        TimeStampNow = str(int(dt.datetime.now().timestamp()))
        params = {'timestamp': TimeStampNow, 'apiKey': str(self.API_KEY), 'apiSecret': str(self.API_SECRET)}
        keys = sorted(params.keys())

        _str = ''
        for key in keys:
            _str = _str + params[key]

        m.update(_str.encode())
        signature = m.hexdigest()
        self.Payload(self.API_KEY, TimeStampNow, signature)



    def GET(self, attach=''):
        url = self.BASE_URL

        if attach != '':
            url = url + str(attach)

        self.Signature()
        req = requests.get(url, params=self.payload)

        return req.text









