from apache.crypto.compute.base_compute import ComputeEngine
from apache.databases.client import DBClient


MAX_STACK = 50
RESAMPLE = 15



class AbstractStack(ComputeEngine):

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.quotes = {}
        self.ohlc = {}



class BaseAggregator(AbstractStack):
    """
        The BaseAggregator class is an abstracted template for different types of data aggregation methods.

        There are 3 different aggregation types supported by the base aggregator, timeseries, tickseries, stat.
        If you want to combine either the timeseries or tickseries type with stat

        log_mode = If the optional param log_mode is set to True for any of the 3 types, data will be returned
                   as the natural log of the current data point "i" divided by its preceding data point "i-1"

                   Formula:
                            delta_x = ln(x[i] / x[i-1])

                            x = any quantitative data points moving in rolling series form

            Required Parameters:
                params = {}

                * params['type'] = 'timeseries' | 'tickseries' | 'stat'

            Optional Parameters (Timeseries):
                * params['exchange']: type(str) - The name of the exchange [default=None]
                * params['resample']: type(int): unit(seconds) - The interval you want resampled (default=30)
                * params['resample']: type(list): unit(seconds) - List format for multiple resample points
                * params['max_stack']: type(int): - The maximum number of queries a dict key can have (default=500)
                * params['log_mode']: type(bool): - Receive agg queries in log mode if set to True (default=False)
                * params['log_buffer']: type(int): - Apply a multiple of 10 buffer if log_mode is True (default=None)

            Optional Parameters (Tickseries):
                * params['exchange']: type(str) - The name of the exchange [default=None]
                * params['log_mode']: type(bool): - Receive tick queries in log mode if set to True (default=False)


                * params['compute']: type(list) - A list of compute methods you want added (default=[roc, sma, std])

    """

    ASSERT_ERRORS = {'_type': 'AssertionError: The BaseAggregator class requires a valid type field'}

    PARAMS = {
        'timeseries': {'resample': 30, 'max_stack': 500},
        'tickseries': {'log_mode': False, 'log_buffer': 1},
        'statseries': {''}}

    METHODS = {'roc' : False, 'sma' : False}


    def __init__(self, **kwargs):
        super().__init__()
        self.params = {'exchange': '', 'resample': []}
        self.db = DBClient().client



    def get_params(self):

        if not hasattr(self, '_type'):
            raise Exception('The Aggregator Field "type" Is A Required Field')

        type_attr = getattr(self, '_type')

        for key in self.PARAMS:  # For Timeseries, Tickseries, Statseries Keys
            if key == type_attr:  # If This Matches The 'type' Attribute

                for _key in self.PARAMS[key].keys():  # For Nested Keys Within One Of The 3 Types Above
                    for userkey in self.attrs.keys():  # For The Key In User Params
                        if userkey == _key:  # If User Key == A Param Key Set It Equal
                            self.PARAMS[key][_key] = self.attrs[userkey]

        if hasattr(self, 'run_stats'):
            assert getattr(BaseAggregator, 'run_stats') is True

        if hasattr(self, 'stat_funcs'):
            for key in self.METHODS.keys():
                if isinstance('stat_funcs', (list, tuple)):
                    for each in getattr(self, 'stat_funcs'):
                        if each == key:
                            self.METHODS[key] = True

                else:
                    if isinstance('stat_funcs', str):
                        if getattr(self, 'stat_funcs') == key:
                            self.METHODS[key] = True


    def mutable_dicts(self, key, symbol):

        """
            Add an additional key to one of the dicts holding data

        :param key: type(str) - key='quote' if adding quote key and key='ohlc' if adding aggregator key
        :param symbol: type(str) - The symbol a key is being added for

        :return: None
        """

        if key == 'quote':
            self.quotes[symbol] = {'price': [], 'size': [], 'unix': []}

        elif key == 'ohlc':
            self.ohlc[symbol] = {
                'open': [], 'high': [], 'low': [], 'close': [], 'volume': [], 'timestamp': [], 'quotes': [], 'tpp': []}


    def insert_quote(self, doc):
        symbol = doc['SYMBOL']

        if symbol not in self.quotes.keys():
            self.mutable_dicts('quote', symbol)

        self.quotes[symbol]['price'].append(doc['PX'])
        self.quotes[symbol]['size'].append(doc['SIZE'])
        self.quotes[symbol]['unix'].append(doc['UNIX-TIME'])

        if symbol not in self.ohlc.keys():
            self.mutable_dicts('ohlc', symbol)


    def insert_stack(self, locals_dict):
        """
            Insert the next reaggregated quote into the stack

        :param locals_dict: type(locals()/dict) - The locals dict returned from the aggregator() method

        :return: None - Inserts next query into correct key of stack
        """

        symbol = locals_dict['symbol']

        for key in self.ohlc[symbol].keys():

            try:

                self.ohlc[symbol][key].append(len(symbol))
                self.ohlc[symbol][key].append(locals_dict[key])
            except Exception as error:
                print(str(error))

            print(self.ohlc[symbol])




