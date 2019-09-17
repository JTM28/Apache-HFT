


class SystemState(object):

    def __init__(self):
        self.SYSTEM_ON  = False
        self.SYSTEM_OFF  = False
        self.SYSTEM_HOLD = False
        self.SYSTEM_MITIGATE = False

    def run_state(self):
        self.SYSTEM_ON = True
        self.OFF = False

    def off_state(self):
        self.RUN = False
        self.OFF = True

    def init_freeze_state(self):
        self.HOLD = True

    def end_freeze_state(self):
        self.HOLD = False

    def is_running(self):

        return True if self.RUN is True else False

    def is_hold_state(self):

        return True if self.HOLD is True else False






class ExchangeState(SystemState):

    def __init__(self):
        super().__init__()
        self.EXCHANGE_ON = False
        self.EXCHANGE_OFF = False
        self.EXCHANGE_FREEZE = False
        self.latency_log = []

    def allow_trades(self):
        if self.is_running() is True:
            self.FREEZE_TRADES = False

    def freeze_trades(self):
        self.FREEZE_TRADES = True

    def allow_transfers(self):
        self.FREEZE_TRANSFERS = False

    def freeze_transfers(self):
        self.FREEZE_TRANSFERS = True


    def exchange_latency(self, delay):
        self.latency_log.append(delay)





class SymbolState(ExchangeState):

    def __init__(self):
        super().__init__()

        self.SYMBOL_ON = False
        self.SYMBOL_OFF = False
        self.SYMBOL_FREEZE = False


    def allow_symbol(self):

        if self.is_running() is True:
            self.FREEZE_TRADES = False


    def freeze_symbol(self):
        self.FREEZE_TRADES = True


    def get_latency(self, doc):
        x = doc['UNIX-TIME'] - doc['TIMESTAMP']
        self.exchange_latency(x)










