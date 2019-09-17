import numpy as np
from apache.crypto.compute import cfuncs
from apache.crypto.trade_bot.state_controls import ExchangeState



class ComputeEngine(ExchangeState):

    def __init__(self):
        super().__init__()


    @staticmethod
    def ADD(x1, x2):
        return cfuncs.ADD(np.float(x1), np.float(x2))

    @staticmethod
    def SUBTRACT(x1, x2):
        return cfuncs.SUBTRACT(np.float(x1), np.float(x2))

    @staticmethod
    def SUBTRACT_ARRAY(x1_arr, x2_arr):
        return cfuncs.SUBTRACT_ARRAY(np.array(x1_arr, dtype=np.float), np.array(x2_arr, dtype=np.float))

    @staticmethod
    def MULTIPLY(x1, x2):
        return cfuncs.MULTIPLY(np.float(x1), np.float(x2))

    @staticmethod
    def DIVIDE(x1, x2):
        return cfuncs.DIVIDE(np.float(x1), np.float(x2))

    @staticmethod
    def LOGDIVIDE(x1, x2):
        return cfuncs.LOGDIVIDE(x1, x2)

    @staticmethod
    def PCT(x1, x2):

        """
            Formula: -->   X = ((x1 - x2) / x2) * 100

        :param x1: type(float) - The current position across the axis
        :param x2: type(float) - The position finding pct diff from

        :return: type(float) - Returns the percentage difference
        """
        return cfuncs.PCT(x1, x2)


    @staticmethod
    def ABSPCT(x1, x2):
        """
            Formula: -->   X = abs(((x1 - x2) / x2) * 100)

        :param x1: type(float) - The current position across the axis
        :param x2: type(float) - The position finding pct diff from

        :return: type(float) - Returns the absolute percentage difference
        """
        return abs(cfuncs.PCT(x1, x2))

    @staticmethod
    def IFT(x1):
        return cfuncs.IFT(x1)

    @staticmethod
    def ROC(x_array):
        """
            Formula: -->  X = ((x_array[-1] - x_array[0]) / x_array[0]) * 100

        :param x_array: type(Array, dtype=float64) - Find ROC from the first index *[0] -> last index *[-1]

        :return: type(float) - Returns a single ROC value of the entire array
        """

        return cfuncs.ROC(np.array(x_array, dtype=np.float))

    @staticmethod
    def SLOPE(x_array):
        return cfuncs.SLOPE(np.array(x_array, dtype=np.float))

    @staticmethod
    def SUM(x_array):
        return cfuncs.SUM(np.array(x_array, dtype=np.float))

    @staticmethod
    def MEAN(x_array):
        return cfuncs.MEAN(np.array(x_array, dtype=np.float))

    @staticmethod
    def STDEV(x_array):
        return cfuncs.STDEV(np.array(x_array, dtype=np.float))

    @staticmethod
    def VWAP(x_array, size_array):
        return cfuncs.VWAP(np.array(x_array, dtype=np.float), np.array(size_array, dtype=np.float))

    @staticmethod
    def RELATIVE_PCT(x_array):
        return cfuncs.RELATIVE_PCT(np.array(x_array, dtype=np.float))

    @staticmethod
    def NORMTPP(high, low, close):

        return cfuncs.NormTPP(np.float(high), np.float(low), np.float(close))


    @staticmethod
    def FULLTPP(open, high, low, close):

        return cfuncs.FullTPP(np.float(open), np.float(high), np.float(low), np.float(close))

    @staticmethod
    def WEIGHTEDTPP(open, high, low, close):
        return cfuncs.WeightedTPP(np.float(open), np.float(high), np.float(low), np.float(close))


    @staticmethod
    def SPLITSPREAD(px, bidask_px):

        return cfuncs.SplitSpread(np.float(px), np.float(bidask_px))


    '''
    The TestBool Can Be Used For Boolean Comparisons Under A High Speed Constraint
    
    For Conditions Testing True The CFuncs Call Will Return 1 & Return 0 If False
    
    This Will Then Be Converted Into A Bool Value On The Return 
    
    '''

    @staticmethod
    def TestBool(self, *args):
        n = len(args)
        points = list(args)

        if n == 2:

            return True if cfuncs._2DiscreteBool(np.float(points[0]), np.float(points[1])) == 1 else False
