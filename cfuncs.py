import numpy as np
from numba import njit, float64, jit


@njit('f8(f8, f8)')
def ADD(x1, x2):

    return x1 + x2

@njit('f8(f8[:])')
def RELATIVE_PCT(x_array):
    high = np.amax(x_array)
    low = np.amin(x_array)
    avg = np.mean(x_array)

    return (high - low) / ((high + low) / 2)


@njit('f8(f8, f8)')
def SUBTRACT(x1, x2):

    return x1 - x2


@njit('f8[:](f8[:], f8[:])')
def SUBTRACT_ARRAY(x1, x2):
    n = len(x1)
    diffs = []

    for i in range(n):
        x = x1[i] - x2[i]
        diffs.append(x)

    return np.array(diffs, dtype=float64)


@njit('f8(f8, f8)')
def MULTIPLY(x1, x2):

    return x1 * x2


@njit('f8(f8, f8)')
def DIVIDE(x1, x2):

    return x1 / x2


@njit('f8(f8, f8)')
def LOGDIVIDE(x1, x2):

    return np.log(x1 / x2)


@njit('f8(f8, f8)')
def PCT(x1, x2):

    return ((x1 - x2) / x2) * 100


@njit('f8(f8)')
def IFT(x):

    return (np.exp(x * 2) - 1) / (np.exp(x * 2) + 1)


@njit('f8(f8[:])')
def ROC(x_array):

    return ((x_array[-1] - x_array[0]) / x_array[0]) * 100


@njit('f8(f8[:])')
def SLOPE(x_array):

    return (x_array[-1] - x_array[0]) / len(x_array)


@njit('f8(f8[:])')
def SUM(x_array):
    n = len(x_array)
    _sum = 0

    for i in range(n+1):
        _sum += x_array[i]

    return _sum

@njit('f8(f8[:])')
def MEAN(x_array):
    n = len(x_array)
    _sum = 0

    for i in range(n+1):
        _sum += x_array[i]

    return _sum / n

@njit('f8(f8[:])')
def STDEV(x_array):
    n = len(x_array)
    mean = MEAN(x_array)
    var = 0

    for i in range(n):
        var += (x_array[i] - mean) ** 2

    return np.sqrt(var / n)


@njit('f8(f8[:], f8[:])')
def VWAP(px_array, volume_array):
    volsum = SUM(volume_array)
    mean_px = MEAN(px_array)

    return (volsum * mean_px) / volsum


@njit('f8(f8, f8, f8)')
def NormTPP(h, l, c):

    return (h + l + c) * 0.3333333


@njit('f8(f8, f8, f8, f8)')
def FullTPP(o, h, l, c):

    return (o + h + l + c) * 0.25


@njit('f8(f8, f8, f8, f8)')
def WeightedTPP(o, h, l, c):

    return (((o + c) * 0.5) * 0.75) + (((h + l) * 0.5) * 0.25)


@njit('f8(f8, f8)')
def SplitSpread(px, bidask_px):

    return (px + bidask_px) * 0.5


# @njit('f8[:](i8)')
def ShapeArray(size):
    arr_shape = []

    for i in range(size):
        arr_shape.insert(0, 0)

    return arr_shape



'''
    Examples Of Using The Boolean compute Functions
    
    x1 = 8 
    x2 = 7
    
    The Following Would Return 1 And 1 == True So It Would Therefor Be True
    
    compare = DiscretePoints(x1, x2) 
    
    The Following Would Return 0 and 0 == False
    
    compare = DiscretePoints(x2, x1)

    
'''


@njit('i4(f8, f8)')
def _2DiscreteBool(x_test, x_compare):  # The First Arg Is The Variable Being Tested Against The 2nd Arg

    if x_test > x_compare:

        return 1

    return 0










