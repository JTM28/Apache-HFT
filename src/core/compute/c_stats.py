import matplotlib.pyplot as plt
import seaborn

from apache._engine.compute.c_base import *


@njit("f8(f8[:], f8[:])")
def pearson_coef(x, y):
    n = len(x)
    k = 1 / n
    x_avg = SUM(x) * k
    y_avg = SUM(y) * k
    coef = 0

    std = 1 / (STDEV(x) * STDEV(y))
    for i in range(n):
        dev = (x[i] - x_avg) * (y[i] - y_avg)
        coef += dev * std
    corr = coef * k

    return corr


@njit('f8(f8[:])')
def z_test(x_array):
    stdev = STDEV(x_array)
    sma = MEAN(x_array)
    n = len(x_array)
    z_scores = []

    for i in range(n):
        z = (1 / np.sqrt(2 * np.pi * stdev**2)) * np.exp(-0.5 * ((x_array[i] - sma) / stdev) ** 2)
        z_scores.append(z)

    return np.array(z_scores, dtype=float64)



def standard_brownian_motion(x, t):

    return 1 / np.sqrt(2 * np.pi * t) * np.exp(-((x ** 2) / (2 * t)))




def random_sample(values, sample_size):
    N = len(values)
    SS = sample_size
    i = np.random.randint(0, N - SS)

    return values[i:i+SS]










