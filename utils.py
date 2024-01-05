import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
#plt.rcParams['text.usetex'] = True

def plot_logreturns(df, plot = False):
    """
    Returns log returns of a dataframe, assumes dataframe has close price as a column named 'close'
    TO DO: Edit function to deal with a df with multiple tickers!
    """
    logrets = np.log(df.Close).diff().dropna()
    logrets=logrets[logrets!=0]

    if plot:
        logrets.plot()
        plt.xlabel("time")
        plt.ylabel("log-returns")
    
    return logrets

from statsmodels.distributions.empirical_distribution import ECDF
def plot_recdf(logrets):
    """
    PLots reciprocal ECDF of log returns r
    """
    x=np.abs(logrets)
    ecdf_r=ECDF(x)

    plt.plot(ecdf_r.x,1-ecdf_r.y)
    plt.xlabel("|r|")
    plt.ylabel("$P_>(|r|)$")
    plt.yscale("log")
    plt.title("Reciprocal ECDF of log returns")
    plt.show()


#do other stylized facts functions
print('liu')
