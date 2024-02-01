import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from numpy import linalg as LA
import math
import networkx as nx
import community
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

def compute_C_minus_C0(lambdas,v,lambda_plus,removeMarketMode=True):
    N=len(lambdas)
    C_clean=np.zeros((N, N))
    
    order = np.argsort(lambdas)
    lambdas,v = lambdas[order],v[:,order]
    
    v_m=np.matrix(v)

    # note that the eivenvalues are sorted
    for i in range(1*removeMarketMode,N):                            
        if lambdas[i]>lambda_plus: 
            C_clean=C_clean+lambdas[i] * np.dot(v_m[:,i],v_m[:,i].T)  
    return C_clean   

def LouvainCorrelationClustering(R):   # R is a matrix of return
    N=R.shape[1]
    T=R.shape[0]

    q=N*1./T
    lambda_plus=(1.+np.sqrt(q))**2

    C=R.corr()
    lambdas, v = LA.eigh(C)


            
    C_s=compute_C_minus_C0(lambdas,v,lambda_plus)
    
    mygraph= nx.from_numpy_array(np.abs(C_s))
    partition = community.community_louvain.best_partition(mygraph)

    DF=pd.DataFrame.from_dict(partition,orient="index")
    return DF

