import numpy as np
from utils import compute_C_minus_C0, LouvainCorrelationClustering
#
# determine market states 
def cal_market_state(all):
    R=np.log(all.diff()) # matrix of returns
    T=R.shape[1]/3
    for t in range(T, R.shape[0]):
        DF=LouvainCorrelationClustering(R[t-T:t].T)
        state=DF[t]
        average_return = DF[DF['']==state].mean()
        R['positions'].loc(t) = (average_return > 0) - (average_return < 0)
        # FDR
        return st

def market_state_strategy(prices):
    
    return 

