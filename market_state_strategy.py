import numpy as np
# from utils import compute_C_minus_C0, LouvainCorrelationClustering
from utils import LouvainCorrelationClustering
import matplotlib.pyplot as plt
import pandas as pd
from statsmodels.stats import multitest
from scipy import stats
from marsili_giada_clustering import aggregate_clusters
import seaborn as sns
from bahc import filterCovariance

def market_state_strategy(r, a=0.05, seed=10, addFDR=False): #matrix of log returns
    np.random.seed(seed)

    T=int(np.floor(r.shape[1]/3))
    ret=[0]
    for t in range(T+1,r.shape[0]):
        R = r.iloc[t-T:t]
        DF = LouvainCorrelationClustering(R.T)
        cur_state=DF.iloc[-1][0]
        I = DF[DF[0]==cur_state].index.tolist()
        my_list = [x+1 for x in I[:-1]]

        if not addFDR:
            ar=R.iloc[my_list].mean(axis=0) 
            pos = np.sign(ar.values) 
        else:
            rej=FDR(R.iloc[my_list], alpha=a) # fdr 
            ar=R.iloc[my_list].mean(axis=0)
            pos = np.sign(ar.values)
            pos[~rej] = 0 # fdr

        ret.append(np.dot(pos, np.exp(r.iloc[t].values)-1)/len(pos))

    all_ret=pd.DataFrame({'Strat_A':ret}, index=r.index[T:])
    all_ret['buy_and_hold']=np.concatenate(([0], np.dot(np.exp(r.iloc[T+1:])-1, np.ones(len(pos)))/len(pos)))
    all_ret['Strat_A_perf']=all_ret['Strat_A']+1
    all_ret['Strat_A_perf'].cumprod().plot(label='market_state_strategy')

    all_ret['BH_perf']=all_ret['buy_and_hold']+1
    all_ret['BH_perf'].cumprod().plot(label='buy_and_hold')
    plt.xlabel('time')
    plt.ylabel('USD')
    plt.title('Cumulative_performance')
    plt.legend()
    plt.show()    

def market_state_strat_upgraded(r, opt=False, is_BAHC=False, trend_measure='med', seed=10): #matrix of log returns

    np.random.seed(seed)
    T=int(np.floor(r.shape[1]/3))
    pos = pd.DataFrame(0.0, index=r.index, columns=r.columns)
    for t in range(T,r.shape[0]-1):
        R = r.iloc[t-T+1:t+1]
        DF = LouvainCorrelationClustering(R.T)
        cur_state=DF.iloc[-1][0]
        I = DF[DF[0]==cur_state].index.tolist()
        my_list = [x+1 for x in I[:-1]]
        if my_list:
            if trend_measure=='med_HL':
                ar=med_HL(R.iloc[my_list])
            elif trend_measure=='med':
                ar=R.iloc[my_list].median(axis=0)
            else:
                ar=R.iloc[my_list].mean(axis=0)
            e = np.sign(ar.values)
            if opt:
                w=_cal_w(R, e, is_BAHC)
            else:
                w = e/r.shape[1]
            pos.iloc[t+1] = w
    return pos 

def strat_eval(pos, r, Strat_Name):
    T=int(np.floor(r.shape[1]/3))
    model_returns=pos.iloc[T:]*r.iloc[T:]
    total_returns=model_returns.sum(axis=1)
    cum_perf=total_returns+1
    cum_return=cum_perf.cumprod()-1
    cum_return.plot(label=Strat_Name)
    return cum_return

def _calc_sharpe(returns):
    return returns.mean() / returns.std() * np.sqrt(252*390)

def med_HL(data):
    n = data.shape[0]
    if n>1:
        pair_list=[]
        for i in range(n):
            for j in range(i+1,n):
                pair_list.append((data.iloc[i] + data.iloc[j]) / 2)
        pairwise_averages = pd.DataFrame(pair_list)
        return pairwise_averages.median(axis=0)
    else:
        return data.median(axis=0)

def FDR(data, alpha=0.05, null_mean=0):

    pvals=[]
    for ticker in data.columns:
        _, p_value = stats.ttest_1samp(data[ticker], null_mean)
        pvals.append(p_value)
    rej, _,_,_ = multitest.multipletests(pvals, alpha=alpha, method='fdr_bh')

    return rej

def _cal_w(R, e, is_BAHC): # T by N
    if is_BAHC:
        x=R.T
        C=filterCovariance(x.values, is_correlation=False)   
    else:
        C=R.cov()
    inv_cov = np.linalg.pinv(C)
    min_var_weights = (inv_cov @ e) / np.dot( e, inv_cov @ e)
    return min_var_weights

def number_of_states(r):
    Num=[]
    T=int(np.floor(r.shape[1]/3))
    for t in range(T,r.shape[0]-1):
        R = r.iloc[t-T+1:t+1]
        DF = LouvainCorrelationClustering(R.T)
        Num.append(DF['Column_A'].nunique())
    return Num

