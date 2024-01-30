import numpy as np
# from utils import compute_C_minus_C0, LouvainCorrelationClustering
from utils import LouvainCorrelationClustering
import matplotlib.pyplot as plt
import pandas as pd
from statsmodels.stats import multitest
from scipy import stats

def market_state_strat(r): #matrix of log returns
    T=int(np.floor(r.shape[1]/3))
    ret=[1]
    for t in range(T+1,r.shape[0]):
        R = r.iloc[t-T:t]
        DF = LouvainCorrelationClustering(R.T)
        pre_state=DF.iloc[T-1][0]
        I = DF[DF[0]==pre_state].index.tolist()
        my_list = [x+1 for x in I[:-1]]
        ar=R.iloc[my_list].mean(axis=0)
        pos = np.sign(ar.values)
        ret.append(np.dot(pos, np.exp(r.iloc[t].values)-1)/len(pos)+1)

    df_chopped = r.iloc[T:]
    df_chopped['rolling_ret']=ret
    df_chopped['cumulative_perf']=df_chopped['rolling_ret'].cumprod()
    df_chopped['cumulative_perf'].plot()
    plt.xlabel('time')
    plt.ylabel('USD')
    plt.title('Cumulative_performance')

def market_state_strat_upgraded(r, trend_measure='med_HL', seed=10): #matrix of log returns
    np.random.seed(seed)

    T=int(np.floor(r.shape[1]/3))
    ret=[0]
    for t in range(T+1,r.shape[0]):
        R = r.iloc[t-T:t]
        DF = LouvainCorrelationClustering(R.T)
        cur_state=DF.iloc[-1][0]
        I = DF[DF[0]==cur_state].index.tolist()
        my_list = [x+1 for x in I[:-1]]
        if not my_list:
            pos=np.sign(np.zeros(r.shape[1]))
        else:
            if trend_measure=='med_HL':
                ar=med_HL(R.iloc[my_list])
            elif trend_measure=='med':
                ar=R.iloc[my_list].median(axis=0)
            else:
                ar=R.iloc[my_list].mean(axis=0)
            pos = np.sign(ar.values)
        ret.append(np.dot(pos, np.exp(r.iloc[t].values)-1)/len(pos))

    all_ret=pd.DataFrame({'Strat_A':ret}, index=r.index[T:])
    all_ret['BH']=np.concatenate(([0], np.dot(np.exp(r.iloc[T+1:])-1, np.ones(len(pos)))/len(pos)))
    all_ret['Strat_A_perf']=all_ret['Strat_A']+1.0
    all_ret['BH_perf']=all_ret['BH']+1.0

    return all_ret

def strat_eval(all_ret, m):
    #robust sharp ratio
    all_ret['Strat_A_perf'].cumprod().plot(label=m)
    all_ret['BH_perf'].cumprod().plot(label='buy_and_hold')
    sharpe_ratio=_calc_sharpe(all_ret['Strat_A'])
    plt.xlabel('time')
    plt.ylabel('USD')
    plt.title(f'Cumulative_performance for {m}, SR={sharpe_ratio}')
    plt.legend()
    plt.show()

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


