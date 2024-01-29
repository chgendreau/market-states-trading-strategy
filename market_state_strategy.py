import numpy as np
# from utils import compute_C_minus_C0, LouvainCorrelationClustering
from utils import LouvainCorrelationClustering
import matplotlib.pyplot as plt

# determine market states 

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

def market_state_strat_upgraded(r, seed=10): #matrix of log returns
    np.random.seed(seed)

    T=int(np.floor(r.shape[1]/3))
    ret=[0]
    for t in range(T+1,r.shape[0]):
        R = r.iloc[t-T:t]
        DF = LouvainCorrelationClustering(R.T)
        cur_state=DF.iloc[-1][0]
        I = DF[DF[0]==cur_state].index.tolist()
        my_list = [x+1 for x in I[:-1]]
        ar=R.iloc[my_list].mean(axis=0)
        pos = np.sign(ar.values)
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

def strat_eval(ret):
    #robust sharp ratio
    
    
    return


