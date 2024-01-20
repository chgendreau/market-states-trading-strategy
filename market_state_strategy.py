import numpy as np
from utils import compute_C_minus_C0, LouvainCorrelationClustering
#
# determine market states 

def market_state_strategy(r): #matrix of log returns
    T=int(np.floor(r.shape[1]/3))
    ret=[1]
    for t in range(T+1,r.shape[0]):
        R = r.iloc[t-T:t]
        df, G, partition = LouvainCorrelationClustering(R.T)
        pre_state=df.iloc[T-1][0]
        I = df[df[0]==pre_state].index.tolist()
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
    return df_chopped

