import numpy as np

import itertools

def expand_grid_unique(x, y, include_equals=False):
    x = list(set(x))
    y = list(set(y))

    def g(i):
        z = [val for val in y if val not in x[:i - include_equals]]
        if z:
            return [x[i - 1]] + z

    combinations = [g(i) for i in range(1, len(x) + 1)]
    return [combo for combo in combinations if combo]


def max_likelihood(c, n):
    if n > 1:
        return np.log(n / c) + (n - 1) * np.log((n * n - n) / (n * n - c))
    else:
        return 0

def max_likelihood_list(cs, ns):
    Lc = {}
    for x in cs.keys():
        if ns[x] > 1:
            Lc[x] = np.log(ns[x] / cs[x]) + (ns[x] - 1) * np.log((ns[x] * ns[x] - ns[x]) / (ns[x] * ns[x] - cs[x]))
        else:
            Lc[x] = 0
    return Lc

def find_max_improving_pair(C, cs, ns, i_s):
    N = len(i_s)
    Lc_new = {}
    Lc_old = max_likelihood_list(cs, ns)
    names_cs = list(cs.keys())
    max_impr = -1e10
    pair_max_improv = []
    
    for i in names_cs[:-1]:
        names_cs_j = names_cs[names_cs.index(i) + 1:]
        for j in names_cs_j:
            ns_new = ns[i] + ns[j]
            i_s_new = i_s[i] + i_s[j]
          #  ipdb.set_trace()
            cs_new = np.sum(C[np.ix_(i_s_new, i_s_new)])
            max_likelihood_new = max_likelihood(cs_new, ns_new)
            improvement = max_likelihood_new - Lc_old[i] - Lc_old[j]

            if improvement > max_impr:
                max_impr = improvement
                pair_max_improv = [i, j]
                Lc_max_impr = max_likelihood_new
#    print(pair_max_improv)
#    print(max_impr)
#    print(Lc_max_impr)
#    print("*************")

    return {"pair": pair_max_improv, "Lc_new": Lc_max_impr, "Lc_old": [Lc_old[x] for x in pair_max_improv]}

def aggregate_clusters(C, only_log_likelihood_improving_merges=False):
    N = C.shape[0]
    cs = {i: 1 for i in range(N)}
    s_i = {i: [i] for i in range(N)}
    ns = {i: 1 for i in range(N)}
    i_s = {i: [i] for i in range(N)}
    Lc = {i: 0 for i in range(N)}
    
    all_pairs = [(i, j) for i in range(1, N + 1) for j in range(1, N + 1)]

    clusters = []
    for i in range(1, N):  # hierarchical merging
#        print(i)
        improvement = find_max_improving_pair(C, cs, ns, i_s)
        Lc_old = improvement['Lc_old']
        Lc_new = improvement['Lc_new']
        
        if Lc_new < sum(Lc_old):
            print(" HALF CLUSTER  Lc.new > max(Lc.old)")
            # browser()
            
        if Lc_new <= max(Lc_old):
            print("Lc.new <= max(Lc.old), exiting")
            break
            
        pair = improvement['pair']
        s_i = [pair[0] if x == pair[1] else x for x in s_i]
    
        cluster1 = pair[0]
        cluster2 = pair[1]
        i_s[cluster1].extend(i_s[cluster2])  # merge the elements of the two clusters
        del i_s[cluster2]  # removes reference to merged cluster2
    
        ns[cluster1] += ns[cluster2]
        del ns[cluster2]
    
        cs[cluster1] = np.sum(C[i_s[cluster1]][:, i_s[cluster1]])  # sums C over the elements of cluster1
        del cs[cluster2]
    
        cs_vec = list(cs.values())
        ns_vec = list(ns.values())
    
        clusters.append({
            'Lc': max_likelihood_list(cs, ns),
            'pair_merged': pair,
            's_i': s_i,
            'i_s': i_s,
            'cs': cs,
            'ns': ns
        })
    
    last_clusters = clusters[-1]

    return last_clusters