import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from datamatch import ThresholdMatcher, ColumnsIndex, StringSimilarity
import numpy


def cluster_officers_by_tracking_number(cprr):
    df = cprr[['uid', 'first_name', 'last_name', 'tracking_number']]
    df = df.drop_duplicates(subset='uid', keep='first').set_index('uid')
    df.loc[:, 'tn'] = df.tracking_number.fillna('').map(lambda x: x[:])
    matcher = ThresholdMatcher(ColumnsIndex('tn'), {
        'tracking_number': StringSimilarity()
    }, df, show_progress=True)
    
    decision = 1
    matcher.save_clusters_to_excel(data_file_path('match/nopd_allegation_clusters_by_tracking_number.xlsx'), decision)

    clusters = matcher.get_index_clusters_within_thresholds(lower_bound=decision)

    c = []
    for cluster in clusters:
        c.append(cluster)
        dfa = pd.DataFrame(c)

        dfa['clusters']= dfa.values.tolist()
        dfa.loc[:, 'clusters'] = dfa.clusters.astype(str).str.lower().str.strip()\
            .str.replace(r'(\, none)+', '', regex=True)
        
        numpy.sort(dfa.clusters, axis=-1, kind='quicksort')
        
        dfa = dfa[['clusters']]
        dfa.to_excel(data_file_path('match/nopd_allegation_clusters_sorted.xlsx'))
    return cprr


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('raw/ipm/nopd_cprr_pprr_merged.csv'))
    cprr = cluster_officers_by_tracking_number(cprr)
    cprr.to_csv(data_file_path('match/nopd_cprr_pprr_merged.csv'))
