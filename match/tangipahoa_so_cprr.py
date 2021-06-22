import sys

import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, NoopIndex, ColumnsIndex

from lib.path import data_file_path, ensure_data_dir

sys.path.append('../')


def deduplicate_cprr_officers(cprr):
    df = cprr[['uid', 'first_name', 'last_name', 'rank_desc']]
    df = df.drop_duplicates(subset=['uid']).set_index('uid')
    df.loc[:, 'fc'] = df.first_name.fillna('').map(lambda x: x[:1])
    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, df)
    decision = .866
    # matcher.save_pairs_to_excel(
    #     data_file_path('match/tangipahoa_so_cprr_2015_2021_deduplicate.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)
    
    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('clean/cprr_tangipahoa_so_2015_2021.csv'))
    cprr = deduplicate_cprr_officers(cprr)
    
