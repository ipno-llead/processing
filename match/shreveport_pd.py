import sys

import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

from lib.path import data_file_path, ensure_data_dir

sys.path.append('../')


def match_cprr_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
    dfa = dfa.drop_duplicates().set_index('uid')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post.loc[
        post.agency == 'shreveport pd',
        ['uid', 'first_name', 'last_name'],
    ]
    dfb = dfb.drop_duplicates().set_index('uid')
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity()
    }, dfa, dfb)
    decision = 0.95
    matcher.save_pairs_to_excel(data_file_path(
        'match/shreveport_pd_cprr_2018_2019_v_post_pprr_2020_11_06.xlsx'
    ), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path(
        'clean/cprr_shreveport_pd_2018_2019.csv'
    ))
    post = pd.read_csv(data_file_path(
        'clean/pprr_post_2020_11_06.csv'
    ))
    cprr = match_cprr_post(cprr, post)
    ensure_data_dir('match')
    cprr.to_csv(data_file_path(
        'match/cprr_shreveport_pd_2018_2019.csv'
    ), index=False)
