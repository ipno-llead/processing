import sys
sys.path.append('../')
import pandas as pd
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.path import data_file_path


def match_cprr_with_brso(cprr, pprr):
    dfa = cprr[['first_name', 'last_name', 'uid']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = pprr[['first_name', 'last_name', 'uid']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)

    decision = .8
    matches = matcher.save_pairs_to_excel(data_file_path(
        'match/post_cprr_2016_2019_v_brso_pprr_2021.xlsx'), decision)
    matcher = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc['uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('clean/pprr_lafayette_pd_2010_2021.csv'))
    cprr = pd.read_csv(data_file_path('clean/cprr_post_2016_2019.csv'))
    cprr = match_cprr_with_brso(cprr, pprr)
    cprr.to_csv(data_file_path(
        'match/cprr_post_2016_2019.csv'), index=False)
