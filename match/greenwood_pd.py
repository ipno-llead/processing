from datamatch import ThresholdMatcher, JaroWinklerSimilarity, NoopIndex
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append('../')


def match(gw_cprr, post):
    dfa = gw_cprr[['first_name', 'last_name', 'mid']]\
        .drop_duplicates().set_index('mid')
    dfb = post[['last_name', 'first_name']].drop_duplicates()
    matcher = ThresholdMatcher(NoopIndex(), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity()
    }, dfa, dfb)
    decision = 0.9
    matcher.save_pairs_to_excel(data_file_path(
        "match/greenwood_pd_cprr_2015_2020_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    uid_dict = dict(matches)
    gw_cprr.loc[:, 'uid'] = gw_cprr.mid.map(lambda x: uid_dict[x])
    return gw_cprr.drop(columns='mid')


if __name__ == '__main__':
    gw_cprr = pd.read_csv(data_file_path(
        'clean/cprr_greenwood_pd_2015_2020.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    gw_cprr = match(gw_cprr, post)
    ensure_data_dir("match")
    gw_cprr.to_csv(
        data_file_path('match/cprr_greenwood_pd_2015_2020.csv'), index=False)
