from lib.match import ThresholdMatcher, JaroWinklerSimilarity, NoopIndex
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append('../')


def match(gw_cprr, post_pprr):
    post_pprr = post_pprr.loc[post_pprr.agency ==
                              'greenwood pd'].set_index('uid', drop=False)
    dfa = gw_cprr[['first_name', 'last_name', 'mid']]\
        .drop_duplicates().set_index('mid')
    dfb = post_pprr[['last_name', 'first_name']].drop_duplicates()
    matcher = ThresholdMatcher(dfa, dfb, NoopIndex(), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity()
    })
    decision = 0.9
    matcher.save_pairs_to_excel(data_file_path(
        "match/greenwood_pd_cprr_2015_2020_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    uid_dict = dict(matches)
    gw_cprr.loc[:, 'uid'] = gw_cprr.mid.map(lambda x: uid_dict[x])
    gw_cprr.loc[:, 'level_1_cert_date'] = gw_cprr.uid.map(
        lambda x: post_pprr.loc[x, 'level_1_cert_date'])
    gw_cprr.loc[:, 'last_pc_12_qualification_date'] = gw_cprr.uid.map(
        lambda x: post_pprr.loc[x, 'last_pc_12_qualification_date'])
    return gw_cprr.drop(columns='mid')


if __name__ == '__main__':
    gw_cprr = pd.read_csv(data_file_path(
        'clean/cprr_greenwood_pd_2015_2020.csv'))
    post_pprr = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    gw_cprr = match(gw_cprr, post_pprr)
    ensure_data_dir("match")
    gw_cprr.to_csv(
        data_file_path('match/cprr_greenwood_pd_2015_2020.csv'), index=False)
