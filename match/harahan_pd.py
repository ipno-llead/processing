import sys

import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

from lib.path import data_file_path, ensure_data_dir
from lib.post import extract_events_from_post

sys.path.append('../')


def match_pd_pprr_with_csd_pprr(pdpprr, csdpprr):
    dfa = pdpprr[['uid', 'first_name', 'last_name']].drop_duplicates()\
        .set_index('uid', drop=True)
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])

    dfb = csdpprr[['uid', 'first_name', 'last_name']].drop_duplicates()\
        .set_index('uid', drop=True)
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 0.97
    matcher.save_pairs_to_excel(data_file_path(
        "match/harahan_pd_pprr_2020_v_csd_pprr_2020.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    pdpprr.loc[:, 'uid'] = pdpprr.uid.map(lambda x: match_dict.get(x, x))
    return pdpprr


def match_pprr_post(pprr, post):
    dfa = pprr[['first_name', 'last_name', 'uid']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates().set_index('uid', drop=True)

    dfb = post[['first_name', 'last_name', 'uid']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates().set_index('uid', drop=True)

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 0.96
    matcher.save_pairs_to_excel(data_file_path(
        "match/harahan_csd_pprr_2020_v_post.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, 'Harahan PD')


if __name__ == '__main__':
    pdpprr = pd.read_csv(data_file_path('clean/pprr_harahan_pd_2020.csv'))
    csdpprr = pd.read_csv(data_file_path('clean/pprr_harahan_csd_2020.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post = post[post.agency == 'harahan pd']
    ensure_data_dir('match')
    pdpprr = match_pd_pprr_with_csd_pprr(pdpprr, csdpprr)
    post_event = match_pprr_post(csdpprr, post)
    pdpprr.to_csv(
        data_file_path('match/pprr_harahan_pd_2020.csv'), index=False
    )
    post_event.to_csv(
        data_file_path('match/post_event_harahan_pd.csv'), index=False
    )
