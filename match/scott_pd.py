from lib.match import (
    ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
)
from lib.path import data_file_path, ensure_data_dir
from lib.post import extract_events_from_post
import pandas as pd
import sys
sys.path.append('../')


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'scott pd']


def match_cprr(cprr, pprr):
    dfa = cprr[['first_name', 'last_name', 'uid']]\
        .drop_duplicates(subset=['uid']).set_index('uid', drop=True)
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfb = pprr[['first_name', 'last_name', 'uid']]\
        .drop_duplicates(subset=['uid']).set_index('uid', drop=True)
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    })
    decision = 0.90
    matcher.save_pairs_to_excel(data_file_path(
        'match/scott_pd_cprr_2020_v_scott_pd_pprr_2021.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)

    match_dict = dict(matches)
    cprr.loc[:, 'uid'] = cprr['uid'].map(lambda v: match_dict.get(v, v))
    return cprr


def match_pprr_and_post(pprr, post):
    dfa = pprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(['fc']), {
        'last_name': JaroWinklerSimilarity(),
        'first_name': JaroWinklerSimilarity(),
    })
    decision = .90
    matcher.save_pairs_to_excel(data_file_path(
        'match/scott_pd_pprr_2021_v_post_pprr_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, 'Scott PD')


def match_cprr_and_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(['fc']), {
        'last_name': JaroWinklerSimilarity(),
        'first_name': JaroWinklerSimilarity(),
    })
    decision = .90
    matcher.save_pairs_to_excel(data_file_path(
        'match/scott_pd_cprr_2020_v_post_pprr_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, 'Scott PD')


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path(
        'clean/cprr_scott_pd_2020.csv'))
    pprr = pd.read_csv(data_file_path('clean/pprr_scott_pd_2021.csv'))
    post = prepare_post_data()
    cprr = match_cprr(cprr, pprr)
    post_event = pd.concat([
        match_pprr_and_post(pprr, post),
        match_cprr_and_post(cprr, post)
    ]).drop_duplicates(ignore_index=True)
    ensure_data_dir('match')
    cprr.to_csv(data_file_path(
        'match/cprr_scott_pd_2020.csv'), index=False)
    post_event.to_csv(data_file_path(
        'match/post_event_scott_pd_2021.csv'), index=False)
