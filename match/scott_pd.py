from datamatch import ColumnsIndex, JaroWinklerSimilarity, ThresholdMatcher
from lib.path import data_file_path, ensure_data_dir
from lib.post import extract_events_from_post
import pandas as pd
import sys
sys.path.append('../')


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post


def match_cprr_20_and_pprr(cprr, pprr):
    dfa = cprr[['first_name', 'last_name', 'uid']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = pprr[['first_name', 'last_name', 'uid']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid', drop=True)

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'last_name': JaroWinklerSimilarity(),
        'first_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 1
    matcher.save_pairs_to_excel(data_file_path(
        'match/scott_pd_cprr_2020_v_scott_pd_pprr_2021.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr['uid'].map(lambda v: match_dict.get(v, v))
    return cprr


def match_cprr_14_and_pprr(cprr, pprr):
    dfa = cprr[['first_name', 'last_name', 'uid']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = pprr[['first_name', 'last_name', 'uid']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid', drop=True)

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'last_name': JaroWinklerSimilarity(),
        'first_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 1
    matcher.save_pairs_to_excel(data_file_path(
        'match/scott_pd_cprr_2009_2014_v_scott_pd_pprr_2021.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr['uid'].map(lambda v: match_dict.get(v, v))
    return cprr


def extract_post_events(pprr, post):
    dfa = pprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'last_name': JaroWinklerSimilarity(),
        'first_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 1
    matcher.save_pairs_to_excel(data_file_path(
        "match/scott_pd_pprr_2021_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, 'Scott PD')


if __name__ == '__main__':
    cprr20 = pd.read_csv(data_file_path('clean/cprr_scott_pd_2020.csv'))
    cprr14 = pd.read_csv(data_file_path('clean/cprr_scott_pd_2009_2014.csv'))
    pprr = pd.read_csv(data_file_path('clean/pprr_scott_pd_2021.csv'))
    post = prepare_post_data()
    cprr20 = match_cprr_20_and_pprr(cprr20, pprr)
    cprr14 = match_cprr_14_and_pprr(cprr14, pprr)
    post_event = extract_post_events(pprr, post)
    ensure_data_dir('match')
    cprr20.to_csv(data_file_path(
        'match/cprr_scott_pd_2020.csv'), index=False)
    cprr14.to_csv(data_file_path(
        'match/cprr_scott_pd_2009_2014.csv'), index=False)
    post_event.to_csv(data_file_path(
        'match/post_event_scott_pd_2021.csv'), index=False)
