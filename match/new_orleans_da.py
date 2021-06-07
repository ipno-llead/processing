from datamatch import ColumnsIndex, JaroWinklerSimilarity, ThresholdMatcher
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append('../')


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'new orleans pd']


def match_cprr(cprr, pprr):
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
        'match/cprr_new_orleans_da_2021_v_pprr_new_orleans_pd_1946_2018.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr['uid'].map(lambda v: match_dict.get(v, v))
    return cprr


def match_cprr_and_post(pprr, post):
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
        'match/cprr_new_orleans_da_2021_v_post_pprr_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr.loc[:, 'uid'] = pprr.uid.map(lambda x: match_dict.get(x, x))
    return pprr


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path(
        'clean/cprr_new_orleans_da_2021.csv'))
    pprr = pd.read_csv(data_file_path('clean/pprr_new_orleans_pd_1946_2018.csv'))
    post = prepare_post_data()
    cprr = match_cprr(cprr, pprr)
    ensure_data_dir('match')
    cprr.to_csv(data_file_path(
        'match/cprr_new_orleans_da_2021.csv'), index=False)

if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path(
        'clean/cprr_st_tammany_so_2011_2021.csv'))
    pprr = pd.read_csv(data_file_path('clean/pprr_st_tammany_so_2020.csv'))
    post = prepare_post_data()
    cprr = match_cprr(cprr, pprr)
    post_event = pd.concat([
        match_pprr_and_post(pprr, post),
        match_cprr_and_post(cprr, post)
    ]).drop_duplicates(ignore_index=True)
    ensure_data_dir('match')
    cprr.to_csv(data_file_path(
        'match/cprr_st_tammany_so_2011_2021.csv'), index=False)
    post_event.to_csv(data_file_path(
        'match/post_event_st_tammany_so_2020.csv'), index=False)
