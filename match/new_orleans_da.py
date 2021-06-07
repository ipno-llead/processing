from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append('../')


def prepare_noso_post_data():
    noso_post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return noso_post[noso_post.agency == 'orleans parish so']


def prepare_nopd_post_data():
    nopd_post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return nopd_post[nopd_post.agency == 'new orleans pd']


def match_cprr_and_nopd_post(cprr, nopd_post):
    dfa = cprr[['uid', 'first_name', 'last_name']].drop_duplicates(subset=['uid'])\
        .set_index('uid', drop=True)
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = nopd_post[['uid', 'first_name', 'last_name']].drop_duplicates(subset=['uid'])\
        .set_index('uid', drop=True)
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity()
    }, dfa, dfb)
    decision = 1
    matcher.save_pairs_to_excel(data_file_path(
        "match/new_orleans_da_v_post_nopd_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict[x])
    return cprr


def match_cprr_and_noso_post(cprr, noso_post):
    dfa = cprr[['uid', 'first_name', 'last_name']].drop_duplicates(subset=['uid'])\
        .set_index('uid', drop=True)
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = noso_post[['uid', 'first_name', 'last_name']].drop_duplicates(subset=['uid'])\
        .set_index('uid', drop=True)
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity()
    }, dfa, dfb)
    decision = 1
    matcher.save_pairs_to_excel(data_file_path(
        "match/new_orleans_da_v_post_noso_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict[x])
    return cprr

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
        'match/new_orleans_da_cprr_2021_v_new_orleans_pd_1946_2018.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr['uid'].map(lambda v: match_dict.get(v, v))
    return cprr


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('clean/cprr_new_orleans_da_2021.csv'))
    pprr = pd.read_csv(data_file_path('clean/pprr_new_orleans_pd_1946_2018.csv'))
    nopd_post = prepare_nopd_post_data()
    noso_post = prepare_noso_post_data()
    cprr = match_cprr(cprr, pprr)
    post_event = pd.concat([
        match_cprr_and_nopd_post(cprr, nopd_post),
        match_cprr_and_noso_post(cprr, noso_post)
    ]).drop_duplicates(ignore_index=True)
    ensure_data_dir('match')
    cprr.to_csv(data_file_path(
        'match/cprr_scott_pd_2020.csv'), index=False)
    post_event.to_csv(data_file_path(
        'match/post_event_new_orleans_da_2021.csv'), index=False)


