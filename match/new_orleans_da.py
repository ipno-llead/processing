from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
from lib.path import data_file_path, ensure_data_dir
from lib.clean import float_to_int_str
import pandas as pd
import sys
sys.path.append('../')

def prepare_new_orleans_so_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'orleans parish so']


def prepare_new_orleans_pd_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'new orleans pd']


def match_cprr_and_noso_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
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
        'match/new_orleans_da_cprr_2021_v_post_noso_pprr_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_and_nopd_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
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
        'match/new_orleans_da_cprr_2021_v_post_nopd_pprr_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_against_new_orleans_pd_personnel(cprr, pprr):
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


def match_against_new_orleans_so_personnel(cprr, pprr_noso):
    dfa = cprr[['first_name', 'last_name', 'uid']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = pprr_noso[['first_name', 'last_name', 'uid']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid', drop=True)

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'last_name': JaroWinklerSimilarity(),
        'first_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 1
    matcher.save_pairs_to_excel(data_file_path(
        'match/new_orleans_da_cprr_2021_v_per_new_orleans_so.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr['uid'].map(lambda v: match_dict.get(v, v))
    return cprr


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('match/pprr_new_orleans_pd_1946_2018.csv'))
    pprr = float_to_int_str(
        pprr, ['first_name', 'last_name'])
    so_per = pd.read_csv(data_file_path('match/per_new_orleans_so.csv'))
    df = pd.read_csv(data_file_path('clean/cprr_new_orleans_da_2021.csv'))
    df = float_to_int_str(
        df, ['first_name', 'last_name'])
    df = match_against_new_orleans_pd_personnel(df, pprr)
    df = match_against_new_orleans_so_personnel(df, so_per)
    post_noso = prepare_new_orleans_so_post_data()
    post_nopd = prepare_new_orleans_pd_post_data()
    post_event = pd.concat([
        match_cprr_and_noso_post(df, post_noso),
        match_cprr_and_nopd_post(df, post_nopd)
    ]).drop_duplicates(ignore_index=True)
    ensure_data_dir('match')
    df.to_csv(data_file_path('match/cprr_new_orleans_da_2021.csv'), index=False)
    post_event.to_csv(data_file_path('match/post_event_new_orleans_da_2021.csv'), index=False)
