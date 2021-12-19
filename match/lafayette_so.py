import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex


def prepare_post():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'Lafayette SO']


def match_cprr_20_and_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = .8
    matcher.save_pairs_to_excel(
        data_file_path('match/lafayette_so_2015_2020_vs_post_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_14_and_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = .95
    matcher.save_pairs_to_excel(
        data_file_path('match/lafayette_so_2009_2014_vs_post_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_08_with_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity()
    }, dfa, dfb)
    decision = .936

    matcher.save_pairs_to_excel(data_file_path(
        'match/cprr_lafayette_so_2006_2008_v_post_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == '__main__':
    cprr20 = pd.read_csv(data_file_path('clean/cprr_lafayette_so_2015_2020.csv'))
    cprr14 = pd.read_csv(data_file_path('clean/cprr_lafayette_so_2009_2014.csv'))
    cprr08 = pd.read_csv(data_file_path('clean/cprr_lafayette_so_2006_2008.csv'))
    post = prepare_post()
    cprr20 = match_cprr_20_and_post(cprr20, post)
    cprr14 = match_cprr_14_and_post(cprr14, post)
    cprr18 = match_cprr_08_with_post(cprr08, post)
    cprr20.to_csv(data_file_path('match/cprr_lafayette_so_2015_2020.csv'), index=False)
    cprr14.to_csv(data_file_path('match/cprr_lafayette_so_2009_2014.csv'), index=False)
    cprr08.to_csv(data_file_path('match/cprr_lafayette_so_2006_2008.csv'), index=False)
