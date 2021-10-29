import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.post import extract_events_from_post


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'hammond pd']


def deduplicate_cprr_14_officers(cprr):
    df = cprr[['uid', 'first_name', 'last_name']]
    df = df.drop_duplicates(subset=['uid']).set_index('uid')
    df.loc[:, 'fc'] = df.first_name.fillna('').map(lambda x: x[:1])
    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, df)
    decision = .85
    matcher.save_clusters_to_excel(data_file_path(
        'match/hammond_pd_cprr_2009_2014_deduplicate.xlsx'
    ), decision, decision)
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    # canonicalize name and uid
    for cluster in clusters:
        uid, first_name, last_name = None, '', ''
        for idx in cluster:
            row = df.loc[idx]
            if (
                uid is None
                or len(row.first_name) > len(first_name)
                or (len(row.first_name) == len(first_name) and len(row.last_name) > len(last_name))
            ):
                uid, first_name, last_name = idx, row.first_name, row.last_name
        cprr.loc[cprr.uid.isin(cluster), 'uid'] = uid
        cprr.loc[cprr.uid == uid, 'first_name'] = first_name
        cprr.loc[cprr.uid == uid, 'last_name'] = last_name
    return cprr


def deduplicate_cprr_20_officers(cprr):
    df = cprr[['uid', 'first_name', 'last_name']]
    df = df.drop_duplicates(subset=['uid']).set_index('uid')
    df.loc[:, 'fc'] = df.first_name.fillna('').map(lambda x: x[:1])
    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, df)
    decision = .92
    matcher.save_clusters_to_excel(data_file_path(
        'match/hammond_pd_cprr_2015_2020_deduplicate.xlsx'
    ), decision, decision)
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    # canonicalize name and uid
    for cluster in clusters:
        uid, first_name, last_name = None, '', ''
        for idx in cluster:
            row = df.loc[idx]
            if (
                uid is None
                or len(row.first_name) > len(first_name)
                or (len(row.first_name) == len(first_name) and len(row.last_name) > len(last_name))
            ):
                uid, first_name, last_name = idx, row.first_name, row.last_name
        cprr.loc[cprr.uid.isin(cluster), 'uid'] = uid
        cprr.loc[cprr.uid == uid, 'first_name'] = first_name
        cprr.loc[cprr.uid == uid, 'last_name'] = last_name
    return cprr


def match_cprr_14_and_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
    dfa = dfa.drop_duplicates(subset='uid').set_index('uid')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
        }, dfa, dfb)
    decision = .9
    matcher.save_pairs_to_excel(
        data_file_path('match/hammond_pd_cprr_2009_2014_v_post_pprr_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_clusters_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_20_and_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb = dfb.drop_duplicates(subset=['uid']).set_index(['uid'])
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = .865
    matcher.save_pairs_to_excel(
        data_file_path('match/hammond_pd_cprr_2015_2020_v_post_pprr_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_08_and_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb = dfb.drop_duplicates(subset=['uid']).set_index(['uid'])
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = .95
    matcher.save_pairs_to_excel(
        data_file_path('match/hammond_pd_cprr_2004_2008_v_post_pprr_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == '__main__':
    cprr_20 = pd.read_csv(data_file_path('clean/cprr_hammond_pd_2015_2020.csv'))
    cprr_14 = pd.read_csv(data_file_path('clean/cprr_hammond_pd_2009_2014.csv'))
    cprr_08 = pd.read_csv(data_file_path('clean/cprr_hammond_pd_2004_2008.csv'))
    post = prepare_post_data()
    cprr_14 = deduplicate_cprr_14_officers(cprr_14)
    cprr_20 = deduplicate_cprr_20_officers(cprr_20)
    cprr_20 = match_cprr_20_and_post(cprr_20, post)
    cprr_14 = match_cprr_14_and_post(cprr_14, post)
    cprr_08 = match_cprr_08_and_post(cprr_08, post)
    ensure_data_dir('match')
    cprr_20.to_csv(data_file_path('match/cprr_hammond_pd_2015_2020.csv'), index=False)
    cprr_14.to_csv(data_file_path('match/cprr_hammond_pd_2009_2014.csv'), index=False)
