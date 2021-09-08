import sys
sys.path.append('../')
import pandas as pd
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, NoopIndex, ColumnsIndex
from lib.path import data_file_path, ensure_data_dir


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'lake charles pd']


# def dedup_cprr(cprr):
#     df = cprr[['last_name', 'uid']].drop_duplicates()\
#         .set_index('uid', drop=True)
#     matcher = ThresholdMatcher(NoopIndex(), {
#         'last_name': JaroWinklerSimilarity(),
#     }, df)
#     decision = 0.89
#     matcher.save_clusters_to_excel(data_file_path(
#         "match/lafayette_pd_cprr_2015_2020_officer_dedup.xlsx"
#     ), decision, lower_bound=decision)
#     clusters = matcher.get_index_clusters_within_thresholds(lower_bound=decision)

#     for cluster in clusters:
#         uid, last_name = None, '', ''
#         for idx in cluster:
#             row = df.loc[idx]
#             if (
#                 uid is None
#                 or (len(row.last_name) > len(last_name))
#             ):
#                 uid, last_name = idx, row.last_name
#         cprr.loc[cprr.uid.isin(cluster), 'uid'] = uid
#         cprr.loc[cprr.uid == uid, 'last_name'] = last_name
#     return cprr


# def assign_first_name_from_post(cprr, post):
#     dfa = cprr.loc[cprr.uid.notna(), ['uid', 'last_name']].drop_duplicates(subset=['uid'])\
#         .set_index('uid', drop=True)
#     dfa.loc[:, 'fc'] = dfa.last_name.fillna('').map(lambda x: x[:1])

#     dfb = post[['uid', 'first_name', 'last_name']].drop_duplicates()\
#         .set_index('uid', drop=True)
#     dfb.loc[:, 'fc'] = dfb.last_name.map(lambda x: x[:1])

#     matcher = ThresholdMatcher(ColumnsIndex('fc'), {

#         'last_name': JaroWinklerSimilarity(),
#     }, dfa, dfb)
#     decision = 0.93
#     matcher.save_pairs_to_excel(data_file_path(
#         "match/new_orleans_so_cprr_20_officer_v_post_pprr_2020_11_06.xlsx"), decision)
#     matches = matcher.get_index_pairs_within_thresholds(decision)
#     match_dict = dict(matches)

#     cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
#     return cprr


# def assign_uid_from_post(cprr, post):
#     dfa = cprr.loc[cprr.uid.notna(), ['uid', 'first_name', 'last_name']].drop_duplicates(subset=['uid'])\
#         .set_index('uid', drop=True)
#     dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

#     dfb = post[['uid', 'first_name', 'last_name']].drop_duplicates()\
#         .set_index('uid', drop=True)
#     dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])

#     matcher = ThresholdMatcher(ColumnsIndex('fc'), {
#         'first_name': JaroWinklerSimilarity(),
#         'last_name': JaroWinklerSimilarity(),
#     }, dfa, dfb)
#     decision = 0.93
#     matcher.save_pairs_to_excel(data_file_path(
#         "match/lake_charles_pd_2020_v_post_pprr_2020_11_06.xlsx"), decision)
#     matches = matcher.get_index_pairs_within_thresholds(decision)
#     match_dict = dict(matches)

#     cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
#     return cprr


def assign_uid_from_post(cprr, post):
    dfa = cprr[['first_name', 'last_name', 'uid']]
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates().set_index('uid', drop=True)

    dfb = post[['last_name', 'first_name', 'uid']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid', drop=True)

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 0.95
    matcher.save_pairs_to_excel(data_file_path(
        "match/lake_charles_pd_202o_v_post.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == '__main__':
    cprr = (data_file_path('clean/cprr_lake_charles_pd_2020.csv'))
    post = (data_file_path('clean/pprr_post_2020_11_06.csv'))
    post = prepare_post_data()
    cprr = assign_uid_from_post(cprr, post)
    ensure_data_dir('match')
    cprr.to_csv(data_file_path(
        'match/cprr_lake_charles_pd_2020.csv'), index=False)
