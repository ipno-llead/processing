from lib.match import (
    ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
)
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append('../')


def prepare_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    post = post[post.agency == 'st tammany parish so']
    duplicated_uids = set(post.loc[post.uid.duplicated(), 'uid'].to_list())
    post = post.set_index('uid', drop=False)
    level_1_cert_dates = post.loc[
        post.uid.isin(duplicated_uids) & (post.level_1_cert_date.notna()),
        'level_1_cert_date']
    for idx, value in level_1_cert_dates.iteritems():
        post.loc[idx, 'level_1_cert_date'] = value
    post = post.sort_values('last_pc_12_qualification_date', ascending=False)
    return post[~post.index.duplicated(keep='first')]


def match_cprr(cprr, pprr):
    dfa = cprr[['first_name', 'last_name', 'uid']]\
        .drop_duplicates(subset=['uid']).set_index('uid', drop=True)
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfb = pprr[['first_name', 'last_name', 'uid']]\
        .drop_duplicates(subset=['uid']).set_index('uid', drop=True)
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["fc"]), {
        "first_name": JaroWinklerSimilarity(),
        "last_name": JaroWinklerSimilarity(),
    })
    decision = 0.94
    matcher.save_pairs_to_excel(data_file_path(
        "match/st_tammany_so_cprr_2015_2019_v_pprr_2020.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)

    match_dict = dict(matches)
    cprr.loc[:, "uid"] = cprr["uid"].map(lambda v: match_dict.get(v, v))
    return cprr


def match_pprr_and_post(pprr, post):
    dfa = pprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["fc"]), {
        "last_name": JaroWinklerSimilarity(),
        "first_name": JaroWinklerSimilarity(),
    })
    decision = 0.955
    matcher.save_pairs_to_excel(data_file_path(
        "match/st_tammany_so_pprr_2020_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr.loc[:, 'level_1_cert_date'] = pprr.uid.map(
        lambda x: post.loc[match_dict[x], 'level_1_cert_date'] if x in match_dict else '')
    pprr.loc[:, 'last_pc_12_qualification_date'] = pprr.uid.map(
        lambda x: post.loc[match_dict[x], 'last_pc_12_qualification_date'] if x in match_dict else '')
    return pprr


def match_cprr_and_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["fc"]), {
        "last_name": JaroWinklerSimilarity(),
        "first_name": JaroWinklerSimilarity(),
    })
    decision = 0.9
    matcher.save_pairs_to_excel(data_file_path(
        "match/st_tammany_so_cprr_2015_2019_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, 'level_1_cert_date'] = cprr.uid.map(
        lambda x: post.loc[match_dict[x], 'level_1_cert_date'] if x in match_dict else '')
    cprr.loc[:, 'last_pc_12_qualification_date'] = cprr.uid.map(
        lambda x: post.loc[match_dict[x], 'last_pc_12_qualification_date'] if x in match_dict else '')
    return cprr


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path(
        'clean/cprr_st_tammany_so_2015_2019.csv'))
    pprr = pd.read_csv(data_file_path('clean/pprr_st_tammany_so_2020.csv'))
    post = prepare_post_data()
    cprr = match_cprr(cprr, pprr)
    pprr = match_pprr_and_post(pprr, post)
    cprr = match_cprr_and_post(cprr, post)
    ensure_data_dir('match')
    cprr.to_csv(data_file_path(
        'match/cprr_st_tammany_so_2015_2019.csv'), index=False)
    pprr.to_csv(data_file_path(
        'match/pprr_st_tammany_so_2020.csv'), index=False)
