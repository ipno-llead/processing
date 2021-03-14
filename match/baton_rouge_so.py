from lib.path import data_file_path, ensure_data_dir
from lib.match import (
    ColumnsIndex, JaroWinklerSimilarity, ThresholdMatcher
)
import pandas as pd
import sys
sys.path.append("../")


def prepare_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    post = post[post.agency == 'e. baton rouge so']
    duplicated_uids = set(post.loc[post.uid.duplicated(), 'uid'].to_list())
    post = post.set_index('uid', drop=False)
    level_1_cert_dates = post.loc[
        post.uid.isin(duplicated_uids) & (post.level_1_cert_date.notna()),
        'level_1_cert_date']
    for idx, value in level_1_cert_dates.iteritems():
        post.loc[idx, 'level_1_cert_date'] = value
    post = post.sort_values('last_pc_12_qualification_date', ascending=False)
    return post[~post.index.duplicated(keep='first')]


def match_csd_pprr_against_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["fc"]), {
        "last_name": JaroWinklerSimilarity(),
        "first_name": JaroWinklerSimilarity(),
    })
    decision = 0.969
    matcher.save_pairs_to_excel(data_file_path(
        "match/baton_rouge_so_cprr_2018_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, 'level_1_cert_date'] = cprr.uid.map(
        lambda x: '' if x not in match_dict else post.loc[match_dict[x], 'level_1_cert_date'])
    cprr.loc[:, 'last_pc_12_qualification_date'] = cprr.uid.map(
        lambda x: '' if x not in match_dict else post.loc[match_dict[x], 'last_pc_12_qualification_date'])
    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('clean/cprr_baton_rouge_so_2018.csv'))
    post = prepare_post_data()
    cprr = match_csd_pprr_against_post(cprr, post)
    ensure_data_dir('match')
    cprr.to_csv(data_file_path(
        'match/cprr_baton_rouge_so_2018.csv'), index=False)
