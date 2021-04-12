from lib.match import (
    ThresholdMatcher, ColumnsIndex, JaroWinklerSimilarity, StringSimilarity, DateSimilarity
)
from lib.date import combine_date_columns
from lib.path import data_file_path, ensure_data_dir
from lib.post import prepare_post
import pandas as pd
import sys
sys.path.append('../')


def match_cprr_with_pprr(cprr, pprr):
    dfa = cprr[['uid', 'last_name', 'rank_desc']].drop_duplicates()\
        .set_index('uid', drop=True)
    dfa.loc[:, 'fc'] = dfa.last_name.fillna('').map(lambda x: x[:1])

    dfb = pprr[['uid', 'last_name', 'rank_desc']].drop_duplicates()\
        .set_index('uid', drop=True)
    dfb.loc[:, 'fc'] = dfb.last_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(['fc']), {
        'last_name': JaroWinklerSimilarity(),
        'rank_desc': StringSimilarity(),
    })
    return matcher


def match_pprr_against_post(pprr, post):
    dfa = pprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'hire_date'] = combine_date_columns(
        pprr, 'hire_year', 'hire_month', 'hire_day')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates().set_index('uid', drop=True)

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'hire_date'] = combine_date_columns(
        post, 'hire_year', 'hire_month', 'hire_day')
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates().set_index('uid', drop=True)

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
        'hire_date': DateSimilarity()
    })
    decision = 0.8
    matcher.save_pairs_to_excel(data_file_path(
        "match/mandeville_csd_pprr_2020_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr.loc[:, 'level_1_cert_date'] = pprr.uid.map(
        lambda x: post.loc[match_dict[x], 'level_1_cert_date'] if x in match_dict else '')
    pprr.loc[:, 'last_pc_12_qualification_date'] = pprr.uid.map(
        lambda x: post.loc[match_dict[x], 'last_pc_12_qualification_date'] if x in match_dict else '')
    return pprr


if __name__ == '__main__':
    post = prepare_post('mandeville pd')
    pprr = pd.read_csv(data_file_path('clean/mandeville_csd_pprr_2020.csv'))
    pprr = match_pprr_against_post(pprr, post)
    ensure_data_dir('match')
    pprr.to_csv(data_file_path(
        'match/mandeville_csd_pprr_2020.csv'), index=False)
