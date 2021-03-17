from lib.date import combine_date_columns
from lib.match import ThresholdMatcher, JaroWinklerSimilarity, NoopIndex, DateSimilarity
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append('../')


def match_csd_pprr_against_post_pprr(pprr, post):
    post = post[post.agency == 'port allen pd'].set_index('uid', drop=False)

    dfa = pprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'hire_date'] = combine_date_columns(
        pprr, 'hire_year', 'hire_month', 'hire_day')
    dfa = dfa.set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'hire_date'] = combine_date_columns(
        post, 'hire_year', 'hire_month', 'hire_day')
    dfb = dfb.set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, NoopIndex(), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
        'hire_date': DateSimilarity(),
    })
    decision = 0.989
    matcher.save_pairs_to_excel(data_file_path(
        "match/port_allen_csd_pprr_2020_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    pprr.loc[:, 'level_1_cert_date'] = pprr.uid.map(
        lambda x: post.loc[match_dict[x], 'level_1_cert_date'] if x in match_dict else '')
    pprr.loc[:, 'last_pc_12_qualification_date'] = pprr.uid.map(
        lambda x: post.loc[match_dict[x], 'last_pc_12_qualification_date'] if x in match_dict else '')
    return pprr


def match_cprr_2018_against_csd_pprr_2020(cprr, pprr):
    dfa = cprr[['first_name', 'last_name', 'uid']].drop_duplicates()\
        .set_index('uid')

    dfb = pprr[['first_name', 'last_name', 'uid']].set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, NoopIndex(), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    })
    decision = 0.96
    matcher.save_pairs_to_excel(data_file_path(
        "match/port_allen_pd_cprr_2018_v_csd_pprr_2020.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_2019_against_csd_pprr_2020(cprr, pprr):
    dfa = cprr[['first_name', 'last_name', 'uid']].drop_duplicates()\
        .set_index('uid')

    dfb = pprr[['first_name', 'last_name', 'uid']].set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, NoopIndex(), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    })
    decision = 0.969
    matcher.save_pairs_to_excel(data_file_path(
        "match/port_allen_pd_cprr_2019_v_csd_pprr_2020.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('clean/pprr_port_allen_csd_2020.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    cprr19 = pd.read_csv(data_file_path('clean/cprr_port_allen_pd_2019.csv'))
    pprr = match_csd_pprr_against_post_pprr(pprr, post)
    cprr19 = match_cprr_2019_against_csd_pprr_2020(cprr19, pprr)
    ensure_data_dir('match')
    pprr.to_csv(data_file_path(
        'match/pprr_port_allen_csd_2020.csv'), index=False)
    cprr19.to_csv(data_file_path(
        'match/cprr_port_allen_pd_2019.csv'), index=False)
