from lib.date import combine_date_columns
from lib.path import data_file_path, ensure_data_dir
from datamatch import (
    ThresholdMatcher, JaroWinklerSimilarity, DateSimilarity, ColumnsIndex, NoopIndex
)
from lib.post import extract_events_from_post
import pandas as pd
import sys
sys.path.append('../')


def match_pprr_against_post(pprr, post):
    dfa = pprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'hire_date'] = combine_date_columns(
        pprr, 'hire_year', 'hire_month', 'hire_day')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'hire_date'] = combine_date_columns(
        post, 'hire_year', 'hire_month', 'hire_day')
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
        'hire_date': DateSimilarity()
    }, dfa, dfb)
    decision = 0.803
    matcher.save_pairs_to_excel(data_file_path(
        "match/new_orleans_pd_pprr_1946_2018_v_post_pprr_2020_11_06.xlsx"), decision)

    matches = matcher.get_index_pairs_within_thresholds(decision)
    return extract_events_from_post(post, matches, 'New Orleans PD')


def match_award_to_pprr(award, pprr):
    dfa = award[['uid', 'first_name', 'last_name']].drop_duplicates()\
        .set_index('uid', drop=True)

    dfb = pprr[['uid', 'first_name', 'last_name']].drop_duplicates()\
        .set_index('uid', drop=True)

    matcher = ThresholdMatcher(NoopIndex(), {
        "first_name": JaroWinklerSimilarity(),
        "last_name": JaroWinklerSimilarity()
    }, dfa, dfb)
    decision = 0.93
    matcher.save_pairs_to_excel(data_file_path(
        "match/new_orleans_pd_award_2016_2021_v_pprr.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    match_dict = dict(matches)
    award.loc[:, 'uid'] = award.uid.map(lambda x: match_dict.get(x, x))
    return award


def match_lprr_to_pprr(lprr, pprr):
    dfa = lprr[['uid', 'first_name', 'last_name', 'middle_initial']]
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = pprr[['uid', 'first_name', 'last_name', 'middle_initial']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
        'middle_initial': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = .80
    matcher.save_pairs_to_excel(data_file_path(
        'match/new_orleans_lprr_2000_2016_v_pprr_new_orleans_pd_1946_2018.xlsx'), decision)
    matches = matcher.get_index_clusters_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    lprr.loc[:, 'uid'] = lprr.uid.map(lambda x: match_dict.get(x, x))
    return lprr


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('clean/pprr_new_orleans_pd_1946_2018.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    award = pd.read_csv(data_file_path('clean/award_new_orleans_pd_2016_2021.csv'))
    lprr = pd.read_csv(data_file_path('clean/lprr_new_orleans_csc_2000_2016.csv'))
    post = post[post.agency == 'new orleans pd'].reset_index(drop=True)
    event_df = match_pprr_against_post(pprr, post)
    award = match_award_to_pprr(award, pprr)
    lprr = match_lprr_to_pprr(lprr, pprr)
    ensure_data_dir('match')
    award.to_csv(data_file_path(
        'match/award_new_orleans_pd_2016_2021.csv'), index=False)
    event_df.to_csv(data_file_path(
        'match/post_event_new_orleans_pd.csv'), index=False)
    lprr.to_csv(data_file_path(
        'match/lprr_new_orleans_csc_2000_2016.csv'), index=False)
