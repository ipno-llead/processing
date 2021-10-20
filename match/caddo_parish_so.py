import sys

from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex, DateSimilarity
import pandas as pd

from lib.path import data_file_path, ensure_data_dir
from lib.date import combine_date_columns
from lib.post import extract_events_from_post, extract_events_from_cprr_post

sys.path.append('../')


def match_pprr_against_post(pprr, post):
    dfa = pprr[['first_name', 'last_name', 'uid']]
    dfa.loc[:, 'hire_date'] = combine_date_columns(
        pprr, 'hire_year', 'hire_month', 'hire_day')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid'], ignore_index=True)\
        .set_index('uid', drop=True)

    dfb = post[['first_name', 'last_name', 'uid']]
    dfb.loc[:, 'hire_date'] = combine_date_columns(
        post, 'hire_year', 'hire_month', 'hire_day')
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid'], ignore_index=True)\
        .set_index('uid', drop=True)

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
        'hire_date': DateSimilarity()
    }, dfa, dfb)
    decision = 0.793
    matcher.save_pairs_to_excel(data_file_path(
        "match/caddo_parish_so_pprr_2020_v_post.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, 'Caddo Parish SO')


def extract_cprr_post_events(pprr, cprr_post):
    dfa = pprr[['first_name', 'last_name', 'uid', 'agency']]
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid'], ignore_index=True)\
        .set_index('uid', drop=True)

    dfb = cprr_post[['first_name', 'last_name', 'uid', 'agency']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid'], ignore_index=True)\
        .set_index('uid', drop=True)

    matcher = ThresholdMatcher(ColumnsIndex(['fc', 'agency']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 0.95
    matcher.save_pairs_to_excel(data_file_path(
        "match/pprr_caddo_parish_so_2020_v_cprr_post_2016_2019.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_cprr_post(cprr_post, matches, 'Caddo Parish SO')


if __name__ == '__main__':
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post = post[post.agency == 'caddo parish so'].reset_index(drop=True)
    pprr = pd.read_csv(data_file_path('clean/pprr_caddo_parish_so_2020.csv'))
    cprr_post = pd.read_csv(data_file_path('match/cprr_post_2016_2019.csv'))
    post_event = match_pprr_against_post(pprr, post)
    cprr_post_event = extract_cprr_post_events(pprr, cprr_post)
    ensure_data_dir('match')
    post_event.to_csv(data_file_path(
        'match/post_event_caddo_parish_so.csv'), index=False)
    cprr_post_event.to_csv(data_file_path(
        'match/cprr_post_event_caddo_parish_so.csv'), index=False)
