import sys

from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex, Swap
import pandas as pd

from lib.path import data_file_path, ensure_data_dir
from lib.post import extract_events_from_post

sys.path.append('../')


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'la state police']


def match_lprr_and_pprr(lprr, pprr):
    dfa = lprr[['uid', 'first_name', 'last_name']].drop_duplicates()\
        .set_index('uid', drop=True)
    dfa = dfa.fillna(value={'first_name': '', 'last_name': ''})
    dfa.loc[:, 'fc'] = dfa.apply(lambda row: ''.join(
        sorted([row.first_name[:1], row.last_name[:1]])
    ), axis=1, result_type='reduce')

    dfb = pprr[['uid', 'first_name', 'last_name']].drop_duplicates()\
        .set_index('uid', drop=True)
    dfb = dfb.fillna(value={'first_name': '', 'last_name': ''})
    dfb.loc[:, 'fc'] = dfb.apply(lambda row: ''.join(
        sorted([row.first_name[:1], row.last_name[:1]])
    ), axis=1, result_type='reduce')

    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb, variator=Swap('first_name', 'last_name'))
    decision = 0.969
    matcher.save_pairs_to_excel(data_file_path(
        "match/louisiana_state_csc_lprr_1991_2020_v_csd_pprr_2021.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    lprr.loc[:, 'uid'] = lprr.uid.map(lambda x: match_dict.get(x, x))
    return lprr


def extract_post_events(pprr, post):
    dfa = pprr[['uid', 'first_name', 'last_name']].drop_duplicates()\
        .set_index('uid', drop=True)
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post[['uid', 'first_name', 'last_name']].drop_duplicates()\
        .set_index('uid', drop=True)
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 0.924
    matcher.save_pairs_to_excel(data_file_path(
        "match/louisiana_state_csd_pprr_2021_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "Louisiana State Police")


def match_pprr_termination(pprr, pprr_term):
    dfa = pprr_term[['uid', 'last_name', 'first_name']]\
        .drop_duplicates(subset=['uid']).set_index('uid', drop=True)
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = pprr[['uid', 'last_name', 'first_name']]\
        .drop_duplicates(subset=['uid']).set_index('uid', drop=True)
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 1
    matcher.save_pairs_to_excel(data_file_path(
        'match/louisiana_state_csd_pprr_2021_v_pprr_term.xlsx'
    ), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr_term.loc[:, 'uid'] = pprr_term.uid.map(lambda x: match_dict.get(x, x))
    return pprr_term


if __name__ == '__main__':
    lprr = pd.read_csv(data_file_path(
        'clean/lprr_louisiana_state_csc_1991_2020.csv'))
    post = prepare_post_data()
    pprr = pd.read_csv(data_file_path('clean/pprr_louisiana_csd_2021.csv'))
    pprr_term = pd.read_csv(data_file_path('clean/pprr_term_louisiana_csd_2021.csv'))
    lprr = match_lprr_and_pprr(lprr, pprr)
    post_events = extract_post_events(pprr, post)
    ensure_data_dir('match')
    pprr_term = match_pprr_termination(pprr, pprr_term)
    lprr.to_csv(data_file_path(
        'match/lprr_louisiana_state_csc_1991_2020.csv'
    ), index=False)
    post_events.to_csv(data_file_path(
        "match/post_event_louisiana_state_police_2020.csv"), index=False)
    pprr_term.to_csv(data_file_path(
        'match/pprr_term_louisiana_csd_2021.csv'
    ), index=False)
