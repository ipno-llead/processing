import sys
sys.path.append('../')
import pandas as pd
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.post import extract_events_from_post
from lib.path import data_file_path


def match_cprr_with_caddo(cprr, pprr):
    dfa = cprr[['first_name', 'last_name', 'uid', 'agency']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = pprr[['first_name', 'last_name', 'uid', 'agency']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(ColumnsIndex(['fc', 'agency']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)

    decision = .8
    matcher.save_pairs_to_excel(data_file_path(
        "match/cprr_post_2016_2019_v_pprr_caddo_parish_so_2020.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_with_brusly(cprr, pprr):
    dfa = cprr[['first_name', 'last_name', 'uid', 'agency']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = pprr[['first_name', 'last_name', 'uid', 'agency']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(ColumnsIndex(['fc', 'agency']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)

    decision = .8
    matcher.save_pairs_to_excel(data_file_path(
        "match/cprr_post_2016_2019_v_pprr_kenner_pd_2020.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_with_ponchatoula(cprr, pprr):
    dfa = cprr[['first_name', 'last_name', 'uid', 'agency']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = pprr[['first_name', 'last_name', 'uid', 'agency']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(ColumnsIndex(['fc', 'agency']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)

    decision = .8
    matcher.save_pairs_to_excel(data_file_path(
        "match/cprr_post_2016_2019_v_pprr_ponchatoula_pd_2010_2020.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_with_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name', 'agency']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name', 'agency']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(ColumnsIndex(["fc", "agency"]), {
        "last_name": JaroWinklerSimilarity(),
        "first_name": JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 0.9
    matcher.save_pairs_to_excel(data_file_path(
        "match/cprr_post_2016_2019_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "Kenner PD")


if __name__ == '__main__':
    caddo = pd.read_csv(data_file_path('clean/pprr_caddo_parish_so_2020.csv'))
    kenner = pd.read_csv(data_file_path('clean/pprr_kenner_pd_2020.csv'))
    ponchatoula = pd.read_csv(data_file_path('clean/pprr_ponchatoula_pd_2010_2020.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    cprr = pd.read_csv(data_file_path('clean/cprr_post_2016_2019.csv'))
    cprr_caddo = match_cprr_with_caddo(cprr, caddo)
    cprr_kenner = match_cprr_with_brusly(cprr, kenner)
    cprr_ponchatoula = match_cprr_with_ponchatoula(cprr, ponchatoula)
    post_event = match_cprr_with_post(cprr, post)
    post_event.to_csv(data_file_path(
        'match/post_event_cprr_post_2016_2019.csv'), index=False)
