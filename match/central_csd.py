import sys
sys.path.append('../')
from lib.path import data_file_path
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
from lib.post import extract_events_from_post
import pandas as pd


def prepare_post():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'Central PD']


def match_pprr_with_post(pprr, post):
    dfa = pprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(ColumnsIndex(["fc"]), {
        "last_name": JaroWinklerSimilarity(),
        "first_name": JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 0.88
    matcher.save_pairs_to_excel(data_file_path(
        "match/central_pd_pprr_2014_2019_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, 'Central PD')


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('clean/pprr_central_csd_2014_2019.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post_events = match_pprr_with_post(pprr, post)
    post_events.to_csv(data_file_path(
        'match/post_events_central_csd_2020.csv'), index=False)
