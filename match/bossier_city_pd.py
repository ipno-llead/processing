import sys
sys.path.append('../')
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.path import data_file_path
from lib.post import extract_events_from_post
import pandas as pd


def extract_post_events(pprr, post):
    dfa = pprr[['first_name', 'last_name', 'uid']]
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['first_name', 'last_name', 'uid']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 0.95
    matcher.save_pairs_to_excel(data_file_path(
        "match/pprr_bossier_city_pd_v_post_2016_2019.xlsx"), decision)
    matches = matcher.get_index_clusters_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, 'Bossier City PD')


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('clean/pprr_bossier_city_pd_2000_2019.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post = post[post.agency == 'bossier city pd']
    post_event = extract_post_events(pprr, post)
    post_event.to_csv(data_file_path(
        'match/post_event_bossier_city_pd.csv'), index=False)
