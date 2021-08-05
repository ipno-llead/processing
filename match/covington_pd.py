import sys

import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

from lib.path import data_file_path, ensure_data_dir
from lib.post import extract_events_from_post

sys.path.append('../')


def extract_post_events(roster, post):
    post = post.loc[post.agency == 'covington pd']

    dfa = roster.set_index('uid', drop=True)
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post[['last_name', 'first_name', 'uid']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid', drop=True)

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 0.9
    matcher.save_pairs_to_excel(data_file_path(
        "match/covington_pd_ah_2021_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "Covington PD")


if __name__ == '__main__':
    ah = pd.read_csv(data_file_path(
        'clean/actions_history_covington_pd_2021.csv'
    ))
    pprr = pd.read_csv(data_file_path(
        'clean/pprr_covington_pd_2020.csv'
    ))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post_events = extract_post_events(
        pd.concat([
            ah[['first_name', 'last_name', 'uid']],
            pprr[['first_name', 'last_name', 'uid']]
        ]).drop_duplicates(),
        post
    )
    ensure_data_dir("match")
    post_events.to_csv(data_file_path(
        "match/post_event_covington_pd_2020.csv"), index=False)
