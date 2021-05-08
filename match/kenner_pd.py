import sys

import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex, DateSimilarity

from lib.path import data_file_path, ensure_data_dir
from lib.post import extract_events_from_post
from lib.date import combine_date_columns

sys.path.append('../')


def extract_post_events(pprr, post):
    post = post.loc[post.agency == 'kenner pd']

    dfa = pprr[['first_name', 'last_name', 'uid']]
    dfa.loc[:, 'hire_date'] = combine_date_columns(
        pprr, 'hire_year', 'hire_month', 'hire_day')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates().set_index('uid', drop=True)

    dfb = post[['last_name', 'first_name', 'uid']]
    dfb.loc[:, 'hire_date'] = combine_date_columns(
        post, 'hire_year', 'hire_month', 'hire_day')
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid', drop=True)

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
        'hire_date': DateSimilarity(),
    })
    decision = 0.89
    matcher.save_pairs_to_excel(data_file_path(
        "match/kenner_pd_pprr_2020_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "Kenner PD")


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('clean/pprr_kenner_pd_2020.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post_events = extract_post_events(pprr, post)
    ensure_data_dir("match")
    post_events.to_csv(data_file_path(
        "match/post_event_kenner_pd_2020.csv"), index=False)
