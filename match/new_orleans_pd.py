from lib.date import combine_date_columns
from lib.path import data_file_path, ensure_data_dir
from lib.match import (
    ThresholdMatcher, JaroWinklerSimilarity, DateSimilarity, ColumnsIndex
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

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
        'hire_date': DateSimilarity()
    })
    decision = 0.886
    matcher.save_pairs_to_excel(data_file_path(
        "match/new_orleans_pd_pprr_1946_2018_v_post_pprr_2020_11_06.xlsx"), decision)

    matches = matcher.get_index_pairs_within_thresholds(decision)
    return extract_events_from_post(post, matches)


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path(
        'clean/pprr_new_orleans_pd_1946_2018.csv'
    ))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post = post[post.agency == 'new orleans pd'].reset_index(drop=True)
    event_df = match_pprr_against_post(pprr, post)
    ensure_data_dir('match')
    event_df.to_csv(data_file_path(
        'match/post_event_new_orleans_pd.csv'), index=False)
