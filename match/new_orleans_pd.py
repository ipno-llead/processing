from lib.date import combine_date_columns
from lib.path import data_file_path, ensure_data_dir
from datamatch import (
    ThresholdMatcher, JaroWinklerSimilarity, DateSimilarity, ColumnsIndex, NoopIndex, Swap
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


def deduplicate_award_personnel(award):
    df = award.loc[
        award.uid.notna(),
        ['badge_no', 'first_name', 'last_name', 'middle_initial', 'uid']
    ].drop_duplicates().set_index('uid', drop=True)

    matcher = ThresholdMatcher(ColumnsIndex('badge_no'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, df, variator=Swap('first_name', 'last_name'))
    decision = 0.9
    matcher.save_clusters_to_excel(data_file_path(
        "match/new_orleans_pd_award_dedup.xlsx"
    ), decision, decision)
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    # canonicalize name and uid
    for cluster in clusters:
        uid, first_name, last_name, middle_initial = None, '', '', ''
        for idx in cluster:
            row = df.loc[idx]
            if (
                uid is None
                or len(row.first_name) > len(first_name)
                or (len(row.first_name) == len(first_name) and len(row.last_name) > len(last_name))
            ):
                uid, first_name, last_name, middle_initial = idx, row.first_name, row.last_name, row.middle_initial
        award.loc[award.uid.isin(cluster), 'uid'] = uid
        award.loc[award.uid == uid, 'first_name'] = first_name
        award.loc[award.uid == uid, 'last_name'] = last_name
        award.loc[award.uid == uid, 'middle_initial'] = middle_initial
    return award


def add_uid_to_award(award, pprr):
    dfa = award[['uid', 'first_name', 'last_name']].drop_duplicates()\
        .set_index('uid', drop=True)

    dfb = pprr[['uid', 'first_name', 'last_name']].drop_duplicates()\
        .set_index('uid', drop=True)

    matcher = ThresholdMatcher(NoopIndex(), {
        "first_name": JaroWinklerSimilarity(),
        "last_name": JaroWinklerSimilarity()
    }, dfa, dfb)
    decision = 0.954
    matcher.save_pairs_to_excel(data_file_path(
        "match/new_orleans_pd_award_2016_2021_v_pprr.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    matches = dict(matches)
    award.loc[:, "uid"] = award.uid.map(lambda i: matches[i])
    return award


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path(
        'clean/pprr_new_orleans_pd_1946_2018.csv'
    ))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    award = pd.read_csv(data_file_path('clean/award_new_orleans_pd_2016_2021.csv'))
    post = post[post.agency == 'new orleans pd'].reset_index(drop=True)
    award = deduplicate_award_personnel(award)
    award = add_uid_to_award(award, pprr)
    event_df = match_pprr_against_post(pprr, post)
    ensure_data_dir('match')
    event_df.to_csv(data_file_path(
        'match/post_event_new_orleans_pd.csv'), index=False)
    award.to_csv(data_file_path(
        'match/award_new_orleans_pd_2016_2021.csv'), index=False)
